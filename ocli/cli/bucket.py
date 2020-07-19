import logging
import os

import click
from geopandas import GeoDataFrame
from pandas import DataFrame
from shapely.geometry import Polygon

from ocli.cli import AliasedGroup
from ocli.cli import output
from ocli.cli.output import OCLIException
from ocli.cli.pruduct_s1 import pairs_load, _cache_pairs_file_name, products_list_options, _list_products
from ocli.cli.roi import option_roi, resolve_roi
from ocli.cli.state import Task, Repo, option_locate_task, option_less, pass_task, pass_repo
from ocli.project import _local_eodata_relative_path
from ocli.project import bucket
from ocli.project.bucket import unitime_delta_factory
from ocli.sent1 import pairs

log = logging.getLogger()


def _task_ms(task: Task) -> (str, str):
    try:
        task.resolve()
        _, _m = task.get_valid_key('main')
        if task.kind == 'cluster':
            _, _s = task.get_valid_key('subordinate')
        else:
            _s = None
        return _m, _s
    except RuntimeError as e:
        log.debug(str(e))
        return None, None


def _bkt_list(repo: Repo, main: str, subordinate: str, geometry: Polygon, fit: int) -> (GeoDataFrame, list):
    """ list avaliable buckets"""
    _df = pairs.load_from_cache(cache_file_name=(_cache_pairs_file_name(repo)))
    try:
        if geometry.area == 0:
            raise AssertionError('ROI has zero area')
        _df['fit'] = _df['geometry'].intersection(geometry).area / geometry.area

        _bk = bucket.create_list(_df, buckets_dir='')  # type: GeoDataFrame
        if _bk is None or _bk.empty:
            raise AssertionError(f'No products found ')

        _t = _bk.groupby('bucket', as_index=False).agg({'fit': ['mean'], 'startDate': ['min', 'max', 'count']})
        if fit is not None:
            _t = _t[_t['fit']['mean'] >= fit]
    except AssertionError as e:
        raise RuntimeError(e)
    headers = ['#', 'bucket', 'mean fit', 'from', 'to', 'Cnt']

    if main or subordinate:

        def _get_bucket_mytitle(t: str):
            _m = _bk.loc[_bk['title'] == t]
            if not _m.empty:
                return _m.iloc[0]['bucket']
            return None

        _m = _get_bucket_mytitle(main)
        _s = _get_bucket_mytitle(subordinate)

        def _ms(b):
            _x = 'm' if _m == b else ' '
            _x += 's' if _s == b else ' '
            return _x

        _t['task'] = _t['bucket'].apply(_ms)

        headers += ['task']

    return _t, headers


def _bkt_info(repo: Repo, task: Task, geometry: Polygon,
              bucket_name: str,
              sort: tuple,
              limit: int,
              column: tuple,
              where: str,
              check=False,
              ) -> (GeoDataFrame, list):
    if geometry.area == 0:
        raise click.BadArgumentUsage('ROI has zero area')
    cache_file_name = _cache_pairs_file_name(repo)
    # TODO check ROI exists

    _df = pairs.load_from_cache(cache_file_name=cache_file_name, geometry=geometry)

    _bk = bucket.create_list(_df, buckets_dir='')

    if _bk is None or _bk.empty:
        raise AssertionError(f'No products could be found for ROI ')
    if bucket_name.isnumeric():
        _t = _bk.groupby('bucket', as_index=False).ngroup()
        _df = _bk[_t == int(bucket_name)]
    else:
        _df = _bk[_bk['bucket'] == bucket_name]
    _f = list(column)
    _ds = _list_products(_df,
                         where=where, sort=list(sort), limit=limit
                         )
    _m, _s = _task_ms(task)
    # log.error(f"{_m} {_s}")
    if task.loaded:
        if _m or _s:
            def _ms(b):
                _x = 'm' if _m == b else ' '
                _x += 's' if _s == b else ' '
                return _x

            _ds['task'] = _ds['title'].apply(_ms)
            _f += ['task']
        _e, eodata = task.get_valid_key('eodata')

        # TODO other formats parsers: Sentinel-1.ZIP ,TOPSAR
        def _ch_fs(b):
            _p = _local_eodata_relative_path(eodata, b)
            if os.path.isfile(os.path.join(_p, 'manifest.safe')):
                _m = os.path.join(_p, 'measurement')
                if os.path.isdir(_m):
                    return '+' if any(os.scandir(_m)) else '~'
            return ''

        if check and not _e:
            _ds['exists'] = _ds['productIdentifier'].apply(_ch_fs)
            _f += ['exists']
            pass

        # _ds = _ds.reindex(['task', *_f], axis=1, copy=False)
        # output.comment(f"Task '{_tname}' applied\n\n")
        headers = ['#', 'task', *_f]
    if _df.empty:
        raise OCLIException(f"bucket {bucket_name} not found")
    bname = _df.iloc[0]['bucket']
    _ds = _ds[_f]
    # remove injected title if not in columns
    return bname, _ds, _ds.columns.to_list()


######################################## CLI ##########################################
@click.group('bucket', cls=AliasedGroup)
def bucket_cli():
    pass


######################################## SHOW INFO  ##########################################
@bucket_cli.command('show')
@option_locate_task
@option_roi
@click.option('--check', 'check', is_flag=True, required=False, default=False,
              help='Check main-subordinate data exists')
@click.option('--update', '-u', 'reload', is_flag=True, required=False, default=False, help='force products load')
@click.argument('bucket_name', metavar='<BUCKET_NAME | RECORD>')
@products_list_options(def_col=None, def_sort=['+startDate'])
@option_less
@pass_task
@pass_repo
@click.pass_context
def bkt_info(ctx, repo, task: Task, roi_id, bucket_name, reload, less, sort, limit, column, where, check):
    """ show bucket info by BUCKET_NAME OR record number

    *  list buckets  names and record numbers  via 'bucket list' command
    """
    def_col = ['productId', 'startDate', 'title', 'relativeOrbitNumber', 'cycle_dt']
    if column is None or not column:
        column = def_col
    else:
        _cp = []
        _cd = []
        for c in column:

            if c.startswith('+'):
                _cp += c[1:].split(',')
            else:
                _cd.extend(c)
            if not _cd:
                _cd = def_col
            column = _cd + _cp
    # log.error(column)
    _id, _roi = resolve_roi(roi_id, repo)
    if reload:
        ctx.invoke(pairs_load, id=_id, reload=True)
    bname, _ds, headers = _bkt_info(repo, task,
                                    geometry=_roi['geometry'],
                                    bucket_name=bucket_name,
                                    sort=sort,
                                    limit=limit,
                                    column=column,
                                    where=where,
                                    check=check,
                                    )
    if limit >= 0:
        output.comment(f"Dataset limited to  {limit} records")
    cols = _ds.columns.to_list()
    if task.loaded:
        output.comment(f"Task: {task.name}")
        if 'task' in cols:
            output.comment(f"INFO: 'task'   column:  'm' -  used as main in task, 's' - used as subordinate in task  ")
        if 'exists' in cols:
            output.comment(f"INFO: 'exists' column:  '+' -  full data loaded, '~' - metadata only loaded")

    output.comment(f'Bucket name: {bname}')
    output.table(_ds, headers=headers, less=less)


######################################## SHOW INFO AROUND MASTER ##########################################
@bucket_cli.command('build')
@click.option('-d', '--delta', type=click.FLOAT, default=12, help="Max cyclic time delta between startDate",
              show_default=True)
@option_locate_task
@option_roi
@click.option('--check', 'check', is_flag=True, required=False, default=False,
              help='Check main-subordinate data exists')
# @products_list_options(def_col=['productId', 'startDate', 'title'], def_sort=['+startDate'])
@option_less
@click.argument('product_id', metavar="PRODUCT_ID")
@click.argument('platform', metavar="PLATFORM", default='')
@pass_task
@pass_repo
def bkt_info(repo: Repo, task: Task, roi_id, less,
             # sort, limit, column, where,
             check, delta, product_id, platform):
    """ find pairs by given PRODUCT_ID

    \b
    PRODUCT_ID:  4-digits hex number (Sentinel product identifier, last 4 symbols in product name).
    PLATFORM:    like 'S1A' or 'S1B' to narrow search in case PRODUCT_ID is ambiguous
    """

    _id, _roi = resolve_roi(roi_id, repo)
    _m, _s = _task_ms(task)
    geometry = _roi['geometry']
    output.comment(f"active task main: {_m}")

    _df = pairs.load_from_cache(cache_file_name=(_cache_pairs_file_name(repo)))
    _df = _df.set_index('productId')
    try:
        _ds = _df.loc[product_id][['startDate', 'platform']]
        # print( _ds)
        if isinstance(_ds, DataFrame):
            # print(f"-----{len(_ds)}--------{type(_ds)}----------")
            if platform != '':
                _ds = _ds[_ds['platform'] == platform].loc[product_id]
                if isinstance(_ds, DataFrame):
                    raise OCLIException(f"Could not resolve  '{product_id}' for platform {platform}")
            else:
                output.table(_ds, headers=['PRODUCT_ID', 'startDate', 'platform'])
                # print( _ds)
                raise OCLIException(f"Product ID {product_id} is ambiguous, use <PALTFORM> argument to narrow search ")
        ts, platform = _ds[['startDate', 'platform']]
        # print(f"----------- {ts}")
    except KeyError:
        raise OCLIException(f'Product id "{product_id}" not found')
    output.comment(f"Building bucket for product {product_id} , startDate={ts}")
    f = unitime_delta_factory(ts)
    _df['cycle_dt'] = _df['startDate'].apply(f)
    _df = _df[(_df['cycle_dt'] <= delta) & (_df['platform'] == platform)]

    cols = ['productId', 'cycle_dt', 'startDate', 'platform', 'relativeOrbitNumber', 'polarisation', 'fit', 'task']
    try:
        if geometry.area == 0:
            raise AssertionError('ROI has zero area')
        _df['fit'] = _df['geometry'].intersection(geometry).area / geometry.area
        _df['task'] = ''
        _df = _df.reset_index()
        _df = _df.set_index('title')
        if _m in _df.index:
            _df.loc[_m, 'task'] = 'm'
        else:
            output.warning('Current task main not found in bucket')
        if _s in _df.index:
            _df.loc[_s, 'task'] = 's'
        else:
            output.warning('Current task subordinate  not found in bucket')
        _df = _df.reset_index()
        _e, eodata = task.get_valid_key('eodata')

        def _ch_fs(b):
            _p = _local_eodata_relative_path(eodata, b)
            if os.path.isfile(os.path.join(_p, 'manifest.safe')):
                _m = os.path.join(_p, 'measurement')
                if os.path.isdir(_m):
                    return '+' if any(os.scandir(_m)) else '~'
            return ''

        if check and not _e:
            _df['exists'] = _df['productIdentifier'].apply(_ch_fs)
            cols += ['exists']
        pass

        _df = _df[cols]

    except AssertionError as e:
        raise RuntimeError(e)

    headers = ['#'] + cols
    output.table(_df, headers=headers, )

    # if main or subordinate:
    #
    #     def _get_bucket_mytitle(t: str):
    #         _m = _bk.loc[_bk['title'] == t]
    #         if not _m.empty:
    #             return _m.iloc[0]['bucket']
    #         return None
    #
    #     _m = _get_bucket_mytitle(main)
    #     _s = _get_bucket_mytitle(subordinate)
    #
    #     def _ms(b):
    #         _x = 'm' if _m == b else ' '
    #         _x += 's' if _s == b else ' '
    #         return _x
    #
    #     _t['task'] = _t['bucket'].apply(_ms)
    #
    #     headers += ['task']
    #
    # return _t, headers


######################################## LIST ##########################################
@bucket_cli.command('list')
@option_locate_task
@option_roi
@click.option('--update', '-u', 'reload', is_flag=True, required=False, default=False, help=' force products load ')
@click.option('--fit', 'fit', type=click.FloatRange(0, 1.0, clamp=True),
              required=False,
              show_default=True,
              default=None, help='filter buckets by product coverage percentage')
@pass_task
@pass_repo
@click.pass_context
def bkt_list(ctx: click.Context, repo: Repo, task: Task, roi_id, reload, fit):
    _id, _roi = resolve_roi(roi_id, repo)
    if reload:
        ctx.invoke(pairs_load, roi_id=_id, reload=True)
    _m, _s = _task_ms(task)
    try:
        _t, headers = _bkt_list(repo,
                                geometry=_roi['geometry'],
                                fit=fit,
                                main=_m,
                                subordinate=_s,
                                )
    except RuntimeError as e:
        raise click.UsageError(str(e))
    output.comment(f'{len(_t)} possible  bucket for roi "{_roi["name"]}" found')
    if fit is not None:
        _t = _t[_t['fit']['mean'] >= fit]
        _l2 = len(_t)
        output.comment(f"shown {_l2} with  fit >= {fit}")
    output.table(_t, headers=headers)

######################################## INTERACTIVE ##########################################
# from PyInquirer import style_from_dict, Token
# custom_style_2 = style_from_dict({
#     Token.Separator: '#6C6C6C',
#     Token.QuestionMark: '#FF9D00 bold',
#     #Token.Selected: '',  # default
#     Token.Selected: '#5F819D',
#     Token.Pointer: '#FF9D00 bold',
#     Token.Instruction: '',  # default
#     Token.Answer: '#5F819D bold',
#     Token.Question: '',
# })
