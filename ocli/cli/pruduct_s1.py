import io
import logging
import os
import re
import traceback
from typing import List, Union

import click
import lxml
from dateutil.parser import parse
from geopandas import GeoDataFrame
from geopandas.geoseries import Series
from lxml import etree
from lxml.etree import Element
from ocli.util.date_parse import parse_to_utc_string

from ocli.cli import AliasedGroup
from ocli.cli import output
from ocli.cli.output import OCLIException
from ocli.cli.roi import option_roi, resolve_roi
from ocli.cli.state import pass_repo, Repo, option_less, option_limit, Task
from ocli.project import bucket
from ocli.sent1 import pairs, get_bucket, s1_prod_id
from ocli.sent1.metadata_extractor import SentinelMetadataExtractor

log = logging.getLogger(__name__)


def _cache_pairs_file_name(repo: Union[Repo,Task]):
    if isinstance(repo,Repo):
        return os.path.join(repo.projects_home, repo.active_project, '.cache', 'sat_data', 'sar.shp')
    if isinstance(repo,Task):
        return os.path.join(repo.projects_home, repo.project, '.cache', 'sat_data', 'sar.shp')


def get_xml_tree(manifest: bytes) -> Element:
    try:
        tree = etree.fromstring(manifest)
        return tree
    except lxml.etree.XMLSyntaxError as e:
        raise AssertionError(f"{e}")


def _list_local(path, limit) -> List:
    extractor = SentinelMetadataExtractor()
    _ds = []
    i = 0
    for path, subdirs, files in os.walk(path):
        if 0 <= limit <= i:
            break
        if path.endswith('.SAFE'):
            for name in files:
                if name != 'manifest.safe':
                    continue

                if 0 <= limit <= i:
                    break

                try:
                    with open(os.path.join(path, name), "rb") as _f:
                        manifest = _f.read()
                except FileNotFoundError as e:
                    log.debug(f"Skipped {name}: {e}")
                    continue
                i += 1
                extractor.root = get_xml_tree(manifest)
                prodname = os.path.basename(path)
                extractor.extractMetadataFromManifestFiles(prodname)
                parsed = extractor.productMetadata
                # pprint(parsed)
                bn = get_bucket(
                    buckets_dir='',
                    # firstBurstIndex=firstBurstIndex,
                    # lastBurstIndex=lastBurstIndex,
                    mission=parsed['ProductClass'] + '1' + parsed['FamilyNameNumber'],
                    sensorMode=parsed['InstrumentMode'],
                    productType=parsed['ProductType'],
                    relativeOrbitNumber=parsed['relativeOrbitNumber'],
                    startDate=parse(parsed['StartTime']),
                )
                _ds.append(dict(
                    productId=s1_prod_id(prodname),
                    platform=parsed['ProductClass'] + '1' + parsed['FamilyNameNumber'],
                    sensorMode=parsed['InstrumentMode'],
                    relativeOrbitNumber=parsed['relativeOrbitNumber'],
                    productType=parsed['ProductType'],
                    startDate=parsed['StartTime'],
                    backetname=bn,
                    productname=prodname,
                ))
    return _ds


@click.group('product', cls=AliasedGroup)
@click.option('-s', '--satellite', type=click.Choice(['Sentinel-1']), default='Sentinel-1',
              help='staellite in question')
@pass_repo
def pairs_cli(repo, satellite):
    """ Satellite products commands """
    pass


@pairs_cli.command('load')
@option_roi
@click.option('-c', '--completion-date', help="MAX product's date", required=False, default=None)
@click.option('--quiet', '-q', 'quiet', is_flag=True, required=False, default=False, help='do not show progress')
@click.option('--update', '-u', 'reload', is_flag=True, required=False, default=False, help=' force load ')
@pass_repo
def pairs_load(repo: Repo, roi_id, reload, quiet, completion_date):
    """ load data into DB """
    # todo convert name to ID
    if completion_date:
        completion_date = parse_to_utc_string(completion_date)
        if completion_date is None:
            raise OCLIException(f"Completion date {completion_date} is invalid")

        output.comment(f"loading products up to {completion_date}")
    if not roi_id and not repo.active_roi:
        raise click.BadOptionUsage('roi_id', "ROI is required , set active ROI or provide --roi option")
    _id = int(roi_id) if roi_id else int(repo.active_roi)
    # check roi exists

    db = repo.roi.db
    try:
        geometry = db.loc[_id, 'geometry']
    except KeyError:
        raise click.BadOptionUsage('roi_id', f'ROI "{_id}" not found')
    cache_file_name = _cache_pairs_file_name(repo)
    finder_conf = repo.get_config('finder', {}).copy()
    if completion_date:
        finder_conf['completionDate'] = completion_date
    if not quiet:
        output.table(finder_conf.items())
    if quiet:
        d = pairs.load_data(geometry,
                            reload=reload,
                            callback=None,
                            finder_conf=finder_conf,
                            cache_file_name=cache_file_name,
                            )
    else:
        with click.progressbar(length=100,
                               label='Loading sat products') as bar:
            def callback(total, step):
                if bar.length != total:
                    bar.length = total
                bar.update(step)

            d = pairs.load_data(geometry,
                                reload=reload,
                                callback=callback,
                                finder_conf=finder_conf,
                                cache_file_name=cache_file_name
                                )
    if d.empty:
        raise OCLIException('0 products loaded, product list is not updated!')
    else:
        output.success(f'{len(d)} products loaded into list')


def products_list_options(def_col, def_sort):
    def _products_list_options(f):
        f = click.option('-s', '--sort',
                         multiple=True,
                         # nargs=-1,
                         default=def_sort,
                         show_default=True,
                         help=' sort columns, prefix with "-" for descending')(f)
        f = click.option('-c', '--column',
                         multiple=True,
                         default=def_col,
                         show_default=True,
                         help='List of columns to display, use +colname1,colname2 to add columns to default, multiple options allowed'
                         )(f)
        f = click.option('-w', '--where',
                         default=None,
                         type=click.STRING,
                         help='pandas Dataframe.query expression (see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html#pandas.DataFrame.query for details)'
                         )(f)
        f = click.option(
            '-l', '--limit', type=click.INT, default=-1, help=' records output limit, -1 for no limit'
        )(f)
        return f

    return _products_list_options


def _list_products(df: GeoDataFrame,
                   where: str, sort: list, limit=-1
                   ) -> GeoDataFrame:
    _df = df
    # where
    if where:
        try:
            _df = _df.query(where)
        except ValueError as e:
            raise ValueError(f"--where {e}")

    # columns
    # soring
    pattern = re.compile("^[+\\-]")
    # _sort = list(sort)  # type list[str]
    _by = [x[1:] if pattern.match(x) else x for x in sort]
    _desc = [x.startswith('-') for x in sort]

    # log.debug(type(sort))
    # log.debug(sort)
    _ds = _df.sort_values(by=_by, ascending=_desc)
    if limit >= 0:
        return _ds.head(limit)
    return _ds


"""
Mutual exclusive options
https://stackoverflow.com/questions/44247099/click-command-line-interfaces-make-options-required-if-other-optional-option-is
"""


@pairs_cli.command('list')
@products_list_options(
    def_col=['productId', 'completionDate', 'platform', 'relativeOrbitNumber', 'polarisation',
             'instrument', 'bucket'],
    def_sort=['+completionDate', '+relativeOrbitNumber']
)
@click.option('--local', 'local_only', type=click.Path(exists=True),
              help="Show only products that exists <local> direcroty")
@click.option('--columns', 'list_columns', is_flag=True, help="list available product columns")
@option_less
@option_roi
@pass_repo
def pairs_list(repo: Repo, less, sort, limit, column, where, list_columns, roi_id, local_only):
    """ list sate data """
    _id, _roi = resolve_roi(roi_id, repo)
    geometry = _roi['geometry']
    cache_file_name = _cache_pairs_file_name(repo)
    log.debug(f'loading sta data from cache "{cache_file_name}"  ')
    try:
        _df = pairs.load_from_cache(cache_file_name=cache_file_name)
        if list_columns:
            k = _df.columns.tolist()
            output.table([[v] for v in k], headers=['available columns'])
        else:
            _df = bucket.create_list(_df, buckets_dir='')
            _f = list(column)
            _ds = _list_products(_df,
                                 where=where, sort=list(sort), limit=limit
                                 )
            _ds['fit'] = _ds['geometry'].intersection(geometry).area / geometry.area
            _f.extend(['fit'])
            _ds = _ds[_f]
            if local_only:
                _locals = [x['productId'] for x in _list_local(local_only, limit)]
                mask = _ds['productId'].apply(lambda x: x in _locals)
                _ds = _ds[mask]
            output.table(_ds, headers=['#', *_f], less=less)
            return _ds
    except ValueError as e:

        output.error(f" Bad value: {e}")

    except KeyError as e:
        output.error(f"column not found : {e}")
    except Exception as e:
        log.exception(e)
        output.error(f"No data loaded, run '{pairs_cli.name} {pairs_load.name}' command to initiate cache")
    return None


@pairs_cli.command('ls')
@click.argument('path', type=click.Path())
@option_limit
@option_less
@pass_repo
def pairs_list(repo: Repo, less, path, limit):
    """ list directory with SAFE files and its  sub-directories and output products and bucket"""
    try:
        _ds = _list_local(path, limit)

        output.table(_ds, less=less, headers='keys')
    except ValueError as e:

        output.error(f" Bad value: {e}")

    except KeyError as e:
        output.error(f"column : {e}")
    except Exception as e:
        log.exception(e)
        log.error(f"{traceback.print_tb(e.__traceback__)}")
        output.error(f"No data loaded, run '{pairs_cli.name} {pairs_load.name}' command to initiate cache")


@pairs_cli.command('show')
@click.argument('value', metavar='<PRODUCT_ID | RECORD>')
@click.option('-i','force_id', is_flag=True, default=False, help="interpret argument as PRODUCT_ID")
@pass_repo
def parirs_info(repo, value: str,force_id:bool):
    """ show products DB info by PRODUCT_ID or RECORD number  """
    try:
        cache_file_name = _cache_pairs_file_name(repo)
        _df = pairs.load_from_cache(cache_file_name=cache_file_name)
        if value.isnumeric() and not force_id:
            _p = _df.iloc[int(value)]  # type: Series
        else:
            _p = _df[_df['productId'] == value].iloc[0]  # type: Series
        a = [[i, str(v)] for (i, v) in _p.iteritems()]
        output.table(a, colalign=('right',), tablefmt='plain')
    except IndexError:
        output.error(f'Could not find product by id/record "{value}"')


@pairs_cli.command('info')
@click.option('-a', '--all', 'all_pairs', is_flag=True, help='all info')
@pass_repo
def pairs_info(repo, all_pairs):
    """ show products DB info """
    cache_file_name = _cache_pairs_file_name(repo)
    _df = pairs.load_from_cache(cache_file_name=cache_file_name)

    output.comment("productd DB information\n\n")
    rons = _df['relativeOrbitNumber'].unique()
    _st = [
        ['Cache file', cache_file_name],
        ['startDate', _df['startDate'].min(axis=0), _df['startDate'].max(axis=0)],
        ['completionDate', _df['completionDate'].min(axis=0), _df['completionDate'].max(axis=0)],
        ['Relative orbits', rons],
        ['Relative orbit number count', len(rons)],
        ['records total', len(_df)]
    ]
    # output.comment("date intervals\n\n")
    output.table(_st, tablefmt='plain')
    if all_pairs:
        buf = io.StringIO()
        _df.info(verbose=True, memory_usage='deep', buf=buf)
        _t = buf.getvalue()
        output.comment("---------------------")
        click.echo(_t)

    pass
