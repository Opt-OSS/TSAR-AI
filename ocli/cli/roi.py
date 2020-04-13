import logging

import click

from ocli.cli import AliasedGroup
from ocli.cli import output
from ocli.cli.output import warning
from ocli.cli.state import option_repo_name, pass_repo, Repo, yes_or_confirm, option_yes, option_less
from ocli.project.roi import get_roi

log = logging.getLogger()



def option_roi(f):
    return click.option('-r', '--roi', 'roi_id', required=False, help=' ROI name or ID in question')(f)


def resolve_roi(roi_id, repo):
    if not roi_id and not repo.active_roi:
        raise click.BadOptionUsage('roi_id', "ROI is required  set active ROI or provide --roi option")
    _id = int(roi_id) if roi_id else int(repo.active_roi)
    try:
        return _id, repo.roi.db.iloc[_id]
    except IndexError:
        raise click.BadOptionUsage('roi_id', f"ROI with id {_id} not found")


@click.group('roi', cls=AliasedGroup)
@option_repo_name
@pass_repo
def roi_cli(repo):
    """ region of interest (ROI) commands"""
    if not repo.active_project:
        click.BadParameter('run activate before using ROI')
    pass


# ####################### DELETE ROI RECORD ########################################
# @delete.command('name')
# @click.argument('mask', type=click.STRING)
# @option_yes
# @pass_repo
def del_by_name(repo: Repo, mask: str, yes: bool):
    """ delete record by name-mask"""
    db = repo.roi.db
    _mask = db['name'].str.contains(mask, na=False)
    _s = _mask.sum()
    if not _s:
        output.error(f'No records found by mask "{mask}"')
        return
    if yes_or_confirm(yes, f'delete {_s} records'):
        _idx = db.loc[_mask].index
        repo.roi.delete(_idx).save()
        if repo.active_roi in _idx:
            repo.active_roi = None


# @delete.command('id')
# @click.argument('id', type=click.INT)
# @option_yes
# @pass_repo
def del_by_id(repo: Repo, roi_id: int, yes: bool):
    """ delete record by Id"""
    if yes_or_confirm(yes, f'delete record ID{roi_id}?'):
        repo.roi.open()
        repo.roi.delete(roi_id).save()
        if repo.active_roi == roi_id:
            repo.active_roi = None


# ####################### DELETE ROI #####################################
@roi_cli.group('delete', invoke_without_command=True)
@click.option('--all', '-a','delete_all', required=False, is_flag=True, default=False, help="delete all records")
@click.option('--id', '-i','roi_id', required=False, default=False, help="use <NAME> as ID")
@click.option('--name', '-n', required=False, help='ROI name (should be unique in project)')
@option_yes
@pass_repo
def roi_delete(repo: Repo, yes, delete_all, roi_id: str, name):
    """ delete project ROI database"""

    if delete_all and (yes or click.confirm(f"'Delete ROI database for project '{repo.active_project}'?")):
        repo.roi.open()  # init db if cli-mode, otherwise ds is empty
        repo.roi.clear()
    elif roi_id:
        del_by_id(repo, int(roi_id), yes)
    elif name:
        del_by_name(repo, name, yes)
    else:
        raise click.BadOptionUsage('name', 'One of --id, --name or --all option is required')


# ####################### RENAME ROI #####################################
@roi_cli.command('rename',
                 # epilog="Either -i or -n options should be provided, if -iprovided, -n will be ignored"
                 )
# @click.option('--name', '-n','from_name', type=click.STRING,required=True, help='ROI  name to be renamed')
@click.option('--id', '-i', is_flag=True, default=False, help="use <NAME> as ID")
@click.argument('old_name', metavar="<NAME | ID>", default=None, type=click.STRING)
@click.argument('new_name', metavar="<NEW NAME>", type=click.STRING, required=True)
@option_yes
@pass_repo
def roi_rename(repo, id, old_name, yes, new_name):
    """ set ROI name by ID."""
    roi = repo.roi
    db = roi.db
    # by ID
    if id:
        if int(old_name) not in db.index:
            output.error(f'ROI with  id {old_name} not found')
            return
        roi.db.loc[int(old_name), 'name'] = new_name
        roi.save()

        # if _df[_df['name'] == from_name].empty:
    else:
        _mask = db['name'].str.contains(old_name, na=False)
        _s = _mask.sum()
        log.debug(_mask)
        if not _s:
            output.error(f'Could not find ROI "{old_name}"')
            return
        if _s > 1 and not yes_or_confirm(yes, f"{old_name} is not unique, will update {_s} records proceed?"):
            return
        roi.db.loc[_mask, 'name'] = new_name
        roi.save()


# ####################### ADD ROI ########################################
@roi_cli.command('add')
@click.argument('file',
                type=click.File(mode='r'),
                )
@click.option('--name', '-n', required=True, help='ROI name (should be unique in project)')
@option_yes
@pass_repo
def roi_add(repo: Repo, name, file, yes):
    """ add ROI to project database """
    try:
        # log.debug(file.name)
        _r = get_roi(file.name)
        # log.debug(_r)
        roi = repo.roi
        _df = roi.db
        if not _df[_df['name'] == name].empty and yes_or_confirm(yes, f"overwrite '{name}'"):
            _df.loc[_df['name'] == name, 'geometry'] = _r
            roi.save()
            return
        else:
            roi.open().add(name=name, geometry=_r).save()
    except Exception as e:
        log.error(f"roi add exception: {e}")
        raise click.BadParameter(f"cold not extract ROI from '{file}'")


# ####################### LIST ROI ########################################
@roi_cli.command('list')
@option_repo_name
@option_less
@pass_repo
def roi_list(repo: Repo, less):
    """ add ROI to project database """
    roi = repo.roi
    db = roi.db
    if not len(db):
        raise click.BadOptionUsage('project', f"No ROI defined for project '{repo.active_project}'")
    try:
        cols = list(db.columns)
        del cols[cols.index('geometry')]
        _ds = db[cols]  # double [] to return DataFrame, not Series
        mask = db['geometry'].notna()
        db = db[mask]
        _ds['active'] = None
        _db_m = db.to_crs({'init': 'epsg:3857'})

        _f = ['minx', 'miny', 'maxx', 'maxy']
        _ds['area'] = None
        for x in _f:
            _ds[x] = None
        if not mask.sum():
            click.BadParameter('No data')
        else:

            _ds.loc[mask, _f] = db.loc[mask, 'geometry'].bounds[_f]
            _ds.loc[mask, 'area'] = _db_m.loc[mask, 'geometry'].area / 1e6
            if repo.active_roi:
                _id = int(repo.active_roi)
                if _id in db.index:
                    _ds.iloc[_id]['active'] = '*'
                else:
                    warning('Project active ROI not found in DB')

            output.table(_ds, headers=['ID', *cols, 'active', 'area km2', *_f], less=less)
            click.echo("")
    except Exception as e:
        log.exception(e)
        raise click.BadParameter(f'Unknown error, reason: {e} ')


# ####################### ACTIVATE ROI ########################################
@roi_cli.command('activate')
@click.option('--print', 'do_print',is_flag=True, default=False, help='Print results')
@option_repo_name
@click.argument('roi_id', metavar="<ROI ID>", type=click.STRING, required=True)
@pass_repo
def roi_activate(repo: Repo, roi_id, do_print):
    """ set ROI as active (default) """
    if not repo.active_project:
        raise click.UsageError("Active project is not set")
    roi = repo.roi
    db = roi.db
    if int(roi_id) not in db.index:
        raise click.BadArgumentUsage(f'ROI with id {roi_id} not found')
    repo.set_config('active_roi', roi_id, autosave=True)
    if do_print:
        click.get_current_context().invoke(roi_list, less=False)


