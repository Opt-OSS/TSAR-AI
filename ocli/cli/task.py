"""
task managment
"""
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime, timedelta
from functools import wraps
from importlib import import_module
from pathlib import Path
from pprint import pprint

import dpath.util

from dpath.exceptions import PathNotFound
from time import perf_counter
from typing import List

import click
import geopandas as gpd

from ocli.ai.Envi import zoneByRoi
from ocli.cli import AliasedGroup, colorful_json
from ocli.cli import output
from ocli.cli.output import OCLIException
from ocli.cli.pruduct_s1 import _cache_pairs_file_name
from ocli.cli.roi import resolve_roi, option_roi
from ocli.cli.state import option_yes, yes_or_confirm, pass_repo, Repo, option_repo_name, pass_task, Task, \
    option_locate_task, TaskRecipe, option_less, MutuallyExclusiveOption, TaskTemplate, TASK_HEAD
from ocli.cli.validator import is_path_exists_or_creatable
from ocli.project import _local_eodata_relative_path
from ocli.project.stack import task_stack_snap
from ocli.sent1 import pairs, s1_prod_id, parse_title

log = logging.getLogger()

cmd_dir = os.path.join(os.path.dirname(__file__), 'scripts')


# ################################ CLI ###################################


def ensure_task_resolved(method):
    @wraps(method)
    def _impl(*method_args, **method_kwargs):
        try:
            task = next(x for x in method_args if isinstance(x, Task))
            log.debug(f"resolving Task {task.projects_home} {task.project} {task.name}")
            task.resolve()
        except StopIteration:
            raise click.UsageError("Internal: Task argument not found")
        except RuntimeError as e:
            raise click.UsageError(f"Could not resolve task,reason {e}")
        log.debug(f"task resolved {task.path}")
        return method(*method_args, **method_kwargs)

    return _impl


@click.group('task', short_help="Task manipulation", cls=AliasedGroup)
def cli_task():
    """    task configuration and executions    """
    pass


# ####################### ACTIVATE TASK ########################################
@cli_task.command('activate')
@option_repo_name
@click.option('-q', '--quiet', is_flag=True, default=False, help='Do not print results')
@pass_task
@pass_repo
@click.argument('name', metavar="<TASK NAME>", type=click.STRING, required=True)
def task_activate(repo: Repo, task: Task, name, quiet):
    """ set Task as active for active project, <TASK NAME | INDEX> is name or index as in  'task list' """
    try:
        if not repo.active_project:
            raise AssertionError("Active project is not set")
        if name.isnumeric():
            name = Task.get_list(repo.get_project_path())[int(name)][0]
        task.name = name
        task.projects_home = repo.projects_home
        task.project = repo.active_project
        task.resolve()
    except AssertionError as e:
        raise click.BadArgumentUsage(f"Could not set  task '{name}' as active, reason: {e}")
    repo.set_config('active_task', task.name, autosave=True)
    if not quiet:
        click.get_current_context().invoke(task_info)


# ######################### CREATE #############################################

# todo --activate option

@cli_task.command('create', short_help="create new task")
@click.option('-t', '--template', help="template module name", required=True, default='ocli.aikp.cluster.sentinel_1')
@option_locate_task
@option_yes
@click.argument('args', nargs=-1)
@pass_task
def task_create(task: Task, yes, args, template):
    """ create new task or reset existed to initial state

     ARGS = key=value pairs forwarded to 'task set' command after creation
     """
    try:
        _path = task.get_path_by_name()
        if not _path:
            raise click.BadArgumentUsage('Could not resolve task directory')
        if os.path.isdir(_path) and not yes_or_confirm(yes, f"Directory {_path} already exists, Proceed?"):
            return
        task.path = _path
        _exists, _f = task.get_task_rc()
        if _exists and not yes_or_confirm(yes, f'Task already exists in {_path}, Overwrite?'):
            return

        if template:
            # TODO check module ot path

            try:
                tpl = import_module(template)
                if not hasattr(tpl, 'create_task'):
                    raise ModuleNotFoundError(f"Template is invalid")
                tpl.create_task(task)
                output.comment(f"Using template {template}")
            except ModuleNotFoundError as e:
                raise OCLIException(f'Could not import "{template}": {e} ')
        # pprint(task.config)
        task.create()
        if len(args):
            click.get_current_context().invoke(task_set, args=args)
        output.success(f'New task "{task.name}" created')
    except RuntimeError as e:
        click.UsageError(str(e))


def _task_update_config(task: Task, d: dict, force=False):
    template = task.config.get('template', None)
    if template:
        """check task created from template set by template """
        try:
            tpl = import_module(template)  # type: TaskTemplate
            if not hasattr(tpl, 'task_set'):
                raise ModuleNotFoundError(f"Template is invalid: task_set is not defined")
            tpl.task_set(task, d)
        except ModuleNotFoundError as e:
            raise OCLIException(f'Could not import "{template}": {e} ')
    for k in d.keys():
        v = d[k]
        if k == 'tag':

            if v.startswith('+'):
                v = v[1:]
                t = task.config.get('tag')
                vs = v.split(',')
                v = [x for x in vs if x not in t]
                task.set_config('tag', t + v)
                # print(f"Add tag {v}")
            elif v.startswith('-'):
                v = v[1:]
                t = task.config.get('tag')
                vs = v.split(',')
                v = [x for x in t if x not in vs]
                task.set_config('tag', v)
            else:
                vs = v.split(',')
                task.set_config('tag', vs)

        else:
            task.set_config(k, v, only_existed=not force)
    task.set_config('_rev', task.config.get('_rev', 0) + 1)


# ######################### SET    #############################################
@cli_task.command('set')
@click.option('-q', '--quiet', is_flag=True, default=False, help='Do not print results')
@click.option('--force', is_flag=True, default=False, help='Add new or set protected tag')
@option_locate_task
@click.argument('args', nargs=-1)
@pass_task
@pass_repo
@ensure_task_resolved
def task_set(repo: Repo, task: Task, args: list, quiet, force):
    """ set parameters of task, multiple key=value pairs allowed"""
    try:
        d = dict(arg.split('=') for arg in args)
    except ValueError:
        raise click.BadArgumentUsage('Wrong format for key=value argument')
    task.upgrade_task()  # bump version, fix issues
    task_config = task.config

    """ prevent changing template"""
    reserved_keys = ['kind', 'template', 'template_version']
    if not force and any(k in reserved_keys for k in d.keys()):
        raise OCLIException(f"keys {reserved_keys}  could not be changed!")
    _task_update_config(task, d, force)
    # template = task_config.get('template', None)
    # if template:
    #     """check task created from template set by template """
    #     try:
    #         tpl = import_module(template)  # type: TaskTemplate
    #         if not hasattr(tpl, 'task_set'):
    #             raise ModuleNotFoundError(f"Template is invalid: task_set is not defined")
    #         tpl.task_set(task, d)
    #     except ModuleNotFoundError as e:
    #         raise OCLIException(f'Could not import "{template}": {e} ')
    # for k in d.keys():
    #     v = d[k]
    #     if k == 'tag':
    #
    #         if v.startswith('+'):
    #             v = v[1:]
    #             t = task.config.get('tag')
    #             vs = v.split(',')
    #             v = [x for x in vs if x not in t]
    #             task.set_config('tag', t + v)
    #             # print(f"Add tag {v}")
    #         elif v.startswith('-'):
    #             v = v[1:]
    #             t = task.config.get('tag')
    #             vs = v.split(',')
    #             v = [x for x in t if x not in vs]
    #             task.set_config('tag', v)
    #         else:
    #             vs = v.split(',')
    #             task.set_config('tag', vs)
    #
    #     else:
    #         task.set_config(k, v, only_existed=not force)
    # task.set_config('_rev', task.config.get('_rev', 0) + 1)

    try:
        task.save()
        output.success("task saved")
    except RuntimeError as e:
        raise click.UsageError(f'Could bot save task RC-file, reason: {e}"')
    if not quiet:
        click.get_current_context().invoke(task_info)


# #########################  UPDATE    #############################################
@cli_task.command('update')
@click.option('--force', is_flag=True, default=False, help='Update if source task is invalid')
@click.option('--template-only', is_flag=True, default=False, help='Update only template keys')
@click.option('--all-values', is_flag=True, default=False, help='DO not skip  source empty values')
@click.option('-s', '--source', 'source', help="Source task name in the task project")
@option_yes
@click.option('-q', '--quiet', is_flag=True, default=False, help='Do not print results')
@click.option('--dry-run', is_flag=True, default=False, help='dry-run, do not perform actual download')
@option_locate_task
@pass_task
@pass_repo
@ensure_task_resolved
def task_set(repo: Repo, task: Task, source, force, dry_run, yes, quiet, template_only, all_values):
    """
    update task's keys with values from --source task (if source value is not empty)

    \b
    Both destination and source task should belong to the same project
    use top-level -v INFO for more verbose output
    """
    skip_keys = TASK_HEAD.keys() if template_only else []
    protected_keys = ['template', 'template_version', 'class', 'kind', 'name', 'project', '_id', '_rev']
    s_rc = Path(task.projects_home, task.project, source, task.TASK_RC)
    if not s_rc.is_file():
        raise OCLIException(f"Could not resolve {source} in project {task.project}")
    output.comment(f"Updating keys of task {task.project}/{task.name} from task {task.project}/{source}")
    try:
        src = Task()
        src.project = task.project
        src.projects_home = task.projects_home
        src.name = source
        src.resolve()
        template = task.config.get('template', None)
        if not template:
            raise OCLIException(f"Task does not have template")
        try:
            if template != src.config['template'] \
                    and not yes_or_confirm(yes, "Task templates are not the same, continue?"):
                return
        except KeyError:
            if not yes_or_confirm(yes, "Could not resolve source template, continue?"):
                return
        err = src.validate_all(ignore=skip_keys)
        if err:
            [output.error(f"SOURCE {x}") for x in err]
            if not force:
                raise AssertionError(f"Source task is invalid, use --force to perform update")
        d = {}
        for k, v in src.config.items():
            if not all_values and (v is None or not v):
                log.info(f"Skipping {k} cause value is None")
                continue
            if k in skip_keys:
                log.info(f"Skipping {k} cause it in skip_keys")
                continue
            if k in protected_keys:
                log.info(f"Skipping {k} cause it in protected")
                continue
            if k not in task.config:
                log.info(f"Skipping {k} cause it not in task config")
                continue

            if task.config[k] == v:
                log.info(f"Skipping {k} cause values are same")
                continue
            if dry_run:
                output.secho(f'{k}')
                output.secho(f' - {task.config[k]}', fg='red')
                output.secho(f' + {v}', fg='green')
            else:
                d[k] = v
        if not dry_run:
            _task_update_config(task, d, force)
            task.save()
            output.success("task saved")
            if not quiet and not dry_run:
                click.get_current_context().invoke(task_info)
    except Exception as e:
        # log.exception(e)
        raise OCLIException(f"Could not update task: {e}")


# ######################### PREVIEW #############################################

# TODO add --save --format (jpeg,png) --open to save pre

@cli_task.command('preview')
@option_locate_task
@click.option('-b', '--burst', 'burst_range', is_flag=False,
              type=click.INT,
              required=False,
              default=None,
              nargs=2,
              help='arguments <start> <stop>,Limit to bursts in range [start:stop]')
@click.option('-s', '--swath', is_flag=False,
              type=click.Choice(['IW1', 'IW2', 'IW3', 'all']),
              required=False,
              default=False,
              multiple=True,
              help='Show only listed swathes')
@click.option('-m', '--map', 'basemap', default='naturalearth', show_default=True,
              type=click.Choice([
                  'terrain-background', 'terrain', 'toner',
                  'watercolor', 'naturalearth',
              ]),
              help='Background map style')
@click.option('-z', '--zoom-basemap', default=8, help='Map zoom')
@click.option('-r', '--roi', 'roi_id', required=False, help='ROI ID in question')
@pass_task
@pass_repo
@ensure_task_resolved
def task_preview(repo: Repo, task: Task, roi_id, swath, basemap,
                 burst_range,
                 zoom_basemap,
                 ):
    """ Preview  task geometry
    """

    # TODO -q option to return just valididty status
    # TODO -s options to return current task status

    _id, _roi = resolve_roi(roi_id, repo)
    try:
        from ocli.preview import preview_roi_swath, preview_roi
        if not swath:
            # show by product lists
            cache_file_name = _cache_pairs_file_name(repo)
            _df = pairs.load_from_cache(cache_file_name=cache_file_name)
            _e, m = task.get_valid_key('main')
            s = None
            title = f"M: {m}"
            if task.config.get('subordinate') is not None:
                _e, s = task.get_valid_key('subordinate')
                title += f"\nS: {s}"
            df = _df[_df['title'].isin([m, s])]
            preview_roi(df, _roi, title=title, zoom=zoom_basemap, basemap=basemap)
            pass
        else:
            # thos requires metadata to be loaded
            try:
                _e, m = task.get_valid_key('main')
                title = f"M: {m}"
                df_main = task.get_geometry_fit_data_frame(_roi.geometry, key='main')
                df_main['main'] = True
                fit = df_main[['IW1_fit', 'IW2_fit', 'IW3_fit']]
                output.comment(f"MASTER ROI '{_roi['name']}': coverage by Swath/Burst\n\n")
                output.table(fit, headers=['burst', 'IW1', 'IW2', 'IW3'])
                output.comment("\n\n")
                output.table(gpd.pd.DataFrame(fit.sum()), headers=['Swath', 'total fit'])
                df = df_main
                df.crs = df_main.crs
                if task.config.get('subordinate') is not None:
                    _e, s = task.get_valid_key('subordinate')
                    title += f"\nS: {s}"
                    if _e:
                        raise click.BadParameter(f'Task key "subordinate" is invalid: {_e}')
                    df_subordinate = task.get_geometry_fit_data_frame(_roi.geometry, key='subordinate')
                    df_subordinate['main'] = False
                    fit = df_subordinate[['IW1_fit', 'IW2_fit', 'IW3_fit']]
                    output.comment(f"SLAVE ROI '{_roi['name']}': coverage by Swath/Burst\n\n")
                    df = gpd.pd.concat([df, df_subordinate])
                    df.crs = df_main.crs

                output.table(fit, headers=['burst', 'IW1', 'IW2', 'IW3'])
                output.comment("\n\n")
                output.table(gpd.pd.DataFrame(fit.sum()), headers=['Swath', 'total fit'])

                swath_id = ['IW1', 'IW2', 'IW3']
                if len(swath):
                    if 'all' in swath:
                        swath_id = ['IW1', 'IW2', 'IW3']
                    else:
                        swath_id = list(swath)
                preview_roi_swath(df, _roi, title=title,
                                  swath_id=swath_id,
                                  burst_range=burst_range,
                                  zoom=zoom_basemap,
                                  basemap=basemap)
            except RuntimeError as e:
                raise click.BadArgumentUsage(str(e))
    except Exception as e:
        log.exception(e)
        raise click.UsageError(str(e))


# ######################### SHOW #############################################

# TODO add --save --format (jpeg,png) --open to save pre

@cli_task.command('show')
@option_locate_task
@click.option('--swath', is_flag=True, default=False, help='show ROI fit by swath/burst')
@click.option('--recipe', is_flag=True, default=False, help='show ROI AI recipe file')
@click.option('-k', '--key', 'recipe_key', required=False, type=click.STRING, help='show recipe key, dot delimited')
@click.option('--no-color', 'no_color', is_flag=True, default=False, help='Disable terminal colors')
@click.option('--edit', 'edit', is_flag=True, default=False, help='Open in editor')
@click.option('--editor', 'editor', default='vi', help='Editor')
@option_less
@click.option('-r', '--roi', 'roi_id', required=False, help='ROI ID in question')
@pass_task
@pass_repo
@ensure_task_resolved
def task_info(repo: Repo, task: Task, swath, roi_id,
              no_color,
              recipe,
              less, edit,
              editor, recipe_key):
    """ show task information

    \b
    --preview requires main and subordinate to be  in product list
    --swath option requires main and subordinate metadata to be loaded
    """

    # TODO -q option to return just valididty status
    # TODO -s options to return current task status

    if recipe:
        try:
            _id, _roi = resolve_roi(roi_id, repo)
            r = TaskRecipe(task=task)
            fname = r.get_ai_recipe_name(_roi['name'])
            log.error(fname)
            if edit:
                click.edit(filename=fname, editor=editor)
            else:
                with open(fname, 'r') as _f:
                    j = _f.read()
                    j = j if no_color else colorful_json(j)

                    if less:
                        click.echo_via_pager(j)
                    else:
                        click.echo(j)
        except (AssertionError, FileNotFoundError) as e:
            raise OCLIException(f'"Could not resolve recipe file for roi "{_roi["name"]}" file: {e}"')
    elif recipe_key:
        try:
            _id, _roi = resolve_roi(roi_id, repo)
            r = TaskRecipe(task=task)
            fname = r.get_ai_recipe_name(_roi['name'])
            with open(fname, 'r') as _f:
                j = json.load(_f)
            _part = dpath.util.get(j, recipe_key, separator='.')
            output.comment(f'key "{recipe_key}" in {fname}:')
            pprint(_part)
        except (AssertionError, FileNotFoundError, KeyError, PathNotFound, IndexError) as e:
            raise click.UsageError(f"could not get path in recipe json: {e}")
    elif swath:
        _id, _roi = resolve_roi(roi_id, repo)
        try:
            df_main = task.get_geometry_fit_data_frame(_roi.geometry, key='main')
            df_main['main'] = True
            fit = df_main[['IW1_fit', 'IW2_fit', 'IW3_fit']]
            output.comment(f"MASTER ROI '{_roi['name']}': coverage by Swath/Burst\n\n")
            output.table(fit, headers=['burst', 'IW1', 'IW2', 'IW3'])
            output.comment("\n\n")
            output.table(gpd.pd.DataFrame(fit.sum()), headers=['Swath', 'total fit'])
            if task.config.get('subordinate') is not None:
                df_subordinate = task.get_geometry_fit_data_frame(_roi.geometry, key='subordinate')
                df_subordinate['main'] = False
                fit = df_subordinate[['IW1_fit', 'IW2_fit', 'IW3_fit']]
                output.comment(f"SLAVE ROI '{_roi['name']}': coverage by Swath/Burst\n\n")
                output.table(fit, headers=['burst', 'IW1', 'IW2', 'IW3'])
            output.comment("\n\n")
            output.table(gpd.pd.DataFrame(fit.sum()), headers=['Swath', 'total fit'])
        except RuntimeError as e:
            raise click.BadArgumentUsage(str(e))
        except Exception as e:
            log.exception(e)
            raise click.UsageError(str(e))
    else:
        _l = task.get_validation_data_frame()
        headers = ['key', 'error', 'value']
        _active = '(active)' if task.name == repo.active_task else ''
        output.comment(f"Name: {task.name} {_active}")

        output.comment(f"config     path: {task.path}")
        # pprint(task.config)
        try:
            _id, _roi = resolve_roi(roi_id, repo)
            r = TaskRecipe(task=task)
            fname = r.get_ai_recipe_name(_roi['name'])
            if not os.path.isfile(fname):
                fname = f"[ NOT EXISTS ] {fname} "
            output.comment(f"recipe     path: {fname}")
        except Exception as e:
            output.warning(f'recipe {e}')
        try:
            output.comment(f"stack      path: {task.get_stack_path(full=True)}")
        except AssertionError as e:

            output.error(f"stack  {e}")
        except Exception as e:
            log.exception(e)
        try:
            output.comment(f"ai_results path: {task.get_ai_results_path(full=True)}")
        except AssertionError as e:
            output.error(f"ai_results  {e}")
        if task.kind == 'cluster':
            if not task.validate_all(['main', 'subordinate']):
                S1_cycle_T = 24 * 3600 * 12
                m = parse_title(task.config['main'])['completionDate']
                s = parse_title(task.config['subordinate'])['completionDate']

                cycle_dt = abs((m - s) / timedelta(seconds=1)) % S1_cycle_T
                cycle_dt = cycle_dt if (cycle_dt <= S1_cycle_T / 2) else S1_cycle_T - cycle_dt
                if cycle_dt > 0.1:
                    output.warning(
                        f'timedelta: main-subordinate timedelta {cycle_dt} > 0.1. Acquisitions could be misaligned')
                else:
                    output.comment(
                        f'timedelta: main-subordinate timedelta {cycle_dt} <= 0.1. Acquisitions could be aligned')
            else:
                output.comment("bucket delta")
        output.comment(f"errors:  {_l['error'].notna().sum()}")
        _l.sort_index(inplace=True)
        output.table(_l, headers=headers)


########################### TEST PARSED NAMES #############################
@cli_task.command('test-pattern')
@option_locate_task
@click.option('--friendly-name', 'fn_prefix', help='Friendly name pattern',
              default=None,
              )
@click.option('--cos-key', 'cos_prefix', help='COS key pattern',
              default=None,
              )
@option_roi
@pass_task
@pass_repo
@ensure_task_resolved
def rvi_test(repo: Repo, task: Task, fn_prefix, cos_prefix, roi_id):
    """test Rvi

    \b
    refer https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
    date format prefix
        %b: Returns the first three characters of the month name. In our example, it returned "Sep"
        %d: Returns day of the month, from 1 to 31. In our example, it returned "15".
        %Y: Returns the year in four-digit format. In our example, it returned "2018".
        %H: Returns the hour. In our example, it returned "00".
        %M: Returns the minute, from 00 to 59. In our example, it returned "00".
        %S: Returns the second, from 00 to 59. In our example, it returned "00".
        %a: Returns the first three characters of the weekday, e.g. Wed.
        %A: Returns the full name of the weekday, e.g. Wednesday.
        %B: Returns the full name of the month, e.g. September.
        %w: Returns the weekday as a number, from 0 to 6, with Sunday being 0.
        %m: Returns the month as a number, from 01 to 12.
        %p: Returns AM/PM for time.
        %y: Returns the year in two-digit format, that is, without the century. For example, "18" instead of "2018".
        %f: Returns microsecond from 000000 to 999999.
        %Z: Returns the timezone.
        %z: Returns UTC offset.
        %j: Returns the number of the day in the year, from 001 to 366.
        %W: Returns the week number of the year, from 00 to 53, with Monday being counted as the first day of the week.
        %U: Returns the week number of the year, from 00 to 53, with Sunday counted as the first day of each week.
        %c: Returns the local date and time version.
        %x: Returns the local version of date.
        %X: Returns the local version of time.
    Example: "{m_completionDate:%Y} then {m_completionDate:%F}"
         will output "2020-01-23 05:41:17 then 2020_2020-01-23"
    """
    _id, roi = resolve_roi(roi_id, repo)
    roi_name = roi['name']
    fields = task._compose_friendly_keys(roi_name)

    output.hint(f'possible PATTERN keys:')
    output.table(fields.items(), headers=['key', 'value'])

    output.comment(f'CURRENT PATTERN friendly name "{task.config["friendly_name"]}"')
    output.comment(f'CURRENT PARSED  friendly name "{task.get_ai_friendly_name(roi_name)}"')
    output.comment(f'CURRENT PATTERN cos key "{task.config["cos_key"]}"')
    output.comment(f'CURRENT PARSED  cos key "{task.get_cos_key(roi_name)}"')
    if fn_prefix:
        task_value = fn_prefix.format(**fields)
        click.echo(f'TEST PARSED friendly name   "{task_value}"')
    if cos_prefix:
        cos_value = cos_prefix.format(**fields)
        click.echo(f'TEST PARSED       cos key   "{cos_value}"')


########################### GET ############################################
# @cli_task.command('get-key')
@cli_task.command('say')
@option_locate_task
@click.option('--raw', is_flag=True, default=False, help="print not-formatted or extended values")
# @click.option('--parsed', is_flag=True, default=False, help="return formatted patterns")
@click.option('--info', is_flag=True, default=False, help="Print errors")
@click.argument('key')
@option_roi
@pass_task
@pass_repo
@ensure_task_resolved
def task_get(repo: Repo, task: Task, key, raw, roi_id, info):
    """Get task config key (for scripting purposes)

    return single  value of the task config, no errors are printed

    for debug errors  exec command with --info option


    \b
    if key is not exists returns empty string with exit code 1
    if key is not valid  returns empty string with exit code 2
    if other errors      returns empty string with exit code 3

    Example: ls -la $(ocli task get-value ai_results)
             outputs list of directories where ai results will be stored


    if --parsed :
        returns parsed or extended values

        transformed keys:
            main - returns main product ID
            subordinate -  returns main product ID

        Example: ls -lastr $( ocli -v INFO t get-key ai_results --parsed )
            outputs content of active task ai_results directory

    """
    # _id, roi = resolve_roi(roi_id, repo)
    # r = TaskRecipe(task=task)
    if key not in task.config:
        info and output.error('key not found')
        click.echo('')
        exit(1)
    e, value = task.get_valid_key(key)
    if e:
        info and output.error(f"{e}")
        click.echo('')
        exit(2)
    if not raw:
        try:
            if key == 'ai_results':
                value = task.get_ai_results_path(full=True)
            elif key == 'stack_results':
                value = task.get_stack_path(full=True)
            elif key in ['main', 'subordinate']:
                value = s1_prod_id(task.config[key])
            elif key in ['main_path', 'subordinate_path']:
                value = _local_eodata_relative_path(task.config['eodata'], task.config[key])
            elif key == 'friendly_name':
                _, roi = resolve_roi(roi_id, repo)
                if roi.empty:
                    raise AssertionError(f"no roi found {roi_id}")
                value = task.get_ai_friendly_name(roi['name'])
            elif key == 'cos_key':
                _id, roi = resolve_roi(roi_id, repo)
                if roi.empty:
                    raise AssertionError(f"no roi found {roi_id}")
                value = task.get_cos_key(roi['name'])

        except Exception as e:
            info and output.error(f"{e}")
            click.echo('')
            exit(3)
        pass

    click.echo(value)
    return value


# ######################### DELETE #############################################
@cli_task.command('delete')
@option_yes
@option_locate_task
@pass_task
@ensure_task_resolved
def task_delete(task: Task, yes):
    """ delete task settings and cache, to delete task results run 'task clear'"""

    try:
        if not yes and not yes_or_confirm(yes, f"Remove task {task.path}?"):
            return
        shutil.rmtree(task.path, ignore_errors=False)
    except OSError:
        raise click.BadParameter(f"could not delete {task.path}")


# ######################### LIST #############################################
@cli_task.command('list')
@option_repo_name
@pass_repo
def task_list(repo: Repo):
    """ list tasks for project"""
    _path = repo.get_project_path()
    if not os.path.isdir(_path):
        raise click.BadOptionUsage('name', f"Project '{repo.active_project}': Could not find directory '{_path}'")
    _l = Task.get_list(_path, repo.active_task)
    if not repo.active_project:
        output.error(f"No active project selected")
    else:
        output.comment(f'Task list for project: {repo.active_project}')
        output.comment(f'Active task: {repo.active_task}\n\n')
        output.table(_l, headers=['#', '', 'name', 'path'], showindex="always")


# ######################### LOAD #############################################
def rsync_meta(options: List[str], remote_path: str, local_path: str):
    """ Download data with rsync command

    :param options:
    :param remote_path:
    :param local_path:
    :return:
    """
    cmd = os.path.join(cmd_dir, 'rsync-metadata.sh')
    # os.chmod(cmd, stat.S_IXUSR)
    os.makedirs(local_path, exist_ok=True)
    subprocess.run([cmd, f'{" ".join(options)}', remote_path, local_path])


# TODO invoke check that loaded data is consistent (some SAFE dirs in dias are empty or contains only preview folder)

@cli_task.command('get-data')
@option_locate_task
@click.option('-d', '--data', 'data', is_flag=True, default=False, help='load meta-data and data')
@click.option('-m', '--main', is_flag=True, default=False, help='load main')
@click.option('-s', '--subordinate', is_flag=True, default=False, help='load subordinate')
@click.option('--dry-run', is_flag=True, default=False, help='dry-run, do not perform actual download')
@pass_task
@ensure_task_resolved
def task_get(task: Task, data, main, subordinate, dry_run):
    """ load satellite date into task.eodata directory"""
    # Zsh and other crazy shells extends patterns passed arguments, so be shure rsync runs in BASH!!!
    if task.config.get('source') != 'Sentinel-1':
        raise click.BadParameter(f"Only Sentinel-1 supported for now, task source is {task.get_valid_key('source')}")
    if not main and not subordinate:
        raise click.BadOptionUsage('main', "at least on of  --main or --salve option is required")
    ks = ['eodata', 'main']
    if task.kind == 'cluster':
        ks.append('subordinate')
    for k in ks:
        e = task.validate(k)
        if e is not None:
            raise click.BadArgumentUsage(f"'{k}' is invalid, reason: {','.join(e)}")

    def _rsync_meta(key, task: Task, options):

        try:
            _p = _local_eodata_relative_path(task.config['eodata'], task.config[key + '_path'])
            local_path = os.path.dirname(_p)
            output.info(f"loading {key} into '{local_path}'")
            rsync_meta(options, task.config[key + '_path'], local_path)
            output.success(f"{key} metadata loaded")
            # rsync_meta('',_target)
            # output.info(f"loading {key} into '{_target}'")
            # os.makedirs(_target, exist_ok=True)
            # subprocess.run([cmd, f'{" ".join(options)}', task.config[key + '_path'], _target])
            # output.success(f"{key} metadata loaded")
        except OSError as er:
            log.exception(e)
            raise click.BadParameter(f"{er}")

    opts = []
    # if not data:
    #     opts.append("--exclude '*.tiff'")
    if dry_run:
        opts.append('--dry-run')
        opts.append('-vv')
    if data:
        e, iw = task.get_valid_key('swath')  # type: str
        if e:
            raise OCLIException(f"Swath is invalid: {e}")
        opts.append('--include "*/"')
        opts.append(f'--include "*{iw.lower()}*.tiff"')
    else:
        opts.append("--exclude 'support/*'")
        opts.append("--exclude 'preview/*'")
        opts.append("--exclude 'annotation/calibration/*'")
    opts.append("--exclude '*.tiff'")
    if main:
        _rsync_meta('main', task, opts)
    if subordinate:
        _rsync_meta('subordinate', task, opts)


# ######################### LS #############################################
@cli_task.command('ls')
@option_locate_task
@click.option('-m', '--main', is_flag=True, default=False, help='list main directory')
@click.option('-s', '--subordinate', is_flag=True, default=False, help='list  subordinate directory')
@click.option('-a', '--list_all', is_flag=True, default=False, help='list  all task directories')
@click.option('--ai', 'ai_results', is_flag=True, default=False, help='list  subordinate directory')
@click.option('--stack', 'stack_results', is_flag=True, default=False, help='list  subordinate directory')
@click.option('-t', '--terse', is_flag=True, default=False, help='terse output')
@pass_task
@ensure_task_resolved
def task_ls(task: Task, main, subordinate, ai_results, stack_results, terse, list_all):
    """ list content of task main or subordinate directory"""

    def comment(str):
        if not terse:
            output.comment(str)

    e, eo_data = task.get_valid_key('eodata')
    if not any([main, subordinate, ai_results, stack_results, list_all]):
        list_all = terse = True
    if terse:
        cmd = ['du', '-shc']
    else:
        cmd = ['ls', '-lahR']
    if e:
        raise click.BadArgumentUsage(f"Task config key 'eodata' is invalid, reason: {','.join(e)}")
    paths = []
    _, kind = task.get_valid_key('kind')
    if list_all or main:
        e, _m = task.get_valid_key('main_path')
        if e:
            raise click.BadArgumentUsage(f"Task config key 'main_path' is invalid, reason: {','.join(e)}")
        _p = _local_eodata_relative_path(eo_data, _m)
        comment(f"main path: {_p}\n\n")
        paths += [_p]
    if kind in ['cluster'] and (list_all or subordinate):

        e, _s = task.get_valid_key('subordinate_path')
        if e:
            raise click.BadArgumentUsage(f"Task config key  'subordinate_path' is invalid, reason: {','.join(e)}")
        _p = _local_eodata_relative_path(eo_data, _s)
        comment(f"main path: {_p}\n\n")
        paths += [_p]
    if list_all or ai_results:
        try:
            _p = task.get_ai_results_path(full=True)
            comment(f"ai results path: {_p}\n\n")
            paths += [_p]
        except AssertionError as e:
            raise click.UsageError(str(e))
    if list_all or stack_results:
        try:
            _p = task.get_stack_path(full=True)
            comment(f"Stack results path: {_p}\n\n")
            paths += [_p]
        except AssertionError as e:
            raise click.UsageError(str(e))
    if not len(paths):
        raise click.UsageError("No ls targets provided")
    try:

        subprocess.run(cmd + paths)
    except Exception as e:
        log.exception(e)


# ######################### CLEAR #############################################
@cli_task.command('clear')
@option_yes
@click.option('-d', '--data', 'data', is_flag=True, default=False, help='clear meta-data and data')
@click.option('-m', '--main', is_flag=True, default=False, help='clear main')
@click.option('-s', '--subordinate', is_flag=True, default=False, help='clear subordinate')
@pass_task
@ensure_task_resolved
def task_clear(task: Task, main, subordinate, data, yes):
    """ delete task data and results"""

    e, eo_data = task.get_valid_key('eodata')
    if e is not None:
        raise click.BadArgumentUsage(f"Task config key 'eodata' is invalid, reason: {','.join(e)}")

    def __clear_data(key):
        e, _p = task.get_valid_key(key)
        if e is not None:
            raise click.BadArgumentUsage(f"Task config key '{key}' is invalid, reason: {','.join(e)}")

        try:
            shutil.rmtree(_local_eodata_relative_path(eo_data, task.config[key]))
        except OSError as er:
            raise click.UsageError(f"{er}")

    if data & yes_or_confirm(yes, f'Remove all product data for task {task.name}?'):
        if main:
            __clear_data('main_path')
        if subordinate:
            __clear_data('subordinate_path')
    if main or subordinate:
        # TODO clean snap and AI out data
        output.comment("Not implemented - remove SNAP intermediate data")
        output.comment("Not implemented - remove AI out data")


# ######################### CLONE #############################################

# todo --activate option

@cli_task.command('clone')
@option_yes
@click.option('--quiet' '-q', 'quiet', is_flag=True, default=False, help='Show cloned task')
@click.option('-a', '--activate', 'activate', is_flag=True, default=False, help="Activate cloned task")
@click.option('--target', 'project', help="target project name , default to active project")
@option_locate_task
@click.argument('new_name', metavar="<NEW NAME>", type=click.STRING, required=True)
@click.argument('args', nargs=-1)
@pass_task
@ensure_task_resolved
def task_clone(task: Task, project, new_name, yes, args, activate, quiet):
    """ clone existed task """
    # TODO - SHARED code with task_create
    try:
        if project:
            task.project = project
        old_name = task.name
        task.name = new_name
        task.path = task.get_path_by_name()
        if not is_path_exists_or_creatable(task.path):
            raise click.UsageError(f"Could not create {task.path}")
        _exists, _rc = task.get_task_rc()
        if _exists and not yes_or_confirm(yes,
                                          f"Target task directory '{task.path}' contains existed task, Overwrite?"):
            return

        task.create()
        output.success(f'task "{old_name}" cloned to "{task.name}" in project "{task.project}"')
        if len(args):
            click.get_current_context().invoke(task_set, args=args, yes=yes)

        if activate:
            click.get_current_context().invoke(task_activate, name=new_name, quiet=quiet)
        # if quiet:
        #     # print task
        #     click.get_current_context().invoke(task_list)
    except OSError as e:
        raise click.BadParameter(f"Could not not clone task: {e}")


# ######################### MAKE #############################################
@cli_task.group('make')
# @pass_task
# @ensure_task_resolved
def task_run():
    """ Run data-processing """
    # output.comment(f"Start processing task '{task.name}'")
    pass


@task_run.group('stack')
def task_run_stack():
    """Make products stack"""
    pass


@task_run_stack.command('snap')
@option_locate_task
@click.option('--dry-run', is_flag=True, default=False, help='dry-run, do not perform actual running')
@click.option('--gpt-cache', default='40G', help='ESA SNAP gpt RAM cache max size')
@option_yes
@pass_task
@pass_repo
@ensure_task_resolved
def task_run_stack_snap(repo: Repo, task: Task, yes, dry_run, gpt_cache):
    """Make stack with ESA SNAP pipeline"""
    kind = task.config.get('kind')
    if kind not in ['cluster']:
        raise click.UsageError(f'Only task with kind "cluster" supported with snap')
    e = task.validate_all(ignore=['predictor'])
    if e:
        raise click.UsageError(f'Task config is invalid, reason: {" ".join(e)} ')
    output.comment(f"Start stacking '{task.name}'")
    snap_path = task.get_stack_path(full=True)
    if os.path.isdir(snap_path):
        if len(os.listdir(snap_path)) != 0 and not yes_or_confirm(yes,
                                                                  f"Stack directory '{snap_path}' exists. Override?"):
            return
    task_stack_snap(task,
                    dry_run=dry_run,
                    gpt_cache=gpt_cache,
                    cmd_dir=cmd_dir,
                    log=click.echo
                    )


# ############################### STACK SARPY ################################

# TODO use original commant from sarpy-cli, mount it here (like pro)
@task_run_stack.command('sarpy')
@option_locate_task
@click.option('--skip-verified', is_flag=True, default=False, help='Skip stack creation if stack is valid', )
@click.option('--dry-run', is_flag=True, default=False, help='dry-run, do not perform actual running')
@click.option('--decimation', 'decimation',
              help='decimation  vertical horizontal',
              type=(int, int), default=(1, 6),
              show_default=True,
              )
@click.option('--filter', 'decimation_filter',
              help='decimation filter',
              type=click.Choice(['gauss', 'median']), default='gauss',
              show_default=True,
              )
@click.option('--single', help='Single product stack',
              is_flag=True, default=False,
              )
@click.option('--no-clean', help='do not clean intermediate results',
              is_flag=True, default=False,
              )
@option_yes
@pass_task
@pass_repo
@ensure_task_resolved
def task_run_stack_sarpy(repo: Repo, task: Task, yes, dry_run, decimation, no_clean, single, skip_verified,
                         decimation_filter):
    """Make stack with ESA SNAP pipeline"""
    e = task.validate_all(ignore=['predictor'])
    if e:
        raise click.UsageError(f'Task config is invalid, reason: {" ".join(e)} ')
    output.comment(f"Start stacking '{task.name}'")
    try:
        _eodata = task.config['eodata']
        snap_path = task.get_stack_path(full=True)
        output.info(f"Creating  products stack in  {snap_path}")
        os.makedirs(snap_path, exist_ok=True)
        from ocli.sarpy.cli import full_stack, single_stack
        p0 = perf_counter()
        kw = dict(
            swath=[task.config['swath']],
            pols=['VV', 'VH'],
            decimation=decimation,
            verbose=repo.verbose,
            out=snap_path,
            yes=yes,
            no_clean=no_clean,
            dry_run=dry_run,
            skip_verified=skip_verified,
            decimation_filter=decimation_filter,
        )

        if single:
            click.get_current_context().invoke(
                single_stack,
                main=_local_eodata_relative_path(_eodata, task.config['main_path']),
                **kw
            )
        else:
            click.get_current_context().invoke(
                full_stack,
                main=_local_eodata_relative_path(_eodata, task.config['main_path']),
                subordinate=_local_eodata_relative_path(_eodata, task.config['subordinate_path']),
                **kw
            )
        p0 = perf_counter() - p0
        conf = task.config
        conf['stack_processor'] = 'sarpy'
        conf['stack_performance'] = p0
        conf['stack_created'] = datetime.now().strftime("%F %T")

    except Exception as e:
        # log.exception(e)
        raise OCLIException(f"{e}")


@task_run.command('recipe')
@option_locate_task
@option_yes
@option_roi
@click.option('--print', 'print_results', is_flag=True, default=False,
              help="Print recipe, do not save file",
              cls=MutuallyExclusiveOption, mutually_exclusive=["file"],
              )
@click.option('--quiet', '-q', is_flag=True, default=False)
@click.option('--edit', 'edit', default=False, is_flag=True, help='Open generated recipe in editor')
@click.option('--override', 'override', is_flag=True, default=False,
              help='Override default recipe file if exists')
@click.option('--force', 'force', is_flag=True, default=False,
              help='dry-run, do not perform most of error checks, use to generate AI recipe for learning phase')
@click.option('-f', '--file', default=None, help='Override auto-generated AI recipe filename and path ',
              cls=MutuallyExclusiveOption, mutually_exclusive=["print"],
              )
@click.option('--zone-by-roi', is_flag=True, default=False,
              cls=MutuallyExclusiveOption, mutually_exclusive=["zone"],
              help='Define zone by ROI envelope (rectangular bounding box containing all ROI points)')
@click.option('-z', '--zone', type=click.INT, nargs=4, default=None,
              cls=MutuallyExclusiveOption, mutually_exclusive=["zone-by-roi"],
              help='Define zone as minY minX maxY maxX in Pixels coordinates')
@click.option('--from-template',
              type=click.STRING,
              cls=MutuallyExclusiveOption, mutually_exclusive=["clusters"],
              default=None,
              help='Add recipe values from template')
@click.option('-c', '--clusters', type=click.INT, default=None,
              cls=MutuallyExclusiveOption, mutually_exclusive=["from-template"],
              help='number of generated clusters in predictor. '
                   'NOTE: used only in fit (learn) phase, ignored in predict phase')
@pass_task
@pass_repo
@ensure_task_resolved
def task_recipe(repo: Repo, task: Task, force, override,
                roi_id: int, file: str, edit: bool,
                zone: tuple, zone_by_roi: bool,
                from_template,
                clusters: int,
                quiet,
                yes,
                print_results):
    """ Generate AI recipe file

    \b
    * use --force to generate recipe file with incomplete task settings (for ex. when creating predictors in learning phase)
    * use --override to override existed task's default recipe file

    """
    # TODO CLEAR RECIPE bofore make (REPL recipe remebers old values for prev run)
    try:
        _id, _roi = resolve_roi(roi_id, repo)
        r = TaskRecipe(task=task)
        e = []
        if from_template:
            try:

                _pred_dir = task.config['predictor']
                if not _pred_dir:
                    raise AssertionError("predictor key for task is required before processing template")

                conf_json = Path(_pred_dir, 'config.json')
                if conf_json.is_file():
                    if not yes_or_confirm(yes, f"Override {conf_json}?"):
                        return

                if from_template == 'default':
                    _e, _tpl = r.get_predictor_config_json_template()
                    e += _e
                else:
                    if not Path(from_template).is_file():
                        raise AssertionError(f'template file "{from_template}" not found')
                    with open(from_template, 'r') as _f:
                        _tpl = json.load(_f)
                # log.error("here")
                # pprint(_tpl)
                os.makedirs(_pred_dir, exist_ok=True)
                with open(conf_json, 'w+') as _f:
                    json.dump(_tpl, _f, indent=4)
                # r.recipe.update(_tpl)
            except Exception as e:
                log.exception(e)
                raise OCLIException(f"template '{from_template}' is invalid: {e}")

        e += r.generate_recipe(roi=_roi)
        if e and not force:
            [output.error(x) for x in e]
            return

        if zone_by_roi:
            try:
                _env_file = None
                try:

                    _env_file = next(Path(task.get_stack_path(full=True)).glob('*.img'), None)
                    if _env_file is None:
                        raise AssertionError(f"Stack is incomplete")
                    _env_file = str(_env_file.absolute())
                except  AssertionError:
                    if not force:
                        raise AssertionError("Stack is incomplete")
                    e += ["Stack is empty"]
                    output.warning("Stack is empty")
                if _env_file:
                    zone = zoneByRoi(_env_file, _roi, roi_crs=repo.roi.db.crs)
                else:
                    raise AssertionError("Could not apply option --zone-by-roi: Stack is incomplete")
            except AssertionError as e:
                raise OCLIException(str(e))
        if zone:
            r.recipe['zone'] = [[zone[0], zone[1]], [zone[2], zone[3]]]
        if clusters:
            r.recipe['num_clusters'] = clusters
        e += r.validate_recipe(force)

        """prevent override only default task recipe"""
        def_fname = r.get_ai_recipe_name(_roi['name'])
        if file is None:
            pass
        elif def_fname != file:
            override = True
        fname = file if file else def_fname
        if not Path(fname).is_file():
            override = True
        save = override and (not print_results) and (force or len(e) == 0)
        j, _ = r.save_recipe(fname, _roi, save=save)
        if (not quiet) or print_results:
            click.echo(j)
        if len(e):
            """unique erors via set()"""
            [output.error(f"{r}\n") for r in set(e)]
        # log.error(f"save {save} {override}")
        if not override:
            output.warning(f'Will not override default task recipe file "{fname}"')
        if save:
            if not quiet:
                output.comment(f'Recipe file: {fname}')
            if edit:
                click.edit(filename=fname)
        else:
            output.error(f'Recipe file NOT SAVED')
    except AssertionError as e:
        # log.exception(e)
        raise OCLIException(f'Task is invalid, reason: {e}')
    except RuntimeError as e:
        raise OCLIException(str(e))
