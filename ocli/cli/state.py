import itertools
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime
from functools import wraps
from importlib import import_module
from pathlib import Path
from shutil import copyfile
from typing import List, Tuple, Dict

import click
import geopandas as gpd
import jsonsempai
import yaml
from geopandas import GeoDataFrame

from ocli.aikp import get_tsar_defaults
from ocli.cli import get_main_rc_name, RC_FILE_NAME, default_main_config, default_project_config
from ocli.cli.output import OCLIException
from ocli.project import roi, _local_eodata_relative_path, slugify
from ocli.sent1 import geoloc
from ocli.sent1 import s1_prod_id, parse_title
from ocli.util.date_parse import parse_to_utc_string
from ocli.util.nested_set import nested_set

log = logging.getLogger()

import collections


def deep_update(d: dict, u: dict):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = deep_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def option_limit(f) -> click.option:
    return click.option('-l', '--limit', type=click.INT, default=-1, help="limited number of records, -1 for no-limit")(
        f)


def option_less(f) -> click.option:
    return click.option('--less', is_flag=True, help="use pagination")(f)


def yes_or_confirm(yes, confirm_message):
    return yes or click.confirm(confirm_message)


def option_yes(f) -> click.option:
    """ decorator: add accept option to kwargs """
    return click.option('-y', '--yes', is_flag=True, default=False,
                        help='Do not ask for confirmation.'
                        )(f)


def option_repo_name(f):
    """ decorator: adds --project option as TEMPORARY active_project to Repo,
    doesn't injects kwarg to function
    use repo.active_project
    """

    def callback(ctx, param, value):
        repo = ctx.ensure_object(Repo)  # type: Repo
        if value:
            if not os.path.isfile(repo.get_project_rc_name(value)):
                raise click.BadOptionUsage('project', f"No project found with name '{value}' ")
            repo.set_config('active_project', value)
            repo.read_project(value)
        return value

    f = click.option('--project', '-p',
                     required=False,
                     expose_value=False,
                     help='project name, use active project if omitted',
                     callback=callback
                     )(f)
    return f


def option_repo_roi(f):
    """ decorator: adds --name option as TEMPORARY active_project to Repo,
    doesn't injects kwarg to function
    use repo.active_project
    """

    def callback(ctx, param, value):
        state = ctx.ensure_object(Repo)  # type: Repo
        if value:
            state.set_config('active_roi', value)
        return value

    f = click.option('--roi', '-r',
                     required=False,
                     expose_value=False,
                     help='project name, use active project if omitted',
                     callback=callback
                     )(f)
    return f


class Roi(object):
    """ Wraps ROI operations for GeoDataFrame """
    path = None
    _db = None
    _db_file = None

    def __init__(self, home):
        self.path = home
        pass

    def open(self) -> 'Roi':
        self._get_db()
        return self

    def _get_db(self):
        if self._db is None or self._db.file != self._db_file:
            # TODO  = file alwaye re-opens self._db.file != self._db_file ALWAYS true! self._db_file is never assigned
            # THis is supposed to remove bug when ROI is stale after project was changed
            if not self.path:
                log.warning('Could not use ROI on not activated projects')
            # invalidate cache
            self._db = roi.get_db(self.path)
            if self._db is None:
                log.error("Could not load ROI Database")

    @property
    def db(self) -> GeoDataFrame:
        self._get_db()
        return self._db

    def _check_db(self) -> [bool, bool]:
        r = [False, False]  # [not is exits,is empty]
        if self._db is None:
            r[0] = True
        if self._db.empty:
            r[1] = True
        return r

    def delete(self, id) -> 'Roi':
        try:
            self._db.drop(id, inplace=True)
        except KeyError:
            log.warning("record wit ID does not exists")
        return self

    def add(self, name, geometry, autosave=False, **kwargs) -> 'Roi':
        """ add new roi , **kwargs is additional fields"""
        _nok, _ = self._check_db()
        if _nok:
            log.warning('Database is not opened')
            return self
        _id = len(self._db)
        self._db.loc[_id, 'name'] = name
        self._db.loc[self._db['name'] == name, 'geometry'] = geometry
        for (k, v) in kwargs.items():
            if k not in self._db.columns:
                self._db[k] = None
            self._db.loc[self._db['name'] == name, k] = v
        if autosave:
            self.save()
        return self

    def save(self):
        _nok, _empty = self._check_db()
        if _nok:
            log.warning('Database is not opened')
        elif _empty:
            log.warning('Could not save empty database')
        else:
            roi.save_db(self._db)

    def clear(self):
        _nok, _ = self._check_db()
        if _nok:
            log.warning('Database is not opened')
        else:
            roi.delete_db(self._db)


class Repo(object):
    is_repl = False
    rc_home = None  # type: str
    verbose = False
    active_project = None  # type: str
    active_roi = None  # type: str
    active_task = None  # type: str
    projects_home = None  # type: str
    tasks_home = None  # type: str
    _roi = None  # type: Roi

    def __init__(self, home=None):
        log.debug('initing repo')
        self._config = {}
        self.rc_home = home if home else os.getenv('HOME', '~')
        self.verbose = False

    @staticmethod
    def gt():
        return time.time()

    @property
    def roi(self) -> Roi:
        if self._roi is None:
            self._roi = Roi(self.get_project_path())
        return self._roi

    def get_config(self, key, default=None):
        return self._config.get(key, default)

    def set_config(self, key, value, autosave=False):
        if key == 'active_project':
            self.active_project = value
        elif key == 'active_roi':
            self.active_roi = value
        elif key == 'active_task':
            self.active_task = value
        elif key == 'projects_home':
            self.projects_home = value
        elif key == 'tasks_home':
            self.tasks_home = value
        nested_set(self._config, key.split('.'), value)
        if self.verbose:
            log.debug(f'config[{key}] = {value}')
        if autosave:
            self.save_config()

    def parse_cmd_args(self, args, autosave=False):
        d = dict(arg.split('=') for arg in args)
        for key, value in d.items():
            if key in ['finder.startDate', 'finder.completionDate']:
                if value is not None and value != '':
                    _value = parse_to_utc_string(value)
                    if _value is None:
                        raise AssertionError(f'could not parse date for {key}={value}')
                    value = _value
                else:
                    log.error("here we are")
            self.set_config(key, value, autosave=False)
        if autosave:
            self.save_config()

    def save_config(self):
        self.save_main()
        self.save_project()

    def save_project(self, rc=None):
        if rc is None:
            rc = self.get_project_rc_name()
        main_keys = default_project_config().keys()
        dc = {k: v for (k, v) in self._config.items() if k in main_keys}
        # log.error(dc)
        with open(rc, 'w') as _f:
            yaml.dump(dc, _f)

    def read_main_config(self, conf_file=None, read_all=True):
        _rc = conf_file if conf_file else os.path.join(self.rc_home, RC_FILE_NAME)
        if not os.path.isfile(_rc):
            raise AssertionError(f'{_rc} not found')
        with open(_rc, 'r') as _f:
            _conf = yaml.safe_load(_f)  # type: dict
            for key, value in _conf.items():
                self.set_config(key, value, autosave=False)
        if read_all and self.active_project:
            try:
                self.read_project()
            except AssertionError  as e:
                log.warning(f"{e}")

    def read_active_project_conf(self, name=None):
        const_k = ['projects_home', 'tasks_home', 'active_project']
        self._config = {k: v for k, v in self._config.items() if k in const_k}
        self.read_project(name)

    def read_project(self, name=None):
        """ read project config into repo.config

        :param name: project name
        :return:
        """
        proj_conf_file = self.get_project_rc_name(name)
        try:
            with open(proj_conf_file) as _f:
                _conf = yaml.safe_load(_f)  # type: dict
                for key, value in _conf.items():
                    self.set_config(key, value, autosave=False)
        except yaml.YAMLError  as e:
            raise AssertionError(f"File  '{proj_conf_file}' has malformed config file: {e}")
        except OSError as e:
            raise AssertionError(f"File  '{proj_conf_file}' could be invalid, reason: {e}")

    def save_main(self, rc=None):
        if rc is None:
            rc = get_main_rc_name(self.rc_home)
        main_keys = default_main_config(self.projects_home).keys()
        dc = {k: v for (k, v) in self._config.items() if k in main_keys}
        with open(rc, 'w') as _f:
            yaml.dump(dc, _f)

    def get_project_path(self, name=None):
        """ refurns active project in name is None otherwise composes path to project"""
        # log.error(self.projects_home)
        _name = name if name else self.active_project
        return os.path.join(self.projects_home, _name)

    def get_project_rc_name(self, name=None):
        return os.path.join(self.get_project_path(name), RC_FILE_NAME)

    def list_projects(self, home: str = None) -> list:
        """ list of projects in home (if set) or projects home,
        aka dirs that contains  RC_FILE_NAME

        :returns list of projects [{is_active:boolean,name:str, path:str},...]

        """
        if home and not self.projects_home:
            raise click.BadParameter("Could not resolve workspace home, run 'init'")
        _home = home if home else self.projects_home
        return [
            dict(active=self.active_project and x == self.active_project, name=x, path=os.path.join(_home, x))
            for x in os.listdir(_home)
            if os.path.isdir(os.path.join(_home, x)) and os.path.isfile(os.path.join(_home, x, RC_FILE_NAME))
        ]

    # ------------- utils ----------------
    def tolist(self):
        """ return some config as list for pretty printing"""
        l = [
            ['home', self.rc_home],
            ['verbose', self.verbose],
        ]
        __c = self._config.copy()
        try:
            __f = __c.pop('finder')
            [l.append([f'finder.{k}', v]) for k, v in __f.items()]
        except KeyError:
            pass
        [l.append([k, v]) for k, v in __c.items()]
        return l

    # def __str__(self):
    #     return f"""
    #     home: {self.rc_home}
    #     """

    # def __repr__(self):
    #     return '<Repo %r>' % self.rc_home


############################################ TASK  ##################################

TASK_HEAD = {
    'version': 0.32,
    '_rev': 1,
    '_id': None,
    'name': None,
    'project': None,
    'tag': [],
    'description': "",
    'processing': 'default',
    'comment': None,
    'friendly_name': "{project}/{name}/{m_completionDate:%Y%m%d}",
    'cos_bucket': 'optoss-pipeline',
    'cos_key': "{project}/{kind}/{name}/{m_completionDate:%Y/%m/%d}/{m_id}_{s_id}_{swath}_{firstBurstIndex}_{lastBurstIndex}_{predictor}",
}

TASK_DEFAULTS = {
    **TASK_HEAD,
}


def updrade_task(task_config: dict):
    upgraded = False
    if task_config['version'] < 0.3:
        log.info(f"Updating task from version {task_config['version']} to 0.3 ")
        task_config['kind'] = 'cluster'
        task_config['source'] = 'Sentinel-1'
        task_config['version'] = 0.3
        task_config['_rev'] += 1
        upgraded = True
    if task_config['version'] < 0.31:
        if 'stack_path' in task_config:
            del task_config['stack_path']
        task_config['friendly_name'] = TASK_HEAD['friendly_name']
        task_config['cos_key'] = TASK_HEAD['cos_key']
        task_config['version'] = 0.31
        task_config['_rev'] += 1
        upgraded = True
    if task_config['version'] < 0.311:
        if 'snap_results' in task_config:
            task_config['stack_results'] = task_config['snap_results']
            del task_config['snap_results']
        task_config['version'] = 0.311
        task_config['_rev'] += 1
        upgraded = True
    if task_config['version'] < 0.32:
        task_config['tag'] = []
        task_config['description'] = "Task description"
        task_config['version'] = 0.32
        task_config['_rev'] += 1
        upgraded = True
    if task_config['version'] < 0.33:
        task_config['bucket'] = ''
        task_config['comment'] = ''
        task_config['version'] = 0.33
        task_config['_rev'] += 1
        upgraded = True
    if task_config['version'] <= 0.34:
        if 'kind' not in task_config:
            task_config['kind'] = 'cluster'
        if 'template' not in task_config:

            if task_config['kind'] == 'cluster':
                task_config['template'] = 'ocli.aikp.cluster.sentinel_1'
                task_config['template_version'] = 0.1

                upgraded = True
            if task_config['kind'] in ['rvi', 'orvi']:
                task_config['template'] = 'ocli,pro.aikp.orvi.sentinel_1'
                task_config['template_version'] = 0.1
                task_config['class'] = 'S1'
                upgraded = True
        if task_config['kind'] in ['rvi', 'orvi']:
            task_config['kind'] = 'orvi'
        task_config['class'] = 'S1'
        task_config['version'] = 0.34
        task_config['cos_bucket'] = TASK_HEAD['cos_bucket']
        task_config['_rev'] += 1
    return upgraded


class MutuallyExclusiveOption(click.Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop('mutually_exclusive', []))
        help = kwargs.get('help', '')
        if self.mutually_exclusive:
            ex_str = ', '.join(self.mutually_exclusive)
            kwargs['help'] = help + (
                    "\nNOTE  This argument is mutually EXCLUSIVE with "
                    ' arguments: [' + ex_str + '].'
            )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        # if self.name == 'name' and 'path' not in opts and '--help' not in args:
        #     if '--help' not in args:
        #         self.required = True
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise click.UsageError(
                "Illegal usage: `{}` is mutually exclusive with "
                "arguments `{}`.".format(
                    self.name,
                    ', '.join(self.mutually_exclusive)
                )
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(
            ctx,
            opts,
            args
        )


def project_options_callback(ctx, param, value):
    repo = ctx.ensure_object(Repo)  # type: Repo
    task = ctx.ensure_object(Task)  # type: Task
    # log.info(f"setting {param.name}={value}")
    if param.name == 'project':
        if value:
            repo.set_config('active_project', value)
            # todo read project, do not clear active_project
            repo.read_active_project_conf()
            # log.error(repo._config)
        task.name = repo.active_task
        task.projects_home = repo.projects_home
        task.project = repo.active_project
    elif param.name == 'name' and value:
        task.name = value
    elif param.name == 'path' and value:
        task.name = None
        task.project = None
        task.path = value
    return value


def option_locate_task(f):
    f = click.option('-p', 'project', help='Project name.', default=None,
                     callback=project_options_callback,
                     expose_value=False,
                     is_eager=True,  # ensure project parsed first
                     cls=MutuallyExclusiveOption, mutually_exclusive=["path"])(f)
    f = click.option('-n', 'name', help='Task name.', default=None,
                     expose_value=False,
                     callback=project_options_callback,
                     cls=MutuallyExclusiveOption, mutually_exclusive=["path"])(f)
    f = click.option('--path', help='Path to task directory.', default=None,
                     expose_value=False,
                     callback=project_options_callback,
                     cls=MutuallyExclusiveOption, mutually_exclusive=["name", "project"])(f)
    return f


def ensure_task_loaded(method):
    @wraps(method)
    def _impl(self, *method_args, **method_kwargs):
        if not self.loaded:
            raise RuntimeError(f"Task is not loaded: project {self.project} name {self.name} path {self.path} ")
        return method(self, *method_args, **method_kwargs)

    return _impl


class Task(object):
    TASK_RC = '.tsar-taskrc'
    name: str = None
    path: str = None
    project: str = None
    projects_home: str = None
    config = TASK_DEFAULTS
    loaded = False

    def __init__(self):
        # No args so ensure=True could be used
        pass

    @property
    def kind(self):
        return self.config.get('kind')

    def upgrade_task(self):
        """ bump version. fix isuues """
        if updrade_task(self.config):
            self.save(backup=True)

    @classmethod
    def get_list(cls, path: str, active_task='') -> List[List[str]]:
        """ Get all tasks list in active project"""
        return [
            ['*' if x == active_task else '',x,  os.path.join(path, x)]
            for x in os.listdir(path)
            if os.path.isdir(os.path.join(path)) and os.path.isfile(os.path.join(path, x, Task.TASK_RC))
        ]

    @staticmethod
    def _task_uuid():
        return str(uuid.uuid4())

    def get_task_rc(self) -> Tuple[bool, str]:
        """ return (bool,path) for active task rc file"""
        _f = os.path.join(self.path, self.TASK_RC)
        return os.path.isfile(_f), _f

    def save(self, backup=False):
        """ save current task to rc file"""
        try:
            _, _f = self.get_task_rc()
            os.makedirs(self.path, exist_ok=True)
            f = Path(_f)
            if backup and f.is_file():
                copyfile(f, _f + '-{date:%F-%T}'.format(date=datetime.now()))
            with open(f, mode='w') as _f:
                yaml.dump(self.config, _f)
        except FileNotFoundError as e:
            raise RuntimeError(str(e))

    def set_config(self, key, value, only_existed=True):
        """ set current task config key-value pair"""
        if only_existed and key not in self.config:
            raise OCLIException(f'Unknown key "{key}"')
        self.config[key] = value

    def create(self):
        """ create empty task and save rc-file"""
        self.config['name'] = self.name
        self.config['project'] = self.project
        self.config['_rev'] = 1
        self.config['_id'] = self._task_uuid()
        self.save()

    def load_task(self):
        """load task from rc-file"""
        _exists, _f = self.get_task_rc()
        if not _exists:
            raise RuntimeError(f"'{self.path}' is not task directory")
        try:
            with open(_f, 'r') as f:
                _cur = yaml.safe_load(f)
            self.config = _cur
            if self.upgrade_task():
                self.save()
            self.loaded = True
        except OSError as e:
            raise RuntimeError(f"{e}")

    def get_path_by_name(self):
        """get task path by name"""
        return os.path.join(self.projects_home, self.project, self.name)

    def resolve(self):
        """ load task
        if we have path:
            try to load task's project and name from RC-file
        elif we have name:
            try to resolve from project/task

        :return:
        """
        if self.path:
            self.load_task()
            self.name = self.config.get('name')
            self.project = self.config.get('project')
        elif self.project and self.name:
            self.path = self.get_path_by_name()
            self.load_task()
        else:
            raise RuntimeError("Could not resolve task")

    @ensure_task_loaded
    def validate(self, key):
        config = self.config
        # TODO validate main-subordinate is in
        if key not in config:
            raise click.BadArgumentUsage(f'Could not validate Key {key}: key not found')
        value = config[key]
        error = []
        template = config.get('template', None)
        if template:

            """check task created from template, validate"""
            try:
                tpl = import_module(template)  # type: TaskTemplate
                if not hasattr(tpl, 'validate_task'):
                    raise ModuleNotFoundError(f"Template is invalid: validate_task is not defined")
                processed, error = tpl.validate_task(self, key)
                if processed:
                    return error if len(error) else None
            except ModuleNotFoundError as e:
                error.append(f'Could not import "{template}": {e} ')
        if key in TASK_HEAD:
            """ DEFAULT VALIDATIONS """
            if key in ['version', '_rev', '_id', 'name', 'project', 'friendly_name', 'cos_key',
                       'processing'] and not value:
                error.append('Required')
        elif not template:
            error.append(f"Temlate required for validation")

        return error if len(error) else None

    @ensure_task_loaded
    def validate_all(self, keys=None, ignore=None):
        all_keys = keys if keys else self.config.keys()
        error = []
        for k in all_keys:
            if ignore and k in ignore:
                continue
            e = self.validate(k)
            if e is not None:
                error.append(f"{k}: {','.join(e)}")
        return error

    @ensure_task_loaded
    def get_valid_key(self, key) -> (list, str):
        """ retrun config value and error
        if value is invalid return (error,None)
        else (None,value)
        :returns tuple (error,value)
        """
        e = self.validate(key)
        if e:
            return e, None
        else:
            return None, self.config[key]

    @ensure_task_loaded
    def get_validation_data_frame(self) -> 'gpd.pd.DataFrame':
        _cur = self.config
        headers = ['key', 'error', 'value']
        _l = gpd.pd.DataFrame([[k, self.validate(k), _cur[k]] for k in _cur], columns=headers)
        _l.set_index('key', inplace=True)
        return _l

    @ensure_task_loaded
    def get_geometry_fit_data_frame(self, geometry, key='main') -> 'gpd.pd.DataFrame':
        # TODO use validate_all
        if key not in ['main', 'subordinate']:
            raise AssertionError("key: only 'main' or 'salve are supported'")
        for k in ['eodata', key + '_path']:
            e = self.validate(k)
            if e:
                raise RuntimeError(f"key '{k}' is invalid: {','.join(e)} ")
        _df = geoloc.swath_table(
            _local_eodata_relative_path(self.config['eodata'], self.config[key + '_path']),
            geometry
        )
        return _df

    @ensure_task_loaded
    def get_stack_path(self, full=False):
        """ return name of directory to save Stack results

            format is :

               cluster Sentinel-1: <mainID>_<subordinateID>_<swath>_<firstBurstIndex>_<lastBurstIndex>
               rvi Sentinel-1: <mainID>_<swath>_<firstBurstIndex>_<lastBurstIndex>

            if full==True os.join with  with <task.config.stack_results>
        """
        template = self.config.get('template')
        if template:

            """check task created from template, validate"""
            try:
                tpl = import_module(template)  # type: TaskTemplate
                if not hasattr(tpl, 'get_stack_path'):
                    raise ModuleNotFoundError(f"Template is invalid: get_stack_path is not defined")
                return tpl.get_stack_path(self, full)
            except ModuleNotFoundError as e:
                AssertionError(f'Could not import "{template}": {e}')
        else:
            raise AssertionError(f"Task has no template!")

        #
        # kind = self.config['kind']
        # source = self.config['source']
        # if kind == 'cluster' and source == 'Sentinel-1':
        #     e = self.validate_all(['stack_results', 'main', 'subordinate', 'swath', 'firstBurstIndex', 'lastBurstIndex'])
        #     if e:
        #         raise AssertionError(','.join(e))
        #     main_id = s1_prod_id(self.config['main'])
        #     subordinate_id = s1_prod_id(self.config['subordinate'])
        #     snap_name = f"{main_id}_{subordinate_id}_{self.config['swath']}" + \
        #                 f"_{self.config['firstBurstIndex']}_{self.config['lastBurstIndex']}"  # noqa
        # elif kind == 'rvi' and source == 'Sentinel-1':
        #     e = self.validate_all(['stack_results', 'main', 'swath', 'firstBurstIndex', 'lastBurstIndex'])
        #     if e:
        #         raise AssertionError(','.join(e))
        #     main_id = s1_prod_id(self.config['main'])
        #     snap_name = f"{main_id}_{self.config['swath']}" + \
        #                 f"_{self.config['firstBurstIndex']}_{self.config['lastBurstIndex']}"  # noqa
        # else:
        #     raise AssertionError(f'Could not build path for task config  kind "{kind}" and source {source} ')
        # return os.path.join(self.config['stack_results'], snap_name) if full else snap_name

    @ensure_task_loaded
    def _compose_friendly_keys(self, roi_name):
        e, main = self.get_valid_key('main')
        prod_fields = {'m_' + k: v for (k, v) in parse_title(main).items()}
        prod_fields = {**prod_fields, **{'m_' + k: v for (k, v) in parse_title(main).items()}}
        prod_fields['m_id'] = s1_prod_id(main)

        prod_fields['s_id'] = ''
        if self.kind == 'cluster':
            e, subordinate = self.get_valid_key('subordinate')
            prod_fields = {**prod_fields, **{'s_' + k: v for (k, v) in parse_title(subordinate).items()}}
            prod_fields['s_id'] = s1_prod_id(subordinate)
        fields = {**self.config, **prod_fields}
        fields['predictor'] = fields['predictor'].split('/')[-1]
        fields['roi'] = roi_name
        return fields

    def format_pattern(self, key, roi_name):
        ms = ['main', 'subordinate'] if self.kind == 'cluster' else ['main']
        e = self.validate_all(ms)
        if e:
            raise AssertionError(','.join(e))

        pattern_fields = self._compose_friendly_keys(roi_name)

        _, pattern = self.get_valid_key(key)
        if e:
            raise AssertionError(','.join(e))

        try:
            return pattern.format(**pattern_fields)
        except KeyError as e:
            raise AssertionError(f"Friendly name pattern is invalid: {e}")

    @ensure_task_loaded
    def get_cos_key(self, roi_name):
        return self.format_pattern('cos_key', roi_name=roi_name)

    @ensure_task_loaded
    def get_ai_friendly_name(self, roi_name):
        return self.format_pattern('friendly_name', roi_name=roi_name)

    @ensure_task_loaded
    def get_predictor_config_file_name(self):
        e, dir = self.get_valid_key('predictor')
        if e:
            raise AssertionError(f"Task predictor config is invalid: {','.join(e)}")
        name = os.path.join(dir, 'config.json')
        return name

    @ensure_task_loaded
    def get_ai_results_path(self, full=False):
        """ return name of directory to save AI results

            format is <snap_name>

            if path==True os.join with  with <task.config.ai_results>
        """
        e = self.validate_all(['ai_results'])
        if e:
            raise AssertionError(','.join(e))
        snap_name = self.get_stack_path(full=False) + '_' + slugify(os.path.basename(self.config['predictor']))
        return os.path.join(self.config['ai_results'], snap_name) if full else snap_name


class TaskTemplate:

    def update_recipe(self, task: Task, recipe: Dict) -> List:
        return []

    def validate_schema(self, schema: Dict, recipe: Dict) -> List:
        return []

    def validate_task(self, task: Task, key: str) -> (bool, List):
        return True, []

    def task_set(self, task: Task, d: Dict):
        pass

    def get_predictor_config_json_template(self) -> (List, Dict):
        return [], {}
    def get_stack_path(self,task:Task,full=False)->str:
        return ""

RECIPE_HEAD_TPL = {
    "version": 1.3,
    "kind": None,
    "class": None,
    "friendly_name": None,
    "description": None,
    "comment": None,
    "DATADIR": None,
    "OUTDIR": None,
    "COS": {
        "type": "AWS",
        "credentials": os.path.join(os.environ.get('HOME', '~'), ".bluemix/cos_credentials.json"),
        "endpoint": "https://s3.eu-central-1.amazonaws.com",
        "bucket": 'optoss-pipeline',
        "ResultKey": None
    },
    "GEOJSON": {
        "schema": "Tangram",
        "type": "Raster",
        "url": "",
        "url_subdomains": [],
    },
    "tag": [],
    "meta": {}
}




class TaskRecipe(object):
    file_pattern = {
        "sigma": [r"Sigma"],
    }
    recipe = RECIPE_HEAD_TPL
    files = []
    _task: Task = None
    kind = 'cluster'

    def __init__(self, task: Task):

        self._task = task
        self.kind = task.config.get('kind')
        # if self.kind not in RECIPE_TPL:
        #     raise AssertionError(f'Task kind "{self.kind}" not supported')
        #
        # self.recipe = RECIPE_TPL[self.kind]['tpl']
        # self.file_pattern = RECIPE_TPL[self.kind]['pattern']
        # path = task.get_stack_path(full=True)
        # if os.path.isdir(path):
        #     # raise AssertionError(f"Task stack directory '{path}' not found")
        #     self.files = os.listdir(path)
        # else:
        #     self.files = []

    def generate_recipe(self, roi):
        errors = []
        task = self._task
        errors += task.validate_all(ignore=['ai_results', 'stack_path'])
        try:
            _defs = get_tsar_defaults(task, 'recipe')
            deep_update(self.recipe, _defs)
        except AssertionError as e:
            errors.append(str(e))
        '''  -------------------- DEFAULT HEAD  -------------------- 
        '''
        try:
            stack_path = task.get_stack_path(full=True)
        except AssertionError as e:
            stack_path = None
            errors.append(str(e))
        try:
            ai_results_path = task.get_ai_results_path(full=True)
        except AssertionError as e:
            ai_results_path = None
            errors.append(str(e))
        fname = task.get_ai_friendly_name(roi_name=roi['name'])
        cos_key = task.get_cos_key(roi_name=roi['name'])
        _tags = task.config['tag'].copy()
        _tags.extend(_aoi_tags(roi, task))
        self.recipe['tag'] = _tags
        self.recipe['description'] = task.config['description']
        self.recipe['friendly_name'] = fname

        self.recipe['DATADIR'] = stack_path
        self.recipe['OUTDIR'] = ai_results_path

        self.recipe['COS']['ResultKey'] = cos_key

        _meta = task._compose_friendly_keys(roi['name'])
        for k in ['m_completionDate', 'm_startDate', 's_completionDate', 's_startDate']:
            """date to str"""
            _meta[k] = "{k:%F %T+00:00}".format(k=_meta[k])
        _meta['cos_key'] = cos_key
        _meta['friendly_name'] = fname
        self.recipe['meta'] = {'task': _meta}

        ''' -------------------- Template fileds  --------------------
        '''
        template = task.config.get('template')
        if template:

            """check task created from template, validate"""
            try:
                tpl = import_module(template)  # type: TaskTemplate
                if not hasattr(tpl, 'update_recipe'):
                    raise ModuleNotFoundError(f"Template is invalid: update_recipe is not defined")
                _errors = tpl.update_recipe(task, self.recipe)
                if _errors:
                    errors.extend(_errors)
            except ModuleNotFoundError as e:
                errors.append(f'Could not import "{template}": {e}')

        return errors

    def get_predictor_config_json_template(self):
        errors = []
        task = self._task
        template = task.config.get('template')
        if template:

            """check task created from template, validate"""
            try:
                tpl = import_module(template)  # type: TaskTemplate
                if not hasattr(tpl, 'get_predictor_config_json_template'):
                    raise ModuleNotFoundError(f"Template is invalid: update_recipe is not defined")
                config = tpl.get_predictor_config_json_template()
            except ModuleNotFoundError as e:
                errors.append(f'Could not import "{template}": {e}')
                return {}

        return errors, config

    def save_recipe(self, file, roi=None, save=False):
        """

        :param file: name of file or  None
        :param roi: if file is None, generate default filename
        :param save: do actual save
        :return:
        """
        j = json.dumps(self.recipe, indent=4) + "\n"
        # print(j)
        # validate syntax
        _n = file if file else self.get_ai_recipe_name(roi['name'])
        log.debug(f"Saving [{save}]: {_n}")
        if save:
            with open(_n, 'w') as _f:
                _f.write(j)
        return j, _n

    def validate_recipe(self, dry_run):
        """
        Draft7 formats: https://json-schema.org/understanding-json-schema/reference/string.html
        for additional format validation refer https://python-jsonschema.readthedocs.io/en/stable/validate/
        for ex. enable uri checks:  sudo -H pip3 install   rfc3987
        :return: list of errors (empty for valid recipe)
        """
        errors = []
        try:

            with jsonsempai.imports():
                from ocli.ai import recipe_head_schema
            task = self._task
            schema = recipe_head_schema.properties.envelope
            template = task.config.get('template')

            _res = True
            if template:
                """check task created from template, validate"""
                try:
                    tpl = import_module(template)  # type: TaskTemplate
                    if not hasattr(tpl, 'validate_schema'):
                        raise ModuleNotFoundError(f"Template is invalid: validate_schema is not defined")
                    errors += tpl.validate_schema(schema, self.recipe)
                except ModuleNotFoundError as e:
                    errors.append(f'Could not import "{template}": {e}')

        except Exception as e:
            # log.exception(e)
            errors.append('Could not perform validation: {e}')

        return errors

    def get_ai_recipe_name(self, roi_name):
        snap_path = self._task.get_stack_path(full=False)
        name = slugify(' '.join(['recipe', str(roi_name), snap_path, self._task.name]))
        return os.path.join(self._task.path, name + '.json')


try:
    from ocli.pro.aoi import AOI


    def _aoi_tags(roi, task: Task):
        try:
            if 'aoi' not in roi:
                raise AssertionError('aoi key missed in roi')
            q = roi.aoi
            if not q:
                raise AssertionError('aoi query missed in roi')
            _path = os.path.join(task.projects_home, task.project)
            aoi = AOI(_path)
            db = aoi.get_db()
            df = db.query(q)
            l = itertools.chain.from_iterable([re.split("[_\s,]", x) for x in df['name']])
            l = [x.lower() for x in l if len(x) > 4]
            # pprint(l)
            return l
        except Exception as e:
            log.warning(f'aoi not resolved for roi')
        return []

except:
    def _aoi_tags(repo=None, roi=None):
        return []

pass_repo = click.make_pass_decorator(Repo)
pass_task = click.make_pass_decorator(Task, ensure=True)
