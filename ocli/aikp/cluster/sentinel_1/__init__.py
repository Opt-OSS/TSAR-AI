from pathlib import Path
from typing import Dict

from ocli.aikp import cluster
from ocli.aikp import get_tsar_defaults
from ocli.aikp.sentinel_1 import SOURCE_CONF, task_set, validate_task
from ocli.aikp import sentinel_1
from ocli.cli import output
from ocli.cli.state import Task, TASK_HEAD
from ocli.logger import getLogger
from ocli.sent1 import s1_prod_id

log = getLogger()

TASK_KIND = {
    **cluster.TASK_KIND_CLUSTER,
    **SOURCE_CONF,
}


def update_recipe(task: Task,recipe:Dict):
    errors=cluster.update_recipe(task,recipe)
    return errors

def validate_schema(schema:Dict,recipe:Dict):
    errors=cluster.validate_schema(schema,recipe)
    return errors


def validate_task(task: Task, key):

    processed, error = cluster.validate_task(task, key)
    if processed:
        return processed, error
    return sentinel_1.validate_task(task, key)


def task_set(task: Task, key_values: Dict):
    """ call parent """
    cluster.task_set(task, key_values)
    sentinel_1.task_set(task, key_values)

def get_predictor_config_json_template():
    return cluster.get_predictor_config_json_template()

def create_task(task: Task):
    """
    create all task params
    :param task:
    :return:
    """
    config = {**TASK_HEAD, **TASK_KIND}
    """get defaults"""
    try:
        _task_items = get_tsar_defaults(task, 'task')
        for key, value in _task_items.items():
            # log.error(f"{key}={value}")
            if key in config:
                config[key] = value
    except AssertionError as e:
        output.warning(str(e))

    config.get('tag', []).append('cluster')
    config['friendly_name'] = "{project}/{name}/{m_completionDate:%Y%m%d}"
    config[
        'cos_key'] = "{project}/{kind}/{name}/{m_completionDate:%Y/%m/%d}/{m_id}_{s_id}_{swath}_{firstBurstIndex}_{lastBurstIndex}_{predictor}"
    config['template'] = __name__
    config['template_version'] = 0.1
    task.config = config

def get_stack_path(task:Task,full=False):
    e = []
    if full:
        _e = cluster.validate_task(task,'stack_results')[1]
        if _e:
            e.append("stack_results: "+",".join(_e))

    for k in ['main', 'subordinate', 'swath', 'firstBurstIndex', 'lastBurstIndex']:
        _e = sentinel_1.validate_task(task, k)[1]
        if _e:
            e.append(f"{k} "+",".join(_e))
    if e:
        raise AssertionError(','.join(e))
    main_id = s1_prod_id(task.config['main'])
    subordinate_id = s1_prod_id(task.config['subordinate'])
    snap_name = f"{main_id}_{subordinate_id}_{task.config['swath']}" + \
                f"_{task.config['firstBurstIndex']}_{task.config['lastBurstIndex']}"  # noqa
    return str(Path(task.config['stack_results'], snap_name).absolute()) if full else snap_name