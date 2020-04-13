from typing import Dict

from ocli.cli.state import Task

SOURCE_CONF = {
    'source': 'Sentinel-1',
    'instrument': 'SAR',
    'productType': 'SLC',
    'swath': None,
    'firstBurstIndex': None,
    'lastBurstIndex': None,
}

REQUIRED="Required by "+__name__
def validate_task(task: Task, key):
    error = []
    processed = False
    my_keys = SOURCE_CONF.keys()
    if key in my_keys:
        processed = True
        if key not in task.config:
            error.append(f"key {key}  not found in config")
        else:
            value = task.config[key]
            if key == 'source' and value != 'Sentinel-1':
                error.append(f'Only Sentinel-1 supported by  {__name__}')
            if key == 'instrument' and value != 'SAR':
                error.append(f'Only SAR supported by  {__name__}')
            if key == 'productType' and value != 'SLC':
                error.append(f'Only SLC supported by  {__name__}')
            if key == 'swath':
                if not value:
                    error.append(REQUIRED)
                elif value not in ['IW1', 'IW2', 'IW3']:
                    error.append("Should be one of ['IW1','IW2','IW3']  ")
            elif key in ['firstBurstIndex', 'lastBurstIndex']:
                if not value:
                    error.append(REQUIRED)
                elif not 0 < int(value) < 11:
                    error.append(f'Should be in range [1-10], got {int(value)}')
                else:
                    _f = int(task.config['firstBurstIndex'])
                    _l = int(task.config['lastBurstIndex'])
                    if _f > _l:
                        error.append(f'firstBurstIndex Should be less or equal to lastBurstIndex ')
    return processed, error


def task_set(task: Task, key_values: Dict):
    task_config = task.config
    """"self keys"""
    my_keys = SOURCE_CONF.keys()
    for k in my_keys:
        if k in key_values:
            value = key_values[k]  # type: string
            if k == 'swath':
                value = value.upper()
            """set new value"""
            # TODO - validate!
            task_config[k] = value
    """remove processed"""
    for k in my_keys:
        key_values.pop(k, None)
