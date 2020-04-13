from pathlib import Path
from typing import Dict

import yaml



def get_tsar_defaults(task, key: str) -> Dict:
    try:
        _defconf = Path(task.projects_home, 'defaults.yaml')
        with open(_defconf, 'r') as _f:
            try:
                _conf = yaml.safe_load(_f)  # type: dict
            except Exception as e:
                raise AssertionError(f"{e}")

            # log.error(_conf)
            return _conf.get(key, {})
    except FileNotFoundError:
        return {}
