import importlib
import json
import os
import pickle
import re
from json import JSONDecodeError
from pathlib import Path
from pprint import pprint
from typing import Dict, List

import jsonsempai
import numpy
from jsonschema import Draft7Validator, draft7_format_checker

from ocli.logger import getLogger
from ocli.project import _local_eodata_relative_path

from ocli.cli.output import OCLIException
from ocli.cli.pruduct_s1 import _cache_pairs_file_name
from ocli.cli.state import Task, RECIPE_HEAD_TPL, deep_update
from ocli.sent1 import pairs

log = getLogger()
REQUIRED = "Required by " + __name__

TASK_KIND_CLUSTER = {
    'kind': 'cluster',
    'class': 'S1',
    'predictor': '/optoss/predictor',
    'eodata': None,
    'ai_results': '/optoss/out',
    'stack_results': '/optoss/stack',
    'slave': None,
    'slave_path': None,
    'master': None,
    'master_path': None,
}

RECIPE_CLUSTER_TPL = {
    "version": 1.4,
    "type": "Cluster",
    "kind": "cluster",
    "class": "S1",
    # "zone":None,
    # 'PREDICTOR_DIR': None,
    "processing":"snap",
    "learn_channels": [0, 1, 2, 3, 4, 5, 6, 7],
    "learn_gauss": 3,
    "predict_gauss": 2,
    "products": {
        "sigma": [15, 1.5],
        "sigma_avg": [15, 1.5],
        "coh": [20, 2],
        "coh_avg": [20, 2]
    },
    "channels":{},
    "band_meta": [
        {
            "band": 1,
            "color": "#FF0580",
            "name": "c1"
        },
        {
            "band": 2,
            "color": "#0000cd",
            "name": "c2"
        },
        {
            "band": 3,
            "color": "#0008ff",
            "name": "c3"
        },
        {
            "band": 4,
            "color": "#004cff",
            "name": "c4"
        },
        {
            "band": 5,
            "color": "#0090ff",
            "name": "c5"
        },
        {
            "band": 6,
            "color": "#00d4ff",
            "name": "c6"
        },
        {
            "band": 7,
            "color": "#29ffce",
            "name": "c7"
        },
        {
            "band": 8,
            "color": "#60ff97",
            "name": "c8"
        },
        {
            "band": 9,
            "color": "#97ff60",
            "name": "c9"
        },
        {
            "band": 10,
            "color": "#ceff29",
            "name": "c10"
        },
        {
            "band": 11,
            "color": "#ffe600",
            "name": "c11"
        },
        {
            "band": 12,
            "color": "#ffa700",
            "name": "c12"
        },
        {
            "band": 13,
            "color": "#ff6800",
            "name": "c13"
        },
        {
            "band": 14,
            "color": "#ff2900",
            "name": "c14"
        },
        {
            "band": 15,
            "color": "#cd0000",
            "name": "c15"
        },
        {
            "band": 16,
            "color": "#800000",
            "name": "c16"
        }
    ],
    "num_clusters": 16
}


FILE_PATTERN = {
    "sigma_avg": [r"Sigma"],
    "sigma": [r"Sigma"],
    # "sigmaVH": [r"Sigma0_.+_VH"],
    # "sigmaVV": [r"Sigma0_.+_VV"],
    # "sigmaHH": [r"Sigma0_.+_HH"],
    "coh": [r"coh"],
    "coh_avg": [r"coh"],
    # "cohVH": [r"coh_.+_VH"],

    # "cohVV": [r"coh_.+_VV"],
    # "cohHH": [r"coh_.+_HH"],
}


def __resolve_files(file_pattern:Dict,files:List,recipe:Dict):
    """Check envi files are valid"""
    # check each file has img-hdr pair
    img = [x[:-4] for x in files if x.endswith('.img')]
    hdr = [x[:-4] for x in files if x.endswith('.hdr')]
    diff = set(img).symmetric_difference(hdr)  # set
    if len(diff):
        raise AssertionError(f"Mismatched pairs for IMG-HDR: {diff}")
    # # compose channels
    learn_channels = 0
    for (k, p) in file_pattern.items():
        _c = [x for x in img if any(re.match(regex, x) for regex in p)]
        # log.error(f"cannel {k}={_c}")
        if len(_c):
            learn_channels += 1 if k.endswith('_avg') else len(_c)
            recipe['channels'][k] = _c
    recipe['learn_channels'] = [i for i in range(learn_channels)]



def get_predictor_config_json_template():
    conf = RECIPE_CLUSTER_TPL.copy()
    for k in ['kind','class','type','version']:
        del conf[k]
    return  conf

def validate_schema(schema: Dict, recipe: Dict):
    errors = []

    try:

        with jsonsempai.imports():
            from ocli.aikp.cluster import recipe_schema
        deep_update(schema, recipe_schema.properties.envelope)
        v = Draft7Validator(schema, format_checker=draft7_format_checker)
        _errors = sorted(v.iter_errors(recipe), key=lambda e: e.path)
        errors.extend([
            f'Recipe schema  "{".".join(e.absolute_path)}" invalid : {e.message}' if e.absolute_path else f"Recipe schema  {e.message}"
            for e in _errors])
        try:
            _t = numpy.load(os.path.join(recipe['PREDICTOR_DIR'], 'tnorm.npy'))
            _mlc = max(recipe['learn_channels'])
            if _t.shape[0] < _mlc:
                errors.append(
                    f"Predictor is invalid: learning chanel {_mlc} could not be used with tnorm.npy shape [0..{_t.shape[0] - 1}]")
            with open(os.path.join(recipe['PREDICTOR_DIR'], 'gm.pkl'), 'rb') as _f:
                _t = pickle.load(_f)
                if recipe['num_clusters'] != _t.n_components:
                    errors.append(
                        f"Predictor is invalid: gm.pkl has {_t.n_components} clusters, \
                            recipe has {recipe['num_clusters']} clusters ")

        except Exception as e:
            errors.append(f"Predictor is invalid: Could not validate tnorm.npy: {e}")
    except Exception as e:
        # log.exception(e)
        errors.append(f'Could not perform validation: {e}')
    return errors


def update_recipe(task: Task, recipe):
    errors = []

    deep_update(recipe, RECIPE_CLUSTER_TPL)

    files=[]
    try:
        path     = task.get_stack_path(full=True)
    except AssertionError as e:
        errors.append(str(e))
        return errors
    if os.path.isdir(path):
        # raise AssertionError(f"Task stack directory '{path}' not found")
        files = os.listdir(path)
    __resolve_files(FILE_PATTERN,files,recipe)
    recipe['PREDICTOR_DIR'] = task.config['predictor']
    recipe['COS']['bucket'] = task.config['cos_bucket']
    try:
        with open(task.get_predictor_config_file_name(), 'r') as _f:
            j = json.load(_f)
            deep_update(recipe, j)  # DO NOT INLINE j
            # pprint(self.recipe)
    except JSONDecodeError as e:
        errors.append(f'Predictor config JSON is invalid, reason: {e}')
    except (RuntimeError, AssertionError) as e:
        errors.append(str(e))

    # pprint(recipe)
    return errors


def validate_task(task, key)->(bool,List[str]):
    my_keys = TASK_KIND_CLUSTER.keys()
    errors = []
    processed = False
    if key in my_keys:
        processed = True
        if key not in task.config:
            errors.append(f"key {key}  not found in config")
        else:
            _eodata = task.config['eodata']
            value = task.config[key]
            if key in ['predictor']:
                if not value:
                    errors.append(REQUIRED)
                elif not Path(value).is_dir():
                    errors.append('Not found')
                else:
                    template_path = Path(importlib.util.find_spec(task.config.get('template')).origin).parent
                    for f in ['config.json']:
                        if not (Path(value, f).is_file() or Path(template_path, f).is_file()):
                            errors.append(f'{f} required by {__name__}')
                    for f in ['gm.pkl', 'tnorm.npy']:
                        if not (Path(value, f).is_file()):
                            errors.append(f'{f} required by {__name__}')
            elif key in ['eodata']:
                if not value:
                    errors.append(REQUIRED)
                elif not os.path.isdir(value):
                    errors.append('Not found')
            elif key in ['stack_results', 'ai_results']:
                if not value:
                    errors.append(REQUIRED)
                elif not Path(value).is_dir():
                    errors.append('Not found')
                elif not os.access(value, os.W_OK):
                    errors.append('Not writable')
            elif key in ['master', 'slave'] and not value:
                errors.append(REQUIRED)
            elif key in ['master_path', 'slave_path']:
                if value is None:
                    errors.append(REQUIRED)
                elif _eodata is None:
                    errors.append('Requires eodata')
                elif not Path(_local_eodata_relative_path(_eodata, value)).is_dir():
                    errors.append('Not found')

    return processed, errors


def task_set(task: Task, d: Dict):
    task_config = task.config
    my_keys = TASK_KIND_CLUSTER.keys()
    for k in my_keys:
        if k in d:
            value = d[k]  # type: string
            """set new value"""
            if k in ['master', 'slave']:
                cache_file_name = _cache_pairs_file_name(task)

                try:
                    _df = pairs.load_from_cache(cache_file_name=cache_file_name)
                    _id = value
                    _p = _df.loc[_df['productId'] == value.upper(), ['title', 'productIdentifier']]
                    if _p.size == 0:
                        _p = _df.loc[_df['title'] == value.upper(), ['title', 'productIdentifier']]
                        if _p.size==0:
                            raise ValueError
                        _p = _p.iloc[0]
                    task_config[k] = _p['title']
                    task_config[k + '_path'] = _p['productIdentifier']
                except RuntimeError as e:
                    raise OCLIException(
                        f'Could not load products for project "{task.project}",reason:{e}')
                except (ValueError, KeyError, IndexError) as e:
                    log.exception(e)
                    raise OCLIException(f'key "{k}" could not find product id {d[k]}')
            else:
                # TODO - validate!
                task_config[k] = value
    """remove processed"""
    for k in my_keys:
        d.pop(k, None)

