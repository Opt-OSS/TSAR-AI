import os
from slugify import slugify as pyslug


def _local_eodata_relative_path(eodata, path):
    """ return path relative to eodata """
    _p = path[1:] if path.startswith('/') else path
    return os.path.join(eodata, _p)

def slugify(value):
    """ wrapper for https://github.com/un33k/python-slugify
    we could chenge behavior
    """
    return pyslug(value,separator="_")
