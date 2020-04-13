import logging
import os
import sys
from pathlib import Path
from time import time
import dateparser
import click
from pygments import highlight, lexers, formatters
from tqdm import tqdm

from ocli.util.date_parse import parse_to_utc_string

CONTEXT_SETTINGS = dict(auto_envvar_prefix='TSAR')
RC_FILE_NAME = '.tsarrc'
PROJECT_DIR = 'TSAR'
TASK_DIR = 'task'

OCLI_EXECUTABLE = os.path.basename(sys.argv[0])


class AliasedGroup(click.Group):

    def get_command(self, ctx, cmd_name):
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv
        matches = [x for x in self.list_commands(ctx)
                   if x.startswith(cmd_name)]
        if not matches:
            return None
        elif len(matches) == 1:
            return click.Group.get_command(self, ctx, matches[0])
        ctx.fail('Too many matches: %s' % ', '.join(sorted(matches)))


class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""

    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize


pos = 1


def pfac(log, **kwargs):
    global pos
    p = Progress(log, pos, **kwargs)
    pos += 1
    return p


def final_pfac():
    print("\n" * pos)


class Progress(object):
    _bar = None
    _lvl = logging.root.level
    _log = None

    _t0 = time()
    _nest = 0

    @property
    def nest(self):
        return self._nest

    @nest.setter
    def nest(self, inc):
        self._nest += inc

    def __init__(self, log, pos, **kwargs):
        ncols = min(click.get_terminal_size()[0], 80)
        self._bar = TqdmUpTo(**kwargs,
                             leave=True,
                             ncols=ncols,
                             position=pos,
                             bar_format='{l_bar}{bar}{r_bar}'
                             )
        self._nest = +1
        if log and log.level != logging.DEBUG:
            self._lvl = log.level
            self._log = log
            log.setLevel(logging.ERROR)

    def callback(self, total, pos, msg):
        if self._bar.total != total:
            self._bar.reset(total)
        self._bar.display("INFO: " + msg, pos=0)
        self._bar.update_to(b=pos, tsize=total)

    def __enter__(self):

        self._t0 = time()
        return self, self.callback

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._bar.set_postfix_str(f'Done in {round(time() - self._t0, 3)} seconds', refresh=True)
        self._bar.close()
        if self._log:
            self._log.setLevel(self._lvl)


def colorful_json(formatted_json):
    return highlight(formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter())


def get_main_rc_name(home):
    return os.path.abspath(os.path.join(home, RC_FILE_NAME))


def default_main_config(home) -> dict:
    return {
        'projects_home': str(Path(home, PROJECT_DIR).absolute()),
        'active_project': "",
    }


def default_project_config():
    return {
        'version': 100,
        'active_roi': "",
        'active_task': "",
        'finder':{
            'source':'CREODIAS',
            'collection': 'Sentinel1',
            'instrument': 'SAR',
            'productType': 'SLC',
            'sensorMode': 'IW',
            'processingLevel': 'LEVEL1',
            'status': 'all',
            'completionDate': parse_to_utc_string('5 minutes ago'),
            'startDate': parse_to_utc_string('6 month ago'),
        }
    }
