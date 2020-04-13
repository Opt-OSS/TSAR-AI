import json
import logging
import textwrap
import warnings
from functools import wraps
from pprint import pprint

import click
import yaml
from click.exceptions import ClickException
from tabulate import tabulate

log = logging.getLogger()


def new_func():
    pass


def deprecated(arg):
    """Define a deprecation decorator.
    An optional string should refer to the new API to be used instead.

    Example:
      @deprecated
      def old_func(): ...

      @deprecated('new_func')
      def old_func(): ..."""

    subst = arg if isinstance(arg, str) else None

    def decorator(func):
        def wrapper(*args, **kwargs):
            msg = "Call to deprecated function \"{}\"."
            if subst:
                msg += "\n Use \"{}\" instead."
            warnings.warn(msg.format(func.__name__, subst),
                          category=DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        return wraps(func)(wrapper)

    if not subst:
        return decorator(arg)
    else:
        return decorator


class OCLIException(ClickException):
    """Styled :py:function:`ClickException`"""

    def show(self, file=None):
        click.echo(error(self.format_message()))


def dedent(msg: str):
    click.echo(textwrap.dedent(msg))


def error(msg: str):
    click.echo(error_style(msg))


def success(msg: str):
    click.secho('✓ Success: ' + msg, fg='green')


def error_style(msg: str):
    return click.style(f'✗ ERROR: {msg}', fg='red')


def secho(msg, **kwargs):
    click.secho(msg, **kwargs)


def hint(msg: str):
    """ print formatted help,"""
    click.secho(msg, fg='cyan')


def UsageError(msg: str):
    return click.UsageError(error_style(msg))


def comment(msg: str):
    [click.secho("→ " + x, fg='yellow') for x in msg.splitlines()]


def debug(msg):
    pprint(msg)


def warning(msg):
    click.secho("⚠️ WARNING: " + msg, fg='yellow')


def echo(msg, **kwargs):
    click.secho(msg, **kwargs)


def info(data):
    click.echo(data)


def table(arry, less=False, headers=(), out_format=None, fg=None, tablefmt="simple", **kwargs):
    # if not isinstance(arry, list):
    #     fl = arry.tolist()
    # else:
    fl = arry
    if out_format:
        if out_format == 'yaml':
            click.echo(yaml.dump(fl))
        elif out_format == 'json':
            click.echo(json.dumps(fl, indent=4))
    if not len(fl):
        click.BaseCommand('No data')
    else:
        try:
            if less:
                click.echo_via_pager(tabulate(fl, headers=headers, tablefmt=tablefmt, **kwargs), color=fg)
            else:
                click.secho(tabulate(fl, headers=headers, tablefmt=tablefmt, **kwargs), fg=fg)
        except Exception as e:
            log.error(f'Could not output type {type(fl)} as table: {e}')
            log.exception(e)
