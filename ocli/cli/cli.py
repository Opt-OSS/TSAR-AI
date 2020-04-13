""" FutureWarnings catch  uncomment below and use  python -W error ocli.cli/cli.py
# import traceback
# import warnings
# import sys
#
# def warn_with_traceback(message, category, filename, lineno, file=None, line=None):
#
#     log = file if hasattr(file,'write') else sys.stderr
#     traceback.print_stack(file=log)
#     log.write(warnings.formatwarning(message, category, filename, lineno, line))
#
# warnings.showwarning = warn_with_traceback
# # warnings.simplefilter("always")
# warnings.simplefilter("ignore", DeprecationWarning)
#
"""
import os

from ocli.logger import init as init_log

init_log(os.environ.get('TSAR_VERBOSE', 'WARNING'))  # to catch import errors etc
# if 'PROJ_LIB' in os.environ:
# check PROJ exists, otherwise core dumped on Ubuntu 19
# logging.info('PROJ_LIB environment is  set, THATS CUASE CORE-DUMP IN CONDA, UNSETING for CLI')
# del os.environ['PROJ_LIB']

import click
from prompt_toolkit.key_binding.bindings.auto_suggest import load_auto_suggest_bindings
from prompt_toolkit.shortcuts import CompleteStyle

from click_repl import repl
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import FileHistory

from ocli.cli.output import warning
from ocli.cli import roi, bucket, task, AliasedGroup, ai, output
from ocli.cli import pruduct_s1, workspace
from ocli.cli import CONTEXT_SETTINGS
from ocli.cli.state import pass_repo, Repo


@click.group(context_settings=CONTEXT_SETTINGS, cls=AliasedGroup)
@click.option('--verbose', '-v', type=click.Choice(['DEBUG', 'WARNING', 'INFO', 'ERROR', 'FATAL']), default='ERROR',
              help="Verbose level")
@click.option('--config', nargs=1, multiple=True, metavar='KEY VALUE', help='Overrides a config key/value pair.')
@click.option('--home',
              type=click.Path(exists=False, file_okay=False, resolve_path=True),
              default=os.path.join(os.environ.get('HOME')),
              show_default=True,
              show_envvar=True,
              help='Changes the folder to operate on.')
@click.pass_context
def cli(ctx: click.Context, verbose, home, config):
    """ CLI for AI pip line"""
    init_log(verbose)
    home_dir = home if home else os.path.expanduser(os.environ.get('HOME'))
    if ctx.obj is None:
        # IMPORTANT - CACHE context for REPL
        ctx.obj = Repo(os.path.abspath(home_dir))
    repo = ctx.obj  # type: Repo
    if ctx.invoked_subcommand != workspace.wp_init.name:
        repo.read_main_config(read_all=True)
    repo.verbose = verbose
    if config:
        try:
            repo.parse_cmd_args(config, autosave=False)
        except AssertionError as e:
            click.secho(f'{e}', fg='red')
            raise click.UsageError(e)


def prompt_message_callback(repo):
    def ppc():
        if repo.active_project:
            return f'tsar:{repo.active_project}:{repo.active_task}:{repo.active_roi}>'
        return u'tsar>'

    return ppc


text_type = str

kb = load_auto_suggest_bindings()
try:
    _b = kb.get_bindings_for_keys(('escape', 'f'))[0]
    kb.add('c-right')(_b)
    _e = kb.get_bindings_for_keys(('right',))[0]
    kb.add('end')(_e)
except IndexError:
    warning('Internal: could not redeclare key bindings')
except StopIteration:
    pass

prompt_kwargs = {
    'complete_style': CompleteStyle.READLINE_LIKE,
    'complete_while_typing': False,
    'key_bindings': kb,
    'history': FileHistory(os.path.join(os.getenv('HOME', '~'), '.ocli-history')),
    'auto_suggest': AutoSuggestFromHistory(),
}


def bottom_toolbar():
    r = click.get_current_context().obj  # type: Repo
    try:
        aa = r.get_config('active_project')
    except Exception as e:
        aa = "  " + str(e)
    return "Repo:" + str(r.gt()) + aa


@cli.command()
def clear():
    click.clear()


@click.command('exit')
def repl_exit():
    """Exit interactive console"""
    click.echo("EXIT")
    click.get_current_context().exit()


# TODO activate project in repl does not activates task and roi in new active proj

@cli.command('repl')
@click.option('--full-screen', 'fullscreen', is_flag=True, default=False, help='bottom toolbar')
@pass_repo
@click.pass_context
def ocli_repl(ctx, repo: Repo, fullscreen):
    """ start interactive console"""
    repo.is_repl = True
    output.comment("""        
        use  ? command to OCLI help
        use :? command to show console help
        use <TAB> to auto-complete commands
        use :q or Ctrl-d to exit       
        """)
    if fullscreen:
        prompt_kwargs['bottom_toolbar'] = bottom_toolbar
    prompt_kwargs['message'] = prompt_message_callback(repo)
    repl(ctx, prompt_kwargs=prompt_kwargs)


def loop():
    # mount some to 1-st level
    cli.add_command(workspace.wp_init)
    cli.add_command(workspace.wp_create)
    cli.add_command(workspace.wp_set)
    cli.add_command(workspace.wp_delete)
    cli.add_command(workspace.wp_activate)
    cli.add_command(workspace.wp_deactivate)
    cli.add_command(workspace.wp_info)
    # mount group actions
    cli.add_command(workspace.cli)
    cli.add_command(pruduct_s1.pairs_cli)
    cli.add_command(bucket.bucket_cli)
    cli.add_command(roi.roi_cli)
    cli.add_command(task.cli_task)
    cli.add_command(ai.cli_ai)
    try:
        from ocli.pro import cli as pro_cli
        pro_cli.mount_commands(cli)
    except Exception as e:
        pass
        # raise e
    # try:
    #     from ocli.sarpy.cli import cli as sarpy_cli
    #     sarpy_cli.mount_commands(cli)
    # except Exception as e:
    #     pass
    #     # raise e
    cli()


if __name__ == '__main__':
    loop()
