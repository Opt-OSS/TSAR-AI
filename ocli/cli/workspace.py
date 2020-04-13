import logging
import os
import shutil
from pathlib import Path

import click
import yaml

from ocli.cli import OCLI_EXECUTABLE
from ocli.cli import get_main_rc_name, default_main_config, RC_FILE_NAME, default_project_config
from ocli.cli import output
from ocli.cli.output import info, error, comment, dedent
from ocli.cli.state import pass_repo, Repo, option_yes, option_repo_name, yes_or_confirm

log = logging.getLogger()


def option_name(f) -> click.option:
    """ decorator: adds --project option to kwargs"""
    return click.option('--name', '-n', 'name',
                        required=True,
                        help='project name',
                        )(f)


@click.group('workspace', invoke_without_command=False)
@pass_repo
def cli(ctx):
    pass


# @click.command()
# @click.option('-o', '--output', type=click.Choice(['yaml', 'json']), help="output formatter")
# @click.option('--less', is_flag=True, help="use pagination")
# @pass_repo
# def wp_show(ctx: Repo, output, less):
#     """ show config """
#     # debug(ctx.tolist())
#     table(ctx, format=output, less=less)
#     print('list')


# #################### INFO PROJECT ##################################
@click.command('info')
@click.option('-l', '--list', 'list_projects', is_flag=True, default=False, help='List all projects in workspace')
@click.option('-c', 'config', is_flag=True, default=False, help='workspace configuration')
@option_repo_name
@pass_repo
def wp_info(repo: Repo, list_projects, config):
    """ Display information about current tsar install."""
    # log.error(f"Here we are {list_projects}")
    if list_projects:

        _dirs = repo.list_projects()
        if not len(_dirs):
            comment(f"no projects found in workspace '{repo.projects_home}'")
            comment(" run 'create --name project name' to create new project\n\n")
        for v in _dirs:
            v['active'] = '*' if v['active'] else ''
        if repo.active_project:
            output.comment(f'active project "{repo.active_project}"')
        else:
            output.error(f"No active project selected")
        output.table(_dirs, showindex=True,
                     headers={'active': '', 'name': 'name', 'path': 'path'}
                     )
    else:
        output.comment(f"config file {os.path.join(repo.rc_home, RC_FILE_NAME)}")
        output.comment(f"Active project config file {repo.get_project_rc_name()}")
        output.table(repo.tolist(), headers=['key', 'value'])


# #################### ACTIVATE PROJECT ##################################

# TODO activate task and roi of new active project!!

@cli.command('activate')
@click.argument('name', metavar='<NAME | RECORD>')
@pass_repo
def wp_activate(repo: Repo, name: str):
    """ Activate project"""
    # check project exists in the current workspace
    if name.isnumeric():
        name = repo.list_projects()[int(name)]['name']

    ap = repo.get_project_path(name)
    # todo check rw permissions? btw read-only access is valid for some opers
    if not os.path.isdir(ap):
        raise click.FileError(ap, hint=f" try to run 'create -n {name}' to create project in workspace")
    repo.set_config('active_project', name, autosave=False)
    repo.read_project()
    repo.save_main()  # save only main to prevent project settings to be overridden
    output.success(f'current active project is "{name}" ({ap})')
    pass


# #################### DEACTIVATE PROJECT ##################################
@cli.command('deactivate')
@pass_repo
def wp_deactivate(repo: Repo):
    """ Deactivate project"""
    # check project exists in the current workspace
    if not repo.active_project:
        log.warning('No project activated yet')
        return
    repo.set_config('active_project', None)
    pass


# #################### CREATE PROJECT DIR ##################################
# TODO --activate
@cli.command('create')
@option_yes
@option_name
# @click.option('-a', '--activate', is_flag=True, default=False, help="Activate project after creation")
@pass_repo
def wp_create(repo: Repo, name, yes):
    """ create new or re-recreate existed project  in current workspace"""
    _dir = repo.get_project_path(name)
    if (os.path.isfile(_dir) or os.path.isdir(_dir)) and not yes_or_confirm(yes,
                                                                            f"Directory already exists in '{_dir}'. Override?"):
        return
    try:
        dedent(f"""\
        ## Project Plan ##
        project location: {_dir}
        
        """)
        if yes_or_confirm(yes, 'Proceed?'):
            os.makedirs(_dir, exist_ok=True)
            rc = repo.get_project_rc_name(name)
            with open(rc, 'w') as _f:
                dc = default_project_config()
                yaml.dump(dc, _f)
        pass

    except FileExistsError:
        raise click.FileError(_dir)
    except Exception as e:
        print(e)
    dedent(f"""
        #                                         
        # To activate this project, use       
        #                                         
        #     $ {OCLI_EXECUTABLE} activate {name}         
        #                                         
        # To deactivate an active project, use
        #                                         
        #     $ {OCLI_EXECUTABLE}  deactivate                  
    """)


# #################### DELETE PROJECT DIR ##################################
@cli.command('delete')
@option_yes
@option_name
@pass_repo
def wp_delete(repo: Repo, name, yes):
    """ delete project directory """
    _dir = repo.get_project_path(name)
    if os.path.isdir(_dir):
        try:
            if not yes and not click.confirm("Remove existing project?"):
                return False
            shutil.rmtree(_dir, ignore_errors=False)
            info(f'A project directory {_dir} removed')
            return True
        except OSError as e:
            error(str(e))
            return False
    else:
        error(f"Could not delete: a project directory '{_dir}' does not exists")
        return False


# #################### INIT APP  ##################################
@click.command('init')
@option_yes
@click.option('--projects-home', '-p',
              # required=False,
              help='path to tsar projects home',
              type=click.Path(
                  exists=False,
                  file_okay=False,
                  writable=True,
                  resolve_path=True
              ),
              )
@pass_repo
def wp_init(repo: Repo, projects_home, yes):
    """ init tsar cli """
    rc = get_main_rc_name(repo.rc_home)
    # if os.path.isfile(rc):
    if Path(rc).is_file() and not yes_or_confirm(yes, f'config file "{rc}" exits, override?'):
        return
    dc = default_main_config(repo.rc_home)
    if projects_home:
        dc['projects_home'] = projects_home
    _phome = dc['projects_home']
    if Path(_phome).is_dir() and not yes_or_confirm(yes, f'Directory "{_phome}" exits, Continue?'):
        return
    os.makedirs(_phome, exist_ok=True)

    with open(rc, 'w') as _f:
        yaml.dump(dc, _f)
    output.success(f"configured project home {rc}")


# ###################### SET MAIN CONFIG #####################
@click.command('set')
@click.option('-q', '--quiet', 'quiet', is_flag=True, default=False, help='Do not print config on success')
@click.option('--reset', 'reset', is_flag=True, default=False, help='Reset project to defaults')
@click.argument('args', nargs=-1, metavar="KEY=VALUE")
@option_repo_name
@pass_repo
@click.pass_context
def wp_set(ctx: click.Context, repo: Repo, quiet, reset, args: list):
    """ set workspace config parameters key=value pairs (multiple space-separated pairs are allowed)

        \b
        IMPORTANT: Keys are case-sensitive

        \b
        examples:
            set finder.sourse=CREODIAS finder.startDate="2 moth ago" finder.completionDate="Jan 1, 2019"
            set -p test finder.startDate="Jul 2019 2:00 pm" finder.completionDate="1 Nov 2013 00:00  +0500"
    """

    try:
        if reset:
            ctx.invoke(wp_create, name=repo.active_project, yes=True)
        else:
            repo.parse_cmd_args(args, autosave=True)
    except AssertionError as e:
        click.secho(f'{e}', fg='red')
        raise click.UsageError(e)

    finally:
        if not quiet:
            repo.read_main_config()
            ctx.invoke(wp_info)

# %%
