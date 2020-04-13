import logging

import click

from ocli.ai.COS.s3_boto import COS
from ocli.ai.Envi import Envi
from ocli.ai.assemble import Assemble
from ocli.ai.process import Process
from ocli.ai.recipe import Recipe
from ocli.cli import output, pfac
from ocli.cli.ai_options import option_locate_recipe, argument_zone, fast_option, resolve_recipe
from ocli.cli.state import Repo, Task, pass_task, \
    pass_repo

log = logging.getLogger()

@click.group('snap')
def cli_ai_snap():
    """ AI  processing"""
    pass




# ################################## ASSEBMLE ###############################

# todo generate new envi header
# todo fix envi header for zone so zone-predict results will have right geo-coordinates

@cli_ai_snap.command('assemble')
@option_locate_recipe
@argument_zone
@fast_option
@pass_task
@pass_repo
def ai_assemble(repo: Repo, task: Task, roi_id, recipe_path: str, zone: str, fast):
    """ assemble tensor from co-registered stack  by given recipe

    [zone|full]- assemble  tensor from full image  or from the part defined by "zone" key in recipe JSON

    if no --recipe provided, recipe will be taken based on active task
    """
    _recipe = recipe_path if recipe_path else resolve_recipe(repo, task, roi_id)
    recipe = Recipe(_recipe)
    try:
        cos = COS(recipe)
    except SystemExit:
        log.warning("Could not use COS")
        output.warning("Could not use COS")
        cos = None
    envi = Envi(recipe, cos)
    log.info('Assembling tensor')
    assembler = Assemble(zone, recipe, envi)
    if repo.verbose == 'DEBUG':
        assembler.run()
    else:
        with pfac(log, total=100,
                  desc='Assembling'
                  ) as (_, callback):
            try:
                assembler.run(callback)
            except AssertionError as e:
                raise click.UsageError(f'{e}')
            except Exception as e:
                log.exception(e)
                raise click.UsageError(f'{e}')


# ############################### PROCESS ########################################

# TODO add clip flag so process wil not clip values in sci mode?

@cli_ai_snap.command('process')
@option_locate_recipe
@argument_zone
@click.argument(
    'pred_type', type=click.Choice(['fit', 'fitpredict', 'predict']), default='fit'
)
@pass_task
@pass_repo
def ai_predict(repo: Repo, task: Task, roi_id, recipe_path, zone, pred_type):
    """Run cluster analysis on assembled tensor

    \b
    * zone- process full tensor
    * full- process part of the tensor as defined by "zone" key in a recipe JSON file
    * fit - run cluster analysis processing only to generate predictor files
    * fitpredict - run cluster analysis learning and fit
    * predict - run cluster analysis based on provided in JSON recipe predictor

    if no --recipe provided, recipe will be taken based on active task
    """
    _recipe = recipe_path if recipe_path else resolve_recipe(repo, task, roi_id)
    recipe = Recipe(_recipe)
    with pfac(log, total=100,
              # show_eta=True,
              # item_show_func=lambda x: str(x),
              desc='Processing') as (_, callback):
        Process(zone, pred_type, recipe).run(callback=callback)
    pass

