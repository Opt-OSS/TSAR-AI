import json
import logging
import os
import shutil
from json import JSONDecodeError
from pathlib import Path
from pprint import pprint

import click
import gdal
from tqdm import tqdm

from ocli.ai.COS.s3_boto import COS
from ocli.ai.Envi import Envi
from ocli.ai.gdal_wrap3 import GDALWrap3
from ocli.ai.recipe import Recipe
from ocli.ai.util import Filenames
from ocli.ai.visualize.visualize_cluster import Visualize
from ocli.cli import output, pfac, colorful_json
from ocli.cli.ai_options import option_locate_recipe, argument_zone, resolve_recipe, cos_key_option
from ocli.cli.ai_preview import ai_preview
from ocli.cli.ai_snap import cli_ai_snap
from ocli.cli.output import OCLIException
from ocli.cli.state import Repo, Task, pass_task, \
    pass_repo, option_less

log = logging.getLogger()


@click.group('ai')
def cli_ai():
    pass


cli_ai.add_command(cli_ai_snap, 'snap')

try:
    from ocli.tilewise_processing.cli_ai import cli_ai_sarpy

    cli_ai.add_command(cli_ai_sarpy, 'sarpy')
except Exception as e:
    log.exception(e)
    pass


# ####################################### VISUALIZE #######################################################
@cli_ai.command('visualize')
@option_locate_recipe
@argument_zone
@pass_task
@pass_repo
def ai_visualize(repo: Repo, task: Task, roi_id, recipe_path, zone):
    """visualize AI processing results"""
    try:
        _recipe = recipe_path if recipe_path else resolve_recipe(repo, task, roi_id)
        recipe = Recipe(_recipe)
        try:
            cos = COS(recipe)
        except SystemExit:
            log.warning("Could not use COS")
            output.warning("Could not use COS")
            cos = None
        envi = Envi(recipe, cos)
        Visualize(zone, recipe, envi).run()
    except AssertionError as e:
        raise click.BadArgumentUsage(str(e))


# ####################################### Make COG #######################################################

# TOD add --no-cache to add some prefix to Result-keys to avoid caching

@cli_ai.command('makecog')
@click.option('-k', '--kind', required=True, default='auto', type=click.Choice(['Rvi', 'Cluster', 'Image', 'auto']),
              help="override Geojson type")
@click.option('--friendly-name', required=False, type=click.STRING,
              help="override friendly name")
@click.option('-s', '--source', required=False, type=click.Path(
    exists=True,
    file_okay=True, dir_okay=False,
    readable=True,

),
              help="source:  ENVI img or GeoTiff file")
@cos_key_option
@click.option('--json-only', is_flag=True, default=False, help="skip GeoTiff generation, generate geojson only")
@click.option( '--quiet', is_flag=True, default=False, help="Do not show progress bar")
@click.option( '--print','print_res', is_flag=True, default=False, help="print resulting GeoJSON")
@click.option('--no-color', 'no_color', is_flag=True, default=False, help='Disable terminal colors')
@click.option('--overview-r', 'overview_resampleAlg', is_flag=False, type=click.Choice(
    ['nearest', 'average', 'gauss', 'cubic', 'cubicspline', 'lanczos', 'average_mp', 'average_magphase', 'mode']
), default='gauss', help='Overview resampling method', show_default=True)
@click.option('--warp-r', 'warp_resampleAlg', is_flag=False, type=click.Choice(
    ['near', 'bilinear', 'cubic', 'cubicspline', 'lanczos', 'average', 'mode', 'max', 'min', 'med', 'Q1', 'Q3']
), default='cubic', help='Warp resampling method', show_default=True)
@option_less
@option_locate_recipe
@argument_zone
@pass_task
@pass_repo
def ai_makecog(repo: Repo, task: Task, roi_id, recipe_path, json_only, quiet, no_color, less, zone,
               kind, source, cos_key, friendly_name,print_res,
               warp_resampleAlg, overview_resampleAlg):
    """    Make COG TIFF from visualized results

    \b
    * to make GeoTIFF from custom ENVI file use --source option with filename of ENVI file (without extension)
    * to override recipe kind, use --kind option,
        example:  making image from 'ai preview --export path/to/envi' file use
         makecog zone --kind Image --source path/to/envi
    * to avoid overriding recipe main results use --cos-key and --friendly-name option
        if --friendly-name starts with '+' value will be used as suffix for friendly_name in GeoJSON
        if --cos-key       starts with '+' value will be used as suffix for COS.ResultKey in GeoJSON

    """
    driver = 'MAKECOG'
    if source:
        try:
            # Only valid ENVI or GeoTiff files are alloed
            str = gdal.Info(source, format='json')
            driver = str['driverShortName']
            if driver not in ['ENVI', 'GTiff']:
                raise OCLIException(f"Unsupported source file type: {str['driverShortName']} ({str['driverLongName']})")
        except Exception as e:
            raise OCLIException(f"Option --source: {e}")
    _recipe = recipe_path if recipe_path else resolve_recipe(repo, task, roi_id)
    recipe = Recipe(_recipe)
    kind = kind if kind != 'auto' else recipe.get('type')
    if kind in ['Rvi', 'Image']:
        recipe['type'] = 'Image'
    # log.error(recipe['COS']['ResultKey'])
    if cos_key:
        if cos_key.startswith('+'):
            recipe['COS']['ResultKey'] += cos_key[1:]
        else:
            recipe['COS']['ResultKey'] = cos_key
    if friendly_name:
        if friendly_name.startswith('+'):
            recipe['friendly_name'] += friendly_name[1:]
        else:
            recipe['friendly_name'] = friendly_name
    # log.error(recipe['COS']['ResultKey'])
    # log.error(recipe['friendly_name'])
    filenames = Filenames(zone, recipe)
    if source:
        input_file = source
    else:
        input_file = filenames.pred8c_img
    out_file = filenames.out_tiff
    cog_file = filenames.out_cog_tiff
    check_file = cog_file if json_only else input_file
    if not Path(check_file).is_file():
        raise OCLIException(f'file not found: {check_file}')
    os.makedirs(Path(cog_file).parent,exist_ok=True)
    w = GDALWrap3(recipe, input_file, out_file, cog_file)
    try:
        if not json_only:
            if driver == 'GTiff':
                try:

                    shutil.copy(input_file, cog_file)
                    output.success(f"file {input_file} copied to {cog_file}")
                except Exception as e:
                    raise OCLIException(f'Could not copy  "{input_file}" to "{cog_file}" : {e}')

            else:
                if not quiet:
                    with pfac(log, total=100,
                              desc='Assembling'
                              ) as (_, callback):
                        def cb(pct, msg, user_data):
                            _ud = user_data[0]
                            # self.log.debug(f"translaing: {round(pct * 100, 2)}% -{msg}- {user_data} {pct}")
                            # reset counter, this t\callback called from multiple prcesses
                            if pct < 0.01:
                                user_data[0] = 0
                            if user_data[0] == 0 or pct - _ud > 0.10:
                                log.debug(f"Local translating : {round(pct * 100, 2)}%")
                                user_data[0] = pct
                            callback(100, pct, 'translating')

                        w.make_cog(cb, warp_resampleAlg, overview_resampleAlg)
                else:
                    w.make_cog(None, warp_resampleAlg, overview_resampleAlg)

        _json = w.make_geo_json()
        _json = _json if no_color else colorful_json(_json)
        if print_res:
            click.echo("\n\n\n")
            if less:
                click.echo_via_pager(_json)
            else:
                click.echo(_json)

    except (RuntimeError, OCLIException) as e:
        raise click.UsageError(output.error_style(f"COG-tiff generation failed,reason: {e}"))
    pass


def hook(t):
    def inner(bytes_amount):
        t.update(bytes_amount)

    return inner


# ####################################### upload COG #######################################################
@cli_ai.command('upload')
@click.option('--dry-run', is_flag=True, default=False, help="Do not do upload")
@cos_key_option
@option_locate_recipe
@pass_task
@pass_repo
def ai_upload(repo: Repo, task: Task, roi_id, recipe_path, cos_key, dry_run):
    """Upload COG TIFF to cloud storage"""
    _recipe = recipe_path if recipe_path else resolve_recipe(repo, task, roi_id)
    recipe = Recipe(_recipe)

    filenames = Filenames('zone', recipe)
    cog_file = filenames.out_cog_tiff

    doc_json = Path(filenames.out_cog_tiff + '.geojson')

    if not doc_json.is_file():
        raise OCLIException(f'Could not find "{doc_json.absolute()}"')
    _json = open(doc_json, 'r').read()
    try:
        doc = json.loads(_json)
    except JSONDecodeError:
        raise OCLIException(f'Could not parse json "{doc_json.absolute()}"')

    if not cos_key and "ResultKey" in doc['properties']:
        cos_key = doc['properties'].get('ResultKey')
    if not cos_key:
        raise click.UsageError("No COS key (upload file name)")

    if not cos_key.endswith('.tiff'):
        cos_key += '.tiff'
    log.info(f"About to upload {cog_file} as {cos_key} to bucket {recipe['COS'].get('bucket')} ")
    try:
        cos = COS(recipe)
    except SystemExit:
        raise click.UsageError(f'Invalid recipe: COS credentials in "{_recipe}" are required for upload')
    try:
        if dry_run:

            output.comment(f'Uploading "{cog_file}" as "{cos_key}" into bucket "{cos.bucket}"')
            output.comment(f'Uploading "{cog_file}.geojson" as "{cos_key}.geojson" into bucket "{cos.bucket}"')
        else:
            filesize = os.stat(cog_file).st_size
            with tqdm(total=filesize, unit='B', unit_scale=True, desc=cos_key) as t:
                cos.upload_to_cos(cog_file, cos_key, hook(t))
            if os.path.isfile(cog_file + '.geojson'):
                filesize = os.stat(cog_file + '.geojson').st_size
                with tqdm(total=filesize, unit='B', unit_scale=True, desc=cos_key + '.geojson') as t:
                    cos.upload_to_cos(cog_file + '.geojson', cos_key + '.geojson', hook(t))
    except SystemExit as e:
        raise click.UsageError(e)

cli_ai.add_command(ai_preview, 'preview')
