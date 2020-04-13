import os
from pprint import pprint

import numpy as np
import click
import geopandas as gpd
import logging
import spectral.io.envi as s_envi
import yaml

from ocli.ai.Envi import Envi, header_transform_map_for_zone
from pathlib import Path
from spectral.io.spyfile import FileNotFoundError

from ocli.ai.recipe import Recipe
from ocli.ai.util import Filenames
from ocli.cli import output
from ocli.cli.ai_options import option_locate_recipe, option_list, option_slice, resolve_recipe, argument_zone, \
    option_save, option_tnorm, option_clip, option_hist, option_data_path, option_columns, option_bands, option_gauss, \
    option_tensor_vis, option_stack_vis, COMMON_MATH_TENSOR_HELP, COMMON_MATH_STACK_HELP
from ocli.cli.output import OCLIException
from ocli.cli.state import Repo, Task, pass_task, pass_repo
from ocli.preview import preview_stack, preview_tnsr, preview_cluster, create_stack_rgb, _vis_rgb, \
    create_tensor_rgb, read_tensor
from ocli.util.docstring_parameter import docstring_parameter

log = logging.getLogger()


def get_tensor_df(tnsr_hdr_fname):
    """ get stack files as geopandas GeoDataFrame

    :param dir_path:  path to stack folder
    :return: tuple(full_shape,GeoDataFrame)
    """
    e = s_envi.open(tnsr_hdr_fname)
    full_shape = e.shape[:2]
    bn = e.metadata['band names']
    df = gpd.GeoDataFrame([[b, f'{full_shape[0]}x{full_shape[1]}'] for b in bn],
                          columns=['name', 'resolution'])
    return full_shape, df


def _show_tnsr_list(tnsr_hdr_fname, df=None):
    output.comment(f'tensor HDR: {tnsr_hdr_fname}')
    if df is None:
        try:
            full_shape, df = get_tensor_df(tnsr_hdr_fname)
        except FileNotFoundError as e:
            raise OCLIException(f"{e}")
    output.table(df, showindex=True, headers=['band', 'name', 'resolution'])

    return


def get_stack_df(dir_path):
    """ get stack files as geopandas GeoDataFrame

    :param dir_path:  path to stack folder
    :return: tuple(full_shape,GeoDataFrame)
    """
    df = gpd.GeoDataFrame([[f[:-4], None, None, None, None] for f in os.listdir(dir_path) if f.endswith('.hdr')],
                          columns=['filename', 'geometry', 'resolution', 'interleave', 'path'])

    full_shape = []
    for i, row in df.iterrows():
        _f = os.path.join(dir_path, row['filename'])
        img = s_envi.open(_f + '.hdr')
        df.at[i, 'resolution'] = f"{img.shape[0]}x{img.shape[1]}"
        df.at[i, 'interleave'] = img.interleave
        df.at[i, 'path'] = _f
        # log.info(img)
        full_shape = img.shape
    return full_shape, df


# ####################################### PREVIEW #######################################################

# todo move to other package, make checks for plot libs

# todo draw ROI on stack
# todo draw ROI on tensor
# todo draw ROI on cluster


def _show_plt(_plt, save=None):
    if save:
        _plt.savefig(save)
        _plt.close()
        output.success(f'image saved to file "{Path(save).absolute()}"')
    else:
        _plt.show()


def _sqave_envy_tnsr(tnsr, export, band_names, data_path, georef, slice_range, title):
    envi = Envi({
        'DATADIR': data_path
    }, cos=None)
    _, envi_header = envi.read_header(georef + '.hdr', is_fullpath=True)
    if slice_range[0] != -1:
        envi_header['map info'] = header_transform_map_for_zone(envi_header, zoneY=int(slice_range[0]),
                                                                zoneX=int(slice_range[1]))
    envi_header['data type'] = 1  # BYTE
    envi_header['description'] = '{' + title + '}'
    envi_header['lines'] = tnsr.shape[0]
    envi_header['samples'] = tnsr.shape[1]
    envi_header['bands'] = tnsr.shape[2]
    envi_header['band names'] = "{" + ",".join(band_names) + "}"
    envi.save_dict_to_hdr(export + '.hdr', envi_header)
    tnsr.tofile(export + '.img')


def _save_envi_rgb(r, g, b, export, data_path, georef, slice_range, title):
    envi = Envi({
        'DATADIR': data_path
    }, cos=None)
    _, envi_header = envi.read_header(georef + '.hdr', is_fullpath=True)
    # _data = (np.clip(np.stack((r, g, b), axis=-1) * 255.5, 0, 255)).astype(np.uint8)  # type: np.ndarray
    _data = np.stack((r, g, b) , axis=-1)  # type: np.ndarray

    if slice_range[0] != -1:
        envi_header['map info'] = header_transform_map_for_zone(envi_header, zoneY=int(slice_range[0]),
                                                                zoneX=int(slice_range[1]))
    # _data = (np.stack((r, g, b), axis=-1))  # type: np.ndarray
    envi_header['data type'] = 1  # BYTE
    envi_header['description'] = '{' + title + '}'
    envi_header['lines'] = _data.shape[0]
    envi_header['samples'] = _data.shape[1]
    envi_header['bands'] = _data.shape[2]
    envi_header['band names'] = "{R, G, B}"
    envi.save_dict_to_hdr(export + '.hdr', envi_header)
    _data.tofile(export + '.img')
    # img_as_ubyte(_data).tofile(export + '.img')


def _resolve_tensor_filenames(repo, task, zone, roi_id, data_path, recipe_path, tnorm) -> Filenames:
    if not data_path:
        try:
            _recipe = recipe_path if recipe_path else resolve_recipe(repo, task, roi_id)
            recipe = Recipe(_recipe)
            output.comment(f'Using recipe file "{_recipe}"')
        except (RuntimeError, AssertionError, click.UsageError) as e:
            output.comment(f'Using tensor from ai_results')
            try:
                data_path = task.get_ai_results_path(full=True)
                if not os.path.isdir(data_path):
                    raise AssertionError(f'Directory "{data_path}" is not exists ')
                recipe = {'OUTDIR': data_path}
            except AssertionError as e:
                raise click.UsageError(f'Could not get ai_results: {e}')
    else:
        recipe = {'OUTDIR': data_path}
    if tnorm and 'PREDICTOR_DIR' not in recipe:
        try:
            _filenames = Filenames(zone, recipe)
            with open(_filenames.process_info, 'r') as f:
                _prcinfo = yaml.load(f, Loader=yaml.FullLoader)
            recipe['PREDICTOR_DIR'] = _prcinfo['process']['PREDICTOR_DIR']
        except Exception as e:
            raise OCLIException(f"Could not resolve tnorm file: {e}")
    return Filenames(zone, recipe)


def preview_math_options(f):
    return f


def options_preview_stack(f):
    f = option_save(f)
    f = option_hist(f)
    f = option_columns(f)
    f = option_bands(f)
    f = option_clip(f)
    f = option_slice(f)
    f = option_list(help_text='list available products')(f)
    return f


def options_preview_stack_math(f):
    f = option_save(f)
    f = option_slice(f)
    f = option_data_path(f)
    f = option_list(help_text='list available products')(f)
    f = click.option('-b1', '--band1', type=click.INT, help='b1 band file index', default=None)(f)
    f = click.option('-b2', '--band2', type=click.INT, help='b2 band file index', default=None)(f)
    f = click.option('-b3', '--band3', type=click.INT, help='b3 band file index', default=None)(f)
    f = option_hist(f)
    f = option_stack_vis(f)
    return f


def options_preview_tnsr(f):
    f = argument_zone(f)
    f = option_data_path(f)
    f = option_save(f)
    f = option_list(help_text='list available bands')(f)
    f = option_columns(f)
    f = option_tnorm(f)
    f = option_hist(f)
    f = option_slice(f)
    f = option_bands(f)
    return f


def options_preview_tensor_math(f):
    f = argument_zone(f)
    f = option_save(f)
    f = option_data_path(f)
    f = option_list(help_text='list available bands')(f)
    f = option_slice(f)
    f = click.option('-b1', '--band1', type=click.INT, help='b1 band file index', default=None)(f)
    f = click.option('-b2', '--band2', type=click.INT, help='b2 band file index', default=None)(f)
    f = click.option('-b3', '--band3', type=click.INT, help='b3 band file index', default=None)(f)
    f = option_gauss(f)
    f = option_tnorm(f)
    f = option_hist(f)
    f = option_tensor_vis(f)
    return f


def options_preview_cluster(f):
    f = option_save(f)
    f = option_hist(f)
    f = option_columns(f)
    f = option_bands(f)
    f = option_list(help_text='list available products')(f)
    f = option_slice(f)
    f = argument_zone(f)
    return f


@click.group('preview')
def ai_preview():
    """Preview """
    pass


@ai_preview.command('stack')
@options_preview_stack
@option_locate_recipe
@pass_task
@pass_repo
def ai_preview_stack(repo: Repo, task: Task, roi_id, recipe_path, slice_range,
                     show_list,
                     # rgb,
                     band, columns, clip, hist, save, export, ylog):
    """ Preview assembled tensor band

        ** use --clip <minl> <max> to apply np.log10(np.clip(.., 10**min, 10**max)) to stack values

        \b
        * Windows WSL: follow  https://www.scivision.dev/pyqtmatplotlib-in-windows-subsystem-for-linux/
    """
    try:

        _recipe = recipe_path if recipe_path else resolve_recipe(repo, task, roi_id)
        recipe = Recipe(_recipe)
        _dir = recipe.get('DATADIR')
    except (RuntimeError, AssertionError, click.UsageError) as e:

        output.comment(f"Could not resolve recipe {e}, fall-back to task")
        try:
            _dir = task.get_stack_path('snap_path')
        except AssertionError as e:
            raise click.UsageError(f'Could not get stack path: {e}')
    except Exception as e:
        log.exception("Could not resolve Stack results")
        raise click.UsageError('Could not resolve Stack results')
    output.comment(f"Stack dir: {_dir}\n\n")
    full_shape, df = get_stack_df(_dir)
    if show_list:
        output.table(df[['filename', 'resolution', 'path']], showindex=True,
                     headers=['band', 'name', 'resolution', 'path'])
    else:
        try:
            # if rgb:
            #     if len(rgb) != 3:
            #         raise AssertionError('rgb', '--rgb should contain exactly 3 digits without spaces')
            #     band = (int(rgb[0]), int(rgb[1]), int(rgb[2]))
            if band[0] == -1:
                band = list(range(0, len(df)))
            else:
                band = list(band)
            _ds = df.iloc[band]  # type: gpd.GeoDataFrame
            output.table(_ds, showindex=True)
            _plt = preview_stack(_ds, _dir,
                                 full_shape=full_shape,
                                 slice_region=slice_range,
                                 band=band,
                                 clip=clip,
                                 columns=columns,
                                 hist=hist,
                                 ylog=ylog
                                 )
            _show_plt(_plt, save=save)
        except AssertionError as e:
            log.exception(e)
            raise click.UsageError(str(e))


@ai_preview.command('cluster')
@options_preview_cluster
@option_locate_recipe
@pass_task
@pass_repo
def ai_preview_cluster(repo: Repo, task: Task, roi_id, recipe_path, slice_range, show_list, band, columns,
                       # threshold,
                       zone,
                       hist, ylog,
                       save, export,
                       rgb=False
                       ):
    """ Preview assembled tensor band

        \b
        * Windows WSL: follow  https://www.scivision.dev/pyqtmatplotlib-in-windows-subsystem-for-linux/
    """
    try:
        _recipe = recipe_path if recipe_path else resolve_recipe(repo, task, roi_id)

        recipe = Recipe(_recipe)
        filenames = Filenames(zone, recipe)
        pred8c_img = filenames.pred8c_img
        pred8c_hdr = filenames.pred8c_hdr
        if not os.path.isfile(pred8c_img):
            raise AssertionError(f"IMG file '{pred8c_img}' not fond")
        if not os.path.isfile(pred8c_hdr):
            raise AssertionError(f"HDR file '{pred8c_hdr}' not fond")
        pred8c_hdr = s_envi.open(filenames.pred8c_hdr)
    except (AssertionError) as e:
        raise click.UsageError(f"Could not visualize:  {e}")
    if show_list:
        output.comment(f'Cluster HDR: {filenames.pred8c_hdr}')
        x, y = pred8c_hdr.shape[:2]
        bn = pred8c_hdr.metadata['band names']
        bn = [[b, f'{x}x{y}'] for b in bn]
        output.table(bn, showindex=True, headers=['band', 'name', 'resolution'])
        return
    # if rgb:
    #     if len(rgb) != 3:
    #         raise click.BadOptionUsage('rgb', '--rgb should contain exactly 3 digits without spaces')
    #     band = (int(rgb[0]), int(rgb[1]), int(rgb[2]))
    if band[0] == -1:
        band = list(range(0, pred8c_hdr.shape[2]))

    preview_cluster(filenames.pred8c_hdr, filenames.pred8c_img,
                    band=band,
                    slice_region=slice_range,
                    columns=columns,
                    rgb=rgb
                    )


@ai_preview.command('stack-math')
@options_preview_stack_math
@docstring_parameter(common=COMMON_MATH_STACK_HELP)
@pass_task
@pass_repo
def ai_preview_stack_math(repo: Repo, task: Task, roi_id, recipe_path, slice_range, show_list,
                          band1, band2, band3,
                          vis_mode, data_path, save, export, hist, ylog):
    """ Band math for stack

    {common}
    """
    if not data_path:
        try:

            _recipe = recipe_path if recipe_path else resolve_recipe(repo, task, roi_id)
            recipe = Recipe(_recipe)
            data_path = recipe.get('DATADIR')
            output.comment(f'Using recipe file "{recipe_path}"')
        except (RuntimeError, AssertionError, click.UsageError) as e:
            output.comment(f'Using stack from task stack_results')
            try:
                data_path = task.get_stack_path('snap_path')
                if not os.path.isdir(data_path):
                    raise AssertionError(f'Directory "{data_path}" is not exists ')
            except AssertionError as e:
                raise click.UsageError(f'Could not get stack_results: {e}')

    output.comment(f"Stack dir: {data_path}\n\n")
    full_shape, df = get_stack_df(data_path)
    if show_list:
        output.table(df, showindex=True)
    else:
        title, (r, g, b) = create_stack_rgb(band1, band2, band3,
                                            df=df,
                                            vis_mode=vis_mode,
                                            slice_range=slice_range,
                                            )

        if export:
            georef = df.iloc[band1].path
            _save_envi_rgb(r, g, b, export=export,
                           georef=georef, data_path=data_path, slice_range=slice_range,
                           title=title
                           )
        else:
            _plt = _vis_rgb(r, g, b, title, hist, ylog)
            _show_plt(_plt, save)


@ai_preview.command('tensor')
@options_preview_tnsr
@option_locate_recipe
@pass_task
@pass_repo
def ai_preview_tnsr(repo: Repo, task: Task, roi_id, recipe_path, show_list
                    , zone, slice_range
                    , band,
                    # rgb,
                    columns, hist, tnorm, save, ylog, export, data_path):
    """ Preview assembled tensor band
        \b
        * Windows WSL: follow https://www.scivision.dev/pyqtmatplotlib-in-windows-subsystem-for-linux/ instructions
    """

    filenames = _resolve_tensor_filenames(
        repo, task,
        zone=zone,
        roi_id=roi_id,
        data_path=data_path,
        recipe_path=recipe_path,
        tnorm=tnorm
    )

    output.comment(f"Data dir: {data_path}")
    full_shape, df = get_tensor_df(filenames.tnsr_hdr)
    if show_list:
        _show_tnsr_list(filenames.tnsr_hdr, df=df)
        return
    try:
        e = s_envi.open(filenames.tnsr_hdr)
    except FileNotFoundError as e:
        raise click.UsageError(e)

    tnsr_name = filenames.tnsr
    tnsr_hdr = filenames.tnsr_hdr
    log.info(tnsr_name)
    log.info(zone)
    if band[0] == -1:
        band = list(range(0, e.shape[2]))
    else:
        band = list(band)
    if tnorm:
        tnorm = filenames.tnorm
    tnsr = read_tensor(band, df,
                       slice_range=slice_range,
                       filenames=filenames,
                       tnorm=tnorm,
                       gauss=None,
                       split=False
                       )
    band_names = df.iloc[band]['name'].tolist()
    if export:
        georef = filenames.tnsr_hdr[:-4]
        _sqave_envy_tnsr(tnsr, export=export,
                         band_names=band_names,
                         georef=georef,
                         data_path=data_path,
                         slice_range=slice_range,
                         title="Normalized" if tnorm else ""
                         )
    else:
        _plt = preview_tnsr(tnsr,
                            band=band,
                            band_names=band_names,
                            hist=hist,
                            slice_range=slice_range,
                            columns=columns,
                            title="Normalized" if tnorm else "",
                            ylog=ylog
                            )
        _show_plt(_plt, save=save)


@ai_preview.command('tensor-math')
@options_preview_tensor_math
@docstring_parameter(COMMON_MATH_TENSOR_HELP)
@option_locate_recipe
@pass_task
@pass_repo
def ai_preview_tensor_math(repo: Repo, task: Task, roi_id, recipe_path, slice_range, show_list,
                           band1, band2, band3,
                           vis_mode, data_path,
                           save, tnorm,
                           zone, gauss, hist, ylog, export):
    """ Bands math for tansor

    {}
    """
    filenames = _resolve_tensor_filenames(
        repo, task,
        zone=zone,
        roi_id=roi_id,
        data_path=data_path,
        recipe_path=recipe_path,
        tnorm=tnorm
    )
    output.comment(f"Data dir: {data_path}")
    full_shape, df = get_tensor_df(filenames.tnsr_hdr)
    if show_list:
        _show_tnsr_list(filenames.tnsr_hdr, df=df)
        return

    try:
        title, (r, g, b) = create_tensor_rgb(band1, band2, band3,
                                             df=df,
                                             filenames=filenames,
                                             tnorm=tnorm,
                                             gauss=gauss,
                                             vis_mode=vis_mode,
                                             slice_range=slice_range,
                                             )
        if export:
            georef = filenames.tnsr_hdr[:-4]
            _save_envi_rgb(r, g, b, export=export,
                           georef=georef, data_path=data_path, slice_range=slice_range,
                           title=title
                           )
        else:
            _plt = _vis_rgb(r, g, b, title, hist, ylog)
            _show_plt(_plt, save)
    except Exception as e:
        log.exception(e)
        raise OCLIException(f"{e}")
