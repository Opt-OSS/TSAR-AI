import logging
import os

import click

from ocli.cli.roi import resolve_roi
from ocli.cli.state import MutuallyExclusiveOption, project_options_callback, Repo, Task, TaskRecipe

log = logging.getLogger()


def fast_option(f):
    return click.option(
        '--fast', 'fast', is_flag=True, default=False,
        help='Use optimised EE libraries if possible'
    )(f)


def option_slice(f):
    return click.option(
        '-s', '--slice', 'slice_range', type=click.INT, nargs=4, default=(-1, -1, -1, -1),
        help='limit preview to bounding box: minY minX maxY maxX'
    )(f)


def cos_key_option(f):
    return click.option('--cos-key', help="COS key", default=None)(f)


def option_bands(f):
    return click.option('-b', '--band', type=click.INT, default=[-1], multiple=True,
                        show_default=True,
                        # cls=MutuallyExclusiveOption, mutually_exclusive=["rgb"],
                        help='Define band to preview')(f)


def option_columns(f):
    return click.option('-c', '--columns', type=click.INT, default=2,
                        # cls=MutuallyExclusiveOption, mutually_exclusive=["rgb"],
                        help='Number of columns in image')(f)


def option_list(help_text):
    def lo(f):
        return click.option('-l', '--list', 'show_list', is_flag=True, default=False,
                            help=help_text)(f)

    return lo


def option_locate_recipe(f):
    f = click.option('-r', '--roi', 'roi_id', required=False,
                     help=' ROI name or ID in question',
                     cls=MutuallyExclusiveOption, mutually_exclusive=['recipe']
                     )(f)
    f = click.option('-p', 'project', help='Project name.', default=None,
                     callback=project_options_callback,
                     expose_value=False,
                     is_eager=True,  # ensure project parsed first
                     cls=MutuallyExclusiveOption, mutually_exclusive=["path", 'recipe']
                     )(f)
    f = click.option('-n', 'name', help='Task name.', default=None,
                     expose_value=False,
                     callback=project_options_callback,
                     cls=MutuallyExclusiveOption, mutually_exclusive=["path", 'recipe'])(f)
    f = click.option('--path', help='Path to task directory.', default=None,
                     expose_value=False,
                     callback=project_options_callback,
                     cls=MutuallyExclusiveOption, mutually_exclusive=["name", "project", 'recipe'])(f)
    f = click.option(
        '--recipe', 'recipe_path',
        cls=MutuallyExclusiveOption, mutually_exclusive=["name", "project", 'path'],
        help="recipe JSON file",
        type=click.Path(
            file_okay=True,
            readable=True,
            exists=True
        )
    )(f)
    return f


def argument_zone(f):
    return click.argument('zone', type=click.Choice(['zone', 'full']))(f)


def resolve_recipe(repo: Repo, task: Task, roi_id):
    try:
        task.resolve()
        _id, _roi = resolve_roi(roi_id, repo)
        r = TaskRecipe(task=task)
        f = r.get_ai_recipe_name(_roi['name'])
        if not os.path.isfile(f):
            raise RuntimeError(f"task recipe file {f} not found")
        log.info(f'recipe resolved via task: {f}')
        return f
    except AssertionError as e:
        raise click.UsageError(f'Task is invalid, reason: {e}')
    except RuntimeError as e:
        raise click.UsageError(str(e))


def option_save(f):
    f = click.option('--save', type=click.Path(file_okay=True, dir_okay=False, writable=True), required=False,
                     help="save rendered image to file name ( output format is inferred from the extension of filename [jpeg,png, etc])",
                     cls=MutuallyExclusiveOption, mutually_exclusive=['export']
                     )(f)
    f = click.option('--export', type=click.Path(file_okay=True, dir_okay=False, writable=True), required=False,
                     help="save data as ENVI file (filename without extension)",
                     cls=MutuallyExclusiveOption, mutually_exclusive=['save', 'hist', 'ylog']
                     )(f)
    return f


def option_tnorm(f):
    f = click.option('--tnorm', is_flag=True, default=False, help="Apply predictor's tnorm",
                     # cls=MutuallyExclusiveOption,
                     # mutually_exclusive=['data_path']
                     )(f)
    return f


def option_gauss(f):
    return click.option('--gauss', type=click.FLOAT, required=False, default=None,
                        help="Apply gauss filtration with given sigma",
                        )(f)


def option_clip(f):
    f = click.option('--clip', help='apply log10(numpy.clip(min, max) ', nargs=2, type=click.FLOAT, default=None,
                     required=False)(f)
    return f


def option_hist(f):
    f = click.option('--ylog', help="logarithmic Y-axis scale (for histograms only)",
                     default=False,
                     is_flag=True,
                     show_default=False,
                     )(f)
    f = click.option('--hist', help="Plot histograms with given number of bins", type=click.INT, required=False,
                     default=None)(f)
    return f


def option_data_path(f):
    return click.option('-d', '--data-path', 'data_path', help='Path to data directory.',
                        default=None,
                        type=click.Path(
                            exists=True,
                            file_okay=False,
                            dir_okay=True,
                            readable=True,
                        ),
                        required=False,
                        cls=MutuallyExclusiveOption,
                        mutually_exclusive=["name", "project", 'recipe', 'path'])(f)


def option_tensor_vis(f):
    f = click.option('--vis', 'vis_mode', help='Visualisation calculations',
                     type=click.Choice([
                         'raw',
                         'sar',
                         'composite',
                         'simple',
                         'rgb-ratio',
                         'rgb-diff',
                         'false-color',
                         'false-color-enhanced',
                     ]),
                     default='sar',
                     is_flag=False,
                     )(f)
    return f


def option_stack_vis(f):
    f = click.option('--vis', 'vis_mode', help='Visualisation calculations',
                     type=click.Choice([
                         'raw',
                         'sar',
                         'composite',
                         'composite-u',
                         'false-color',
                         'false-color-enhanced',
                     ]),
                     default='sar',
                     is_flag=False,
                     )(f)
    return f


COMMON_MATH_TENSOR_HELP = """
    use --list option to get channels index

    \b
    --vis:                bands         R           G           B                 comment
           ---------------------------  ---------  ------------ ----------  --------------------
           raw              3           b1          b2          b3          usable for histograms (--hist)
           composite        3           b1          b2          b3          b1-coh, b2-VV, b3-VH (same as raw)
           sar              3           b1          (b2+b3)/2   b2-b3       b1-coh, b2-main, b3-subordinate (use the same polarization)
           simple           2           b1          b2          b1/b2       (VV, VH,  VV/VH)
           rgb-ratio        2           b1          2*b2        (b1/b2)/100   (VV, 2VH, VV/VH/100)
           rgb-diff         2           b1          b2          b1-b2       (VH, VV,  VH-VV)
           false-color      2                                               b1=VH b2=VV
           false-color-enhanced  - same params  as for false-color


    false-color: use  b1 VV and b2 VH as  channels
    for false-color details visit https://github.com/sentinel-hub/custom-scripts/tree/master/sentinel-1/sar_false_color_visualization
"""
COMMON_MATH_STACK_HELP = """
    use --list option to get channels index

    \b
    --vis:                bands         R           G                    B                     comment
           ---------------------------  ---------  ------------          ----------      --------------------
           raw              3           b1          b2                   b3              useful for histograms (--hist)
           sar              3           b1          (lg(b2)+lg(b3))/2    lg(b2)-lg(b3)   b1-coh, b2-main, b3-subordinate (use the same polarization)
           composite-u      3           lg(b1)      lg(b2)               b3              b1-main, b2-subordinate, b3-coh (use the same polarization)   
           composite        3           b1          lg(b2)               lg(b3)          b1-coh, b2-subordinate, b3-main (use the same polarization)   
           false-color      2           b1=VH       b2=VV
           false-color-enhanced                                                         - same params  as for false-color

    false-color: use  b1 VV and b2 VH as  channels
    for false-color details visit https://github.com/sentinel-hub/custom-scripts/tree/master/sentinel-1/sar_false_color_visualization
"""