#!/usr/bin/env python3
import logging
from pathlib import Path
from time import perf_counter

import click
import matplotlib.pyplot as plt
import numba as nb
import numpy as np
from ocli.pro.smoothing.anisotropic_diffusion import anisotropic_diffusion

from ocli.ai.Envi import Envi
from ocli.cli.ai import slice_option


@nb.njit((nb.f4[:, :], nb.int64), parallel=True)
def nb_clip_log_2D(inp, pr=1):
    min = np.math.log10(1e-6)
    max = np.math.log10(10)
    R, C = inp.shape
    for r in nb.prange(R):
        for c in nb.prange(C):
            v = inp[r, c]
            if v <= 1e-6:
                inp[r, c] = min
            elif v >= 10.0:
                inp[r, c] = max
            else:
                inp[r, c] = np.math.log10(inp[r, c])


#

@click.group('tune')
def tune():
    """  paramneter tune utility

    :return:
    """


@tune.command('aniso')
@slice_option
@click.option('-f', '--file', help="ENVI header file to test with",
              type=click.Path(
                  exists=True,
                  dir_okay=False,
                  readable=True,

              ),
              )
@click.option('--cmap', help='color map', default='gray', show_default=True)
@click.option('-c', '--columns', type=click.INT, help='number of columns', default=2, show_default=True)
@click.option('-n', '--niters', multiple=True, help='number of iterations, multiple allowed', type=click.INT,
              default=[10], show_default=True)
@click.option('-k', '--kappa', multiple=True,
              help='Kappa, multiple allowed', type=click.FLOAT,
              default=[2.0], show_default=True)
@click.option('-g', '--gamma', multiple=True,
              help='Gamma, multiple allowed', type=click.FLOAT,
              default=[0.2], show_default=True)
def anisotropic(file, slice_range, niters, kappa, gamma, columns, cmap):
    """
    anisotropic filter parameters

    --cmap is matplotlib color map name,
    refer https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html for more info

    """
    p = Path(file)
    _no_ext = str(Path(file).with_suffix(''))

    try:
        envi = Envi(recipe={'DATADIR': p.parent}, cos=None)
        slice_region = slice_range
        if (slice_range[0] != -1):
            zone = [[slice_region[0], slice_region[1]], [slice_region[2], slice_region[3]]]
            loader = envi.get_file_loader('zone', zone)
        else:
            loader = envi.get_file_loader('full')
        img, hdr = loader(_no_ext)
        print("Clipping...")
        nb_clip_log_2D(img, 1)
        print("Clipped")
        total_images = len(niters) * len(kappa) * len(gamma) + 2  # one for un-filtered
        cols = min(columns, total_images)
        rows = np.math.ceil(total_images / cols)
        fig = plt.figure(figsize=(rows, cols))
        fig.subplots_adjust(top=0.95, bottom=0.01, left=0.2, right=0.99)
        ax = fig.add_subplot(rows, cols, 1)
        try:
            ax.imshow(img, cmap=cmap)
        except ValueError as e:
            print(f"Error: {e}")
            return
        ax.set_axis_off()
        ax.set_title('original', fontdict={'fontsize': 8})
        #### CMAP ####
        ax = fig.add_subplot(rows, cols, 2)
        gradient = np.linspace(0, 1, 256)
        gradient = np.vstack((gradient, gradient))
        ax.imshow(gradient, aspect='auto', cmap=plt.get_cmap(cmap))
        ax.set_ylim(80)
        ax.axis('off')
        ax.set_title('color map', fontdict={'fontsize': 8})

        fign = 2
        voxelspacing = np.array([1.0, 1.0], dtype=np.float32)
        for i in range(len(niters)):
            for k in range(len(kappa)):
                for g in range(len(gamma)):
                    fign += 1
                    _gamma = gamma[g]
                    _kappa = kappa[k]
                    _niters = niters[i]
                    b = img.copy()
                    t0 = perf_counter()
                    anisotropic_diffusion(b,
                                          niter=_niters,
                                          kappa=_kappa,
                                          gamma=_gamma,
                                          voxelspacing=voxelspacing,
                                          option=1,
                                          )
                    t1 = perf_counter() - t0
                    print("")
                    ax = fig.add_subplot(rows, cols, fign)
                    ax.imshow(b, cmap=cmap)
                    ax.axis('off')
                    title = f"n={_niters} k={_kappa} g={_gamma} ({round(t1, 3)}s)"
                    print(title)
                    ax.set_title(title, fontdict={'fontsize': 8})
                    plt.tight_layout()
        plt.subplots_adjust(bottom=0.01, left=0.04, wspace=0.1, hspace=0.1, right=0.99, top=0.95)
        plt.show()
    except Exception as e:
        logging.exception(e)
    pass


if __name__ == '__main__':
    tune()
