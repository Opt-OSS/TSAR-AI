"""

IMPORTANT pyproj == 19.6 IS REQUIRED
> 2.0 uses PROJ6 without or malformed epsg:4326

"""
import logging
import os
from pathlib import Path
from pprint import pprint

from scipy.stats import describe

import cartopy.crs as ccrs
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import shapely
import spectral.io.envi as envi
from matplotlib import ticker
from matplotlib.patches import Patch
from skimage import exposure

from ocli.ai.util import Filenames
from ocli.cli.output import OCLIException
from ocli.preview.cfeatures import add_basemap
import spectral.io.envi as s_envi

log = logging.getLogger()

# TODO create check packages installed and imported, fallback to
DEF_ROI_CRS = {'init': 'epsg:4326'}


def preview_roi(fit, roi, title='', zoom=8, basemap=None, roi_crs=None):
    bounds = []
    # crs = ccrs.epsg('3857')
    # crs = ccrs.PlateCarree()
    crs = ccrs.GOOGLE_MERCATOR
    crs_proj4 = crs.proj4_init
    # print(crs_proj4)
    # print(roi_crs)
    ax = plt.axes(projection=crs)
    ''' remove The bottom and top margins cannot be made large '''
    ax.outline_patch.set_visible(False)
    ax.background_patch.set_visible(False)
    roi_crs = roi_crs if roi_crs else DEF_ROI_CRS
    _r = gpd.GeoDataFrame({'geometry': [roi.geometry]}, crs=roi_crs)
    _r = _r.to_crs(crs_proj4)
    # _r = _r.to_crs(epsg=3857)
    bounds.append(_r.total_bounds.tolist())
    iw = fit.to_crs(crs_proj4)
    # iw = fit.to_crs(epsg=3857)
    bounds.append(iw.total_bounds.tolist())
    """
    https://matplotlib.org/3.1.0/gallery/lines_bars_and_markers/linestyles.html
    """

    bounds = gpd.np.array(bounds)
    border = 0.3  # in degrees
    ##  xmin, ymin, xmax, ymax ##
    xmin, ymin = bounds[:, [0, 1]].min(axis=0) - border
    xmax, ymax = bounds[:, [2, 3]].max(axis=0) + border

    ax.set_extent((xmin, xmax, ymin, ymax), crs=crs)
    _r.plot(facecolor=(1, 0, 1, 0.2), edgecolor=(1, 0, 1, 1), linewidths=(0.5),
            ax=ax, zorder=11
            )
    iw.plot(
        edgecolor=('blue', 'red'), facecolor='none', linewidths=(1.2), linestyles=[(0, (10, 10)), (10, (10, 10))],
        ax=ax, zorder=10)

    add_basemap(basemap, ax, crs=crs, zoom=zoom)
    ax.set_title(title, fontdict={'fontsize': 6})
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.06, left=0.04, wspace=0, hspace=0, right=0.9, top=0.95)

    plt.show()


def preview_roi_swath(df, roi, title='', zoom=8, basemap=None, roi_crs={'init': 'epsg:4326'},
                      burst_range=None,
                      swath_id=None,
                      ):
    # bounds = []
    # crs = ccrs.PlateCarree()
    crs = ccrs.GOOGLE_MERCATOR
    crs_proj4 = crs.proj4_init
    plt.figure(figsize=(10, 6))
    ax = plt.axes(projection=crs)
    _r = gpd.GeoDataFrame({'geometry': [roi.geometry]}, crs=roi_crs)
    _r = _r.to_crs(crs_proj4)
    bounds = np.array(_r.total_bounds.tolist())

    _r.plot(ax=ax, zorder=11,
            facecolor=(0.2, 0.2, 0.2, 0.2), edgecolor=(0.2, 0.2, 0.2), linewidths=(0.5),

            )
    _r.envelope.plot(
        ax=ax,
        zorder=9,
        edgecolor='r',
        linestyle='--',
        facecolor='none'
    )
    legend = []

    for (_iw, _cl) in [('IW1', [(1, 0, 0), (0, 1, 1)]), ('IW2', [(0, 1, 0), (1, 0, 1)]),
                       ('IW3', [(0, 0, 1), (1, 1, 0)])]:
        if swath_id and _iw not in swath_id:
            continue
        df.set_geometry(_iw, inplace=True)
        iw = df[df[_iw].notna()].to_crs(crs_proj4)
        for i, v in enumerate([[True, 'master'], [False, 'slave']]):
            _d = iw[iw['master'] == v[0]]

            if burst_range:
                _d = _d.iloc[burst_range[0] - 1:burst_range[1]]
            if _d.empty:
                continue
            # _d['coords'] = _d[_iw].apply(lambda x: x.representative_point().coords[:])
            # _d['coords'] = [coords[0] for coords in _d['coords']]
            edgecolor, facecolor = _cl[i], (*_cl[i], 0.5)
            _d.plot(
                edgecolor=edgecolor,
                facecolor=facecolor,
                linewidths=(0.3),
                linestyles='-.',
                ax=ax,
                zorder=10,
            )
            x, y = shapely.geometry.box(*_d.total_bounds).exterior.xy
            ax.plot(x, y, color=_cl[i], linewidth='0.3', zorder=9, linestyle=(0, (10, 5)))
            legend += [
                Patch(edgecolor=_cl[i],
                      facecolor=(*_cl[i], 0.5),
                      linewidth=0.3,
                      label=f'{v[1]} {_iw}')
            ]
            _d['coords'] = _d.centroid.apply(lambda x: x.coords[:][0])
            for idx, row in _d.iterrows():
                l = f'm{idx}' if v[0] else f"s{idx}"
                a = 'right' if v[0] else 'left'
                plt.annotate(s=l, xy=row['coords'],
                             fontsize=8,
                             color=(0.2, 0.2, 0.2),
                             fontweight='ultralight',
                             horizontalalignment=a,
                             va='center_baseline',
                             zorder=12,
                             )
            bounds = np.append(bounds, _d.total_bounds.tolist())
    legend += [
        Patch(edgecolor='r',
              facecolor='none',
              linewidth=0.3,
              linestyle=(0, (10, 5)),
              label=f'image footprint')
    ]

    bounds = bounds.reshape((-1, 4))

    border = 0.25  # in degrees
    ##  xmin, ymin, xmax, ymax ##
    xmin, ymin = bounds[:, [0, 1]].min(axis=0) - border
    xmax, ymax = bounds[:, [2, 3]].max(axis=0) + border

    ax.legend(handles=legend, loc='top right', fontsize='xx-small')
    ax.set_title(title, fontdict={'fontsize': 6})
    ax.set_extent((xmin, xmax, ymin, ymax), crs=crs)
    add_basemap(basemap, ax, crs=crs, zoom=zoom)
    plt.subplots_adjust(bottom=0.05, left=0.01, wspace=0.1, hspace=0.1, right=0.93, top=0.95)

    plt.tight_layout()
    plt.show()


@ticker.FuncFormatter
def major_formatter(x, pos):
    return "[%.2f]" % x


def preview_stack(df, dir, full_shape: list, slice_region: tuple,
                  band: list, clip: tuple, columns: int,
                  hist=None, ylog=False
                  ):
    import spectral.io.envi as envi
    if (slice_region[0] != -1):
        arr = np.zeros([slice_region[2] - slice_region[0], slice_region[3] - slice_region[1], len(band)])
        log.info(f'Slice:{slice_region} from {full_shape}')
    else:
        arr = np.zeros([*full_shape[:2], len(band)])
    idx = 0
    # log.error(arr.shape)
    ymin, xmin, ymax, xmax = slice_region
    band_names = []
    for i, row in df.iterrows():
        band_names.append(row['filename'])
        _p = os.path.join(dir, row['filename'])
        img = envi.open(_p + '.hdr', _p + '.img')
        b = img.read_band(0, use_memmap=True)
        if (slice_region[0] != -1):
            arr[..., idx] = b[slice_region[0]:slice_region[2], slice_region[1]:slice_region[3]]
        else:
            arr[..., idx] = b
            # ymin, xmin, ymax, xmax = (0, 0, full_shape[0], full_shape[1])
        if clip:
            minval = 10 ** clip[0]
            maxval = 10 ** clip[1]
            arr[..., idx] = np.log10(np.clip(arr[..., idx], minval, maxval))
        idx += 1

    if len(band):
        cols = min(columns, len(band))
        rows = np.math.ceil(len(band) / cols)
        fig = plt.figure(figsize=(10, 10))
        # fig_y_inches = 4 if rows < 4 else rows  # 1 inch per row-image
        # fig_x_inches = 4 if cols < 4 else cols  # 1 inch per col-image
        # fig = plt.figure(figsize=(fig_y_inches, fig_x_inches))

        for i in range(0, len(band)):
            ax = fig.add_subplot(rows, cols, i + 1)
            ax.set_title(band_names[i], fontdict={'fontsize': 8})
            if hist:
                _a = arr[..., i].reshape(-1)
                if ylog:
                    plt.yscale("log")
                plt.hist(_a, bins=hist)
                _d = describe(_a)
                plt.xlabel(f"min:{_d.minmax[0]} max:{_d.minmax[1]}\nmean:{_d.mean} dev: {np.sqrt(_d.variance)}")
            else:
                plt.imshow(arr[..., i])
                plt.colorbar(ax=ax)
                ax.tick_params(axis='both', which='major', labelsize=8)
                ax.tick_params(axis='both', which='minor', labelsize=6)
                ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x + xmin)}'))
                ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f'{int(y + ymin)}'))
    sup = f"STACK {arr.shape[0]}x{arr.shape[1]}"
    if clip:
        sup += f" clip-log {clip}"
    plt.suptitle(sup)
    # plt.subplots_adjust(bottom=0.01, left=0.04, wspace=0.1, hspace=0.1, right=0.99, top=0.99)
    plt.tight_layout()
    return plt


def _vis_rgb(r, g, b, title, hist=None, ylog=False):
    if hist:
        fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(10, 10))
        if ylog:
            [ax.axes.set_yscale('log') for ax in axes]
        fig.suptitle(title, fontdict={'fontsize': 8})
        fig.canvas.set_window_title(f"Band math histogram")
        axes[0].hist(r.reshape(-1), bins=hist, color='r')
        axes[1].hist(g.reshape(-1), bins=hist, color='g')
        axes[2].hist(b.reshape(-1), bins=hist, color='b')
        # ax.set_xlabel('B')
    else:
        img = np.stack((r, g, b), axis=-1)  # RGB
        fig1, ax1 = plt.subplots()
        fig1.canvas.set_window_title(f"Band math")
        ax1.imshow(img)
        ax1.set_title(title, fontdict={'fontsize': 8})
    plt.tight_layout()
    return plt


def compute_stack_pol2(b1, b2, vis_mode='simple'):
    """
    https://github.com/sentinel-hub/custom-scripts/tree/master/sentinel-1/sar_false_color_visualization

    :param df:
    :param band:
    :param slice_region:
    :return:
    """
    sigma_min = -2
    sigma_range = 2
    if vis_mode == 'rgb-ratio':
        band1 = b1
        band2 = 2 * b2
        band3 = b1 / (100.0 * b2)
    elif vis_mode == 'raw':
        band1 = b1
        band2 = b2
        band3 = b2
    elif vis_mode == 'rgb-diff':

        b1 = np.clip(np.log10(b1 + 1e-6) - sigma_min, 0, sigma_range) / sigma_range
        b2 = np.clip(np.log10(b2 + 1e-6) - sigma_min, 0, sigma_range) / sigma_range
        band1 = b2
        band2 = b1
        band3 = b2 - b1
    elif vis_mode == 'false-color':
        c1 = 10e-4
        c2 = 0.01
        c3 = 0.02
        c4 = 0.03
        c5 = 0.045
        c6 = 0.05
        c7 = 0.9
        c8 = 0.25
        band1 = c4 + np.log(c1 - np.log(c6 / (c3 + 2 * b1)))
        band2 = c6 + np.exp(c8 * (np.log(c2 + 2 * b1) + np.log(c3 + 5 * b2)))
        band3 = 1 - np.log(c6 / (c5 - c7 * b1))
    elif vis_mode == 'false-color-enhanced':
        c1 = 10e-4
        c2 = 0.01
        c3 = 0.02
        c4 = 0.03
        c5 = 0.045
        c6 = 0.05
        c7 = 0.9
        c8 = 0.25
        band1 = c4 + np.log(c1 - np.log(c6 / (c3 + 2.5 * b1)) + np.log(c6 / (c3 + 1.5 * b2)))
        band2 = c6 + np.exp(c8 * (np.log(c2 + 2 * b1) + np.log(c3 + 7 * b2)))
        band3 = 0.8 - np.log(c6 / (c5 - c7 * b1))
    else:
        raise ValueError(f"unknown 2-band vis-mode '{vis_mode}'")
    return (band1, band2, band3)


def img_to_log_uint8(img, min_log, range_log):
    ary = (np.log10(np.maximum(img, 1e-6)) - min_log) / range_log
    return ary


def compute_stack_pol3(b1: np.ndarray, b2: np.ndarray, b3: np.ndarray, vis_mode='simple',

                       ):
    """
    https://github.com/sentinel-hub/custom-scripts/tree/master/sentinel-1/sar_false_color_visualization

    :param vis_mode:
    :param title:
    :param b3:
    :param b2:
    :param b1:
    :return:
    """

    sigma_min = -2.5
    sigma_range = 2
    if vis_mode == 'raw':
        band1 = b1
        band2 = b2
        band3 = b3
    elif vis_mode == 'composite-u':
        band1 = img_to_log_uint8(b1, sigma_min, sigma_range)
        band2 = img_to_log_uint8(b2, sigma_min, sigma_range)
        band3 = b3
    elif vis_mode == 'composite':
        band1 = np.log10(b1 * 4.5)
        band2 = img_to_log_uint8(b2, sigma_min, sigma_range)
        band3 = img_to_log_uint8(b3, sigma_min, sigma_range)
    elif vis_mode == 'sar':
        band1 = np.log10(b1 * 4.5)
        b2 = img_to_log_uint8(b2, sigma_min, sigma_range)
        b3 = img_to_log_uint8(b3, sigma_min, sigma_range)
        band2 = (b2 + b3) / 2
        band3 = b2 - b3
    else:
        raise ValueError(f"unknown 3-band vis-mode '{vis_mode}'")
    return (band1, band2, band3)


def compute_tensor_pol2(b1, b2, vis_mode, hist=None, ylog=False, return_array=False):
    """
    https://github.com/sentinel-hub/custom-scripts/tree/master/sentinel-1/sar_false_color_visualization

    :param df:
    :param band:
    :param slice_region:
    :return:
    """
    if vis_mode == 'rgb-diff':
        band1 = b1
        band2 = b2
        band3 = b1 - b2
    elif vis_mode == 'rgb-ratio':
        band1 = b2
        band2 = b1
        band3 = b2 - b1
    elif vis_mode == 'simple':
        band1 = b1
        band2 = b2 * 2
        band3 = (b1 / b2) / 100
    elif vis_mode == 'false-color':
        c1 = 10e-4
        c2 = 0.01
        c3 = 0.02
        c4 = 0.03
        c5 = 0.045
        c6 = 0.05
        c7 = 0.9
        c8 = 0.25
        band1 = c4 + np.log(c1 - np.log(c6 / (c3 + 2 * b1)))
        band2 = c6 + np.exp(c8 * (np.log(c2 + 2 * b1) + np.log(c3 + 5 * b2)))
        band3 = 1 - np.log(c6 / (c5 - c7 * b1))
    elif vis_mode == 'false-color-enhanced':
        c1 = 10e-4
        c2 = 0.01
        c3 = 0.02
        c4 = 0.03
        c5 = 0.045
        c6 = 0.05
        c7 = 0.9
        c8 = 0.25
        band1 = c4 + np.log(c1 - np.log(c6 / (c3 + 2.5 * b1)) + np.log(c6 / (c3 + 1.5 * b2)))
        band2 = c6 + np.exp(c8 * (np.log(c2 + 2 * b1) + np.log(c3 + 7 * b2)))
        band3 = 0.8 - np.log(c6 / (c5 - c7 * b1))
    else:
        raise ValueError(f"unknown 2-band vis-mode '{vis_mode}'")
    return (band1, band2, band3)
    # return _vis_rgb(band1, band2, band3, title, hist, ylog)


def compute_tensor_pol3(b1: np.ndarray, b2: np.ndarray, b3: np.ndarray, vis_mode,
                        hist=None, ylog=False
                        ):
    """
    https://github.com/sentinel-hub/custom-scripts/tree/master/sentinel-1/sar_false_color_visualization

    :param vis_mode:
    :param title:
    :param b3:
    :param b2:
    :param b1:
    :return:
    """

    if vis_mode == 'raw' or vis_mode == 'composite':
        band1 = b1
        band2 = b2
        band3 = b3
    elif vis_mode == 'sar':
        band1 = b1
        band2 = (b2 + b3) / 2
        band3 = b2 - b3
    else:
        raise ValueError(f"unknown 3-band vis-mode '{vis_mode}'")
    return (band1, band2, band3)
    # title = f"{vis_mode} {band1.shape[0]}x{band1.shape[1]}\n{title}"
    # return _vis_rgb(band1, band2, band3, title, hist, ylog)


def preview_tnsr(arr,  band,band_names, columns, slice_range, hist, title, ylog):

    if (slice_range[0] != -1):
        ymin, xmin, ymax, xmax = slice_range
    else:
        ymin, xmin, ymax, xmax = (0, 0, arr.shape[0], arr.shape[1])

    if len(band):
        cols = min(columns, len(band))
        rows = np.math.ceil(len(band) / cols)
        fig = plt.figure(figsize=(10, 10))

        for i in range(0, len(band)):
            ax = fig.add_subplot(rows, cols, i + 1)

            ax.set_title(band_names[i], fontdict={'fontsize': 8})
            log.info(f"add band  {i} {band_names[i]} to plot")
            if hist:
                _a = arr[..., i].reshape(-1)
                if ylog:
                    plt.yscale("log")
                plt.hist(_a, bins=hist)
                _d = describe(_a)
                plt.xlabel(f"min:{_d.minmax[0]} max:{_d.minmax[1]}\nmean:{_d.mean} dev: {np.sqrt(_d.variance)}")
                pass
            else:
                plt.imshow(arr[..., i])
                plt.colorbar()
                ax.tick_params(axis='both', which='major', labelsize=8)
                ax.tick_params(axis='both', which='minor', labelsize=6)
                ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x + xmin)}'))
                ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f'{int(y + ymin)}'))
            # plt.ioff()
    plt.suptitle(f"TENSOR {arr.shape[0]}x{arr.shape[1]} {title}")
    plt.subplots_adjust(bottom=0.01, left=0.04, wspace=0.1, hspace=0.1, right=0.99, top=0.95)
    plt.tight_layout()
    log.debug(f"image show...")
    return plt


def preview_hist_pol(ary: np.ndarray, bins: int, bnames):
    """ preview arry histograms """
    fig = plt.figure(figsize=(10, 10))
    cols = ary.shape[2]
    for i in range(0, cols):
        ax = fig.add_subplot(1, cols, i + 1)
        ax.set_title(bnames[i])
        log.error(ax)
        plt.hist(ary[..., i].reshape(-1), bins=bins)
        plt.tight_layout()
    return plt


def preview_cluster(pred8c_hdr, pred8c_img, band, slice_region, columns, rgb):
    img = envi.open(pred8c_hdr, pred8c_img)
    full_shape = img.shape[:2]
    band_names = img.metadata['band names']
    if (slice_region[0] != -1):
        ymin, xmin, ymax, xmax = slice_region
        sl = np.s_[slice_region[0]:slice_region[2], slice_region[1]:slice_region[3]]
    else:
        ymin, xmin, ymax, xmax = (0, 0, full_shape[0], full_shape[1])
        sl = np.s_[:, :]
    if rgb:
        fig1, ax1 = plt.subplots()
        r = img.read_band(band[0], use_memmap=True)[sl]
        g = img.read_band(band[1], use_memmap=True)[sl]
        b = img.read_band(band[2], use_memmap=True)[sl]
        ax1.imshow(np.stack((r, g, b), axis=-1))
    elif len(band):
        cols = min(columns, len(band))
        rows = np.math.ceil(len(band) / cols)
        fig = plt.figure(figsize=(rows, cols))
        for i, _b in enumerate(band):
            b = img.read_band(_b, use_memmap=True)[sl]
            ax = fig.add_subplot(rows, cols, i + 1)
            plt.imshow(b)
            plt.tight_layout()
            ax.tick_params(axis='both', which='major', labelsize=8)
            ax.tick_params(axis='both', which='minor', labelsize=6)
            ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x + xmin)}'))
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f'{int(y + ymin)}'))
            ax.set_title(band_names[_b], fontdict={'fontsize': 8})
    plt.subplots_adjust(bottom=0.01, left=0.04, wspace=0.1, hspace=0.1, right=0.99, top=0.99)
    plt.show()


def create_stack_rgb(band1, band2, band3, df, vis_mode, slice_range):
    bands_2 = ['false-color', 'false-color-enhanced']
    bands_3 = ['sar', 'composite', 'raw']

    if band1 is None and band2 is None:
        raise OCLIException('At least b1 and b2 Bands are required for preview')
    if vis_mode in bands_2 and band3 is not None:
        raise OCLIException(f"'{vis_mode}': requires  band1 and band2 only ")
    if vis_mode in bands_3 and band3 is None:
        raise OCLIException(f"'{vis_mode}': requires 3 bands ")
    try:
        import spectral.io.envi as s_envi
        if band3 is None:
            _b1 = df.iloc[band1].path
            _b2 = df.iloc[band2].path

            title = f"B1: {_b1}\nB2: {_b2}"
            b1 = s_envi.open(_b1 + '.hdr', _b1 + '.img').read_band(0, use_memmap=True)
            b2 = s_envi.open(_b2 + '.hdr', _b2 + '.img').read_band(0, use_memmap=True)
            if slice_range[0] != -1:
                title += f"\n slice {slice_range}"
                b1 = b1[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3]]
                b2 = b2[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3]]
            (r, g, b) = compute_stack_pol2(b1, b2, vis_mode=vis_mode)
        else:
            _b1 = df.iloc[band1].path
            _b2 = df.iloc[band2].path
            _b3 = df.iloc[band3].path
            b1 = s_envi.open(_b1 + '.hdr', _b1 + '.img').read_band(0, use_memmap=True)
            b2 = s_envi.open(_b2 + '.hdr', _b2 + '.img').read_band(0, use_memmap=True)
            b3 = s_envi.open(_b3 + '.hdr', _b3 + '.img').read_band(0, use_memmap=True)
            title = f"B1: {_b1}\nB2: {_b2}\nB3: {_b3}"
            if slice_range[0] != -1:
                title += f"\n slice {slice_range}"
                b1 = b1[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3]]
                b2 = b2[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3]]
                b3 = b3[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3]]
            (r, g, b) = compute_stack_pol3(b1, b2, b3, vis_mode=vis_mode, )
            title = f"{vis_mode} {b1.shape[0]}x{b1.shape[1]}\n{title}"
        return title, (r, g, b)

    except ValueError as e:
        raise OCLIException(str(e))


def _read_bands(band1, band2, band3, df, slice_range):
    _b1 = df.iloc[band1].path
    _b2 = df.iloc[band2].path

    title = f"B1: {_b1}\nB2: {_b2}"
    b1 = s_envi.open(_b1 + '.hdr', _b1 + '.img').read_band(0, use_memmap=True)
    b2 = s_envi.open(_b2 + '.hdr', _b2 + '.img').read_band(0, use_memmap=True)
    b3 = None
    if slice_range[0] != -1:
        b1 = b1[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3]]
        b2 = b2[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3]]
    if band3:
        _b3 = df.iloc[band3].path
        title += f"\nB3: {_b3}"
        b3 = s_envi.open(_b3 + '.hdr', _b3 + '.img').read_band(0, use_memmap=True)
        if slice_range[0] != -1:
            b3 = b3[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3]]
    if slice_range[0] != -1:
        title += f"\n slice {slice_range}"
    return title, (b1, b2, b3)


def read_tensor(blist, df: gpd.GeoDataFrame, slice_range, filenames, tnorm, gauss, split) -> (str, np.ndarray):
    ary = np.load(filenames.tnsr, mmap_mode='r')  # type: np.ndarray
    ns = df.iloc[blist]['name'].tolist()
    title = " ".join([f"B{i}:{n}" for i, n in enumerate(ns)])
    if slice_range[0] != -1:
        tnsr = ary[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3], blist]  # type: np.ndarray
    else:
        tnsr = ary[..., blist]  # type: np.ndarray

    if tnorm:
        tn = np.load(filenames.tnorm)
        for i, b in enumerate(blist):
            tnsr[..., i] -= tn[b, 0]
            tnsr[..., i] /= tn[b, 1]
        title += "\nNormalized"
    else:
        title += "\nNot normalised"

    if gauss:
        from scipy.ndimage.filters import gaussian_filter
        for i in range(0, tnsr.shape[2]):
            tnsr[..., i] = gaussian_filter(tnsr[..., i], gauss)
        title += f"\nGauss={gauss}"
    if split:
        if len(blist) == 3:
            return title, (tnsr[..., 0], tnsr[..., 1], tnsr[..., 2])
        else:
            return title, (tnsr[..., 0], tnsr[..., 1], None)
    else:
        return tnsr


def create_tensor_rgb(band1, band2, band3, df, vis_mode, slice_range, filenames, tnorm, gauss):
    bands_2 = ['simple', 'rgb-ratio', 'rgb-diff', 'false-color', 'false-color-enhanced']
    bands_3 = ['sar', 'composite', 'composite-u']

    if band1 is None and band2 is None:
        raise OCLIException('At least b1 and b2 Bands are required for preview')
    if vis_mode in bands_2 and band3 is not None:
        raise OCLIException(f"'{vis_mode}': requires  band1 and band2 only ")
    if vis_mode in bands_3 and band3 is None:
        raise OCLIException(f"'{vis_mode}': requires 3 bands ")
    if tnorm and not Path(filenames.tnorm).is_file():
        raise OCLIException(f"tensor normalisation file '{filenames.tnorm}' not found")
    if band3:
        blist = [band1, band2, band3]
    else:
        blist = [band1, band2]
    try:
        df.iloc[blist]
    except IndexError:
        raise AssertionError("Band number is invalid")
    try:
        title, (b1, b2, b3) = read_tensor(blist,
                                          df=df,
                                          slice_range=slice_range,
                                          filenames=filenames,
                                          tnorm=tnorm,
                                          gauss=gauss,
                                          split=True
                                          )
        if band3:
            (r, g, b) = compute_tensor_pol3(b1, b2, b3, vis_mode=vis_mode)
        else:
            (r, g, b) = compute_tensor_pol2(b1, b2, vis_mode=vis_mode)
        title = f"{vis_mode} {r.shape[0]}x{r.shape[1]}\n{title}"
        return title, (r, g, b)

    except ValueError as e:
        raise OCLIException(str(e))


def create_tensor_plt(band1, band2, band3, vis_mode, slice_range, filenames: Filenames,
                      tnorm=False,
                      gauss=None, hist=None, ylog=False):
    # TODO Google Earth overlay? https://ocefpaf.github.io/python4oceanographers/blog/2014/03/10/gearth/
    bands_2 = ['simple', 'rgb-ratio', 'rgb-diff', 'false-color', 'false-color-enhanced']
    bands_3 = ['sar', 'composite', 'composite-u']

    if band1 is None and band2 is None:
        raise OCLIException('At least b1 and b2 Bands are required for preview')
    if vis_mode in bands_2 and band3 is not None:
        raise OCLIException(f"'{vis_mode}': requires  band1 and band2 only ")
    if vis_mode in bands_3 and band3 is None:
        raise OCLIException(f"'{vis_mode}': requires 3 bands ")
    if tnorm and not Path(filenames.tnorm).is_file():
        raise OCLIException(f"tensor normalisation file '{filenames.tnorm}' not found")
    try:
        import spectral.io.envi as s_envi
        try:
            bn = s_envi.open(filenames.tnsr_hdr).metadata['band names']
            ary = np.load(filenames.tnsr, mmap_mode='r')  # type: np.ndarray
        except Exception as e:
            raise OCLIException(e)

        if band3 is None:
            title = f"tensor: {filenames.tnsr}\n B1: {bn[band1]} B2: {bn[band2]}"
            blist = [band1, band2]
        else:
            title = f"tensor: {filenames.tnsr}\n B1: {bn[band1]} B2: {bn[band2]} B3: {bn[band3]}"
            blist = [band1, band2, band3]
        try:
            if slice_range[0] != -1:
                tnsr = ary[slice_range[0]:slice_range[2], slice_range[1]:slice_range[3], blist]
            else:
                tnsr = ary[..., blist]
        except KeyError:
            raise OCLIException(f"One of band not found in tensor")

        if tnorm:
            tnsr = tnsr.copy()
            tn = np.load(filenames.tnorm)
            # pprint(tn)
            for i, b in enumerate(blist):
                # pprint(tn[b])
                tnsr[..., i] -= tn[b, 0]
                tnsr[..., i] /= tn[b, 1]
                title += "\n"
        else:
            title += "\n not normalised"
        if gauss:
            from scipy.ndimage.filters import gaussian_filter
            for i in range(0, tnsr.shape[2]):
                tnsr[..., i] = gaussian_filter(tnsr[..., i], gauss)
            title += f" Gauss={gauss}"

        if band3 is None:
            _plt = compute_tensor_pol2(tnsr[..., 0], tnsr[..., 1], title=title,
                                       vis_mode=vis_mode, hist=hist, ylog=ylog)
        else:

            _plt = compute_tensor_pol3(tnsr[..., 0], tnsr[..., 1], tnsr[..., 2], title=title,
                                       vis_mode=vis_mode, hist=hist, ylog=ylog)
        return _plt

    except Exception as e:
        log.exception(e)
        if slice_range[0] != -1:
            raise OCLIException(f"Could not render image, check bands and slice: {e}")
        else:
            raise OCLIException(f"Could not render image, check bands: {e}")
