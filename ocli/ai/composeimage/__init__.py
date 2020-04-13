import logging
import os
import warnings
from typing import Dict

import matplotlib.pyplot as plt
import numpy as np
from skimage import exposure
from skimage import img_as_ubyte

from ocli.ai import Envi
from ocli.ai.recipe import Recipe
from ocli.ai.util import Filenames, zone_slice


class OptossImage(object):
    log = logging.getLogger('image-processor')
    tnsr_full = None
    tnsr_full_hdr = None
    full_shape = None
    file_loader = None
    dpi = 80

    def __init__(self, mode: str, recipe: Recipe, envi: Envi):
        self.envi = envi
        self.mode = mode
        self.recipe = recipe
        self.envi.DATADIR = self.recipe.get("DATADIR")
        self.WORKDIR = self.recipe.get("OUTDIR")
        self.filenames = Filenames(mode, recipe)

        self.zone = np.array(recipe.get('zone'))
        self.zone_shape = (self.zone[1][0] - self.zone[0][0], self.zone[1][1] - self.zone[0][1])
        self.do_zone = mode == 'zone'
        self.do_full = mode == 'full'

        self.tnsr_filename = self.filenames.tnsr

    def _zone_slice(self, arr):
        return zone_slice(self.zone, arr)

    def _check_zone(self):
        self.log.warning("TODO: check zone fit into full_shape")

    def save_tnsr(self):
        if not os.path.exists(self.WORKDIR):
            os.makedirs(self.WORKDIR)
        self.log.debug(f"Saving tnsr to {self.tnsr_filename}")

        np.save(self.tnsr_filename, self.tnsr_full)
        self.envi.save_dict_to_hdr(self.tnsr_filename, self.tnsr_full_hdr)
        self.log.info('tensor assembled')

    def assemble(self) -> bool:
        if self.mode == 'zone' and self.zone is None:
            self.log.error('No zone info in recipe')
            return False
        """ memory optimized
            using loader - it loads either full or zone part of image, so no need for separate processing of parts
        """
        processed_files = []
        self.tnsr_full = np.empty((self.zone_shape[0], self.zone_shape[1], 3), dtype=np.float32)

        colors = {
            'R': self.recipe.get_channel('R'),
            'G': self.recipe.get_channel('G'),
            'B': self.recipe.get_channel('B')
        }
        """ create target array """
        channel_names = colors['R'] + colors['G'] + colors['B']
        """ get shape just by first file shape """
        self.full_shape, self.tnsr_full_hdr = self.envi.read_header(channel_names[0])
        if self.do_full:
            """stretch zone to full image"""
            self.zone_shape = self.full_shape
            self.zone = [[0, 0], [self.full_shape]]
            self.file_loader = self.envi.get_file_loader(self.mode)
        else:
            self._check_zone()
            self.file_loader = self.envi.get_file_loader(self.mode, self.zone)
        self.tnsr_full = np.empty((self.zone_shape[0], self.zone_shape[1], 3), dtype=np.float32)
        for color, names in colors.items():
            if not len(names):
                self.log.error(f"No file name for color '{color}' ")
                return False
            if len(names) > 1:
                self.log.error("Only one file per color channel supported")
                return False
            name = names[0]
            ci = 0 if color == 'R' else 1 if color == 'G' else 2
            self.log.info(f"importing color {color} from {names[0]} {self.zone[0]}-{self.zone[1]}   into layer {ci}")
            self.tnsr_full[..., ci] = self.file_loader(name)[0]
        self.save_tnsr()
        return True

    def display(self, arr, title='title'):
        fig = plt.figure(figsize=(arr.shape[1] / self.dpi, arr.shape[0] / self.dpi), dpi=self.dpi)
        fig.figimage(arr)
        plt.show()

    def save_image(self, arr) -> bool:
        if self.mode == 'zone':
            self.log.error("Image could be saved only in full mode")
            return False
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.envi.save(self.filenames.pred8c, img_as_ubyte(arr),
                           map_info=self.tnsr_full_hdr['map info'],
                           coord_string=self.tnsr_full_hdr['coordinate system string'],
                           chnames=['R', 'G', 'B'], desc='Overlay image')
        self.log.info(f"saving image {self.filenames.pred8c}")

    def rescale_intensity(self, src: np.ndarray, args: Dict):
        v_min, v_max = np.percentile(src, args['percentile'])
        return exposure.rescale_intensity(src, in_range=(v_min, v_max))

    def adjust_sigmoid(self, src: np.ndarray, args: Dict):
        return exposure.adjust_sigmoid(src, **args)

    def adjust_gamma(self, src: np.ndarray, args: Dict):
        return exposure.adjust_gamma(src, **args)

    def adjust_log(self, src: np.ndarray, args: Dict):
        return exposure.adjust_gamma(src, **args)

    def process(self, show=False, show_intermediate=False):
        if True or self.tnsr_full is None:
            if not os.path.exists(self.tnsr_filename):
                self.log.error(
                    f"could not find {self.tnsr_filename}, did you forget run image assemble {self.mode}")
            self.tnsr_full = np.load(self.tnsr_filename)
            _, self.tnsr_full_hdr = self.envi.read_header(self.tnsr_filename, is_fullpath=True)
        else:
            self.log.info("reusing data from previous step")
        src, dst = self.tnsr_full, np.empty(self.tnsr_full.shape)
        if show_intermediate:
            self.display(src)
        products = self.recipe['products'].get('RGB', [])
        if not len(products) and self.mode == 'full':
            self.save_image()
            return True
        filters = products.get('filters', [])
        if not len(filters):
            self.save_image()
        for filter in filters:
            name = list(filter)[0]
            args = filter[name]
            filtered = False
            if name == 'rescale_intensity':
                filtered = True
                dst = self.rescale_intensity(src, args)
            if name == 'adjust_sigmoid':
                filtered = True
                dst = self.adjust_sigmoid(src, args)
            if name == 'adjust_gamma':
                filtered = True
                dst = self.adjust_gamma(src, args)
            if name == 'adjust_log':
                filtered = True
                dst = self.adjust_log(src, args)
            if filtered:
                self.log.info(f"about to apply filter {name}")
            else:
                self.log.error(f"Unknown filter {name}")
            if show_intermediate:
                self.display(dst)
            src, dst = dst, src

        """ loop sets src to last result """
        if show:
            self.display(src)
        if self.mode == 'full':
            self.save_image(src)
