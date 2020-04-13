#!/usr/bin/env python3
import logging
import os

import numpy as np
# from memory_profiler import profile
from skimage import img_as_ubyte

from ocli.ai.Envi import Envi
from ocli.ai.recipe import Recipe
from ocli.ai.util import Filenames


class Visualize(object):
    """
    Make ENVI image from predictions (full|zone)_prob_pred*.npy
    """
    log = logging.getLogger('Visualize')

    def __init__(self, mode: str, recipe: Recipe, envi: Envi):
        """

        :type envi: Envi
        :type recipe: Recipe
        :type mode: str
        """
        self.mode = mode
        self.envi = envi
        self.recipe = recipe.recipe
        self.DATADIR = self.recipe.get("DATADIR")
        self.WORKDIR = self.recipe.get("OUTDIR")
        self.envi.DATADIR = self.DATADIR
        self.filenames = Filenames(mode, recipe)

    def create_pred_img(self, the_np_file: str, the_np_hdr_file: str, the_out_img_file: str):
        """

        :param the_np_file: numpy_array file WITH EXTENSION , output of prediction
        :param th_np_hdr_file:  ENVI header file WITH EXTENSION from which to grab geometry and projection
        :param te_out_img_file: output ENVI file name WITHOUT EXTENSION
        """
        _in = np.load(the_np_file, mmap_mode='r')
        self.log.info(f'data loaded from {the_np_file}, shape is {_in.shape}')
        full_shape, hdict = self.envi.read_header(the_np_hdr_file, 'r')
        self.log.info(f'producing cluster visualisation {self.mode} {full_shape} from {the_np_file}')

        # hdict = dict([tuple(val[:-1].split(' = ')) for val in header.readlines()[1:] if len(val) > 1])
        # header.close()
        bands = _in.shape[-1]
        bn = [f'c{i:02}' for i in range(1, bands + 1)]
        bmap = self.recipe['band_meta']
        for d in bmap:
            bn[d['band']-1] = d['name']
        hdr = {
            'description': 'description',
            'samples': _in.shape[1],
            'lines': _in.shape[0],
            'header offset': 0,
            'file type': 'ENVI Standard',
            'data type ': 1,
            'interleave': 'bip',
            'Data Ignore Value': 0,
            'byte order': 0,
            'map info': hdict['map info'],
            'coordinate system string': hdict['coordinate system string'],
            'bands': bands,
            'band names': '{' + ','.join(bn) + '}',
        }
        self.envi.save_dict_to_hdr(the_out_img_file + '.hdr', hdr)
        self.log.info(f'ENVI HDR done, file {the_out_img_file}')
        img_as_ubyte(_in).tofile(the_out_img_file + '.img')
        self.log.info(f'ENVI cluster visualization done, IMG file {the_out_img_file}')

    def run(self):
        the_np_file = self.filenames.prob_pred
        the_np_hdr_file = self.filenames.tnsr_hdr
        if not os.path.isfile(the_np_file):
            raise AssertionError(
                f"Could not locate numpy data file '{the_np_file}' file! check recipe and produced data")
        if not os.path.isfile(the_np_hdr_file):
            raise AssertionError(
                F"Could not locate ENVI header file '{the_np_hdr_file}'! check recipe and produced data")
        the_out_img_file = self.filenames.pred8c
        self.log.info(f'ENVI header source from {the_np_hdr_file}')
        self.log.info(f'visualisation ENVI output to {the_out_img_file}')
        self.create_pred_img(the_np_file=the_np_file, the_np_hdr_file=the_np_hdr_file,
                             the_out_img_file=the_out_img_file)
