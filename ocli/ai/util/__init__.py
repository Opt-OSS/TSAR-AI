import logging
import os
from typing import Union, Dict

from ocli.ai.recipe import Recipe


class Filenames(object):
    """ generates filenames with full path and extenstion"""
    log = logging.getLogger('Filenames')

    def __init__(self, mode: str, recipe: Union[Recipe,Dict]):
        self.DATADIR = recipe.get('DATADIR')
        self.OUTDIR = recipe.get('OUTDIR')
        self.PREDICTOR_DIR = recipe.get('PREDICTOR_DIR')
        self.prefix = 'zone_' if mode == 'zone' else 'full_'

    def __concat(self, dir, base_name, ext):
        return os.path.join(dir, self.prefix + base_name + '.' + ext)
    @property
    def process_info(self):
        """ assemple data result """
        return os.path.join(self.OUTDIR,'process-'+ self.prefix[:-1] + '.info.yaml')

    @property
    def tnsr(self):
        """ assemple data result """
        return os.path.join(self.OUTDIR, self.prefix + 'tnsr.npy')

    @property
    def tnsr_hdr(self):
        """ assemple data result """
        return os.path.join(self.OUTDIR, self.prefix + 'tnsr.npy.hdr')

    @property
    def bd(self):
        """ assemple data (bad pixels) result """
        return os.path.join(self.OUTDIR, self.prefix + 'bd.npy')

    @property
    def prob_pred(self):
        """ process data result """
        return os.path.join(self.OUTDIR, self.prefix + 'prob_pred.npy')

    @property
    def pred_config(self):
        """ process data result """
        return os.path.join(self.PREDICTOR_DIR, 'config.json')

    @property
    def gm(self):
        """ generated predictor """
        return os.path.join(self.PREDICTOR_DIR, 'gm.pkl')

    @property
    def tnorm(self):
        """ generated normalisation params """
        return os.path.join(self.PREDICTOR_DIR, 'tnorm.npy')

    @property
    def pred8c(self):
        """ visualisation result WITHOUT extension """
        return os.path.join(self.OUTDIR, self.prefix + 'pred8c')

    @property
    def pred8c_img(self):
        """ generated predictor image """
        return self.pred8c + '.img'

    @property
    def pred8c_hdr(self):
        """ generated predictor image """
        return self.pred8c + '.hdr'

    @property
    def out_tiff(self):
        """ cog-processing intermediate tiff """
        return os.path.join(self.OUTDIR, 'out.tiff')

    @property
    def out_cog_tiff(self):
        """ cog-processing intermediate tiff """
        return os.path.join(self.OUTDIR, 'cog-out.tiff')

    def __get(self, base_file):
        if base_file in ['tnsr', 'bd', 'prob_pred']:
            return os.path.join(self.OUTDIR, self.prefix + base_file + '.npy')

        """ temporary  images without extension """
        if base_file in ['pred8c']:
            return os.path.join(self.OUTDIR, self.prefix + base_file)
        """ resulting tiff files"""
        if base_file in ['out', 'cog-out']:
            return os.path.join(self.OUTDIR, self.prefix + base_file)

        """ predictor reusable result zone independent  """
        if base_file in ['gm']:
            return os.path.join(self.DATADIR, base_file + '.pkl')
        if base_file in ['tnorm']:
            return os.path.join(self.DATADIR, base_file + '.npy')

        raise AssertionError(f"filename '{base_file}' is not registered")


def zone_slice(zone, arr):
    """

    :param zone: array-like
    :param arr: array-like
    :return: array-like slice
    """
    return arr[zone[0][0]:zone[1][0], zone[0][1]:zone[1][1]]
