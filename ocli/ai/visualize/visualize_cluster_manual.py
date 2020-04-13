import logging

import re

import matplotlib.pyplot as plt
import numpy as np

# todo pass recipe as JSON object
from ocli.ai.Envi import Envi
from ocli.ai.recipe import Recipe
from ocli.ai.util import Filenames


class Visualize(object):
    """
    Make ENVI image from predictions prob_pred_*.npy
    """
    log = logging.getLogger('Visualize')

    def __init__(self, mode:str, recipe:Recipe, envi:Envi):
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

    def run(self):
        if self.mode not in ('zone', 'full'):
            self.log.error("Bad mode '%s'. Allowed  [zone|full]", self.mode)
            return -1
        file_name = self.filenames.prob_pred
        self.log.debug("loading %s", file_name)
        pred = np.load(file_name)
        self.log.debug({'shape': pred.shape, 'stype': pred.dtype})

        vis = np.zeros(pred.shape[:-1], dtype=np.uint8)

        # order is important in this case, starting from
        # lower values and moving up till more than 0.3, pick experimentally
        # should match the obvious clusters in the input tensor.

        green_1 = (pred[..., 0] > 0.3).astype(np.bool)
        green_2 = (pred[..., 6] > 0.3).astype(np.bool)

        xz = (pred[..., 1] > 0.1).astype(np.bool)

        manmade_1 = (pred[..., 2] > 0.1).astype(np.bool)
        manmade_2 = (pred[..., 3] > 0.1).astype(np.bool)
        agr = (pred[..., 4] > 0.2).astype(np.bool)

        water = ((pred[..., 5] > 0.09) | (pred[..., 5] > 0.09)).astype(np.bool)

        # change color assignment and order of layers here

        # Color  | Value  | Cluster
        # ----------------------------
        # Black  |   0    |  n/a
        # Blue   |   1    |
        # Red    |   2    |
        # Brown  |   3    |
        # Orange |   4    |
        # Yellow |   5    |
        # Green  |   6    |
        # Turq   |   7    |

        vis = np.where(xz, 7, vis)
        vis = np.where(manmade_1, 2, vis)
        vis = np.where(manmade_2, 5, vis)
        vis = np.where(agr, 3, vis)
        vis = np.where(green_2, 1, vis)
        vis = np.where(green_1, 4, vis)
        vis = np.where(water, 6, vis)

        vis = np.where(vis == 0, np.where(pred.max(axis=-1) == 0, 0, np.argmax(pred, axis=2) + 1), vis)

        self.log.info("number of clusters: %d ", pred.shape[-1] + 1)
        self.log.debug({"Unique entries in vis: ": np.unique(vis)})

        # vis = median_filter(vis,4)

        cs3 = plt.get_cmap('Set3').colors
        colors = [
            (0, 0, 0),
            (0, 0, 1.),
            (1., 0., 0.),
            (1., 0.3, 0),
            (1., 0.7, 0),
            (1., 0.99, 0),
            (0., 1., 0.3),
            (0., 1., 0.8),
            (1., 0., 1.)
        ]

        colors.extend(cs3)
        # cmap.colors = tuple(colors)

        colors = colors[:pred.shape[-1] + 1]
        self.log.info("Number of colors used: %d", len(colors))
        # cmap = lcmap(colors)

        fnames = self.envi.locate(self.DATADIR)
        self.log.debug(f"vis header candidates {fnames}")
        fnames = [el for el in fnames if not re.search(r'_[dD][bB]', el)]
        fnames = [el for el in fnames if re.search(r'(Sigma)|(coh)', el)]
        full_shape, hdict = self.envi.read_header(fnames[0])

        colors = (np.array(colors) * 254).astype(np.uint8)
        imgc = np.empty(vis.shape + (3,))
        np.choose(vis, colors[..., 0], out=imgc[..., 0])
        np.choose(vis, colors[..., 1], out=imgc[..., 1])
        np.choose(vis, colors[..., 2], out=imgc[..., 2])
        imgc[0, 0, :] = np.array((0, 0, 0), dtype=np.uint8)
        imgc[-1, -1, :] = np.array((255, 255, 255), dtype=np.uint8)
        fname = self.filenames.pred8c
        self.envi.save(fname, imgc.astype(np.uint8), hdict['map info'],
                       hdict['coordinate system string'],
                       chnames=['R', 'G', 'B'], desc='clustered, colors')
        self.log.info(f"image {fname} saved ")

        # plt.imshow(vis, cmap=cmap)
        # plt.show()
        return 0
