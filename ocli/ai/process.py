import json
import logging
import os
from pickle import dump, load

import numpy as np
from scipy.ndimage.filters import gaussian_filter
from sklearn.cluster import MiniBatchKMeans
from sklearn.mixture import GaussianMixture as GM
from sklearn.utils import shuffle

from ocli.ai.recipe import Recipe
from ocli.ai.util import Filenames


class Process(object):
    log = logging.getLogger('Process')
    _progress_total = 1
    _progress_current = 0
    _progress_cb = None

    def __init__(self, mode, prediction_type, recipe: Recipe):
        """

        :type prediction_type: str
        :type mode: str
        """

        self.mode = mode
        self.type = prediction_type
        self.recipe = recipe
        self.WORKDIR = self.recipe.get("OUTDIR")
        self.DATADIR = self.recipe.get("DATADIR")
        self.filenames = Filenames(mode, recipe)

    def _generate_config(self, n_clusters, save=True):
        from ocli.ai.jet_256_colors import COLORS
        conf = {k: v for (k, v) in self.recipe.recipe.items() if
                k in ['learn_gauss', "predict_gauss", "products", "num_clusters", "band_meta"]}
        if "band_meta" not in conf or not len(conf['band_meta']):
            gradient = np.linspace(0, 255, n_clusters, dtype=np.uint8)
            conf["band_meta"] = [
                {'band': idx + 1, 'color': COLORS[gradient[idx]], 'name': f'c{(idx + 1):02}'}
                for idx, c in enumerate(gradient)
            ]
        j = json.dumps(conf, indent=4)
        if save:
            conf_json_file = self.filenames.pred_config
            with open(conf_json_file, 'w') as _f:
                _f.write(j + "\n")
            self.log.info(f"config file saved to {conf_json_file}")

    def _restart_porgress(self, total):
        self._progress_total = total
        self._progress_current = 0

    def progress(self, msg, step=1):
        if self._progress_cb:
            self._progress_current = step
            self._progress_cb(self._progress_total, self._progress_current, msg)
        else:
            self.log.info(msg)
    def __fit(self,tnsr):
        pass
    def run(self, precision=np.uint8, clipping=True, callback=None):
        self._progress_cb = callback
        if self.type not in ('fit', 'predict', 'fitpredict'):
            self.log.error("Bad mode '%s'. Allowed  [fit|predict|fitpredict]", self.mode)
            return -1

        if self.mode not in ('zone', 'full'):
            self.log.error("Bad mode '%s'. Allowed  [zone|full]", self.mode)
            return -1
        if precision != np.float32 and precision != np.uint8:
            self.log.error("Bad precision '%s'. Allowed  [float32|uint8]", self.mode)
            return -1

        cselect = tuple(self.recipe['learn_channels'])

        """ file names and locations """
        gm_file = self.filenames.gm
        tnorm_file = self.filenames.tnorm
        tnsr_file = self.filenames.tnsr
        bad_data_file = self.filenames.bd
        prob_pred_file = self.filenames.prob_pred

        self._restart_porgress(3)
        self.progress('loading tensor', 1)

        tnsr = np.load(tnsr_file) # type: np.ndarray
        self.log.info(f"tensor loaded from {tnsr_file}")
        bad_data = np.load(bad_data_file)
        # TODO do not make tnsr_copy (tnsr_or) better open it again
        tnsr = tnsr[..., cselect]
        self.log.debug({'tnsr.shape': tnsr.shape})
        if self.type == 'fitpredict' or self.type == 'fit':
            n_clusters = self.recipe['num_clusters']
            gauss_sz = self.recipe['learn_gauss']
            if self.type == 'fitpredict':
                tnsr_or = tnsr.copy()
            tnorm = np.empty((tnsr.shape[-1], 2))
            for n in range(tnsr.shape[-1]):  # type: int
                tnorm[n, 0] = tnsr[..., n].mean()
                tnsr[..., n] -= tnorm[n, 0]
                tnorm[n, 1] = tnsr[..., n].std()
                tnsr[..., n] /= tnorm[n, 1]
                # tnsr[bad_data,n] = tnorm[n,0]
                if gauss_sz:
                    tnsr[..., n] = gaussian_filter(tnsr[..., n], gauss_sz)

            tnsr_learn = tnsr[~bad_data, :].reshape((-1, tnsr.shape[-1]))
            self.progress(f'KMeans clusters: {n_clusters}', 1)
            self.log.debug(f'tnsr_learn.shape:{tnsr_learn.shape}')
            self.log.info(f'KMeans clusters: {n_clusters}')
            tnsr_learn = shuffle(tnsr_learn)

            predictor = MiniBatchKMeans(n_clusters=n_clusters, batch_size=1000000, compute_labels=False).fit(tnsr_learn)
            os.makedirs(os.path.dirname(tnorm_file), exist_ok=True)
            np.save(tnorm_file, tnorm)
            self.log.info(f"tnorm file saved to {gm_file}")
            cc = np.array(predictor.cluster_centers_)
            # self.log.debug(cc)
            self.progress('Fitting model', 1)
            gm = GM(cc.shape[0], max_iter=10, means_init=cc, tol=0.01)
            self.log.info(f"fitting GM {cc.shape}")
            gm.fit(shuffle(tnsr_learn)[:(4000000 if self.mode == 'full' else 2000000)])
            os.makedirs(os.path.dirname(gm_file), exist_ok=True)
            dump(gm, open(gm_file, 'wb'))
            self.log.info(f"gm file saved to {gm_file}")
            self._generate_config(n_clusters, save=True)
            if self.type == 'fitpredict':
                tnsr = tnsr_or
            else:
                return 0
        self._restart_porgress(1)
        self.progress('loading tensor', 0)
        tnorm = np.load(tnorm_file)
        self.progress('tensor loaded', 1)
        # predictor = load(open(DATADIR+'predictor.pkl','rb'))
        gm = load(open(gm_file, 'rb'))  # type: GM
        Ncc = len(gm.weights_)

        # TODO make in on-dist memmap file
        prob_pred = np.empty(tnsr.shape[:-1] + (Ncc,), dtype=precision)

        gauss_sz = self.recipe['predict_gauss']
        """
        TODO
        ns param should be auto-selected cause tstr defined by it and 
        exec time of gm.predict_proba strongly depends on it
            for tnsr.shape == (6926, 4958, 3)
                ns 150 exec 40 sec
                ns 100 exec 25 sec
                ns 50  exec 40 sec 
            for tnsr.shape === (2007, 2861, 3)
                
                ns 150 exec 5.8 sec
                ns 100 exec 7.5 sec
                ns 50  exec 
        some magic: on win if ppstr.nbytes close to 32M we have best time
        ppstr.nbytes = (ns+2*d)*width*K * float64.size
        where K  - number of clusters = gm.n_components
              width is image width
        ns = 32M/(W*K*8) -2d
        ********************************************
        32M - this is like sum of CPU L3 caches
        since intel 8-gen CPU could have 'smart'
        L3 cache its not clear how to get this value
        ********************************************
        """
        d = int(gauss_sz * 2.5)+2  # number of _strings_ to read
        ns = np.math.ceil(
            (9 * 6) * 1024 * 1024 / (tnsr.shape[1] * gm.n_components * np.dtype(np.float64).itemsize)) - 2 * d
        self.log.info(f"computed step = {ns} for width {tnsr.shape[1]} and num clusters {gm.n_components}")
        # ns=27
        clip_val = 255 * 0.2 if precision == np.uint8 else 0.01
        # preallocate memory
        ppstr = np.zeros((tnsr.shape[1] * (ns + 2 * d), gm.n_components), dtype=np.float64)
        iters = tnsr.shape[0]
        self._restart_porgress(iters)
        for i in range(0, iters, ns):
            self.progress(f'Predicting {i} of {iters}', i)
            d1 = min(d, i)
            d2 = max(0, min(tnsr.shape[0] - i - ns, d))
            tstr = tnsr[i - d1:i + ns + d2, :, :].copy()
            bdstr = bad_data[i - d1:i + ns + d2, :]

            strshape = tstr.shape
            tstr -= tnorm[np.newaxis, np.newaxis, :, 0]
            tstr /= tnorm[np.newaxis, np.newaxis, :, 1]
            # tstr[bdstr, :] = tnorm[:,0]
            if gauss_sz:
                for n in range(tstr.shape[-1]):
                    tstr[..., n] = gaussian_filter(tstr[..., n], gauss_sz)

            ppstr = gm.predict_proba(tstr.reshape((-1, strshape[-1])))  # type: np.array
            self.log.debug(
                f'{i} of {iters}: GM ppstr.nbytes {ppstr.nbytes} ppstr.shape {ppstr.shape} tstr.size {tstr.nbytes} tsrt.shape {tstr.shape}')
            if precision == np.uint8:
                ppstr = (ppstr * 255).astype(np.uint8).reshape(strshape[:-1] + (Ncc,))
            else:
                ppstr = ppstr.astype(np.float32).reshape(strshape[:-1] + (Ncc,))

            # prob_pred[i:i+ns,...] = ppstr
            ppstr = np.where(bdstr[..., np.newaxis], 0, ppstr)
            if clipping:
                ppstr[ppstr < clip_val] = 0
            if d2 == 0:
                prob_pred[i:i + ns, ...] = ppstr[d1:, ...]
            else:
                prob_pred[i:i + ns, ...] = ppstr[d1:-d2, ...]
        self.progress(f'Predicted ', iters)
        ############### saving results
        if not os.path.exists(self.WORKDIR):
            os.makedirs(self.WORKDIR)
        self._restart_porgress(1)
        self.progress(f'Saving results', 0)
        np.save(prob_pred_file, prob_pred)
        self.progress(f'Saved {prob_pred_file}', 1)
        self.log.info("Process results saved as '%s'", prob_pred_file)

        return 0  # all good
