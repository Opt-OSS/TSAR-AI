import logging
# sys.path.insert(0, "./")
import os
import time

import numpy as np

from ocli.ai.Envi import Envi, header_transform_map_for_zone
# from ocli.ai.filter.smoothing import anisotropic_diffusion, fix_pixels
from ocli.ai.recipe import Recipe
from ocli.ai.util import Filenames

log = logging.getLogger()


from ocli.ai.filter.anisotropic_diffusion import anisotropic_diffusion
from ocli.ai.filter.fix_pixels import fix_pixels


class Assemble(object):
    """
    collect bytes from ENVI channels in recipe  (full or part by mode key)
    preprocess data by recipe channels options,
    assemble input data data in numpy multidimensional array (each ENVI file in z-dimension)
    saves data self.filenames.tnsr and self.filenames.bd numpy files
    """
    # recipe = None  # type: Dict
    log = logging.getLogger('tensor-assembler')
    _progress_total = 1
    _progress_current = 0
    _progress_cb = None

    def __init__(self, mode: str, recipe: Recipe, envi: Envi):
        """

        :type mode: str
        :type envi: Envi
        :type recipe: Recipe
        """
        self.envi = envi
        self.mode = mode
        self.recipe = recipe
        self.envi.DATADIR = self.recipe.get("DATADIR")
        self.WORKDIR = self.recipe.get("OUTDIR")
        self.filenames = Filenames(mode, recipe)

    def progress(self, msg, step=1):
        if self._progress_cb:
            self._progress_current += step
            self._progress_cb(self._progress_total, self._progress_current, msg)
        else:
            self.log.info(msg)

    def run(self, progress=None):
        mode = self.mode
        if mode not in ('zone', 'full'):
            self.log.error(f"Unlnowm mode '{mode}' . Allowed: [zone|full]")
            return -1
        recipe = self.recipe
        zone = recipe.get('zone')
        products = recipe['products']

        if mode in ('zone') and zone is None:
            self.log.error('No zone info in recipe')
            return -1

        sigma_avg_names = recipe['channels'].get('sigma_avg', [])
        sigma_names = recipe['channels'].get('sigma', [])
        sigma_vv_names = recipe['channels'].get('sigmaVV', [])
        sigma_vh_names = recipe['channels'].get('sigmaVH', [])

        coh_avg_names = recipe['channels'].get('coh_avg', [])
        coh_names = recipe['channels'].get('coh', [])
        coh_vv_names = recipe['channels'].get('cohVV', [])
        coh_vh_names = recipe['channels'].get('cohVH', [])

        r_names = recipe.get_channel('R')
        g_names = recipe.get_channel('G')
        b_names = recipe.get_channel('B')
        colors = {'R': r_names, 'G': g_names, 'B': b_names}

        channel_names = sigma_names + sigma_avg_names + sigma_vv_names + sigma_vh_names + \
                        coh_names + coh_avg_names + coh_vv_names + coh_vh_names + \
                        r_names + g_names + b_names

        full_shape, envi_header = self.envi.read_header(channel_names[0] + '.hdr')

        band_names = []
        # zone = [[0, 0], [full_shape[0], full_shape[1]]]
        if mode in ('zone'):
            zone = np.array(zone)
            self.log.debug(f"zone: {zone}")
            zone_shape = (zone[1][0] - zone[0][0], zone[1][1] - zone[0][1])
            self.log.info(f'Zone: {zone.reshape(-1)}')
            if zone[1][0] > full_shape[0] or zone[1][0] > full_shape[0] \
                    or zone[0][1] > full_shape[1] or zone[1][1] > full_shape[1]:
                self.log.fatal(f"Zone {zone.reshape(-1)} does not fit into image bounds {full_shape}")
                return
            full_shape = zone_shape
            self.log.info(f'Fitting zone shape: {(full_shape[0], full_shape[1])}')
        file_loader = self.envi.get_file_loader(self.mode, zone)
        nproducts = ((len(sigma_names) if 'sigma' in products else 0) +
                     (1 if 'sigma_avg' in products else 0) +
                     (len(sigma_vv_names) if 'sigma_hypot' in products else 0) +
                     (len(sigma_vv_names) if 'sigma_pol' in products else 0) +

                     (len(coh_names) if 'coh' in products else 0) +
                     (1 if 'coh_avg' in products else 0) +
                     (len(coh_vv_names) if 'coh_hypot' in products else 0) +
                     (len(coh_vv_names) if 'coh_pol' in products else 0) +

                     (len(r_names) if 'R' in products else 0) +
                     (len(g_names) if 'G' in products else 0) +
                     (len(b_names) if 'B' in products else 0)

                     )
        self.log.info(f'Full shape: {(full_shape[0], full_shape[1], nproducts)}')
        tnsr_full = np.empty((full_shape[0], full_shape[1], nproducts), dtype=np.float32)
        bd_full = np.zeros((full_shape[0], full_shape[1]), dtype=np.bool)

        # if mode in ('zone'):
        #     tnsr_zone = np.empty((zone_shape[0], zone_shape[1], nproducts), dtype=np.float32)
        #     bd_zone = np.zeros((zone_shape[0], zone_shape[1]), dtype=np.bool)
        # else:
        #     tnsr_full = np.empty((full_shape[0], full_shape[1], nproducts), dtype=np.float32)
        #     bd_full = np.zeros((full_shape[0], full_shape[1]), dtype=np.bool)
        self._progress_total = nproducts
        self._progress_cb = progress
        product_index = 0
        # todo use self.fileloader to simplify full|zone logic
        if 'sigma' in products:
            # TODO sigma and sigma avg do the same math exept of anisotropic_diffusion filter calling
            """             todo
            for sn in sigma_names+sigma_avg_names
                s = do clip,log,fix_pixels
                if sn in sigma:
                    do anisotropic_diffusion(s)
                    save s in productindex
                is sn in sigma_avg_names:
                    avg_coun ++
                    s_avg += s
                if avg_count:
                    do anisotropic_diffusion(savg_zone)
                    save s_avg in productindex
            if 
            """
            self.log.info(f'sigma assembling started')
            params = products['sigma']
            for sn in sigma_names:
                self.progress(f'sigma: {sn}', 0)
                _st = time.time()
                _stt = time.time()
                self.log.debug(f'#{product_index} sigma {sn}')
                s,_ = file_loader(sn)
                self.log.debug(f'#{product_index} sigma  loaded in {time.time() - _st}')
                _st = time.time()
                bad_data = (s < 1e-6) | (s > 10)
                self.log.debug(f'#{product_index} sigma bad_data in {time.time() - _st}')
                _st = time.time()
                s = np.clip(s, 1e-6, 10)
                s = np.log10(s)
                self.log.debug(f'#{product_index} sigma clamped in {time.time() - _st}')
                _st = time.time()
                self.log.debug(f'#{product_index} sigma fixing  {bad_data.sum()} pixels')
                fix_pixels(s, bad_data)
                # self.log.error('EXITING HERE!!!')
                # return
                self.log.debug(f'#{product_index} sigma fix_pixels in {time.time() - _st}')
                _st = time.time()
                s = anisotropic_diffusion(s, params[0], params[1], 0.2, option=1)
                self.log.debug(f'#{product_index} sigma anisotropic_diffusion in {time.time() - _st}')
                _st = time.time()

                tnsr_full[..., product_index] = s
                bd_full |= bad_data
                self.log.debug(f'#{product_index} sigma bad_data in {time.time() - _st}')
                self.log.info(f'#{product_index} sigma {sn} done in {time.time() - _stt}')
                band_names.append(sn)
                product_index += 1
                self.progress(f'sigma: {sn}')

        if 'sigma_avg' in products:
            self.progress(f'sigma_avg', 0)
            _stt = time.time()
            _st = time.time()
            params = products['sigma_avg']
            savg = np.zeros(full_shape, dtype=np.float32)
            self.log.debug(f'#{product_index} sigma_avg  np.zeros in {time.time() - _st}')

            for sn in sigma_avg_names:
                self.log.debug(f'#{product_index} sigma_avg {sn}')
                s,_ = file_loader(sn)
                self.log.debug(f'#{product_index} sigma_avg loaded in {time.time() - _st}')
                _st = time.time()
                bad_data = (s < 1e-6) | (s > 10)
                s = np.clip(s, 1e-6, 10)
                s = np.log10(s)
                self.log.debug(f'#{product_index} sigma_avg clamp,bad_data  in {time.time() - _st}')
                _st = time.time()
                fix_pixels(s, bad_data)
                self.log.debug(f'#{product_index} sigma_avg fix_pixels in {time.time() - _st}')
                _st = time.time()
                savg += s
                bd_full |= bad_data
                self.log.debug(f'#{product_index} coh_avg avg and bad_data in {time.time() - _st}')

            tnsr_full[..., product_index] = anisotropic_diffusion(savg / len(sigma_avg_names), params[0],
                                                                  params[1], 0.2, option=1)
            self.log.debug(f'#{product_index} sigma_avg anisotropic_diffusion in {time.time() - _st}')
            self.log.info(f'#{product_index} sigma done in {time.time() - _stt}')
            band_names.append('sigma_avg')
            product_index += 1
            self.progress(f'sigma_avg')

        if 'coh' in products:
            params = products['coh']
            for cn in coh_names:
                self.progress(f'coh: {cn}', 0)
                _st = time.time()
                _stt = time.time()
                self.log.debug(f'#{product_index} coh {cn}')
                c = file_loader(cn)[0]
                self.log.debug(f'#{product_index} coh  loaded in {time.time() - _st}')
                _st = time.time()
                bad_data = (c < 0) | (c > 1)
                c = np.clip(c, 0, 1)
                self.log.debug(f'#{product_index} coh clamped in {time.time() - _st}')
                _st = time.time()
                fix_pixels(c, bad_data)
                self.log.debug(f'#{product_index} coh fix_pixels in {time.time() - _st}')
                _st = time.time()
                c = anisotropic_diffusion(c, params[0], params[1], 0.2, option=1)
                self.log.debug(f'#{product_index} coh anisotropic_diffusion in {time.time() - _st}')
                _st = time.time()

                tnsr_full[..., product_index] = c
                bd_full |= bad_data
                product_index += 1
                self.log.debug(f'#{product_index} coh bad_data in {time.time() - _st}')
                self.log.info(f'#{product_index} coh {cn} done in {time.time() - _stt}')
                band_names.append(cn)
                self.progress(f'coh: {cn}')

        if 'coh_avg' in products:
            self.progress(f'coh_avg assembling', 0)
            _stt = time.time()
            _st = time.time()

            cavg_full = np.zeros(full_shape, dtype=np.float32)
            self.log.debug(f'#{product_index} coh_avg  np.zeros in {time.time() - _st}')
            params = products['coh_avg']
            for cn in coh_avg_names:
                self.log.debug(f'#{product_index} coh_avg {cn}')
                c,_ = file_loader(cn)
                self.log.debug(f'#{product_index} coh_avg  loaded in {time.time() - _st}')
                _st = time.time()
                bad_data = (c < 0) | (c > 1)
                c = np.clip(c, 0, 1)
                self.log.debug(f'#{product_index} coh clamp, bad data in {time.time() - _st}')
                _st = time.time()
                fix_pixels(c, bad_data)
                self.log.debug(f'#{product_index} coh_avg fix in {time.time() - _st}')
                _st = time.time()
                cavg_full += c
                bd_full |= bad_data
                self.log.debug(f'#{product_index} coh_avg added to avg in {time.time() - _st}')

            tnsr_full[..., product_index] = anisotropic_diffusion(cavg_full / len(coh_avg_names), params[0],
                                                                  params[1], 0.2, option=1)
            self.log.debug(f'#{product_index} coh_avg anisotropic_diffusion in {time.time() - _st}')
            self.log.info(f'#{product_index} coh_avg done in {time.time() - _stt}')
            product_index += 1
            band_names.append('coh_avg')
            self.progress(f'coh_avg assembled')


        for color in ['R', 'G', 'B']:
            if color in products:
                params = products[color]
                if len(r_names) > 1:
                    self.log.error("Only one file per color channel supported ")
                self.log.debug(f"processing {color} in {r_names[0]}")
                s = self.envi.load(colors[color][0])[0]
                if mode == 'zone':
                    s = s[zone[0][0]:zone[1][0], zone[0][1]:zone[1][1]]
                # TODO set pre-prcess filters here

                tnsr_full[..., product_index] = s
                product_index += 1

        # ########## saving tnsr ##################
        if not os.path.exists(self.WORKDIR):
            os.makedirs(self.WORKDIR)
        self.log.debug("Saving tnsr and bd into %s", self.WORKDIR)

        if mode in ('zone'):
            envi_header['map info'] = header_transform_map_for_zone(envi_header, zoneY=zone[0][0],
                                                                              zoneX=zone[0][1])
        np.save(self.filenames.tnsr, tnsr_full)
        np.save(self.filenames.bd, bd_full)
        envi_header['lines'] = tnsr_full.shape[0]
        envi_header['samples'] = tnsr_full.shape[1]
        envi_header['bands'] = tnsr_full.shape[2]
        envi_header['band names'] = "{" + ",".join(band_names) + "}"
        self.envi.save_dict_to_hdr(self.filenames.tnsr_hdr, envi_header)
        self.log.info('tensors processed')
        # system("say 'assembling complete'")
        return 0
