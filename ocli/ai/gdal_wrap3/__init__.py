import logging
import math
import os
import json
from datetime import datetime, timezone
from typing import List

from osgeo import gdal
from osgeo.gdalconst import GA_ReadOnly, GCI_GrayIndex

from ocli.ai.recipe import Recipe

MIN_RECIPE_VER = 1.3
def _get_zoom(resolution: float):
    return math.ceil(
        math.log((2 * math.pi * 6378137) /
                 (resolution * 256), 2))

def get_zoom_levels(getGeoTransform:List,RasterXSize:int,RasterYSize:int)->List[int]:
    resolution = max(abs(getGeoTransform[1]), abs(getGeoTransform[5]))
    zoom = _get_zoom(resolution)
    # log.error(zoom)
    levels = []
    for z in range(1, zoom):
        l = int(2 ** z)
        levels.append(l)
        # stop when overviews fit within a single block (even if they cross)
        if RasterXSize / l < 512 and RasterYSize / l < 512:
            break
    # log.error(levels)
    return levels

log = logging.getLogger()
class GDALWrap3(object):
    log = logging.getLogger(__name__)

    def __init__(self, recipe: Recipe, input_file: str, out_file: str, cog_file: str) -> None:
        """         GDAL transformation for visulize output file

        :type recipe: Recipe
        :type cog_file: str
        :type input_file: str
        :type out_file: str
        """
        self.recipe = recipe
        self.version = self.recipe.get('version', 1.0)
        if self.version < MIN_RECIPE_VER:
            raise AttributeError(f"Min recipe version supported {MIN_RECIPE_VER}")

        self.cog_file = cog_file
        self.out_file = out_file
        self.input_file = input_file

    def error_handler(self, err_level, err_no, err_msg):
        self.log.warning(f"{err_level}: {err_no} : {err_msg}")



    def _map_bands_1_3(self, gdal_info):
        bm = self.recipe.get('band_meta', [])

        bands = gdal_info['bands']
        geojson_bands = {'band_ids': [], 'band_meta': []}
        # self.log.info(bands)
        # self.log.info(self.recipe.get('band_meta', []))
        # self.log.error(bands)
        if len(bands):
            for b in bands:
                if 'description' in b:
                    d = b['description']
                elif 'colorInterpretation' in b:
                    d = b['colorInterpretation']
                else:
                    d = "U"
                geojson_bands['band_ids'].append(d)
                try:
                    _bname = next(filter(lambda x: x['band'] == b['band'], bm))
                    geojson_bands['band_meta'].append(_bname)
                except StopIteration:
                    pass
        return geojson_bands

    def _map_bands(self, gdal_info):
        return self._map_bands_1_3(gdal_info)

    def _compose_tangram_url(self,gdal_info):
        """
        compose source property for tangram renderer
        :return: Dict
        """
        _p = self.recipe['GEOJSON']
        _c = self.recipe['COS']
        _s = {
            "type": _p['type'],  # required in schema
            "url": _p['url'],  # required in schema

        }
        _gt = gdal_info['geoTransform']
        RasterXSize,RasterYSize = gdal_info['size']
        zooms = get_zoom_levels(_gt,RasterXSize,RasterYSize)

        # optional fields
        # _opt = ['url_subdomains', 'max_zoom']
        if _p.get('url_subdomains') is not None:
            _s['url_subdomains'] = _p.get('url_subdomains')
        # _s['zooms'] = zooms
        # _s['max_zoom'] = _p.get('max_zoom', 32) # TODO add tabngram max-zoom, maz-display-zoom zooms
        _s['tileSize'] = _p.get('tileSize', 512)
        # compose tiler url
        _s['url_params'] = _p.get('url_params', {})
        if 'url' in _s['url_params']:
            # append bucket/resultkey
            _s['url_params']['url'] += '/' + _c['bucket'] + '/' + _c['ResultKey']
        else:
            # build full url
            _s['url_params']['url'] = _c['endpoint'] + '/' + _c['bucket'] + '/' + _c['ResultKey']

        return _s

    def make_geo_json(self):
        try:
            if "COS" not in self.recipe or "GEOJSON" not in self.recipe or "ResultKey" not in self.recipe["COS"]:
                self.log.warning("recipe not configured for Geo-json generation")
                return
            str = gdal.Info(self.cog_file, format='json')
            """ wgs84Extent has wrong coordinates order (Batterfly like but we wants square )
            so use cornerCoordinates of GDALinfo
            """
            # log.error(str)

            cos = self.recipe['COS']
            endpoint = cos.get('endpoint')
            bucket = cos.get('bucket')
            result_key = cos.get('ResultKey')

            if endpoint is None or bucket is None or result_key is None:
                self.log.error("COS endpoint, bucket and resultKey are required ")
                return
            if not result_key.endswith('.tiff'):
                cos['ResultKey'] += '.tiff'
                # self.log.error(result_key)
            # geo_url = self.recipe["GEOJSON"]["COGURL"] + endpoint + '/' + bucket + '/' + result_key
            url_schema = self.recipe["GEOJSON"]["schema"]
            if url_schema == 'Tangram':
                geo_url = self._compose_tangram_url(str)
            else:
                raise AttributeError(f'Unknown goeojson url schema {url_schema}')

            coords = str["wgs84Extent"]
            tags = self.recipe['tag'] if 'tag' in self.recipe else []
            properties = {
                "version": self.version,
                "friendly_name": self.recipe['friendly_name'],
                "type": self.recipe['type'],
                "ResultKey": result_key,
                "bands": self._map_bands(str),
                "tag": tags,
                "source": geo_url,
            }

            geojson = {
                "type": "Feature",
                "properties": properties,
                "geometry": coords,
                "created": datetime.now(timezone.utc).strftime('%F %T%z')
            }
            if self.recipe['version'] < 1.4:
                if 'kind' in self.recipe:
                    if self.recipe['kind'] == 'rvi':
                        geojson['kind'] = 'orvi'
                    else:
                        geojson['kind'] = self.recipe['type'].lower()
                geojson['class'] = 'S1'
            else:
                geojson['kind'] = self.recipe['kind']
                geojson['class'] = self.recipe['class']
            if 'meta' in self.recipe:
                geojson['meta'] = self.recipe['meta']
            _json = json.dumps(geojson, indent=4)
            with open(self.cog_file + '.geojson', 'w') as _f:
                _f.write(_json)
                _f.write("\n")
            self.log.info("generated file info %s", self.cog_file + '.geojson')
            return _json
        except (BaseException, AttributeError) as e:
            self.log.exception(e)
            self.log.error(f"cold not save geojson: {e}")
            # self.log.exception("cold not save geojson")

    def translate_callback(self, pct, msg, user_data):
        _ud = user_data[0]
        # self.log.debug(f"translaing: {round(pct * 100, 2)}% -{msg}- {user_data} {pct}")
        # reset counter, this t\callback called from multiple prcesses
        if pct < 0.01:
            user_data[0] = 0
        if user_data[0] == 0 or pct - _ud > 0.10:
            self.log.debug(f"translating : {round(pct * 100, 2)}%")
            user_data[0] = pct

    def warp_callback(self, pct, msg, user_data):
        self.log.info(f"overvies: warp_callback '{msg}' {round(pct * 100, 2)}%")

    # noinspection PyUnresolvedReferences
    # @profile
    def make_cog(self, callback=None, warp_resampleAlg='near', overview_resampleAlg='nearest'):
        _callback = callback if callback else self.translate_callback
        if not os.path.isfile(self.input_file):
            raise AssertionError("File does not not exists! %s", self.input_file)
        if os.path.isfile(self.cog_file):
            os.unlink(self.cog_file)
        gdal.PushErrorHandler(self.error_handler)
        gdal.UseExceptions()
        # gdal.PushErrorHandler('CPLQuietErrorHandler')
        self.log.debug('reading.....')
        # gdal.SetConfigOption('CPL_LOG', 'OFF')  # redirect messages to error_handler
        gdal.SetConfigOption('NUM_THREADS', 'ALL_CPUS')
        # gdal.SetConfigOption('GDAL_NUM_THREADS', '10') #causes core-dump on Ubuntu 16
        gdal.SetConfigOption('NUM_THREADS_OVERVIEW', 'ALL_CPUS')
        # gdal.SetConfigOption('COMPRESS_OVERVIEW', 'WEBP')
        # gdal.SetConfigOption('COMPRESS', 'DEFLATE')

        gdal.SetConfigOption('BLOCKXSIZE', '512')
        gdal.SetConfigOption('BLOCKYSIZE', '512')
        gdal.SetConfigOption('TILED', 'YES')
        # gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE', '512')
        # gdal.SetConfigOption('TILED_OVERVIEW', 'NO')
        # gdal.SetConfigOption('BLOCKXSIZE_OVERVIEW', 'YES')
        # gdal.SetConfigOption('BLOCKYSIZE_OVERVIEW', 'YES')
        # gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE ', '512')

        # gdal.SetConfigOption('STREAMABLE_OUTPUT', 'YES')
        ds = gdal.Open(self.input_file, GA_ReadOnly)  # type: gdal.Dataset
        ttab = [0, 0]
        for i in range(1, ds.RasterCount):
            b = ds.GetRasterBand(i)  # type: gdal.Band
            b.SetColorInterpretation(GCI_GrayIndex)
        # 1, w.gdaltranslate()
        op_t_1 = gdal.TranslateOptions(
            callback=_callback,
            callback_data=ttab,
            format='GTiff',
            noData=0,
            # outputSRS='EPSG:3857',
            creationOptions=[
                'INTERLEAVE=BAND',
                'COMPRESS=DEFLATE',
                # 'COMPRESS_OVERVIEW=DEFLATE',
                # 'ZLEVEL=5',
                'NUM_THREADS=ALL_CPUS',
                # 'NUM_THREADS_OVERVIEW=ALL_CPUS',
                'BIGTIFF=IF_SAFER',
                'TILED=YES',
                'BLOCKXSIZE=512',
                'BLOCKYSIZE=512',
                # 'COPY_SRC_OVERVIEWS=YES',
            ]

        )
        """ 
        # TODO Warp produces artefacts an pillowed tiles
        #
        # For now we publish COG with WGS-84 CRS so marblecutter tiler wraps it for us  
        #
        #warop_opt = gdal.WarpOptions(
        #    callback=_callback,
        #    callback_data=ttab,
        #    multithread=True,
        #    dstSRS='EPSG:3857',
        #    format='VRT',
        #    creationOptions=[
        #        'NUM_THREADS=ALL_CPUS'
        #    ]
        #)
        #self.log.debug('warping.....')
        #dsw = gdal.Warp(
        #     resampleAlg=warp_resampleAlg,
        #     destNameOrDestDS=self.cog_file + '.vrt',
        #     srcDSOrSrcDSTab=ds,
        #     options=warop_opt
        #)  # type: gdal.Dataset
        """
        dsw = ds

        # calc overviews (see get_zoom,get_resolution form marblecuter-tools git)
        '''
        refer README.md COG calcs    
        '''

        # _gt = dsw.GetGeoTransform()
        # resolution = max(abs(_gt[1]), abs(_gt[5]))
        # zoom = self.get_zoom(resolution)
        # levels = []
        # for z in range(1, zoom):
        #     l = int(2 ** z)
        #     levels.append(l)
        #     # stop when overviews fit within a single block (even if they cross)
        #     if dsw.RasterXSize / l < 512 and dsw.RasterYSize / l < 512:
        #         break
        levels = get_zoom_levels(dsw.GetGeoTransform(),dsw.RasterXSize,dsw.RasterYSize)
        self.log.debug(f"Overviews resolution levels {levels}")
        # dsw.BuildOverviews(overview_resampleAlg, levels, callback=self.warp_callback)
        # dsw.BuildOverviews("NEAREST", levels, callback=self.warp_callback)

        self.log.debug('translating.....')

        cds = gdal.Translate(self.cog_file, dsw, options=op_t_1)  # type: gdal.Dataset
        cds.BuildOverviews(overview_resampleAlg, levels, callback=self.warp_callback)
        self.log.debug('flushing caches.....')

        cds.FlushCache()
        # _info = gdal.Info(cds, format='json')  # use json to get dict
        self.log.debug('Done.')
        return True
