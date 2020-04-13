import logging
import os
import time
from glob import glob
from typing import Optional, Dict, Union

import affine
import geopandas as gpd
import numpy as np
# from botocore.exceptions import CredentialRetrievalError
from cachetools.func import ttl_cache
from botocore.exceptions import ClientError, CredentialRetrievalError

# from ocli.ai.COS.ibm_boto3 import COS
from ocli.ai.COS.s3_boto import COS
from ocli.ai.recipe import Recipe
from ocli.ai.util import zone_slice

datatypeDict = {1: np.uint8, 2: np.int16, 3: np.int32, 4: np.float32, 5: np.float64, 6: np.complex64, 9: np.complex128,
                12: np.uint16, 13: np.uint32, 14: np.int64, 15: np.uint64}
datatypeDictInv = {np.dtype(v).name: k for k, v in datatypeDict.items()}

log = logging.getLogger('ENVI')


class Envi(object):
    """AI file operations (downloads missed files from  COS)"""
    log = log

    def __init__(self, recipe: Union[Recipe, Dict], cos: Optional[COS]):
        self.cos = cos
        self.DATADIR = recipe.get("DATADIR")

    @ttl_cache(maxsize=128, ttl=600, timer=time.time, typed=False)
    def object_etag(self, file):
        if not self.cos or self.cos.resource is None:
            self.log.warning("COS is not valid")
            return None
        object_summary = self.cos.resource.ObjectSummary(self.cos.bucket, file)
        try:
            # fetch data
            return object_summary.e_tag
        except (ClientError, CredentialRetrievalError) as e:
            self.log.critical(e)
            return None

    def cache_cos(self, file: str, dir: str):
        full_name = os.path.join(dir, file)

        if os.path.isfile(full_name):
            #self.log.debug(f"Local file  '{full_name}' found ")
            return full_name
        else:
            pass
            #self.log.debug(f"Local file '{full_name}' found")
        # todo - cache etags, download if needed
        if not self.cos:
            # No COS, local only not fond
            self.log.debug(f"No COS configured")
            return False
        if self.object_etag(file) is None:
            raise AssertionError(f"File '{file}' does not exists in COS")
        try:
            start_time = time.time()
            self.log.info('Starting download %s into %s', file, full_name)
            self.cos.resource.Bucket(self.cos.bucket).download_file(file, full_name)
            self.log.info('Done download %s in %s seconds', file, time.time() - start_time)
        except (ClientError, CredentialRetrievalError) as e:
            self.log.critical(e)
            return False
        return full_name

    def load(self, l_path: str):
        imshape, hdict = self.read_header(l_path + '.hdr', is_fullpath=False)
        if int(hdict['bands']) != 1:
            self.log.debug(hdict)
            raise AssertionError(f"Full file load '{l_path}': Multibands not implemented")

        datatype = datatypeDict[int(hdict['data type'])]
        load_datatype = datatype
        if int(hdict['byte order']):
            load_datatype = np.dtype(datatype).newbyteorder('>')
        file_img = self.cache_cos(l_path + '.img', self.DATADIR)
        if not file_img:
            self.log.error('Could not load %s', file_img)
            exit(-1)
        arr = np.fromfile(file_img, load_datatype).astype(datatype)
        arr = arr.reshape(imshape)
        np.nan_to_num(arr, copy=False)
        return arr, hdict

    def get_file_loader(self, mode, zone=None):
        _self = self
        if mode == 'zone' and zone is not None:
            def _load_zone(fname):
                return self.load_zone(zone, fname)

            return _load_zone
        else:
            return self.load

    def load_mmap(self, l_path: str):
        imshape, hdict = self.read_header(l_path + '.hdr', is_fullpath=False)
        if int(hdict['bands']) != 1:
            raise AssertionError(f"Memmap load file '{l_path}': Multibands not implemented")

        load_datatype = datatypeDict[int(hdict['data type'])]
        if int(hdict['byte order']):
            load_datatype = np.dtype(load_datatype).newbyteorder('>')
        file_img = self.cache_cos(l_path + '.img', self.path)
        if not file_img:
            self.log.error('Could not load %s', file_img)
            exit(-1)
        arr = np.memmap(file_img, dtype=load_datatype, mode='r', shape=imshape)
        return arr, hdict

    def load_zone(self, zone, l_path: str):
        """memory effective zone loader"""
        # todo use self.read_header here
        imshape, hdict = self.read_header(l_path + '.hdr', is_fullpath=False)
        if int(hdict['bands']) != 1:
            raise AssertionError(f"Zone load file '{l_path}': Multibands not implemented")

        datatype = datatypeDict[int(hdict['data type'])]
        load_datatype = datatype
        if int(hdict['byte order']):
            load_datatype = np.dtype(datatype).newbyteorder('>')
        file_img = self.cache_cos(l_path + '.img', self.DATADIR)
        if not file_img:
            self.log.error('Could not load %s', file_img)
            exit(-1)
        self.log.debug(f"zone loaded: {load_datatype} as {datatype}")
        arr = np.memmap(file_img, dtype=load_datatype, mode='r', shape=imshape)
        # arr = np.fromfile(file_img, load_datatype).astype(datatype)
        a2 = zone_slice(zone, arr).astype(datatype)
        del arr
        np.nan_to_num(a2, copy=False)
        return a2, hdict

    def read_header(self, l_path, is_fullpath=False):
        datadir = '' if is_fullpath else self.DATADIR
        file_hdr = self.cache_cos(l_path, datadir)

        if not file_hdr:
            raise AssertionError('Could not load %s', l_path)
        with open(file_hdr, 'r') as header:
            mergedlines = []
            bracecount = 0
            for line in header.readlines():
                line = line.strip(' \t\n\r')
                assert(bracecount >= 0)
                if bracecount:
                    mergedlines[-1] += line
                else:
                    mergedlines.append(line)
                bracecount += line.count('{') - line.count('}')
            try:
                hdict = dict([tuple(el.strip(' \t') for el in  val.split('=', 1)) for val in mergedlines[1:] if val])
            except ValueError as e:
                raise AssertionError(f'file {file_hdr} ENVI field is invalid: {e}')

            # print(f"--------------- {l_path } ---------")
            # pprint(hdict)

            # assert(int(hdict['bands']) == 1)
            imshape = (int(hdict['lines']), int(hdict['samples']))
            return imshape, hdict

    def save_dict_to_hdr(self, fname, dict):
        with open(fname, 'w') as hdr:
            hdr.write("ENVI\n")
            hdr.write("\n".join("{!s} = {!s}".format(key, val) for (key, val) in dict.items()))
            hdr.write("\n")

    def save(self, l_path, arr, map_info, coord_string, chnames='my_ch_name', desc='my description', interleave='bip'):
        path = l_path
        nbands = 0
        if len(arr.shape) == 2:
            nbands = 1
            ch_shape = arr.shape
        elif len(arr.shape) == 3:
            if interleave == 'bip':
                nbands = arr.shape[2]
                ch_shape = arr.shape[:2]
            elif interleave == 'bsq':
                nbands = arr.shape[0]
                ch_shape = arr.shape[1:]
            else:
                print('Not Implemented')
                exit(-1)
        else:
            print('Wrong arr shape')
            exit(-1)

        hdr = open(path + '.hdr', 'w')
        hdr.write('ENVI\n')
        hdr.write('description = ' + desc + '\n')
        hdr.write('samples = ' + str(ch_shape[1]) + '\n')
        hdr.write('lines = ' + str(ch_shape[0]) + '\n')
        hdr.write('bands = ' + str(nbands) + '\n')
        hdr.write('header offset = 0\n')
        hdr.write('file type = ENVI Standard\n')
        hdr.write('data type = ' + str(datatypeDictInv[arr.dtype.name]) + '\n')
        hdr.write('interleave = ' + interleave + '\n')
        hdr.write('byte order = 0\n')

        if isinstance(chnames, str):
            hdr.write('band names = { ' + chnames + ' }\n')
        elif isinstance(chnames, list):
            hdr.write('band names = { ' + ', '.join(chnames) + ' }\n')
        else:
            print('Wrong chnames format')
            exit(-1)

        hdr.write('map info = ' + map_info + '\n')
        hdr.write('coordinate system string = ' + coord_string + '\n')
        hdr.close()

        arr.tofile(path + '.img')

    def locate(self, l_path='./'):
        return [os.path.split(os.path.splitext(el)[0])[1] for el in glob(os.path.join(l_path, '*.hdr'))]


def header_transform_map_for_zone(hdict, zoneY, zoneX):
    """ https://gis.stackexchange.com/questions/42790/gdal-and-python-how-to-get-coordinates-for-all-cells-having-a-specific-value
    Map info fields:
    Lists geographic information in the following order:

    0: Projection name : Geographic Lat/Lon
    1: Reference (tie point) pixel x location (in file coordinates) : 5727.0
    2: Reference (tie point) pixel y location (in file coordinates) : 1378.0
    3: Pixel easting : 6.09426834145908
    4: Pixel northing : 53.28197899462582
    5: x pixel size : 1.2504548754943743E-4
    6: y pixel size : 1.2504548754943743E-4
    Projection zone (UTM only) : None
    North or South (UTM only) : None
    Datum : WGS84
    Units : units=Degrees

    from https://github.com/OSGeo/gdal/blob/b1c9c12ad373e40b955162b45d704070d4ebf7b0/gdal/frmts/raw/envidataset.cpp
    ```c
    adfGeoTransform[0] = pixelEasting - (xReference - 1) * xPixelSize;
    adfGeoTransform[1] = cos(dfRotation) * xPixelSize;
    adfGeoTransform[2] = -sin(dfRotation) * xPixelSize;
    adfGeoTransform[3] = pixelNorthing + (yReference - 1) * yPixelSize;
    adfGeoTransform[4] = -sin(dfRotation) * yPixelSize;
    adfGeoTransform[5] = -cos(dfRotation) * yPixelSize;
    ```
    """
    map_info = hdict['map info'].split(',')
    xReference = float(map_info[1])
    yReference = float(map_info[2])
    pixelEasting = float(map_info[3])
    pixelNorthing = float(map_info[4])
    xPixelSize = float(map_info[5])
    yPixelSize = -1 * float(map_info[6])
    log.debug(f"PXSize {xPixelSize},{yPixelSize}")
    upper_left_x = pixelEasting - (xReference - 1) * xPixelSize
    upper_left_y = pixelNorthing - (yReference - 1) * yPixelSize
    log.debug(
        f"ENVI computed Origin=({upper_left_x},{upper_left_y}) by  Center=({pixelEasting},{pixelNorthing}), ")
    # TODO do we need +pixelSize/2
    zoneOriginX = zoneX * xPixelSize + upper_left_x  # + xPixelSize / 2
    zoneOriginY = zoneY * yPixelSize + upper_left_y  # + yPixelSize / 2
    log.debug(f"ENVI ZoneOrigin=({zoneOriginX},{zoneOriginY})")
    map_info[1] = '1'  # move reference to new
    map_info[2] = '1'  # uper left (as GDAL doing)
    map_info[3] = str(zoneOriginX)  # ser tie point to
    map_info[4] = str(zoneOriginY)  # new lon/lat origin
    return ','.join(map_info)


def pixelsToCoordAffine(dx, dy, geoTransform):
    # print("Hello")
    forward_transform = affine.Affine.from_gdal(*geoTransform)
    px, py = forward_transform * (dx, dy)
    return px, py


def pixelsToCoord(fname, dx, dy):
    from osgeo import gdal
    from osgeo.gdalconst import GA_ReadOnly
    file = gdal.Open(fname, GA_ReadOnly)

    if not file:
        raise AssertionError(f"Could not read '{file}'")
    # GDAL affine transform parameters, According to gdal documentation xoff/yoff are image left corner, a/e are pixel wight/height and b/d is rotation and is zero if image is north up.
    # xoff, a, b, yoff, d, e = file.GetGeoTransform()

    forward_transform = affine.Affine.from_gdal(*file.GetGeoTransform())
    px, py = forward_transform * (dx, dy)
    return px, py


def coordToPixels(fname, dx, dy):
    from osgeo import gdal
    from osgeo.gdalconst import GA_ReadOnly
    file = gdal.Open(fname, GA_ReadOnly)

    if not file:
        raise AssertionError(f"Could not read '{file}'")
    # GDAL affine transform parameters, According to gdal documentation xoff/yoff are image left corner, a/e are pixel wight/height and b/d is rotation and is zero if image is north up.
    # xoff, a, b, yoff, d, e = file.GetGeoTransform()

    forward_transform = affine.Affine.from_gdal(*file.GetGeoTransform())
    reverse_transform = ~forward_transform
    px, py = reverse_transform * (dx, dy)
    px, py = int(px + 0.5), int(py + 0.5)
    return px, py


def getProj4AndRes(fname):
    from osgeo import gdal, osr
    from osgeo.gdalconst import GA_ReadOnly
    file: gdal.Dataset = gdal.Open(fname, GA_ReadOnly)

    if not file:
        raise AssertionError(f"Could not read '{file}'")
    srs = osr.SpatialReference(wkt=file.GetProjection())
    return file.RasterXSize, file.RasterYSize, srs.ExportToProj4()


def zoneByRoi(fname, roi, roi_crs) -> tuple:
    df = gpd.GeoDataFrame([roi], crs=roi_crs)
    rwidth, rheigh, prj = getProj4AndRes(fname)
    df = df.to_crs(prj)
    minx, miny, maxx, maxy = df.iloc[0].geometry.bounds
    log.info(f"GDAL info for roi {roi['name']} WxH={rwidth}x{rheigh} bounds {(minx, miny, maxx, maxy)} '{prj}'")
    minx, miny = coordToPixels(fname, minx, miny)
    maxx, maxy = coordToPixels(fname, maxx, maxy)
    log.debug(f"un-clipped zone  roi {roi['name']} ({miny} {minx} {maxy} {maxx})")
    cx = gpd.np.clip([minx, maxx], 0, rwidth).astype(gpd.np.uint32)
    cy = gpd.np.clip([miny, maxy], 0, rheigh).astype(gpd.np.uint32)
    minx, maxx, miny, maxy = min(cx), max(cx), min(cy), max(cy)  # rearange Geo-min to pixels-min
    log.debug(f"Clipped zone  roi {roi['name']} ({miny} {minx} {maxy} {maxx})")
    zone = (int(miny), int(minx), int(maxy), int(maxx))
    return zone
