"""
    DIAS Finder API (see Readme)
"""
import json
import os
from typing import Dict
from urllib.parse import quote, urlencode

import geopandas as gpd
import pandas
import requests
from dateutil.parser import parse
from requests import Request
from requests.adapters import HTTPAdapter
from shapely.geometry import Polygon
from urllib3 import Retry

from ocli import logger, sent1
from ocli.sent1 import relative_orbit_number, QPROJ, s1_prod_id

# logger.init()
log = logger.getLogger(__name__)

'''set output width to max'''
pandas.set_option('display.max_colwidth', None)

''' WGS84'''
# QPROJ = pycrs.parse.from_epsg_code(4326).to_proj4()

payload_defautl_dias = {
    'collection': 'Sentinel1',
    'instrument': 'SAR',
    'productType': 'SLC',
    'sensorMode': 'IW',
    'processingLevel': 'LEVEL1',
    'maxRecords': 100,  # TODO pass in CLI
    # 'completionDate': '2019-05-13T00:00:00Z',
    'startDate': '2019-01-01T00:00:00Z',
    'sortParam': 'startDate',
    'sortOrder': 'descending',
    'status': 'all',
    # 'geometry': roi.wkt,
    'dataset': 'ESA-DATASET'
}
"""
DIAS Finder params
"""


def fix_esri_names(data):
    """
    GeoDataFrame Why limited the length of field to 10 https://github.com/GeospatialPython/pyshp/issues/2
    """
    d = {'cloudCover': 'cloudCover',
         'collection': 'collection',
         'completion': 'completionDate',
         'descriptio': 'description',
         'relativeOr': 'relativeOrbitNumber',
         'instrument': 'instrument',
         'missionTak': 'missionTakeId',
         'orbitDirec': 'orbitDirection',
         'orbitNumbe': 'orbitNumber',
         'organisati': 'organisationName',
         'parentIden': 'parentIdentifier',
         'platform': 'platform',
         'polarisati': 'polarisation',
         'processing': 'processingLevel',
         'productIde': 'productIdentifier',
         'productTyp': 'productType',
         'published': 'published',
         'quicklook': 'quicklook',
         'resolution': 'resolution',
         'sensorMode': 'sensorMode',
         'snowCover': 'snowCover',
         'startDate': 'startDate',
         'status': 'status',
         'swath': 'swath',
         'thumbnail': 'thumbnail',
         'title': 'title',
         'updated': 'updated',
         'processed': 'processed',
         }
    data.rename(index=str, columns=d, inplace=True)


def pp_json(json_thing, sort=False, indents=4):
    if type(json_thing) is str:
        print(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    else:
        print(json.dumps(json_thing, sort_keys=sort, indent=indents))
    return None


def retry_session():
    # This will give the total wait time in minutes:
    # >>> sum([min((0.3 * (2 ** (i - 1))), 120) / 60 for i in range(24)])
    # >>> 30.5575
    # This works by the using the minimum time in seconds of the backoff time
    # and the max back off time which defaults to 120 seconds. The backoff time
    # increases after every failed attempt.
    session = requests.Session()
    retry = Retry(
        total=3,  # 24
        read=5,
        connect=3,  # 24
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        method_whitelist=('GET', 'POST'),
    )
    adapter = HTTPAdapter(max_retries=retry)
    # session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


default_cols = {
    'polarisation': 'NA',
    'swath': 'NA',
    'cloudCover': 0,
    'snowCover': 0,
}


def add_data(res: Dict):
    gdf = gpd.GeoDataFrame(crs=QPROJ)
    ''' You can pass the json directly to the GeoDataFrame constructor: '''
    gdf = gdf.from_features(res['features'], crs=QPROJ)
    gdf['productId'] = gdf['title'].apply(s1_prod_id)
    gdf['processed'] = False
    gdf['relativeOrbitNumber'] = gdf.apply(lambda x: relative_orbit_number(x['platform'], x['orbitNumber']), axis=1)
    # Add missed data

    cols = gdf.columns.tolist()

    for k, v in default_cols.items():
        if not k in cols:
            gdf[k] = v

    return gpd.GeoDataFrame(pandas.concat([sent1.__data, gdf], ignore_index=True), crs=QPROJ)


def data_witening(data):
    """ remove incompatible and not important data from frame """
    _no_shp_columms = ['centroid', 'license', 'links', 'services', 'keywords', 'gmlgeometry']
    data.drop(_no_shp_columms, axis=1, inplace=True)

    # [data.drop(x, axis=1, inplace=True) for x in _no_shp_columms]
    # pass


def __getRequest(payload):
    payload = dict({**payload_defautl_dias, **payload})
    collection = payload.pop('collection', 'Sentinel1')
    qry = urlencode(payload, quote_via=quote)
    # TODO check collection is valid for payload.source
    p = Request('GET', f'https://finder.creodias.eu/resto/api/collections/{collection}/search.json?' + qry,
                # params=payload
                ).prepare()
    return p


def fix_esri(data: gpd.GeoDataFrame):
    """ fix fiona.errors.DriverSupportError: ESRI Shapefile does not support datetime fields
    save dates as string, but return dataset with DateTime

    :param data: GeoDataFrame
    :return: dataframe with converted str->DateTime
    """
    fix_esri_names(data)
    data['completionDate'] = data['completionDate'].apply(parse)
    data['startDate'] = data['startDate'].apply(parse)
    data['updated'] = data['updated'].apply(parse)



def __load_data(roi: Polygon, finder_conf={}, callback=None, cache_file_name=None):
    finder_conf['geometry'] = roi.wkt
    log.debug(roi.wkt)
    log.debug(f"searching {finder_conf}")
    try:
        p = __getRequest(finder_conf)
        while True:
            log.debug(f"Downloading {p.url}")
            r = retry_session().send(p)
            res = r.json()

            if res['properties']['itemsPerPage']:
                if callback is not None:
                    callback(res['properties']['totalResults'], res['properties']['itemsPerPage'])
                sent1.__data = add_data(res)
            """ check we have more data to GET """
            _next = next((x for x in res['properties']['links'] if x.get('rel') == 'next'), None)
            if _next is None:
                break
            p = Request('GET', _next.get('href')).prepare()
        if not sent1.__data.empty:
            data_witening(sent1.__data)
            if cache_file_name:
                os.makedirs(os.path.dirname(cache_file_name), exist_ok=True)
                log.debug(f'updating cache {cache_file_name}')
                sent1.__data.to_file(cache_file_name)
            fix_esri(sent1.__data)
        return sent1.__data
    except Exception as e:
        log.error(f'{e.__class__}{e}')
        raise RuntimeError(e)


def load_from_cache(cache_file_name, geometry=None) -> gpd.GeoDataFrame:
    # TODO return warning if cache is not for geometry
    try:
        # log.error(cache_file_name)
        data = gpd.GeoDataFrame.from_file(cache_file_name)
        valid = False
        try:
            with open(os.path.join(cache_file_name, '.roi'), 'r') as _f:
                _md = _f.read()
                valid = geometry.wkt == _md
        except OSError:
            pass
        if not valid:
            pass
            # log.error("_md is invalid")
        fix_esri(data)
        return data
    except Exception as e:
        log.fatal(e)
        raise RuntimeError(e)


def load_data(geometry: Polygon, reload=True, callback=None, finder_conf={}, cache_file_name=sent1.__data_filename):
    if reload or not os.path.isfile(cache_file_name):
        data = __load_data(geometry,
                           callback=callback,
                           finder_conf=finder_conf,
                           cache_file_name=cache_file_name)
    else:
        data = load_from_cache(cache_file_name, geometry)
    return data
