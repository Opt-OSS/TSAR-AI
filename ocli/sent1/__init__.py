import logging
import os
from datetime import timedelta, datetime

import geopandas as gpd
from pandas._libs.tslibs.timestamps import Timestamp
from shapely.geometry import Polygon

from ocli import logger

# logger.init()
log = logger.getLogger(__name__)


def relative_orbit_number(mission: str, orbitNumber: int) -> int:
    """

    :param mission: S1A | S1B
    :param orbitNumber: Absolute orbit number
    """
    if mission == 'S1A':
        return (orbitNumber - 73) % 175 + 1
    if mission == 'S1B':
        return (orbitNumber - 27) % 175 + 1
    if mission == 'S2B' or mission == 'S2A':
        return 1
    raise ValueError("mission should be one of 'S1A','S1B'")


log.debug("Initint S1")
QPROJ =  'epsg:4326' ## gpd >= 0.7 See https://jorisvandenbossche.github.io/blog/2020/02/11/geopandas-pyproj-crs/
__data_dir = './GeoDataFrame'
__data_filename = os.path.join(__data_dir, 'sar.shp')
__data = gpd.GeoDataFrame(crs=QPROJ)
log.debug("Initint S1 done")


def get_bucket(
        buckets_dir: str,
        mission: str, sensorMode: str, productType: str,
        relativeOrbitNumber: int, startDate: Timestamp) -> str:
    """ returns <bucket_name> (see Readme.md workflow)
        https://sentinel.esa.int/web/sentinel/user-guides/sentinel-1-sar/naming-conventions

    :param mission:  S1A | S1B
    :param sensorMode: IW for S1 SAR
    :param productType: SLS, GRD...
    :param relativeOrbitNumber: that is
    :param startDate: acquisition start Timestamp

    """
    # TODO we don't use buckets cache so no need of buckets_dir
    bucket = None
    bucket_pfx = f"{mission}_{sensorMode}_{productType}_{relativeOrbitNumber}"
    if buckets_dir:
        possible_buckets = [x.split('_') for x in os.listdir(buckets_dir) if
                            os.path.isdir(os.path.join(buckets_dir, x)) and x.startswith(bucket_pfx)]

        _smin, _smax = startDate - timedelta(seconds=24), startDate + timedelta(seconds=24)
        _smin = _smin.hour * 10000 + _smin.minute * 100 + _smin.second
        _smax = _smax.hour * 10000 + _smax.minute * 100 + _smax.second
        bucket = next(('_'.join(x) for x in possible_buckets if _smin < int(x[4]) < _smax), None)
    if bucket is None:
        # todo move boucket name producer to S1 package
        """
         Sentinel 1 relative orbit from filename :  
            https://forum.step.esa.int/t/sentinel-1-relative-orbit-from-filename/7042
         Naming convetion https://sentinel.esa.int/web/sentinel/user-guides/sentinel-1-sar/naming-conventions
         create bucket name as <Mission>_<Instrument>_<productType>_<RelativeOrbitNumber>_<HHMMSS>
         """
        _t = "{0:02d}{1:02d}{2:02d}".format(startDate.hour, startDate.minute, startDate.second)
        bucket = f"{bucket_pfx}_{_t}"
        if buckets_dir is not None:
            bucket = os.path.join(buckets_dir, bucket)
        # os.makedirs(bucket, exist_ok=True)
        # log.info(f"will create new bucket {bucket}")
    else:
        # log.debug(f"use existed bucket {bucket}")
        pass
    return bucket


def get_roi(fname='./roi.geojson'):
    """
    get ROI suitable for using with CREODIAS finder geometry
    """
    try:
        _roi = gpd.read_file(fname, driver="geojson")
        _roi = _roi.to_crs(QPROJ)
        """get 1-st polygon as roi"""
        # TODO merge all polygons in dataframe to create composed olygon
        roi: Polygon = _roi.loc[0, 'geometry']

        # test triangle out of bounds
        # roi = wkt.loads('POLYGON((-6.8175657838583 +12.570648483963055,-6.313293389976025 +11.83079339514586,-5.982604846358299 +12.685358356480464,-6.8175657838583 +12.570648483963055))') # noqa
        """
        test  cover with 2 buckets 2 diff relative orbit numbers
        https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json?maxRecords=10&productType=SLC&processingLevel=LEVEL1&sensorMode=IW&sortParam=startDate&sortOrder=descending&status=all&geometry=POLYGON((-5.240478515624999+13.459613714944382,-5.227294787764548+13.237271948996565,-5.139404162764549+13.1944905040978,-5.240478515624999+13.459613714944382))&dataset=ESA-DATASET
        """
        # roi = wkt.loads(
        #     'POLYGON((-5.240478515624999 +13.459613714944382,-5.227294787764548 +13.237271948996565,-5.139404162764549 +13.1944905040978,-5.240478515624999 +13.459613714944382))')  # noqa
        """
        test 2 cover sar with diff centroids - should produce 2 buckets with the same relative Orbit number
        https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json?collection=Sentinel1&instrument=SAR&productType=SLC&sensorMode=IW&processingLevel=LEVEL1&maxRecords=10&startDate=2019-01-01T00%3A00%3A00Z&sortParam=startDate&sortOrder=descending&status=all&dataset=ESA-DATASET&geometry=POLYGON((-6.462844815105201 +12.365624447661261,-6.444168090820313 +12.294518901214957,-6.156326327472927 +12.350331643440427,-6.191482543945313 +12.416862093420903,-6.462844815105201 +12.365624447661261))
        """
        # roi = wkt.loads('POLYGON((-6.462844815105201 +12.365624447661261,-6.444168090820313 +12.294518901214957,-6.156326327472927 +12.350331643440427,-6.191482543945313 +12.416862093420903,-6.462844815105201 +12.365624447661261))')  # noqa
        return roi
    except Exception as e:
        logging.debug(e)
        logging.fatal(f"Could not get Region of interest file '{fname}' in working directory {os.getcwd()}")
        exit(-1)


def s1_prod_id(title: str):
    """ sentinel product ID extractor from product name """
    if not title.startswith(('S1B', 'S1A','S2A','S2B')):
        raise ValueError("Title should start with ('S1B','S1A')")
    return title[63:63 + 4]


def s1_prod_date(title: str) -> datetime:
    if not title.startswith(('S1B', 'S1A')):
        raise ValueError("Title should start with ('S1B','S1A')")

    """ sentinel competition date extractor extractor from product name """
    return datetime.strptime(title[33:33 + 15], '%Y%m%dT%H%M%S')


def s1_mission(title: str) -> str:
    """ sentinel competition date extractor extractor from product name """
    if not title.startswith(('S1B', 'S1A','S2A','S2B')):
        raise ValueError("Title should start with ('S1B','S1A')")
    return title[:3]

def s1_orbit_number(title: str) -> str:
    """ sentinel competition date extractor extractor from product name """
    if not title.startswith(('S1B', 'S1A','S2A','S2B')):
        raise ValueError("Title should start with ('S1B','S1A')")
    return title[49:49+6]

def parse_title(title:str):
    if not title.startswith(('S1B', 'S1A','S2A','S2B')):
        raise ValueError("Title should start with ('S1B','S1A')")
    m,orbit_number= title[:3],int(title[49:49+6])
    return {
        'platform':m,
        'orbit_number':orbit_number,
        'relative_orbit_number':relative_orbit_number(m,orbit_number),
        'completionDate': datetime.strptime(title[33:33 + 15], '%Y%m%dT%H%M%S'),
        'startDate':datetime.strptime(title[17:17 + 15], '%Y%m%dT%H%M%S'),
    }
