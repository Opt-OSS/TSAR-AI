import geopandas as gpd
import os
import logging
from shapely.geometry import Polygon

log = logging.getLogger()
ROI_DS = 'roi-1.gpkg'  # roi-VERSION.DriverExt
ROI_DRIVER = 'GPKG'
ROI_PROJ = {'init': 'epsg:4326'}


def get_db(path: str, filename=ROI_DS, file_format=ROI_DRIVER) -> gpd.GeoDataFrame:
    """ loads ROI GeoDataFrame from file, adds ds.file attribute,
    empty GeoDataFrame could not be saved.....

    :param path: path to file directory
    :param filename: file name
    :param file_format: format of file
    :returns GeoDataFrame: GeoDataFrame with file attribute
    """
    _f = os.path.join(path, ROI_DS)
    if os.path.isfile(_f):
        ds = gpd.read_file(_f, driver=ROI_DRIVER)
        ds.file = _f
    else:
        ds = gpd.GeoDataFrame({'name': [],'aoi':[], 'geometry': []})
        ds.crs = ROI_PROJ
        ds.file = _f
        # ds['geometry'] = None
        # ds['name'] = None
        # ds.to_file(_f, driver=ROI_FORMAT)
    return ds


def delete_db(ds: gpd.GeoDataFrame, filename=None):
    """ delete GeoDataFrame file """
    if not filename and not hasattr(ds, 'file'):
        log.debug('Attempting to clear GeoDataFrame without file parameter')
        return
    _f = filename if filename else ds.file
    if not os.path.isfile(_f):
        log.debug(f'File {_f} not fond, nothing to delete')
    else:
        os.unlink(_f)


def save_db(ds: gpd.GeoDataFrame, filename=None):
    if not filename and not hasattr(ds, 'file'):
        log.error('Attempting to save GeoDataFrame without file')
    _f = filename if filename else ds.file
    ds.to_file(_f, driver=ROI_DRIVER)


def get_roi(fname='./roi.geojson') -> Polygon:
    """
    get ROI suitable for using with CREODIAS finder geometry
    """
    _roi = gpd.read_file(fname, driver="geojson")
    _roi = _roi.to_crs(ROI_PROJ)
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
