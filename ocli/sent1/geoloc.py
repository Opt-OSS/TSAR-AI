import os
from glob import glob
from xml.dom.minidom import parse as xml_parse

import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon, MultiPoint


def xmlGetByPath(dom, path):
    nd = dom
    for el in path.split('/'):
        if el == '':
            continue
        nd = nd.getElementsByTagName(el)[0]
    return nd


def get_geoloc(dom):
    ggrid = xmlGetByPath(dom, '/product/geolocationGrid/geolocationGridPointList')
    garr = []
    for node in ggrid.getElementsByTagName('geolocationGridPoint'):
        l = int(node.getElementsByTagName('line')[0].childNodes[0].nodeValue)
        p = int(node.getElementsByTagName('pixel')[0].childNodes[0].nodeValue)
        lat = float(node.getElementsByTagName('latitude')[0].childNodes[0].nodeValue)
        lon = float(node.getElementsByTagName('longitude')[0].childNodes[0].nodeValue)
        garr.append((l, p, lon, lat))
    garr = np.array(garr)
    agrd = np.unique(garr[:, 0])
    rgrd = np.unique(garr[:, 1])
    garr = garr.reshape((agrd.size, rgrd.size, 4))
    assert (np.all(garr[..., 0] == garr[:, 0, 0][:, np.newaxis]))
    assert (np.all(garr[..., 1] == garr[0, :, 1][np.newaxis, :]))
    # swath = Polygon([garr[0, 0, 2:],
    #                  garr[0, -1, 2:],
    #                  garr[-1, -1, 2:],
    #                  garr[-1, 0, 2:],
    #
    #                  ])
    # LatLon = namedtuple('LatLon', ('lat', 'lon'))
    # bbox = (LatLon(garr[..., 2].min(), garr[..., 3].min()),
    #         LatLon(garr[..., 2].max(), garr[..., 3].max()))

    bursts = []
    for _y in range(garr.shape[0] - 1):
        _burst = garr[_y:_y + 2].reshape(-1, 4)
        bursts.append(MultiPoint(_burst[:, 2:4]))
    df = gpd.GeoDataFrame(geometry=bursts, crs={'init': 'epsg:4326'})
    df['geometry'] = df.convex_hull
    df.index += 1  # make bursts start from 1
    # return bbox, swath, df
    return df


def swath_table(path: str, roi: Polygon) -> 'gpd.pd.DataFrame':
    """ create Dataframe with Index as burst_id:
            IW1 burst's Polygon
            IW1_fit burst/roi intersection % (0...1)
    """
    _gl = os.path.join(path, 'annotation', '*.xml')
    # print(_gl)
    xml_pathl = glob(_gl)
    if not roi.area:
        raise ValueError("Zero area ROI is not allowed")
    processed = []
    _df_joined = gpd.GeoDataFrame(crs={'init': 'epsg:4326'})  # type: gpd.GeoDataFrame
    _df_joined.reset_index()
    for p in xml_pathl:
        dom = xml_parse(p)
        _ra = roi.area
        iw = xmlGetByPath(dom, '/product/adsHeader/swath').childNodes[0].nodeValue
        if iw not in processed:
            # print(f"# ---- {iw} {p} -----")
            df = get_geoloc(dom)

            _df_joined[iw] = df['geometry']
            _df_joined[iw + '_fit'] = df['geometry'].convex_hull.intersection(roi).area / roi.area
            # df[1:].plot(edgecolor='red', facecolor='green', ax=ax, legend=True)
            processed.append(iw)
    return _df_joined
