import os
import re
import pandas as pd
import requests
from dateutil.parser import parse

RESORB_URL = 'https://step.esa.int/auxdata/orbits/Sentinel-1/RESORB'
POEORB_URL = 'https://step.esa.int/auxdata/orbits/Sentinel-1/POEORB'

__data = None
__data_dir = './GeoDataFrame'
__data_filename = os.path.join(__data_dir, 'orbits.pkl')


def build_path(mission, orbit_type, date):
    """Build URL to scrap orbit files


    :type orbit_type: str
    :param orbit_type: RESORB | POEORB
    :param date: format YYYMM
    :type date: str
    :param mission: S1A|S1B
    :type mission: str
    """
    year = date[0:4]
    month = date[4:6]
    path = POEORB_URL if orbit_type == 'POEORB' else RESORB_URL
    path = f"{path}/{mission}/{year}/{month}"
    return path


def get_valid_start(row: pd.Series):
    """ extract START date from filename  field of DataFrane
        S1A_OPER_AUX_POEORB_OPOD_20190422T120811_V20190401T225942_20190403T005942.EOF.zip
    """
    # ..._V20190401T225942_... -> V20190401T225942 -> 20190401T225942
    st = row['filename'].split('_')[6].split('V')[1]
    return parse(st + 'Z')  # +'Z' to fit UTC


def get_valid_end(row: pd.Series):
    """ extract END date from filename  field of DataFrane
        S1A_OPER_AUX_POEORB_OPOD_20190422T120811_V20190401T225942_20190403T005942.EOF.zip
    """
    # print (row)
    # ...20190403T005942.EOF.zipZ... -> 20190403T005942.EOF.zipZ -> 20190403T005942

    st = row['filename'].split('_')[7].split('.')[0]
    return parse(st + 'Z')  # +'Z' to fit UTC


def __get_df(mission, orbit_type, date):
    """get mission orbits


    :type orbit_type: str
    :param orbit_type: RESORB | POEORB
    :param date: format YYYMMDD
    :type date: str
    :param mission: S1A|S1B
    :type mission: str
    """
    url = build_path(mission=mission, orbit_type=orbit_type, date=date)
    print(f"loading orbits: {url}")
    payload = ""
    headers = {
        # 'User-Agent': "PostmanRuntime/7.11.0",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        # 'Postman-Token': "4b229046-9c16-436a-bf1f-c36c36c3685b,db914d16-1a5a-40b3-86ea-a24889ca4427",
        # 'Host': "step.esa.int",
        # 'accept-encoding': "gzip, deflate",
        # 'content-length': "",
        # 'Connection': "keep-alive",
        'cache-control': "no-cache"
    }

    r = requests.request("HEAD", url, data=payload, headers=headers)
    if r.status_code != 200:
        return None
    r = requests.request("GET", url, data=payload, headers=headers)
    html = r.content.decode("utf-8")
    reg = r"href=\"(.*\.zip)\""
    df = pd.DataFrame(re.findall(reg, html), columns=['filename'])
    df['catalog_date'] = date
    df['mission'] = mission
    df['orbit_type'] = orbit_type
    df['valid_start'] = df.apply(get_valid_start, axis=1)
    df['valid_end'] = df.apply(get_valid_end, axis=1)
    df['url'] = url + '/' + df['filename']
    return df


def get_res(mission, date):
    """Build URL to scrap orbit files


    :param date: format YYYMMDD
    :type date: str
    :param mission: S1A|S1B
    :type mission: str
    """
    return __get_df(mission=mission, orbit_type='RESORB', date=date)


def get_poe(mission, date):
    """Build URL to scrap orbit files


    :param date: format YYYMMDD
    :type date: str
    :param mission: S1A|S1B
    :type mission: str
    """
    return __get_df(mission=mission, orbit_type='POEORB', date=date)


def sync_orbits(force=False, dates=None, orbit_types=None, missions=None):
    global __data
    """ sync local orbits cache
        if force is False and some data present for Date/mission/orbit_type
        returns cached data, otherwise deletes old data Date/mission/orbit_type  and loads fresh values

        :type dates: str[]
        :param dates: list o dates to sync in form YYYYMM
        :type orbit_types: str[]
        :param orbit_types: list of orbit types POEORB|RESORB to sync
        :type force: bool
        :param force: forse sync for given dates
    """
    do_save = False
    if missions is None:
        missions = []
    if orbit_types is None:
        orbit_types = []
    if dates is None:
        dates = []
    if os.path.isfile(__data_filename):
        __data = pd.read_pickle(__data_filename)
    else:
        os.makedirs(__data_dir, exist_ok=True)
        __data = pd.DataFrame(columns=['filename', 'catalog_date', 'mission', 'orbit_type', 'valid_start',
                                       'valid_end', 'url'])
        do_save = True
    # Do we need to load data?
    for date in dates:
        for orbit_type in orbit_types:
            for mission in missions:
                do_load = False
                ds = __data[(__data['catalog_date'] == date) & (__data['mission'] == mission) & (
                        __data['orbit_type'] == orbit_type)]
                if ds.empty:
                    do_load = True
                else:
                    if force:
                        """ delete old data """
                        __data.drop(ds.index)
                        do_load = True
                if do_load:
                    do_save = True
                    df = __get_df(mission=mission, date=date, orbit_type=orbit_type)
                    if df is not None:
                        __data = pd.concat([__data, df], ignore_index=True)
                else:
                    print(f"orbits cache hit {mission} {orbit_type} {date}")
    if do_save:
        __data.to_pickle(__data_filename)
    return __data


def load_orbits(reload=False, dates=[]):
    if not reload and len(dates):
        raise ValueError("dates should be provided only on reload=True")

    for date in ['20190401', '20190501']:
        _df = get_poe(mission='S1A', date=date)
        if _df is not None:
            df = pd.concat([df, _df], ignore_index=True)
        _df = get_res(mission='S1A', date=date)
        if _df is not None:
            df = pd.concat([df, _df], ignore_index=True)
