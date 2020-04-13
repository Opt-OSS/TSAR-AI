import logging
from datetime import datetime, timedelta

from geopandas import GeoDataFrame

from ocli.sent1 import get_bucket

import mgrs

# OPTOSS_DATA_DIR = '/optoss/data'
# OPTOSS_SNAP_AUXDATA = '/optoss/snap/auxdata'
# OPTOSS_BUCKETS_DIR = os.path.join(OPTOSS_DATA_DIR, 'buckets')

log = logging.getLogger()

BUCKET_THRESHOLD = 10  # pairs max startDate diff in secs in the bucket
MGRSPRECISION = 0  # 1 - 10km, 2 - 1km, 3 - 100m ....


def toMgrs():
    m = mgrs.MGRS()

    def _t(point):
        return m.toMGRS(point.y, point.x, MGRSPrecision=MGRSPRECISION).decode()
    return _t

def unitime_delta(t0, t1):
    if isinstance(t0, datetime):
        return (t1 - t0) / timedelta(seconds=1)
    else:
        raise AssertionError(f'Unknown time type {t0}')


def unitime_delta_factory(d0):
    S1_cycle_T = 24 * 3600 * 12  # 12 days in seconds

    def _fn(d1):
        cycle_dt = abs(unitime_delta(d0, d1)) % S1_cycle_T
        cycle_dt = cycle_dt if (cycle_dt <= S1_cycle_T / 2) else S1_cycle_T - cycle_dt
        return cycle_dt

    return _fn


def create_list(odf: GeoDataFrame, buckets_dir='.') -> GeoDataFrame:
    """ add bucket coulmnt to product list"""
    if odf.empty:
        log.error("No full-cover SAR images found for ROI")
        return None

    df = odf.sort_values(by=['startDate'], inplace=False, ascending=False)
    df.reset_index(inplace=True, drop=True)
    df['bucket'] = None
    df['processed'] = False
    df['cycle_dt'] = None
    fn = toMgrs()
    df['centroid'] = df.centroid
    df['mgrs'] = df.centroid.apply(fn)
    i = 0
    for idx in df.index.to_list():

        q = df.iloc[idx]  # sar in question
        if q['processed']:
            continue
        """ ------------- #1 - get all pairs for current idx ----------------"""
        f = unitime_delta_factory(q['startDate'])
        # print("------------------------------------------------------------")
        # print(q[['productId', 'startDate', 'platform', 'relativeOrbitNumber']])
        _sdf = df[~df['processed']
                  & (df['platform'] == q['platform'])
                  & (df['polarisation'] == q['polarisation'])
                  & (df['swath'] == q['swath'])
                  & (df['relativeOrbitNumber'] == q['relativeOrbitNumber'])
                  ]
        """ ------------ #2 filter pairs by  BUCKET_THRESHOLD ----------------"""
        _sdf['cycle_dt'] = _sdf['startDate'].apply(f)
        _sdf_pairs = _sdf[(_sdf['cycle_dt'] <= BUCKET_THRESHOLD)]
        # print(_sdf_pairs[['productId', 'startDate', 'platform', 'relativeOrbitNumber', 'cycle_dt']])
        """---------------- #3 Get median product ----------------"""
        med = _sdf_pairs['cycle_dt'].median()
        # print(f"m------------ {med}")
        med_row = _sdf_pairs.iloc[(_sdf_pairs['cycle_dt'] - med).abs().argsort()].iloc[0]
        # print(med_row[['productId', 'startDate', 'platform', 'relativeOrbitNumber']])
        startDate,mgrs = med_row[['startDate','mgrs']]
        _t = "{0:02d}{1:02d}{2:02d}".format(startDate.hour, startDate.minute, startDate.second)
        bname = '_'.join(
            med_row[['platform', 'sensorMode', 'productType', 'relativeOrbitNumber']].astype(str).values.tolist()
        ) + '_' + mgrs
        # print(bname)
        f = unitime_delta_factory(startDate)
        _sdf['cycle_dt'] = _sdf['startDate'].apply(f)
        _sdf_pairs = _sdf[(_sdf['cycle_dt'] <= BUCKET_THRESHOLD)]
        # if bname=='S1B_IW_SLC_15_171615':
        #     print(_sdf_pairs[['productId', 'startDate', 'platform', 'relativeOrbitNumber', 'cycle_dt']])
        #     print(_sdf_pairs.index)
        # print(_sdf_pairs[['productId', 'startDate', 'platform', 'relativeOrbitNumber', 'cycle_dt','mgrs']])
        df.loc[_sdf_pairs.index, ['bucket', 'processed', 'cycle_dt']] = bname, True, _sdf_pairs['cycle_dt'].round(3)
        df.loc[df['productId'] == med_row['productId'], 'cycle_dt'] = '-0-'
    return df


def create_list_old(odf: GeoDataFrame, buckets_dir='.') -> GeoDataFrame:
    """ add bucket coulmnt to product list"""
    if odf.empty:
        log.error("No full-cover SAR images found for ROI")
        return None

    df = odf.sort_values(by=['startDate'], inplace=False, ascending=False)
    df.reset_index(inplace=True, drop=True)
    df['bucket'] = None
    df['median'] = False
    i = 0
    for idx in df.index.to_list():
        q = df.iloc[idx]  # sar in question
        if q['processed']:
            continue
        df.loc[idx, 'median'] = True
        # print(q)
        bucket = get_bucket(
            buckets_dir=buckets_dir,
            # firstBurstIndex=firstBurstIndex,
            # lastBurstIndex=lastBurstIndex,
            mission=q['platform'],
            sensorMode=q['sensorMode'],
            productType=q['productType'],
            relativeOrbitNumber=q['relativeOrbitNumber'],
            startDate=q['startDate']
        )
        # log.error(f"Processing record #{idx} {q['title']} into bucket {bucket}")
        _sdf = df[idx + 1:]
        _sdf = _sdf[(~_sdf['processed'])
                    & (_sdf['platform'] == q['platform'])
                    & (_sdf['polarisation'] == q['polarisation'])
                    & (_sdf['swath'] == q['swath'])
                    & (_sdf['relativeOrbitNumber'] == q['relativeOrbitNumber'])
                    ]  # all other not yet processed
        if _sdf.empty:
            continue
        """ -------------  compute helper deltas -------------
          check start time in interval 12D +-24sec
          <time delta in days> mod 12  should be 0, <time delta in seconds> should be < 24.000 seconds 
        """
        s = q['startDate'] - _sdf['startDate']
        # print(s)
        _sdf['tdd'], _sdf['tds'] = s.dt.days % 12, s.dt.seconds + s.dt.microseconds / 1e6
        # print(_sdf[['startDate', 'productId', 'tdd', 'tds', 'median']])
        c = _sdf[(_sdf['tdd'] == 0) & (_sdf['tds'] < 24.000)]
        if c.empty:
            continue
        df.loc[idx, 'bucket'] = bucket  # assign q to bucket
        df.loc[c.index, 'bucket'] = bucket  # assign candidates to bucket
        df.loc[c.index, 'processed'] = True  # prevent further processing
        # print(f"Candidates by orbit number {o_num}", c['title'])
        df.loc[idx, 'processed'] = True
        # i += 1
        # if i >= 1:
        #     break
    return df
