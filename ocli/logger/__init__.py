import logging

import coloredlogs as coloredlogs

FORMAT_DEBUG = '%(asctime)s.%(msecs)03d  %(levelname)-8s %(name)-10s %(relativeCreated)-10d : %(message)s| %(pathname)s:%(lineno)d'
FORMAT = '%(levelname)-8s  %(name)-10s : %(message)s | time since start:  %(relativeCreated)-10d'




def init(level='DEBUG'):
    coloredlogs.DEFAULT_FIELD_STYLES = {'asctime': {'color': 'green'}, 'hostname': {'color': 'magenta'},
                                        'levelname': {'color': 'white', 'bold': True}, 'name': {'color': 'blue'},
                                        'programname': {'color': 'cyan'}}
    # logging.basicConfig(format=FORMAT,level=logging.getLevelName(level))
    if level == 'DEBUG':
        coloredlogs.install(fmt=FORMAT_DEBUG, level=level)
    else:
        coloredlogs.install(fmt=FORMAT_DEBUG, level=level)
    logging.captureWarnings(True)
    # warnings.simplefilter(action='ignore', category=FutureWarning)
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    logging.getLogger('ibm_s3transfer').setLevel(logging.WARNING)
    logging.getLogger('ibm_botocore').setLevel(logging.WARNING)
    # logging.getLogger('ibmcloudenv').setLevel(logging.WARNING)
    logging.getLogger('s3transfer').setLevel(logging.WARNING)
    logging.getLogger('cloudant').setLevel(logging.DEBUG)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('fiona').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    # logging.getLogger('ibm_s3transfer.utils').setLevel(logging.INFO)
    # logging.getLogger('ibm_s3transfer.tasks').setLevel(logging.INFO)
    # logging.getLogger('ibm_s3transfer.futures').setLevel(logging.INFO)


def getLogger(category="main"):
    return logging.getLogger(category)
