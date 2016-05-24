"""Configuration routines
"""

import json
import logging
import os
import socket

import pymongo

HOSTNAME = None

# global variable to store the specified .json config file
CAX_CONFIGURE = ''

def mongo_password():
    """Fetch passsword for MongoDB

    This is stored in an environmental variable MONGO_PASSWORD.
    """
    mongo_pwd = os.environ.get('MONGO_PASSWORD')
    if mongo_pwd is None:
        raise EnvironmentError('Environmental variable MONGO_PASSWORD not set.'
                               ' This is required for communicating with the '
                               'run database.  To fix this problem, Do:'
                               '\n\n\texport MONGO_PASSWORD=xxx\n\n'
                               'Then rerun this command.')
    return mongo_pwd


def get_hostname():
    """Get hostname of the machine we're running on.
    """
    global HOSTNAME
    if HOSTNAME is not None:
        return HOSTNAME
    return socket.gethostname().split('.')[0]

def set_json( confg ):
    """Set the cax.json file at your own
    """
    global CAX_CONFIGURE
    CAX_CONFIGURE = confg

def load():

    # User-specified config file
    if CAX_CONFIGURE:
        filename = os.path.abspath( CAX_CONFIGURE )

    # Default config file
    else:
        dirname = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(dirname, 'cax.json')

    logging.debug('Loading config file %s' % filename)

    return json.loads(open(filename, 'r').read())

def get_config(hostname=get_hostname()):
    """Returns the cax configuration for a particular hostname
    NB this currently reloads the cax.json file every time it is called!!
    """
    for doc in load():
        if doc['name'] == hostname:
            return doc
    raise LookupError("Unknown host %s" % hostname)


def get_transfer_options(transfer_kind='upload', transfer_method=None):
    """Returns hostnames that the current host can upload or download to.
    transfer_kind: 'upload' or 'download'
    transfer_method: is specified and not None, return only hosts with which
                     we can work using this method (e.g. scp)
    """
    try:
        transfer_options = get_config(get_hostname())['%s_options' % transfer_kind]
    except LookupError:
        logging.info("Host %s has no known transfer options.",
                     get_hostname())
        return []

    if transfer_method is not None:
        transfer_options = [to for to in transfer_options
                            if get_config(to['host'])['receive'] == 'method']

    return transfer_options

def get_pax_options(option_type='versions'):
    try:
        options = get_config(get_hostname())['pax_%s' % option_type] 
    except LookupError as e:
        logging.info("Unknown config host: %s", get_hostname())
        return []

    return options

def get_dataset_list():
    try:
        options = get_config(get_hostname())['dataset_list']
    except LookupError as e:
        logging.debug("dataset_list not specified, operating on entire DB")
        return []

    return options

def get_task_list():
    try:
        options = get_config(get_hostname())['task_list']
    except LookupError as e:
        logging.debug("task_list not specified, running all tasks")
        return []

    return options

def mongo_collection():
    # For the event builder to communicate with the gateway, we need to use the DAQ network address
    # Otherwise, use the internet to find the runs database
    if get_hostname().startswith('eb'):
        c = pymongo.MongoClient('mongodb://eb:%s@gw:27017/run' % os.environ.get('MONGO_PASSWORD'))
    else:
        uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017/run'
        uri = uri % os.environ.get('MONGO_PASSWORD')
        c = pymongo.MongoClient(uri,
                                replicaSet='runs',
                                read_preference=pymongo.ReadPreference.PRIMARY_PREFERRED)
    db = c['run']
    collection = db['runs_new']
    return collection


def data_availability(hostname=get_hostname()):
    collection = mongo_collection()

    results = []
    for doc in collection.find({'detector': 'tpc'},
                               ['name', 'data']):
        for datum in doc['data']:
            if datum['status'] != 'transferred':
                continue
            if 'host' in datum and datum['host'] != hostname:
                continue
            results.append(doc)
    return results
