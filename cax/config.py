"""Configuration routines
"""

import json
import logging
import os
import socket

import pymongo

def mongo_password():
    """Fetch passsword for MongoDB

    This is stored in an environmental variable MONGO_PASSWORD.
    """
    mongo_password = os.environ.get('MONGO_PASSWORD')
    if mongo_password is None:
        raise EnvironmentError('Environmental variable MONGO_PASSWORD not set.'
                               ' This is required for communicating with the '
                               'run database.  To fix this problem, Do:'
                               '\n\n\texport MONGO_PASSWORD=xxx\n\n'
                               'Then rerun this command.')
    return mongo_password


def get_hostname():
    """Get hostname of the machine we're running on.
    """
    return socket.gethostname().split('.')[0]


def load():
    dirname = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(dirname, 'cax.json')
    logging.debug('loading %s' % filename)
    return json.loads(open(filename, 'r').read())


def get_config(name):
    for doc in load():
        if doc['name'] == name:
            return doc
    raise LookupError("Unknown host %s" % name)


def get_options(option_type='upload', method=None):
    if method is None:
        try:
            options = get_config(get_hostname())['%s_options' % option_type]
        except LookupError as e:
            logging.info("Unknown config host: %s", get_hostname())
            return []

        return options

    options = []

    for x in get_options(option_type,
                         None):
        if get_config(x)['receive'] == method:
            options.append(x)

    return options


def get_pax_options(option_type='versions'):
    try:
        options = get_config(get_hostname())['pax_%s' % option_type] 
    except LookupError as e:
        logging.info("Unknown config host: %s", get_hostname())
        return []

    return options


def mongo_collection():
    c = pymongo.MongoClient('mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017/run' % os.environ.get('MONGO_PASSWORD'),
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
