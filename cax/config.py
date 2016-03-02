import json
import socket
import pymongo
import os

FILENAME = 'cax.json'

def get_hostname():
    return socket.gethostname().split('.')[0]

def load():
    return json.loads(open(FILENAME, 'r').read())
    
def get_config(name):
    for doc in load():
        if doc['name'] == name:
            return doc
    raise NotFoundError()

def get_options(option_type = 'upload', method=None):
    if method is None:
        return get_config(get_hostname())['%s_options' % option_type]

    options = []

    for x in get_options(option_type,
                         None):
        if get_config(x)['receive'] == method:
            options.append(x)

    return options

def upload_options(method=None):
    return get_options('upload', method=method)

def mongo_collection():
    c = pymongo.MongoClient('mongodb://eb:%s@copslx50.fysik.su.se:27017/run' % os.environ.get('MONGO_PASSWORD'),
                            read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
    db = c['run']
    collection = db['runs_new']
    return collection
    
def data_availability(hostname=get_hostname()):
    collection = mongo_collection()
    
    results = []
    for doc in collection.find({'detector' : 'tpc'},
                               ['name', 'data']):
        for datum in doc['data']:
            if datum['status'] != 'transferred':
                continue
            if 'host' in datum and datum['host'] != hostname:
                continue
            results.append(doc)
    return results
