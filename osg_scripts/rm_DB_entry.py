import os
import pymongo
import json
import pprint
import socket
import subprocess
import sys

#from cax.tasks.process import get_pax_hash
from cax import config
from pax import __version__

def update_path(number, data_type, detector):
    thishost = socket.gethostname()

    # Connect to database                                                                                                                                 
    uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017/run'
    uri = uri % os.environ.get('MONGO_PASSWORD')
    c = pymongo.MongoClient(uri,
                            replicaSet='runs',
                            read_preference=pymongo.ReadPreference.PRIMARY_PREFERRED)
    db = c['run']
    collection = db['runs_new']

    if detector == 'muon_veto':
        id = 'name'
        
    else:
        id = 'number'
        number = int(number)

    query = {id: number}
             # This 'data' gets deleted later and only used for checking
             #'data': { '$elemMatch': { 'host': thishost,
             #                          'type': data_type,
             #                        }}
    
    run_doc = collection.find_one(query) 

    if not run_doc:
        return

    data_docs = run_doc.get('data',0)
    if data_docs == 0:
        return

    events = run_doc.get('trigger',{}).get('events_built', 0)

    for data_doc in data_docs:

        if 'host' not in data_doc: #            or (data_doc['host'] != "osg-connect-srm" and data_doc['host'] != "midway-srm"):
            continue

        if data_doc['type'] != data_type:
            continue

        if data_doc['host'] != "login":
            continue
        
        if data_doc['pax_version'] != 'v' +__version__:
            continue

        if data_doc['status'] != 'transferring':
            continue

        datum = data_doc

        print( "\nRun ", number)
        pprint.pprint( datum )


        collection.update({'_id': run_doc['_id']}, {'$pull': {'data' : data_doc}})

        break

if __name__ == "__main__":
    number = sys.argv[1]
    detector = sys.argv[2]
    update_path(number, "processed", detector)
    
