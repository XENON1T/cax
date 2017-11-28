#!/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/pax_head/bin/python

import pymongo
import os
import sys
from cax.api import api

def get_did(run_id, detector='tpc'):
    uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017,zenigata.uchicago.edu:27017/run'
    uri = uri % os.environ.get('MONGO_PASSWORD')
    c = pymongo.MongoClient(uri,
                            replicaSet='runs',
                            readPreference='secondaryPreferred')
    db = c['run']
    collection = db['runs_new']
    
    if detector == 'tpc':
        query = {"detector" : "tpc",
                 "number" : int(run_id)}
    elif detector == 'muon_veto':
        query = {"detector" : "muon_veto",
                 "name" : run_id}             

    cursor = collection.find(query, {"number" : True,
                                     "name" : True,
                                     "data" : True,
                                     "_id" : False})


    
    data = list(cursor)[0]['data']
    did = None
    
    for d in data:
        if d['host'] == 'rucio-catalogue' and d['status'] == 'transferred':
            did = d['location']


    return did

if __name__ == '__main__':
    print(get_did(sys.argv[1]))

