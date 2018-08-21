import os
import pymongo
import json
import pprint
import socket
import subprocess
import sys
import shutil
import time
from pax import __version__

#from cax.tasks.process import get_pax_hash
from cax import config


uri = 'mongodb://eb:%s@xenon1t-daq.lngs.infn.it:27017,copslx50.fysik.su.se:27017/run'
uri = uri % os.environ.get('MONGO_PASSWORD')
c = pymongo.MongoClient(uri,
                        replicaSet='runs',
                        read_preference=pymongo.ReadPreference.PRIMARY_PREFERRED)
db = c['run']
collection = db['runs_new']
host_name = "login"
status = "error"

def find_errors():
    query = {"data" : {"$elemMatch" : {"status" : status, 
                                       "host" : host_name,
                                       "type" : "processed",
                                       "pax_version" : "v" + __version__
                                       }
                       }
             }

    cursor = collection.find(query, {"_id" : False,
                                     "data" : True,
                                     "name" : True,
                                     "number" : True,
                                     "detector" : True
                                     }
                             )


    tpc = []
    mv = []
    tpc_num = []
    for run in list(cursor):
        if run['detector'] == 'tpc':
            tpc.append(run['name'])
            tpc_num.append(run['number'])
        elif run['detector'] == 'muon_veto':
            mv.append(run['name'])

    ret = {"tpc" : tpc,
           "muon_veto" : mv,
           "tpc_number" : tpc_num
           }

    return ret

def remove_entry(name, detector):

    data_type = 'processed'
    query = {"name" : name,
             "detector" : detector}
    
    run_doc = collection.find_one(query) 

    if not run_doc:
        return

    data_docs = run_doc.get('data',0)
    if data_docs == 0:
        return

    print (run_doc['name'])

    events = run_doc.get('trigger',{}).get('events_built', 0)

    for data_doc in data_docs:

        if 'host' not in data_doc: #            or (data_doc['host'] != "osg-connect-srm" and data_doc['host'] != "midway-srm"):
            continue

        if data_doc['type'] != data_type:
            continue

        if data_doc['host'] != host_name:
            continue
        
        if data_doc['pax_version'] != "v" + __version__:
            continue

        if data_doc['status'] != status:
            continue

        datum = data_doc

        print( "\nRun ", run)
        pprint.pprint( datum )

        collection.update({'_id': run_doc['_id']}, {'$pull': {'data' : data_doc}})

        break


def remove_dag(run_name):
    dir = "/scratch/processing/pax_v{version}/{name}/dags".format(version= __version__,
                                                                  name=run_name)
    shutil.rmtree(dir)
    time.sleep(0.5)

if __name__ == "__main__":
    runs_to_rm = find_errors()
    tpcruns, mvruns = runs_to_rm['tpc'], runs_to_rm['muon_veto']
    for i, run in enumerate(tpcruns):
        num = runs_to_rm['tpc_number'][i]
        if num > 10000:
            print("Clearing error for TPC run %d" % num)
            remove_entry(run, 'tpc')
#            remove_dag(run)
        
    for run in mvruns:
        print("Clearing error for MV run %s" % run)
        remove_entry(run, 'muon_veto')
#        remove_dag(run + "_MV")
        

