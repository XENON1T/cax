"""Pre script for large dag reprocessing
"""

import datetime
import sys
import os
from collections import defaultdict
import time
import pax
import checksumdir
import json
import time
import subprocess
import re
#from pymongo import ReturnDocument

from cax import config
from cax.api import api

def has_tag(doc, name):
    if 'tags' not in doc:
        return False
    
    for tag in doc['tags']:
        if name == tag['name']:
            return True
        return False

def clear_errors(id, pax_version, detector='tpc'):

    if detector == 'tpc':
        identifier = 'number'
        id = int(id)
    elif detector == 'muon_veto':
        identifier = 'name'
    else:
        raise ValueError("detector is neither tpc nor muon_veto")

    query = {'detector': detector,
             identifier   : id,
             "data" : {"$elemMatch" : {"host" : 'login',
                                      "type" : "processed",
                                      "pax_version" : pax_version
                                      #"status" : "error"
                                      }
                       }
             }

    query = {'query' : json.dumps(query)}

    API = api()
    doc = API.get_next_run(query)

    # if query returns northing, there is no error
    if doc is None:
        return

    # if there is an error, remove that entry from database
    else:
        parameters = None
        for entry in doc["data"]:
            if (entry["host"] == 'login' and entry["type"] == 'processed' and 
                entry['pax_version'] == pax_version and entry["status"] in ['transferring', 'error']):
                parameters = entry

        if parameters is not None:
            print("Clearing errors for run %s" %id)
            API.remove_location(doc["_id"], parameters)
            time.sleep(0.5)
        else:
            print("Could not find relevant entry in doc")

def is_on_stash(rucio_name):
    # checks if run with rucio_name is on stash
    out = subprocess.Popen(["rucio", "list-rules", rucio_name], stdout=subprocess.PIPE).stdout.read()
    out = out.decode("utf-8").split("\n")
    for line in out:
        line = re.sub(' +', ' ', line).split(" ")
        if len(line) > 4 and line[4] == "UC_OSG_USERDISK" and line[3][:2] == "OK":
            return True
    return False


def rucio_AddRule(dataset):
    command = ['/home/ershockley/rucio_addrule.sh', dataset]

    execute = subprocess.Popen(command,
                              stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT,
                              shell=False,
                              universal_newlines=False)
    stdout_value, stderr_value = execute.communicate()
    stdout_value = stdout_value.decode("utf-8")
    stdout_value = stdout_value.split("\n")
    stdout_value = list(filter(None, stdout_value)) # fastest way to remove '' from list
    #time.sleep(20*60)
    return stdout_value, stderr_value
             
def pre_script(run_name, pax_version, run_number, detector = 'tpc'):

    # first clear any relevant errors
    if detector == 'tpc':
        run_id = int(run_number)
        identifier = 'number'

    elif detector == 'muon_veto':
        run_id = run_name
        identifier = 'name'
    else:
        raise ValueError("Detector %s does not exist" % detector)

    clear_errors(run_id, pax_version, detector)
    
    # query that checks if it's okay that we process this run
    query = {identifier: run_id,
             'detector': detector,
             "data": {"$not": {"$elemMatch": {"type": "processed",
                                              "pax_version": pax_version,
                                              "host": "login",
                                              "$or": [{"status": "transferred"},
                                                      {"status": "transferring"}
                                                      ]
                                              }
                               }
                      },
             "$or": [{"$and": [{'number': {"$gte": 2000}},
                               {'processor.DEFAULT.gains': {'$exists': True}},
                               {'processor.DEFAULT.electron_lifetime_liquid': {'$exists': True}},
                               {'processor.DEFAULT.drift_velocity_liquid': {'$exists': True}},
                               {'processor.correction_versions': {'$exists': True}},
                               {'processor.WaveformSimulator': {'$exists': True}},
                               {'processor.NeuralNet|PosRecNeuralNet': {'$exists': True}},
                               ]
                      },
                     {"detector": "muon_veto"}
                     ],
             'reader.ini.write_mode': 2,
             'trigger.events_built': {"$gt": 0},
             'tags': {"$not": {'$elemMatch': {'name': 'donotprocess'}}},
             }
    
    query = {'query' : json.dumps(query)}

    # initialize api instance
    API = api()
    doc = API.get_next_run(query)
    time.sleep(0.5)
    print(query)
        

    # if run doesn't satisfy above query, we don't process
    if doc is None:
        print("Run %s is not suitable for OSG processing. Check run doc" % id)
        sys.exit(1)


    # check if we should download from rucio:
    on_rucio = False
    on_stash = False
    on_midway = False

    rses = []
    for entry in doc["data"]:
        if entry["type"] != "raw" and entry["host"]=="login" and entry["status"]=="transferred":
            if os.path.exists(entry["location"]):
                on_stash = True
        if entry["type"] == "raw" and entry["host"]=="rucio-catalogue" and entry["status"]=="transferred":
            on_rucio = True
            rucio_dataset = entry["location"]
            rses = entry["rse"]

        if entry["type"] == "raw" and entry["host"]=="midway-login1" and entry["status"]=="transferred":
            on_midway = True

    # perform rucio add rule if needed
    if on_rucio and not (on_stash or on_midway):
        if "UC_OSG_USERDISK" not in rses:
            print("Data not on stash or in UC_OSG_USERDISK: Aborting (NOT ABORTING, TEMPORARY")
            #sys.exit(1)
            #stdout, stderr = rucio_AddRule(rucio_dataset)
            #for out in stdout:
            #    print("RUCIO: ", out)
            #time.sleep(20*60)

#    if len(doc["processor"]["DEFAULT"]["gains"]) == 254:
#        print("Adding gains for acquisition monitor")
#        doc["processor"]["DEFAULT"]["gains"] += [2.5e6 / 31.25] + [1e5] * 5

    name = doc["name"]
    
    procdir = config.get_processed_zip_dir("login", pax_version) + "/" + name

    if detector == 'muon_veto':
        procdir = procdir + "_MV"

    datum = {'host'          : 'login',
             'type'          : 'processed',
             'pax_version'   : pax_version,
             'status'        : 'transferring',
             'location'      : procdir,
             'checksum'      : None,
             'creation_time' : datetime.datetime.utcnow(),
             'creation_place': 'OSG'}

    if detector == 'tpc':
        json_file = "/xenon/ershockley/jsons/" + name + ".json"

    elif detector == 'muon_veto':
        json_file = "/xenon/ershockley/jsons/" + name + "_MV.json"
    
    API.add_location(doc['_id'], datum)
    print('new entry added to database')
    return

def main():
    pre_script(*sys.argv[1:])


if __name__ == "__main__":
    main()
