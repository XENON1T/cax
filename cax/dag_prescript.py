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
#from pymongo import ReturnDocument

from cax import config
from cax.task import Task
from cax.api import api

def has_tag(doc, name):
    if 'tags' not in doc:
        return False
    
    for tag in doc['tags']:
        if name == tag['name']:
            return True
        return False

def clear_errors(run_number, pax_version):
    query = {'detector': 'tpc',
             'number'    : int(run_number),
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
        print("no errors need clearing")
        return

    # if there is an error, remove that entry from database
    else:
        parameters = None
        for entry in doc["data"]:
            if (entry["host"] == 'login' and entry["type"] == 'processed' and 
                entry['pax_version'] == pax_version and entry["status"] in ['transferring', 'error']):
                parameters = entry

        if parameters is not None:
            print("Clearing errors for run %s" %run_number)
            API.remove_location(doc["_id"], parameters)
            time.sleep(0.5)
        else:
            print("Could not find relevant entry in doc")
            
             
def pre_script(run_number, pax_version, update_database=True):

    # first clear any relevant errors
    
    clear_errors(run_number, pax_version)
    
    # query that checks if it's okay that we process this run
    query = {'detector': 'tpc',
             'number'    : int(run_number),
             "data" : {"$not" : {"$elemMatch" : {"host" : 'login',
                                                 "type" : "processed",
                                                 "pax_version" : pax_version
                                                 }
                                 },
                       "$elemMatch" : {"host" : 'login',
                                       "type" : "raw"}
                       },
             'reader.ini.write_mode' : 2,
             'trigger.events_built' : {"$gt" : 0},
             'processor.DEFAULT.gains' : {'$exists' : True},
             'processor.DEFAULT.electron_lifetime_liquid' : {'$exists' : True},
             'tags' : {"$not" : {'$elemMatch' : {'name' : 'donotprocess'}}},
             }
    
    query = {'query' : json.dumps(query)}

    # initialize api instance
    API = api()
    doc = API.get_next_run(query)
    time.sleep(0.5)
    print(query)
        

    # if run doesn't satisfy above query, we don't process
    if doc is None:
        print("Run %s is not suitable for OSG processing. Check run doc" % run_number)
        sys.exit(1)

    name = doc["name"]
    
    procdir = config.get_processing_dir("login", pax_version) + "/" + name

    datum = {'host'          : 'login',
             'type'          : 'processed',
             'pax_version'   : pax_version,
             'status'        : 'transferring',
             'location'      : procdir,
             'checksum'      : None,
             'creation_time' : datetime.datetime.utcnow(),
             'creation_place': 'OSG'}


    json_file = "/xenon/ershockley/jsons/" + str(name) + ".json"
    with open(json_file, "w") as f:
        json.dump(doc, f)
    
    API.add_location(doc['_id'], datum)
    print('new entry added to database')
    return

def main():
    pre_script(*sys.argv[1:])

main()
