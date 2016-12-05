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

def pre_script(run_number, pax_version, update_database=True):

    query = {'detector' : 'tpc',
             'number' : run_number}
    
    # initialize api instance
    API = api()
    doc = API.get_next_run(query)

    name = doc["name"]
    
    # Check if suitable run found (i.e. no other processing or error, etc)
    if doc is None:
        print("Run name " + name + " not suitable")
        return 1
    
    if doc["reader"]["ini"]["write_mode"] != 2:
        return 1
    if doc["trigger"]["events_built"] == 0:
        return 1
    
    if has_tag(doc, 'donotprocess'):
        print("Do not process tag found, skip processing")
        return 1
    
    if 'processor' not in doc or 'DEFAULT' not in doc['processor']:
        return 1
    
    processing_parameters = doc['processor']['DEFAULT']
    
    if 'gains' not in processing_parameters or \
            'electron_lifetime_liquid' not in processing_parameters:
        print('no gains or electron lifetime! skipping processing')
        return 1

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
