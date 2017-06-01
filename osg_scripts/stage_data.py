from cax.dag_writer import dag_writer
from cax.api import api
from cax.dag_prescript import rucio_AddRule
import numpy as np
import pymongo
from make_runlist import make_runlist
import time 
import os

#runlist = make_runlist()

runlist = [6635, 6577, 6439, 6168, 5948]
#runlist = list(reversed(runlist))
print(len(runlist))

for run in runlist:
    print(run)
    API = api()
    query = {"number" : run}
    doc = API.get_next_run(query)

    if 'data' not in doc:
        continue

    on_rucio = False
    rucio_loc = None
    rses = None
    on_stash = False
    on_stash_rucio = False
    
    ready_to_process = []
    for entry in doc['data']:
        if entry["type"] == 'raw' and entry["host"] == 'rucio-catalogue' and entry["status"] == 'transferred':
            on_rucio = True
            rucio_loc = entry["location"]
            rses = entry["rse"]
            
        if entry["type"] == "raw" and entry["host"] == "login" and entry["status"] == "transferred":
            if os.path.exists(entry["location"]):
                on_stash = True

    if on_rucio and not on_stash:
        if "UC_OSG_USERDISK" not in rses:
            print("Adding rule for run %d" % run)
            stdout, stderr = rucio_AddRule(rucio_loc)
            for out in stdout:
                print(out)
            #print("Sleeping...")
            time.sleep(1)
        else:
            print("Run %d already on Stash" % run)
            on_stash_rucio = True
    
    
    if on_stash or on_stash_rucio:
        ready_to_process.append(run)

    time.sleep(0.5)
print(ready_to_process)







