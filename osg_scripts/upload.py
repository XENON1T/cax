"""
Called in DAG post script to do ROOT hadd and DB handling
"""

import datetime
import hashlib
import subprocess
import sys
import os
from collections import defaultdict
import time
import pax
import checksumdir
import json
#from pymongo import ReturnDocument

from cax import qsub, config
from cax.task import Task
from cax.api import api

def verify():
    """Verify the file

    Now is nothing.  Could check number of events later?
    """
    return True



def _upload(rawdir, pax_version, update_database=True):
    
    # get run name from raw directory
    name = rawdir.split('/')[-1]

    # check how many processed zip files we have back 
    procdir = config.get_processing_dir("login", pax_version) + "/" + name
    n_processed = len(os.listdir(procdir))
    print("Processed files: ",n_processed)

    # does number of processed files match the raw zip files?
    n_zips = len([f for f in os.listdir(rawdir) if f.startswith("XENON")])
    print("nzips: ", n_zips)


    # do we want a fancier query here? Could clean up below slightly then.
    query = {'detector' : 'tpc',
             'name' : name}
    
    API = api()
    doc = API.get_next_run(query)

    # entry we will add/update
    updatum = {'host'          : 'login',
               'type'          : 'processed',
               'pax_version'   : pax_version,
               'status'        : 'transferred',
               'location'      : procdir,
               'checksum'      : None,
               'creation_time' : datetime.datetime.utcnow(),
               'creation_place': 'OSG'}
    
    datum = None
    for entry in doc["data"]:
        if (entry["host"] == 'login' and entry["type"] == 'processed' and
            entry["pax_version"] == pax_version):
            datum = entry


    # if we don't have expected number of root files back, tell DB there was an error
    if n_processed != n_zips:
        print("There was an error during processing. Missing root files")
        updatum["status"] = 'error'

    # if all root files present, then perform hadd, checksum, and register to database
    else:
        print("merging %s" % name)
        subprocess.Popen("/home/ershockley/cax/osg_scripts/merge_roots.sh " + procdir, 
                         shell = True).wait()

        # final location of processed root file
        final_location = procdir + ".root"   

        checksum = checksumdir._filehash(final_location,
                                         hashlib.sha512)        
        updatum['checksum'] = checksum
        updatum['location'] = final_location
        
    # if there is no entry for this site/pax_version, add a new location
    if datum is None:
        API.add_location(doc['_id'], updatum)
        print('new entry added to database')
    # if there is already an entry, update it
    else:
        API.update_location(doc['_id'], datum, updatum)
        print('entry updated in database')

    # if there was an error (we're missing root files), 
    # exit with code 1 so that DAG sees this node as a failure
    if updatum["status"] == 'error':
        sys.exit(1)


def main():
    _upload(*sys.argv[1:])

main()
