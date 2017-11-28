"""
Called in DAG post script to do ROOT hadd and DB handling
"""

import datetime
import hashlib
import subprocess
import sys
import os
import checksumdir

from cax import config
from cax.api import api

def verify():
    """Verify the file

    Now is nothing.  Could check number of events later?
    """
    return True


def get_ziplist(name):
    query = {'detector' : 'tpc',
             'name' : name}

    API = api()
    doc = API.get_next_run(query)

    # see if raw data is on stash
    on_stash=False
    rawdir = None
    on_rucio = False
    rucio_scope=None

    for datum in doc["data"]:
        if datum["type"] == "raw" and datum["host"] == "login":
            on_stash=True
            rawdir = datum["location"]

        elif datum["type"] == "raw" and datum["host"] == "rucio-catalogue":
            on_rucio=True
            rucio_scope = datum["location"]

    # if on stash, get ziplist by looking in rawdir
    if on_stash:
        ziplist = [file for file in os.listdir(rawdir) if file.startswith('XENON1T')]

    # if not, must go through rucio
    elif on_rucio:
        out = subprocess.Popen(["rucio", "-a", "xenon-analysis", "list-file-replicas", rucio_scope],
                           stdout=subprocess.PIPE).stdout.read()
        out = str(out).split("\\n")
        files = set([l.split(" ")[3] for l in out if '---' not in l and 'x1t' in l])
        ziplist = list(sorted([f for f in files if f.startswith('XENON1T')]))

    else:
        print("Run %i not on stash or rucio" % doc['number'])
        return

    return ziplist

def _upload(name, n_zips, pax_version, detector = "tpc", update_database=True):

    if detector == "tpc":
        MV = ""
    else:
        MV = "_MV"

    # check how many processed zip files we have back 
    proc_zip_dir = config.get_processed_zip_dir("login", pax_version) + "/" + name + MV
    print(proc_zip_dir)
    n_processed = len(os.listdir(proc_zip_dir))
    print("Processed files: ",n_processed)

    # get dir where we want the merged root file
    proc_merged_dir = config.get_processing_dir("login", pax_version)
    final_location = os.path.join(proc_merged_dir, name + MV + ".root")

    # does number of processed files match the raw zip files?
    n_zips = int(n_zips)
    print("nzips: ", n_zips)

    # do we want a fancier query here? Could clean up below slightly then.
    query = {'detector' : detector,
             'name' : name}
    
    API = api()
    doc = API.get_next_run(query)

    # entry we will add/update
    updatum = {'host'          : 'login',
               'type'          : 'processed',
               'pax_version'   : pax_version,
               'status'        : 'transferred',
               'location'      : final_location,
               'checksum'      : None,
               'creation_time' : datetime.datetime.utcnow(),
               'creation_place': 'OSG'}
    
    datum = None
    for entry in doc["data"]:
        if (entry["host"] == 'login' and entry["type"] == 'processed' and entry["pax_version"] == pax_version):
            datum = entry


    # if we don't have expected number of root files back, tell DB there was an error
    if n_processed != n_zips:
        print("There was an error during processing. Missing root files for these %i zips:" % (n_zips - n_processed))
        for zip in get_ziplist(name):
            if not os.path.exists(proc_zip_dir + "/" + zip.replace(".zip", ".root")):
                print("\t%s" % zip)
        updatum["status"] = 'error'

    # if all root files present, then perform hadd, checksum, and register to database
    else:
        print("merging %s" % name)
        cax_dir = os.path.expanduser("~") + "/cax"
        print (cax_dir)
        print([cax_dir + "/osg_scripts/merge_roots.sh", proc_zip_dir, proc_merged_dir])
        merge_script = os.path.join(cax_dir, 'osg_scripts/merge_roots.sh')
        P = subprocess.Popen([merge_script, proc_zip_dir, proc_merged_dir])
        P.communicate()
        if P.returncode != 0:
            raise ValueError("Subprocess returned non zero value")
        

        # final location of processed root file
        final_location = proc_merged_dir + "/%s.root" % (name + MV)

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

