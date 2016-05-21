import sys
import datetime
import hashlib
import json
import checksumdir
import subprocess
import hashlib
from pymongo import ReturnDocument

from cax import qsub, config
from cax.task import Task

def get_pax_hash(pax_version, host):
    """Obtain pax repository hash"""

    PAX_DEPLOY_DIR = ''
    if host == 'midway-login1':
        # Location of GitHub source code on Midway
        PAX_DEPLOY_DIR="/project/lgrandi/deployHQ/pax"
    elif host == 'tegner-login-1':
        # Location of GitHub source code on Stockholm
        PAX_DEPLOY_DIR="/afs/pdc.kth.se/projects/xenon/software/pax"

    # Get hash of this pax version
    if pax_version == 'head':
        git_args = "--git-dir="+PAX_DEPLOY_DIR+"/.git rev-parse HEAD"
    else:
        git_args = "--git-dir="+PAX_DEPLOY_DIR+"/.git rev-parse "+pax_version

    git_out = subprocess.check_output("git "+git_args, shell=True)
    pax_hash = git_out.rstrip().decode('ascii')

    return pax_hash

def verify():
    return True

def process(name, in_location, host, pax_version, pax_hash, out_location,
            ncpus=1):
    print('Welcome to cax-process')

    # Import pax so can process the data
    from pax import core

    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    output_fullname = out_location + '/' + name

    # New data location
    datum = {'host'       : host,
             'type'       : 'processed',
             'pax_hash'   : pax_hash,
             'pax_version': pax_version,
             'status'     : 'transferring',
             'location'   : output_fullname + '.root',
             'checksum'   : None,
             'creation_time' : datetime.datetime.utcnow(),
             'creation_place' : host}

    # This query is used to find if this run has already processed this data
    # in the same way.  If so, quit.
    query = {'detector': 'tpc',
             'name' : name,

             # This 'data' gets deleted later and only used for checking
             'data': { '$elemMatch': { 'host': host,
                                       'type': 'processed',
                                       'pax_hash': pax_hash,
                                       'pax_version': pax_version}}}
    doc = collection.find_one(query)  # Query DB
    if doc is not None:
        print("Already processed.  Clear first.  %s" % json.dumps(doc))
        return

    # Not processed this way already, so notify run DB we will
    doc = collection.find_one_and_update({'detector': 'tpc', 'name' : name},
                                         {'$push': {'data': datum}},
                                         return_document=ReturnDocument.AFTER)

    # Determine based on run DB what settings to use for processing.
    if doc['reader']['self_trigger']:
        pax_config='XENON1T'
    else:
        pax_config='XENON1T_LED'

    # Try to process data.
    try:
        print('processing', name, in_location, pax_config)
        p = core.Processor(config_names=pax_config,
                           config_dict={'pax': {'input_name' : in_location,
                                                'output_name': output_fullname,
                                                'n_cpus': ncpus}})
        p.run()

    except Exception as exception:
        # Data processing failed.
        datum['status'] = 'error'
        collection.update(query, {'$set': {'data.$': datum}})
        raise

    datum['status'] = 'verifying'
    collection.update(query, {'$set': {'data.$': datum}})

    datum['checksum'] = checksumdir._filehash(datum['location'],
                                              hashlib.sha512)
    if verify():
        datum['status'] = 'transferred'
    else:
        datum['status'] = 'failed'
    collection.update(query, {'$set': {'data.$': datum}})


class ProcessBatchQueue(Task):
    "Create and submit job submission script."

    def submit(self, in_location, host, pax_version, pax_hash, out_location, ncpus):
        '''Submission Script
        '''
 
        name = self.run_doc['name']
        run_mode = ''

        # Script parts common to all sites
        script_template = """#!/bin/bash
#SBATCH --job-name={name}_{pax_version}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={ncpus}
#SBATCH --mem-per-cpu=2000
#SBATCH --mail-type=ALL
"""
        # Midway-specific script options
        if host == "midway-login1":

            script_template += """
#SBATCH --output=/project/lgrandi/xenon1t/processing/logs/{name}_{pax_version}_%J.log
#SBATCH --error=/project/lgrandi/xenon1t/processing/logs/{name}_{pax_version}_%J.log
#SBATCH --account=pi-lgrandi
#SBATCH --qos=xenon1t
#SBATCH --partition=xenon1t
#SBATCH --mail-user=pdeperio@astro.columbia.edu

export PATH=/project/lgrandi/anaconda3/bin:$PATH

export PROCESSING_DIR=/project/lgrandi/xenon1t/processing/{name}_{pax_version}
        """
# ^ Hardcode warning for previous line: PROCESSING_DIR must be same as in #SBATCH --output and --error

        # Stockolm-specific script options
        elif host == "tegner-login-1":

            script_template = """
#SBATCH --output=/cfs/klemming/projects/xenon/common/xenon1t/processing/logs/{name}_{pax_version}_%J.log
#SBATCH --error=/cfs/klemming/projects/xenon/common/xenon1t/processing/logs/{name}_{pax_version}_%J.log
#SBATCH --account=xenon
#SBATCH --partition=main
#SBATCH -t 72:00:00
#SBATCH --mail-user=Boris.Bauermeister@fysik.su.se

source /afs/pdc.kth.se/home/b/bobau/load_4.8.4.sh

export PROCESSING_DIR=/cfs/klemming/projects/xenon/xenon1t/processing/{name}_{pax_version}
# WARNING: Boris should check this directory ^ 
#     multiple instances of pax should be run in separate directories to avoid clash of libraries

        """
# ^ Hardcode warning for previous line: PROCESSING_DIR must be same as in #SBATCH --output and --error

        else:
            self.log.error("Host %s processing not implemented", host)
            return

        # Script parts common to all sites
        script_template += """mkdir -p ${{PROCESSING_DIR}} {out_location}
cd ${{PROCESSING_DIR}}

source activate pax_{pax_version}

echo time cax-process {name} {in_location} {host} {pax_version} {pax_hash} {out_location} {ncpus}
time cax-process {name} {in_location} {host} {pax_version} {pax_hash} {out_location} {ncpus}

mv ${{PROCESSING_DIR}}/../logs/{name}_*.log ${{PROCESSING_DIR}}/.
"""

        script = script_template.format(name=name, in_location=in_location, host=host, pax_version=pax_version, pax_hash=pax_hash, out_location=out_location, ncpus=ncpus)
        self.log.info(script)
        qsub.submit_job(script, name+"_"+pax_version)

    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def each_run(self):

        thishost = config.get_hostname()

        # Only process at Midway or Stockholm for now
        if not thishost == "midway-login1" and not thishost== "tegner-login-1":
            self.log.debug("Host %s processing not implemented", thishost)
            return
                
        # Get desired pax versions and corresponding output directories
        versions = config.get_pax_options('versions')
        out_locations = config.get_pax_options('locations')
        
        have_processed = [False for i in range(len(versions))]
        have_raw = False
        
        # Iterate over data locations to know status
        for datum in self.run_doc['data']:

            # Is host known?
            if 'host' not in datum:
                continue

            # If the location doesn't refer to here, skip
            if datum['host'] != thishost:
                continue

            # Raw data must exist
            if datum['type'] == 'raw' and datum['status'] == 'transferred':
                have_raw = datum

            # Check if processed data already exists in DB
            if datum['type'] == 'processed':
                for ivers in range(len(versions)):
                    if versions[ivers] == datum['pax_version'] and get_pax_hash(versions[ivers], thishost) == datum['pax_hash'] and datum['status'] != 'error':
                        have_processed[ivers] = True

        # Skip if no raw data
        if not have_raw:
            self.log.debug("Skipping %s with no raw data", self.run_doc['name'])
            return

        # Get number of events in data set
        events = self.run_doc.get('trigger', {}).get('events_built', 0) 
                
        # Skip if 0 events in dataset
        if events <= 0:
            self.log.debug("Skipping %s with 0 events", self.run_doc['name'])
            return

        # Specify number of cores for pax multiprocess
        ncpus = 4 # based on Figure 2 here https://xecluster.lngs.infn.it/dokuwiki/doku.php?id=xenon:xenon1t:shockley:performance#automatic_processing
        # Should be tuned on Stockholm too

        # Reduce to 1 CPU for small number of events (sometimes pax stalls with too many CPU)
        if events < 500:
            ncpus = 1

        # Process all specified versions
        for ivers in range(len(versions)):

            pax_version=versions[ivers]
            pax_hash = get_pax_hash(pax_version, thishost)
            out_location=out_locations[ivers]

            if have_processed[ivers]:
                self.log.debug("Skipping %s already processed with %s", self.run_doc['name'], pax_version)
                continue

            queue_list = qsub.get_queue(thishost)
            if self.run_doc['name'] in queue_list: # Should check pax_version here too 
                self.log.debug("Skipping %s currently in queue", self.run_doc['name']) 
                continue

            if self.run_doc['reader']['ini']['write_mode'] == 2:
                
                self.log.info("Submitting %s with pax_%s (%s), output to %s",
                              self.run_doc['name'], pax_version, pax_hash, out_location)

                self.submit(have_raw['location'], thishost, pax_version, pax_hash, out_location, ncpus)

# Arguments from process function: (name, in_location, host, pax_version, pax_hash, out_location, ncpus):
def main():
    process(*sys.argv[1:])

