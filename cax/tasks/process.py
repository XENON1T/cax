import sys
import datetime
import hashlib
import subprocess

from cax import qsub, config
from cax.task import Task

# Obtain pax repository hash
def get_pax_hash(pax_version):

    # Location of GitHub source code on Midway
    PAX_DEPLOY_DIR="/project/lgrandi/deployHQ/pax"

    # Get hash of this pax version
    if pax_version == 'head':
        git_args = "--git-dir="+PAX_DEPLOY_DIR+"/.git rev-parse HEAD"
    else:
        git_args = "--git-dir="+PAX_DEPLOY_DIR+"/.git rev-parse "+pax_version

    git_out = subprocess.check_output("git "+git_args, shell=True)
    pax_hash = git_out.rstrip().decode('ascii')

    return pax_hash


def filehash(location):
    sha = hashlib.sha512()
    with open(location, 'rb') as f:
        while True:
            block = f.read(2**10) # Magic number: one-megabyte blocks.
            if not block: break
            sha.update(block)
    return sha.hexdigest()

def verify():
    return True

def process(name, in_location, host, pax_version, pax_hash, out_location):

    from pax import core

    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    output_fullname = out_location + '/' + name

    # New data
    datum = {'type'       : 'processed',
             'host'       : host,
             'status'     : 'processing',
             'location'   : output_fullname + '.root',
             'checksum'   : None,
             'pax_version': pax_version,
             'pax_hash'   : pax_hash,
             'creation_time' : datetime.datetime.utcnow()}
    query = {'detector': 'tpc',
             'name'    : name}

    # Skip if processed file with this pax_hash already exists
    if collection.find_one({'detector': 'tpc',
                            'name' : name,
                            "data": { "$elemMatch": { "host": host,
                                                      "type": "processed",
                                                      "status": "transferred",
                                                      "pax_hash": pax_hash}}}) is not None:
        return

    elif collection.find_one({'detector': 'tpc',
                              'name' : name,
                              "data": { "$elemMatch": { "host": host,
                                                        "type": "processed",
                                                        "status": "processing",
                                                        "pax_hash": pax_hash}}}) is not None:
        print ("Skip ", host, name, ", currently processing")
        return

    elif collection.find_one({'detector': 'tpc',
                              'name' : name,
                              "data": { "$elemMatch": { "host": host,
                                                        "type": "processed",
                                                        "status": "error",
                                                        "pax_hash": pax_hash}}}) is not None:
        print ("Skip ", host, name, ", previous processing error. Check logs.")
        return

    collection.update(query,
                      {'$push': {'data': datum}})

    query['data.type'] = datum['type']
    query['data.host'] = datum['host']
    query['data.pax_version'] = datum['pax_version']
    query['data.pax_hash'] = datum['pax_hash']

    doc = collection.find_one(query)

    if doc['reader']['self_trigger']:
        pax_config='XENON1T'
    else:
        pax_config='XENON1T_LED'

    try:
        print('processing', name, in_location, pax_config)
        p = core.Processor(config_names=pax_config,
                           config_dict={'pax': {'input_name' : in_location,
                                                'output_name': output_fullname,
                                                'n_cpus': 4}})
        p.run()

    except Exception as exception:
        datum['status'] = 'error'
        collection.update(query,
                          {'$set': {'data.$': datum}})
        raise

    datum['status'] = 'verifying'
    collection.update(query,
                      {'$set': {'data.$': datum}})

    datum['checksum'] = filehash(datum['location'])
    if verify():
        datum['status'] = 'transferred'
    else:
        datum['status'] = 'failed'
    collection.update(query,
                      {'$set': {'data.$': datum}})

    print (datum)

class ProcessBatchQueue(Task):
    "Create and submit job submission script."

    def submit(self, in_location, host, pax_version, pax_hash, out_location):
        '''Midway Submission Script
        Last update:   2016.04.29
        '''
        name = self.run_doc['name']
        run_mode = ''
        script_template = """#!/bin/bash
#SBATCH --output=/project/lgrandi/xenon1t/processing/logs/{name}_%J.log
#SBATCH --error=/project/lgrandi/xenon1t/processing/logs/{name}_%J.log
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --account=pi-lgrandi

echo "nprocs = `nproc`"
export PATH=/project/lgrandi/anaconda3/bin:$PATH
source activate pax_{pax_version}
cd /project/lgrandi/xenon1t/processing
echo /home/pdeperio/160429-cax_processing/cax/cax/tasks/process.py {name} {in_location} {host} {pax_version} {pax_hash} {out_location}
python /home/pdeperio/160429-cax_processing/cax/cax/tasks/process.py {name} {in_location} {host} {pax_version} {pax_hash} {out_location}
        """

        script = script_template.format(name=name, in_location=in_location, host=host, pax_version=pax_version, pax_hash=pax_hash, out_location=out_location)
        #print ("fake qsub ", script)
        qsub.submit_job(script, name)

    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def each_run(self):

        # (Hard-code) Auto-processing only at Midway for now
        if not config.get_hostname() == "midway-login1":
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
            if datum['host'] != config.get_hostname():
                continue

            # Raw data must exist
            if datum['type'] == 'raw' and datum['status'] == 'transferred':
                have_raw = datum

            # Check if processed data already exists in DB
            if datum['type'] == 'processed':
                for ivers in range(len(versions)):
                    if versions[ivers] == datum['pax_version'] and get_pax_hash(versions[ivers]) == datum['pax_hash']:
                        have_processed[ivers] = True

        # Process all specified versions
        for ivers in range(len(versions)):

            pax_version=versions[ivers]
            pax_hash = get_pax_hash(pax_version)
            out_location=out_locations[ivers]

            if have_raw and not have_processed[ivers]:
            
                if self.run_doc['reader']['ini']['write_mode'] == 2:
                
                    self.log.info("Submitting %s with pax_%s (%s), output to %s",
                                  self.run_doc['name'], pax_version, pax_hash, out_location)
                    
                    self.submit(have_raw['location'],
                                have_raw['host'], pax_version, pax_hash, out_location)

if __name__ == "__main__":
    process(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
