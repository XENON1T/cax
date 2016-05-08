import sys
import datetime
import hashlib
import subprocess

from pax import __version__ as pax_version

from cax import qsub, config
from cax.task import Task

# Obtain pax repository hash
def get_pax_hash(pax_version, processat):

    PAX_DEPLOY_DIR = ''
    if processat == 'midway-login-1':
        # Location of GitHub source code on Midway
        PAX_DEPLOY_DIR="/project/lgrandi/deployHQ/pax"
    if processat == 'tegner-login-1':
        # Location of GitHub source code on Midway
        PAX_DEPLOY_DIR="/afs/pdc.kth.se/projects/xenon/software/pax"

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

def process(name, location, host, pax_version, pax_hash, out_location):
    from pax import core
    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    # New data
    datum = {'type'       : 'processed',
             'host'       : host,
             'status'     : 'transferring',
             'location'   : location + '.root',
             'checksum'   : None,
             'pax_version': pax_version,
             'pax_has': pax_hash,
             'creation_time' : datetime.datetime.utcnow()}
    query = {'detector': 'tpc',
             'name'    : name}


    if collection.find_one({'detector': 'tpc',
                            'name' : name,
                            "data": { "$elemMatch": { "host": datum['host'],
                                                      "type": "processed",
                                                      "status": "transferred"}}}) is not None:
        print("Abort", name, "already processed")
        return

    elif collection.find_one({'detector': 'tpc',
                            'name' : name,
                            "data": { "$elemMatch": { "host": datum['host'],
                                                      "type": "processed",
                                                      "status": "transferring"}}}) is not None:
        print("Abort", name, "already currently processing")
        return

    collection.update(query,
                      {'$push': {'data': datum}})

    query['data.type'] = datum['type']
    query['data.host'] = datum['host']
    query['data.pax_version'] = datum['pax_version']

    doc = collection.find_one(query)

    if doc['reader']['self_trigger']:
        pax_config='XENON1T'
    else:
        pax_config='XENON1T_LED'

    try:
        print('processing', name, location)
        p = core.Processor(config_names=pax_config,
                           config_dict={'pax': {'input_name' : location,
                                                'output_name': location,
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


class ProcessBatchQueue(Task):
    "Perform a checksum on accessible data."

    def submitMidway(self, in_location, host, pax_version, pax_hash, out_location):
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
        print ("fake qsub ", script)
        #qsub.submit_job(script, name, 'midway-login-1')


    def submitPDC(self, in_location, host, pax_version, pax_hash, out_location):
        '''Stockholm Submission Script
        '''
        name = self.run_doc['name']
        run_mode = ''
        script_template = """#!/bin/bash
#SBATCH --job-name=Xe1tReprocessing
#SBATCH --partition=main
#SBATCH --output=/cfs/klemming/projects/xenon/common/xenon1t/slurm_reprocessing_logs/{name}_output.log
#SBATCH --error=/cfs/klemming/projects/xenon/common/xenon1t/slurm_reprocessing_logs/{name}_error.log
#SBATCH -A xenon
#SBATCH --nodes=1
#SBATCH --cpus-per-task=32
#SBATCH -t 72:00:00
#SBATCH --mem=24000
#SBATCH --mail-user=Boris.Bauermeister@fysik.su.se
#SBATCH --mail-type=ALL
source /afs/pdc.kth.se/home/b/bobau/load_4.8.4.sh
source activate pax_head
cd /cfs/klemming/projects/xenon/xenon1t/root/pax_head
echo python /afs/pdc.kth.se/projects/xenon/software/cax/cax/tasks/process.py {name} {in_location} {host} {pax_version} {pax_hash} {out_location}
python /afs/pdc.kth.se/projects/xenon/software/cax/cax/tasks/process.py {name} {in_location} {host} {pax_version} {pax_hash} {out_location}
        """
        script = script_template.format(name=name, in_location=in_location, host=host, pax_version=pax_version, pax_hash=pax_hash, out_location=out_location)
        print ("fake qsub ", script)
        #qsub.submit_job(script, name, 'tegner-login-1')

    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def each_run(self):
        
        # Get desired pax versions and corresponding output directories
        versions = config.get_pax_options('versions')
        out_locations = config.get_pax_options('locations')
        #print( versions, out_locations)
        
        have_processed = [False for i in range(len(versions))]
        have_raw = False
                
        #have_raw = False
        #have_processed = False

        # Iterate over data locations to know status
        for datum in self.run_doc['data']:
            # Is host known?
            if 'host' not in datum:
                continue

            # If the location doesn't refer to here, skip
            if datum['host'] != config.get_hostname():
                print( datum['host'])
                continue

            if datum['type'] == 'raw':
                if datum['status'] == 'transferred':
                    have_raw = datum

            if datum['type'] == 'processed':
                if datum['status'] != 'error':
                    have_processed = True

        # Process all specified versions
        for ivers in range(len(versions)):
            #print(config.get_hostname())
            
            pax_version=versions[ivers]
            pax_hash = get_pax_hash(pax_version, config.get_hostname())
            out_location=out_locations[ivers]

            if have_raw and not have_processed:
                if self.run_doc['reader']['ini']['write_mode'] == 2:
                
                  if config.get_hostname() == "tegner-login-1":
                    print('tegner')
                    # Get number of events in data set
                    events = self.run_doc.get('trigger', {}).get('events_built', 0) 
                
                    # Only submit jobs for datasets with events
                    if events > 0:
                        self.log.info("Submitting %s (@Tegner)", self.run_doc['name'], pax_version, pax_hash, out_location)
                    
                        self.submitPDC(have_raw['location'], have_raw['host'], pax_version, pax_hash, out_location)

                    else:
                        self.log.info("Skipping %s with 0 events", self.run_doc['name'])
                
                
                  if config.get_hostname() == "midway-login1":

                    # Get number of events in data set
                    events = self.run_doc.get('trigger', {}).get('events_built', 0) 
                
                    # Only submit jobs for datasets with events
                    if events > 0:
                        self.log.info("Submitting %s (@Tegner)", self.run_doc['name'], pax_version, pax_hash, out_location)
                    
                        self.submitMidway(have_raw['location'], have_raw['host'], pax_version, pax_hash, out_location)

                    else:
                        self.log.info("Skipping %s with 0 events", self.run_doc['name'])


if __name__ == "__main__":
    process(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
