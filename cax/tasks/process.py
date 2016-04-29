import sys
import datetime
import hashlib

from cax import qsub, config
from cax.task import Task

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

def process(name, in_location, host, pax_version, out_location):

    from pax import core

    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    # New data
    datum = {'type'       : 'processed',
             'host'       : host,
             'status'     : 'processing',
             'location'   : out_location + '.root',
             'checksum'   : None,
             'pax_version': pax_version,
             'creation_time' : datetime.datetime.utcnow()}
    query = {'detector': 'tpc',
             'name'    : name}

    if collection.find_one({'detector': 'tpc',
                            'name' : name,
                            "data": { "$elemMatch": { "host": "midway-login1",
                                                       "type": "processed"}}}) is not None:
        return

    #collection.update(query,
    #                  {'$push': {'data': datum}})

    query['data.type'] = datum['type']
    query['data.host'] = datum['host']
    query['data.pax_version'] = datum['pax_version']

    doc = collection.find_one(query)

    if doc['reader']['self_trigger']:
        pax_config='XENON1T'
    else:
        pax_config='XENON1T_LED'

    try:
        print('processing', name, in_location)
        #p = core.Processor(config_names=pax_config,
        #                   config_dict={'pax': {'input_name' : in_location,
        #                                        'output_name': out_location}})
        #p.run()

    except Exception as exception:
        datum['status'] = 'error'
        #collection.update(query,
        #                  {'$set': {'data.$': datum}})
        raise

    datum['status'] = 'verifying'
    #collection.update(query,
    #                  {'$set': {'data.$': datum}})

    datum['checksum'] = filehash(datum['location'])
    if verify():
        datum['status'] = 'transferred'
    else:
        datum['status'] = 'failed'
    #collection.update(query,
    #                  {'$set': {'data.$': datum}})


class ProcessBatchQueue(Task):
    "Create and submit job submission script."

    def submit(self, in_location, host, pax_version, out_location):
        '''Midway Submission Script
        Last update:   2016.04.29
        '''
        name = self.run_doc['name']
        run_mode = ''
        script_template = """#!/bin/bash
#SBATCH --output=/project/lgrandi/xenon1t/processing/logs/{name}_%J.log
#SBATCH --error=/project/lgrandi/xenon1t/processing/logs/{name}_%J.log
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --qos=xenon1t
#SBATCH --partition=xenon1t
#SBATCH --account=pi-lgrandi

export PATH=/project/lgrandi/anaconda3/bin:$PATH
source activate pax_{pax_version}
cd /project/lgrandi/xenon1t/processing
python /home/pdeperio/160429-cax_processing/cax/cax/tasks/process.py {name} {in_location} {host} {pax_version} {out_location}
        """

        script = script_template.format(name=name, in_location=in_location, host=host, pax_version=pax_version, out_location=out_location)
        print ("fake qsub ", script)
        #qsub.submit_job(script, name)

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
                    if versions[ivers] == datum['pax_version']:
                        have_processed[ivers] = True

        # Process all specified versions
        for ivers in range(len(versions)):

            pax_version=versions[ivers]
            out_location=out_locations[ivers]

            if have_raw and not have_processed[ivers]:
            
                if self.run_doc['reader']['ini']['write_mode'] == 2:
                
                    self.log.info("Submitting %s with pax_%s, output to %s",
                                  self.run_doc['name'], pax_version, out_location)
                    
                    self.submit(have_raw['location'],
                                have_raw['host'], pax_version, out_location)

if __name__ == "__main__":
    process(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
