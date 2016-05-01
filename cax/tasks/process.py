import sys
import datetime
import hashlib

from pax import __version__ as pax_version

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

def process(name, location, host):
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
             'creation_time' : datetime.datetime.utcnow()}
    query = {'detector': 'tpc',
             'name'    : name}

    if collection.find_one({'detector': 'tpc',
                            'name' : name,
                            "data": { "$elemMatch": { "host": "midway-login1",
                                                      "type": "processed",
                                                      "status": "transferred"}}}) is not None:
        return

    elif collection.find_one({'detector': 'tpc',
                            'name' : name,
                            "data": { "$elemMatch": { "host": "midway-login1",
                                                      "type": "processed",
                                                      "status": "transferring"}}}) is not None:
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
                                                'n_cpus': 1}})
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

    def submit(self, location, host):
        '''Submit XENON100 pax processing jobs to ULite
        Author: Chris, Bart, Jelle, Nikhef
        Last update:   2015.09.07
        '''
        name = self.run_doc['name']
        run_mode = ''
        script_template = """#!/bin/bash
#SBATCH --output=/project/lgrandi/xenon1t/processing/logs/{name}_%J.log
#SBATCH --error=/project/lgrandi/xenon1t/processing/logs/{name}_%J.log
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --account=pi-lgrandi

export PATH=/project/lgrandi/anaconda3/bin:$PATH
source activate pax_head
cd /project/lgrandi/xenon1t/processing
echo python /project/lgrandi/deployHQ/cax/cax/tasks/process.py {name} {location} {host}
python /project/lgrandi/deployHQ/cax/cax/tasks/process.py {name} {location} {host}
        """

        script = script_template.format(name=name, location=location, host=host)
        self.log.info(script)
        qsub.submit_job(script, name)

    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def each_run(self):
        have_raw = False
        have_processed = False

        # Iterate over data locations to know status
        for datum in self.run_doc['data']:
            # Is host known?
            if 'host' not in datum:
                continue

            # If the location doesn't refer to here, skip
            if datum['host'] != config.get_hostname():
                continue

            if datum['type'] == 'raw':
                if datum['status'] == 'transferred':
                    have_raw = datum

            if datum['type'] == 'processed':
                if datum['status'] != 'error':
                    have_processed = True

        if have_raw and not have_processed:
            if self.run_doc['reader']['ini']['write_mode'] == 2:
                if config.get_hostname() == "midway-login1":

                    # Get number of events in data set
                    events = self.run_doc.get('trigger', {}).get('events_built', 0) 
                
                    # Only submit jobs for datasets with events
                    if events > 0:
                        self.log.info("Submitting %s", self.run_doc['name'])
                    
                        self.submit(have_raw['location'], have_raw['host'])

                    else:
                        self.log.info("Skipping %s with 0 events", self.run_doc['name'])


if __name__ == "__main__":
    process(sys.argv[1], sys.argv[2], sys.argv[3])
