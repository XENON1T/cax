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

def process(name, location, host,
            pax_config='XENON1T_LED'):
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
    collection.update(query,
                      {'$push': {'data': datum}})

    query['data.type'] = datum['type']
    query['data.host'] = datum['host']
    query['data.pax_version'] = datum['pax_version']

    try:
        print('processing', name, location)
        p = core.Processor(config_names=pax_config,
                           config_dict={'pax': {'input_name' : location,
                                                'output_name': location}})
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
#!/bin/bash
#SBATCH --output=/home/tunnell/test/myout_000002.txt
#SBATCH --error=/home/tunnell/test/myerr_000002.txt
#SBATCH --ntasks=16
#SBATCH --account=kicp
#SBATCH --partition=kicp
#SBATCH --exclusive
source activate pax_head
#paxer --input /project/lgrandi/xenon1t/160317_2324 --output ~/test/blah --stop_after 10 --config XENON1T
    export PATH=/data/xenon/anaconda/envs/pax/bin:$PATH
    source activate pax
    cd /user/ctunnell/cax/cax/
    python /user/ctunnell/cax/cax/tasks/process.py {name} {location} {host}
        """

        script = script_template.format(name=name, location=location, host=host)
        qsub.submit_job(script, name, 'generic')

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
                have_processed = True

        if have_raw and not have_processed:
            self.log.info("Submitting",
                          self.run_doc['name'])

            self.submit(have_raw['location'],
                        have_raw['host'])


if __name__ == "__main__":
    process(sys.argv[1], sys.argv[2], sys.argv[3])
