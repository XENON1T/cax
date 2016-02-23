import config
import qsub
import sys
import checksum
from pax import __version__ as pax_version

def process(name, location, host, pax_config='XENON1T_LED'):
    from pax import core
    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    # New data
    datum = {'type' : 'processed',
             'host' : host,
             'status' : 'processing',
             'location' : location + '.root',
             'checksum' : None,
             'pax_version' : pax_version}
    query = {'detector' : 'tpc',
             'name' : name}
    collection.update(query,
                      {'$push': {'data': datum}})

    query['data.type']        = datum['type']
    query['data.host']        = datum['host']
    query['data.pax_version'] = datum['pax_version']

    try:
        print('processing', name, location)
        p = core.Processor(config_names=pax_config,
                           config_dict={'pax': { 'input_name':  location,
                                                 'output_name': location}})
        p.run()
    except Exception as exception:
        datum['status'] = 'error'
        collection.update(query,
                          {'$set': {'data.$' : datum}})
        raise

    datum['status'] = 'verifying'
    collection.update(query,
                       {'$set': {'data.$' : datum}})

    datum['checksum'] = checksum.filehash(datum['location'])
    if verify():
        datum['status'] = 'processed'
    else:
        datum['status'] = 'failed'
    collection.update(query,
                       {'$set': {'data.$' : datum}})

def submit(name, location, host):
    '''Submit XENON100 pax processing jobs to ULite
    Author: Chris, Bart, Jelle, Nikhef
    Last update:   2015.09.07
    This is meant to help you do bulk processing on the XENON100 data on the LNGS ULite cluster.
    This command will produce a shell script for you to run, the shell script does the actual submission.
    The shell scripts which ULite has to run (with calls to pax) are placed in temporary files.
    Call with --help to discover syntax and options.
    '''

    script_template = """#!/bin/bash
export PATH=/data/xenon/anaconda/envs/pax/bin:$PATH
source activate pax
cd /user/ctunnell/cax/cax/
python /user/ctunnell/cax/cax/process.py {name} {location} {host}
    """

    script = script_template.format(name=name, location=location, host=host)
    qsub.submit_job(script, name, 'generic')


def verify():
    return True

def process_all():
    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    # For each TPC run, check if should be uploaded
    for doc in collection.find({'detector' : 'tpc'}):
        if 'data' not in doc:
            continue

        have_raw = False
        have_processed = False

        # Iterate over data locations to know status
        for datum in doc['data']:
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
            print("Submitting", doc['name'])
            
            submit(doc['name'],
                   have_raw['location'],
                   have_raw['host'])


if __name__ == "__main__":
    if len(sys.argv) == 1:
        process_all()
    else:
        process(sys.argv[1], sys.argv[2], sys.argv[3])
