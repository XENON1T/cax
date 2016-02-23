from paramiko import SSHClient, util
from scp import SCPClient
import config
import os
import tempfile
import argparse
import subprocess
import qsub
from pax import __version__ as pax_version

def process():
    pax_instance = core.Processor(config_names=config)
    pass

def submit(name, location, config='XENON1T_LED'):
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
echo paxer --input {location} --output {location} --config {config}
paxer --input {location} --output {location} --config {config}
    """

    script = script_template.format(location=location,
                                    config=config)
    qsub.submit_job(script, name, 'generic')


def verify():
    pass

def process_all():
    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    # For each TPC run, check if should be uploaded
    for doc in collection.find({'detector' : 'tpc'}):
        # For this run, where can we upload to?

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
            local_config = config.get_config(config.get_hostname())

            # New data
            datum = {'type' : 'processed',
                     'host' : config.get_hostname(),
                     'status' : 'processing',
                     'location' : have_raw['location'] + '.root',
                     'checksum' : None,
                     'pax_version' : pax_version}
            #collection.update({'_id': doc['_id']},
            #                  {'$push': {'data': datum}})
            print('processing', doc['name'], have_raw['location'])
            submit(doc['name'],
                   have_raw['location'])

            datum['status'] = 'verifying'
            #collection.update({'_id': doc['_id'],
            #                   'data.host' : datum['host'],
            #                   'data.type' : datum['type']},
            #                   {'$set': {'data.$' : datum}})

            if verify():
                datum['status'] = 'processed'
            else:
                datum['status'] = 'failed'
            #collection.update({'_id': doc['_id'],
            #                   'data.host' : datum['host'],
            #                   'data.type' : datum['type']},
            #                   {'$set': {'data.$' : datum}})





if __name__ == "__main__":
    process_all()
