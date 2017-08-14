"""Process raw data into processed data

Performs batch queue operations to run pax.
"""

import datetime
import hashlib
import subprocess
import sys
import os
from collections import defaultdict

import pax
import hax
import checksumdir
from pymongo import ReturnDocument

from cax import qsub, config
from cax.task import Task


def init_hax(in_location, pax_version, out_location):
    hax.init(experiment='XENON1T',
             main_data_paths=[in_location+'pax_'+pax_version],
             minitree_paths = [out_location])

def verify():
    """Verify the file

    Now is nothing.  Could check number of events later?
    """
    return True


def _process_hax(name, in_location, host, pax_version,
             out_location, detector='tpc'):
    """Called by another command.
    """
    print('Welcome to cax-process')

    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    if detector == 'muon_veto':
        output_fullname = out_location + '/' + name + '_MV'
    elif detector == 'tpc':
        output_fullname = out_location + '/' + name

    os.makedirs(out_location, exist_ok=True)

    try:
        print ('creating hax minitrees', name, in_location)
        init_hax(in_location, pax_version, out_location)   # may initialize once only
        hax.minitrees.load_single_dataset(name, ['Corrections', 'Basics', 'Fundamentals',
                                  'DoubleScatter', 'LargestPeakProperties',
                                  'TotalProperties',  'Extended', 'Proximity'])

    except Exception as exception:
        raise


class ProcessBatchQueueHax(Task):
    "Create and submit job submission script."

    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def each_run(self):

        thishost = config.get_hostname()

        version = 'v%s' % pax.__version__
        have_processed, have_raw = self.local_data_finder(thishost,
                                                          version)

        # Skip if no raw data
        if not have_processed:
            self.log.debug("Skipping %s with no processed data", self.run_doc['name'])
            return

        in_location = config.get_processing_dir(thishost,version)
        out_location = config.get_minitrees_dir(thishost,version)
            
        queue_list = qsub.get_queue(thishost)
            # Should check version here too
        if self.run_doc['name'] in queue_list:
            self.log.debug("Skipping %s currently in queue",
                           self.run_doc['name'])
            return

        self.log.info("Processing %s with hax_%s, output to %s",
                      self.run_doc['name'], version, 
                      out_location)


        _process_hax(self.run_doc['name'], in_location, thishost,
                     version, out_location,
                     self.run_doc['detector'])


    def local_data_finder(self, thishost, version):
        have_processed = False
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
            if datum['type'] == 'processed' and datum['status'] == 'transferred':
                if version == datum['pax_version']:
                    have_processed = True
                    
        return have_processed, have_raw


# Arguments from process function: (name, in_location, host, pax_version,
#                                   out_location, ncpus):
def main():
    _process_hax(*sys.argv[1:])
