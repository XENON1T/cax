"""Process raw data into processed data

Performs batch queue operations to run pax.
"""

import datetime
import hashlib
import subprocess
import sys
from collections import defaultdict

import checksumdir
from pymongo import ReturnDocument

from cax import qsub, config
from cax.task import Task
from cax.api import api

def get_pax_hash(pax_version, host):
    # Get hash of this pax version
    if pax_version == 'head':
        git_args = "--git-dir=" + config.PAX_DEPLOY_DIRS[host] + "/.git rev-parse HEAD"
    else:
        git_args = "--git-dir=" + config.PAX_DEPLOY_DIRS[host] + "/.git rev-parse v" + pax_version

    git_out = subprocess.check_output("git " + git_args,
                                      shell=True)
    pax_hash = git_out.rstrip().decode('ascii')

    return pax_hash


def verify():
    """Verify the file

    Now is nothing.  Could check number of events later?
    """
    return True


def _process(name, in_location, host, pax_version, pax_hash, out_location, ncpus=1):
    """Called by another command.
    """
    print('Welcome to cax-process, OSG development')

    if pax_version[0] != 'v':
        pax_version = 'v' + pax_version

    # Import pax so can process the data
    from pax import core

    # Grab the Run DB so we can query it
    #collection = config.mongo_collection()

    output_fullname = out_location + '/' + name

    # New data location
    datum = {'host'          : host,
             'type'          : 'processed',
             'pax_hash'      : pax_hash,
             'pax_version'   : pax_version,
             'status'        : 'transferring',
             'location'      : output_fullname + '.root',
             'checksum'      : None,
             'creation_time' : datetime.datetime.utcnow(),
             'creation_place': host}

    # This query is used to find if this run has already processed this data
    # in the same way.  If so, quit.
    query = {'detector': 'tpc',
             'name'    : name
             }
    
    API = api()
    doc = API.get_next_run(query)
    print(query)
    
    if doc is None:
        print("Run name " + name + " not found")
        return 1

    for d in doc['data']:
        if 'pax_version' in d.keys():
            print(d['pax_version'])
    
    # Sorry
    if any( ( d['host'] == host and d['type'] == 'processed' and
              d['pax_version'] == pax_version ) for d in doc['data'] ):
        print("Already processed %s.  Clear first.  %s" % (name,
                                                           pax_version))
        print('removing...')
        API.remove_location(doc['_id'],datum)
        print('done')
        return 1
    # Not processed this way already, so notify run DB we will

    print('Not processed yet, adding to database')
    API.add_location(doc['_id'], datum)

    API = api()
    doc = API.get_next_run(query)
    print(query)
    
    if doc is None:
        print("Error finding doc after update")
        return 1

    # Determine based on run DB what settings to use for processing.
    if doc['reader']['self_trigger']:
        pax_config = 'XENON1T'
    else:
        pax_config = 'XENON1T_LED'

    # Try to process data.
    try:
        print('processing', name, in_location, pax_config)
        p = core.Processor(config_names=pax_config,
                           config_dict={'pax': {'input_name' : in_location,
                                                'output_name': output_fullname,
                                                'n_cpus'     : ncpus}})
        p.run()

    except Exception as exception:
        # Data processing failed.
        datum['status'] = 'error'
        if config.DATABASE_LOG == True:
            print("Would update database...")
            # API.update_location(doc['_id'], datum)
        raise

    datum['status'] = 'verifying'
    if config.DATABASE_LOG == True:
        print("Would update database...")
        # API.update_location(doc['_id'], datum)

    datum['checksum'] = checksumdir._filehash(datum['location'],
                                              hashlib.sha512)
    if verify():
        datum['status'] = 'transferred'
    else:
        datum['status'] = 'failed'

    if config.DATABASE_LOG == True:
        print("Would update database...")
        # API.update_location(doc['_id'], datum)
        
class ProcessBatchQueue(Task):
    "Create and submit job submission script."

    def submit(self, in_location, host, pax_version, pax_hash, out_location,
               ncpus):
        '''Submission Script
        '''

        name = self.run_doc['name']

        script_template = config.processing_script(host)
        
        script = script_template.format(name=name, in_location=in_location,
                                        processing_dir=config.get_processing_base_dir(host),
                                        host=host, pax_version=pax_version,
                                        pax_hash=pax_hash,
                                        out_location=out_location,
                                        ncpus=ncpus)
        self.log.info(script)
        qsub.submit_job(host, script, name + "_" + pax_version)

    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def each_run(self):
        if self.has_tag('donotprocess'):
            self.log.debug("Do not process tag found")
            return

        if 'processor' not in self.run_doc:
            return

        thishost = config.get_hostname()

        # Get desired pax versions and corresponding output directories
        versions = config.get_pax_options('processing_versions')
        if versions is None:
            self.log.debug("No pax versions specified for processing")
            return

        have_processed, have_raw = self.local_data_finder(thishost, versions)

        # Skip if no raw data
        if not have_raw:
            self.log.debug("Skipping %s with no raw data",
                           self.run_doc['name'])
            return

        if self.run_doc['reader']['ini']['write_mode'] != 2:
            return

        # Get number of events in data set
        events = self.run_doc.get('trigger', {}).get('events_built', 0)

        # Skip if 0 events in dataset
        if events <= 0:
            self.log.debug("Skipping %s with 0 events", self.run_doc['name'])
            return

        # Specify number of cores for pax multiprocess
        if events < 500:
            # Reduce to 1 CPU for small number of events (sometimes pax stalls
            # with too many CPU)
            ncpus = 1
        else:
            ncpus = 4  # based on Figure 2 here https://xecluster.lngs.infn.it/dokuwiki/doku.php?id=xenon:xenon1t:shockley:performance#automatic_processing

        # Process all specified versions
        for version in versions:
            version = version
            pax_hash = get_pax_hash(version, thishost)

            out_location = config.get_processing_dir(thishost, version)

            if have_processed[version]:
                self.log.debug("Skipping %s already processed with %s",
                               self.run_doc['name'],
                               version)
                continue
            
            #queue_list = qsub.get_queue(thishost)
            # Should check version here too
            #if self.run_doc['name'] in queue_list:
               # self.log.debug("Skipping %s currently in queue",
                 #              self.run_doc['name'])
                #continue

            self.log.info("Submitting %s with pax_%s (%s), output to %s",
                          self.run_doc['name'], version, pax_hash,
                          out_location)

            self.submit(have_raw['location'], thishost, version,
                        pax_hash, out_location, ncpus)

    def local_data_finder(self, thishost, versions):
        have_processed = defaultdict(bool)
        have_raw = False
        # Iterate over data locations to know status
        for datum in self.run_doc['data']:

            # Is host known?
            if 'host' not in datum:
                continue

            # If the location is Midway SRM...
            if datum['host']  == "midway-srm":
                # ... must access from midway-login1
                if thishost != "midway-login1":
                    continue

            # Otherwise, if the location doesn't refer to here, skip
            elif datum['host'] != thishost:
                continue

            # Raw data must exist
            if datum['type'] == 'raw' and datum['status'] == 'transferred':
                have_raw = datum

            # Check if processed data already exists in DB
            if datum['type'] == 'processed':
                for version in versions:
                    if version == datum['pax_version']:
                        have_processed[version] = True

        return have_processed, have_raw


# Arguments from process function: (name, in_location, host, pax_version,
#                                   pax_hash, out_location, ncpus):
def main():
    _process(*sys.argv[1:])
