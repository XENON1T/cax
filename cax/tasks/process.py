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
import checksumdir
#from pymongo import ReturnDocument

from cax import qsub, config
from cax.task import Task
from cax.api import api

def verify():
    """Verify the file

    Now is nothing.  Could check number of events later?
    """
    return True


def _process(name, in_location, host, pax_version, pax_hash,
             out_location, ncpus=1, disable_updates=False):
    """Called by another command.
    """
    print('Welcome to cax-process, OSG development with pax mod')

    print('disable_updates =', disable_updates)
    print('disable_updates == True:',  disable_updates=='True')

    if disable_updates == 'True':
        print("disabling updates")
        config.set_database_log(False)

    else:
        print("enabling updates")
        config.set_database_log(True)

    print("database_log = ",config.DATABASE_LOG)


    if pax_version[0] != 'v':
        pax_version = 'v' + pax_version

    # Import pax so can process the data
    from pax import core, configuration   

    output_fullname = out_location + '/' + name

    if host != 'login':
        os.makedirs(out_location, exist_ok=True)
    else:
        os.makedirs('output', exist_ok=True)

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

    # need an updated datum dict for api.update_location()
    updatum = datum.copy()
    
    # This query is used to find if this run has already processed this data
    # in the same way.  If so, quit.
    query = {'detector': 'tpc',
             'name'    : name
             }

    # initialize instance of api class
    API = api()
    
    doc = API.get_next_run(query)
    
    if doc is None:
        print("Run name " + name + " not found")
        return 1
   
    # Sorry
    if any( ( d['host'] == host and d['type'] == 'processed' and
              d['pax_version'] == pax_version ) for d in doc['data'] ):
        print("Already processed %s.  Clear first.  %s" % (name,
                                                           pax_version))
        #return 1
    
    # Not processed this way already, so notify run DB we will
    print('Not processed yet')
    if config.DATABASE_LOG == True:
        API.add_location(doc['_id'], datum)
        print('location added to database')

    API = api()
    doc = API.get_next_run(query)
    
    if doc is None:
        print("Error finding doc after update")
        return 1

    
    # Determine based on run DB what settings to use for processing.
    if doc['reader']['self_trigger']:
        pax_config = 'XENON1T'
    else:
        pax_config = 'XENON1T_LED'
    
    if host == 'login':
        output_fullname = 'output/' + name

    config_dict = {'pax': {'input_name' : in_location,
                           'output_name': output_fullname,
                           'n_cpus'     : ncpus,
                           # 'stop_after' : 20,
                           # 'decoder_plugin' : 'BSON.DecodeZBSON',
                           'look_for_config_in_runs_db' : False
                           }
                   }

    mongo_config = doc['processor']
    config_dict = configuration.combine_configs(mongo_config, override=config_dict)

    # Add run number and run name to the config_dict
    config_dict.setdefault('DEFAULT', {})
    config_dict['DEFAULT']['run_number'] = doc['number']
    config_dict['DEFAULT']['run_name'] = doc['name'] 


    # Try to process data.
    try:
        print('processing', name, in_location, pax_config)
        print('saving to', output_fullname)
        p = core.Processor(config_names=pax_config,
                           config_dict=config_dict)
        p.run()

    except Exception as exception:
        # Data processing failed.
        updatum['status'] = 'error'
        if config.DATABASE_LOG == True:
            API.update_location(doc['_id'], datum, updatum)
            # update datum to current state
            datum = updatum.copy()  
        raise

    updatum['status'] = 'verifying'
    if config.DATABASE_LOG == True:
        API.update_location(doc['_id'], datum, updatum)
        datum = updatum.copy()

    updatum['checksum'] = checksumdir._filehash(datum['location'],
                                                hashlib.sha512)
    if verify():
        updatum['status'] = 'transferred'
    else:
        updatum['status'] = 'failed'
        
    if config.DATABASE_LOG == True:
        API.update_location(doc['_id'], datum, updatum)
        
class ProcessBatchQueue(Task):
    "Create and submit job submission script."

    def submit(self, in_location, host, pax_version, pax_hash, out_location,
               ncpus, disable_updates):
        '''Submission Script
        '''

        name = self.run_doc['name']
        number = self.run_doc['number']

        script_args = dict(host=host,
                           name=name,
                           pax_version='v%s' % pax.__version__,
                           number=number,
                           out_location = out_location,
                           ncpus = ncpus,
                           disable_updates = disable_updates
                           )

        script = config.processing_script(script_args)
        self.log.info(script)
        qsub.submit_job(host, script, name + "_" + pax_version)

    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def each_run(self):
        if self.has_tag('donotprocess'):
            self.log.debug("Do not process tag found")
            return

        if 'processor' not in self.run_doc or 'DEFAULT' not in self.run_doc['processor']:
            return

        processing_parameters = self.run_doc['processor']['DEFAULT']
        
        if 'gains' not in processing_parameters or \
            'electron_lifetime_liquid' not in processing_parameters:
            self.log.debug('no gains or electron lifetime!')
            #return

        thishost = config.get_hostname()

        versions = ['v%s' % pax.__version__]

        have_processed, have_raw = self.local_data_finder(thishost,
                                                          versions)

        # Skip if no raw data
        if not have_raw:
            self.log.debug("Skipping %s with no raw data",
                           self.run_doc['name'])
            return
        print('raw data present')

        if self.run_doc['reader']['ini']['write_mode'] != 2:
            return

        print('rundoc write mode != 2')

        # Get number of events in data set (not set for early runs <1000)
        events = self.run_doc.get('trigger', {}).get('events_built', -1)

        # Skip if 0 events in dataset
        if events == 0:
            self.log.debug("Skipping %s with 0 events", self.run_doc['name'])
            return

        # Specify number of cores for pax multiprocess
        if events < 1000:
            # Reduce to 1 CPU for small number of events (sometimes pax stalls
            # with too many CPU)
            ncpus = 1
        else:
            ncpus = 8  # based on Figure 2 here https://xecluster.lngs.infn.it/dokuwiki/doku.php?id=xenon:xenon1t:shockley:performance#automatic_processing


        disable_updates = not config.DATABASE_LOG

        print(versions)
        # Process all specified versions
        for version in versions:
            pax_hash = "n/a"

            out_location = 'output/' #config.get_processing_dir(thishost, version)

            if have_processed[version]:
                self.log.info("Skipping %s already processed with %s",
                               self.run_doc['name'],
                               version)
                continue
            
            #queue_list = qsub.get_queue(thishost)
            # Should check version here too
            #if self.run_doc['name'] in queue_list:
               # self.log.debug("Skipping %s currently in queue",
                 #              self.run_doc['name'])
                #continue

            self.log.info("Processing %s with pax_%s (%s), output to %s",
                          self.run_doc['name'], version, pax_hash,
                          out_location)


            # _process(self.run_doc['name'], have_raw['location'], thishost,
                     # version, pax_hash, out_location, ncpus)


            self.submit(have_raw['location'], thishost, version,
                        pax_hash, out_location, ncpus, disable_updates)


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
