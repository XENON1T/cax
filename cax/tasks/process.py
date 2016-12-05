"""Process raw data into processed data

Performs batch queue operations to run pax.
"""

import datetime
import hashlib
import subprocess
import sys
import os
from collections import defaultdict
import time
import pax
import checksumdir
import json
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
             out_location, ncpus=1, disable_updates=False, json_file=""):
    """Called by another command.
    """
    print('Welcome to cax-process, OSG development with pax mod')

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


    if json_file == "":
        print("No JSON file supplied.")
        return 1

    # if processing on OSG
    elif json_file != "":
        with open(json_file, "r") as doc_file:
            doc = json.load(doc_file)
        
    
    # Determine based on run DB what settings to use for processing.

    if doc['detector'] == 'muon_veto':
        pax_config = 'XENON1T_MV'
        decoder = 'BSON.DecodeZBSON'

    elif doc['detector'] == 'tpc':
        #decoder = 'Pickle.DecodeZPickle'
        if doc['reader']['self_trigger']:
            pax_config = 'XENON1T'
        else:
            pax_config = 'XENON1T_LED'
    
    if host == 'login':
        output_fullname = 'output/' + name

    config_dict = {'pax': {'input_name' : in_location,
                           'output_name': output_fullname,
                           #'n_cpus'     : ncpus,
                           #'stop_after' : 20,
                           #'decoder_plugin' : decoder,
                           'look_for_config_in_runs_db' : False
                           },
                   'DEFAULT': {'lock_breaking_timeout': 600},
                   'Queues': {'event_block_size': 1,
                              'max_blocks_on_heap': 1000,
                              'timeout_after_sec': 600}}

    mongo_config = doc['processor']
    config_dict = configuration.combine_configs(mongo_config,config_dict)

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
        # processing failed
        raise
        
class ProcessBatchQueue(Task):
    "Create and submit job submission script."

    def submit(self, in_location, host, pax_version, pax_hash, out_location,
               ncpus, disable_updates, json_file):
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
                           disable_updates = disable_updates,
                           json_file = json_file
                           )

        script = config.processing_script(script_args)
        self.log.info(script)

        if host == 'login':
            outer_dag_dir = "/xenon/ershockley/cax/{pax_version}/{name}/dags".format(name=name,
                                                                                      pax_version=pax_version)
            inner_dag_dir = outer_dag_dir + "/inner_dags"

            if not os.path.exists(inner_dag_dir):
                os.makedirs(inner_dag_dir)

            joblog_dir = "/xenon/ershockley/cax/{pax_version}/{name}/joblogs".format(name=name, pax_version=pax_version)

            if not os.path.exists(joblog_dir):
                os.mkdir(joblog_dir)
            
            outer_dag_file = outer_dag_dir + "/{name}_outer.dag".format(name=name)
            inner_dag_file = inner_dag_dir + "/{name}_inner.dag".format(name=name)

            qsub.submit_dag_job(number, outer_dag_file, inner_dag_file, out_location, script, pax_version, json_file)

        else:
            qsub.submit_job(host, script, name + "_" + pax_version)

    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def each_run(self):
        # check if too many jobs on grid 
        if len(qsub.get_queue()) > 1000:
            self.log.info("Too many jobs in queue, wait 2 minutes and try again")
            time.sleep(120)
            return

        thishost = config.get_hostname()

        version = 'v%s' % pax.__version__

        # Specify number of cores for pax multiprocess
        ncpus = 1

        disable_updates = not config.DATABASE_LOG

        pax_hash = "n/a"
        
        out_location = config.get_processing_dir(thishost, version)
        if not os.path.exists(out_location):
            os.makedirs(out_location)
            
        name = self.run_doc['name']
            
        # New data location
        datum = {'host'          : thishost,
                 'type'          : 'processed',
                 'pax_hash'      : pax_hash,
                 'pax_version'   : version,
                 'status'        : 'transferring',
                 'location'      : out_location + "/" + str(name),
                 'checksum'      : None,
                 'creation_time' : datetime.datetime.utcnow(),
                 'creation_place': thishost}
        

        # This query is used to find if this run has already processed this data
        # in the same way.  If so, quit.
        query = {'detector': 'tpc',
                 'name'    : name
                 #"data" : {"$not" : {"$elemMatch" : {"host" : thishost,
                 #                                    "type" : "processed",
                 #                                    "pax_version" : version}
                 #                    },
                 #          "$elemMatch" : {"host" : thishost,
                 #                          "type" : "raw"}
                 #          },
                 #'reader.ini.write_mode' : 2,
                 #'trigger.events_built' : {"$gt" : 0},
                 #'processor.DEFAULT.gains' : {'$exists' : True},
                 #'processor.DEFAULT.electron_lifetime_liquid' : {'$exists' : True},
                 #'tags' : {"$not" : {'$elemMatch' : {'name' : 'donotprocess'}}},
                 }
        #from pprint import pprint
        #pprint(query)
        # initialize instance of api class
        API = api()
        doc = API.get_next_run(query)
        
        #print()
        #pprint(doc["data"])
        #print()
        
        # Check if suitable run found (i.e. no other processing or error, etc)
        if doc is None:
            print("Run name " + name + " not suitable")
            return 1

        if doc["reader"]["ini"]["write_mode"] != 2:
            return 1
        if doc["trigger"]["events_built"] == 0:
            return 1

        if self.has_tag('donotprocess'):
            self.log.debug("Do not process tag found")
            return

        if 'processor' not in self.run_doc or 'DEFAULT' not in self.run_doc['processor']:
            return

        processing_parameters = self.run_doc['processor']['DEFAULT']
        
        if 'gains' not in processing_parameters or \
            'electron_lifetime_liquid' not in processing_parameters:
            self.log.debug('no gains or electron lifetime! skipping processing')
            return

        if any( ( d['host'] == thishost and d['type'] == 'processed' and
                  d['pax_version'] == version ) for d in doc['data'] ):
            print("Already processed %s.  Clear first.  %s" % (name,
                                                               version))
            return


        json_file = "/xenon/ershockley/jsons/" + str(name) + ".json"
        with open(json_file, "w") as f:
            json.dump(doc, f)

        self.log.info("Processing %s with pax_%s (%s), output to %s",
                      self.run_doc['name'], version, pax_hash,
                      out_location)

        
        # get raw data path for this run
        raw_location = None
        for entry in doc["data"]:
            if (entry["host"] == thishost and entry["type"] == "raw"):
                raw_location = entry["location"]

        if raw_location is None:
            print("Couldn't find raw data location for " + str(name))
            return 1 

        self.submit(raw_location, thishost, version,
                    pax_hash, out_location, ncpus, disable_updates, json_file)
        
        if config.DATABASE_LOG == True:
            API.add_location(doc['_id'], datum)
            print('location added to database')
            
        time.sleep(5)

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
                    if version == datum['pax_version'] and datum['status'] == 'transferred':
                        have_processed[version] = True

        return have_processed, have_raw


# Arguments from process function: (name, in_location, host, pax_version,
#                                   pax_hash, out_location, ncpus):
def main():
    _process(*sys.argv[1:])
