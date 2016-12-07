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
<<<<<<< HEAD
             out_location, ncpus=1, disable_updates=False, json_file=""):
=======
             out_location, detector='tpc',  ncpus=1):
>>>>>>> 0bbf65660307d56b75db4158d709785dd0f3c89c
    """Called by another command.
    """
    print('Welcome to cax-process, OSG development with pax mod')

    # Import pax so can process the data
<<<<<<< HEAD
    from pax import core, configuration   
=======
    from pax import core, parallel

    # Grab the Run DB so we can query it
    collection = config.mongo_collection()
>>>>>>> 0bbf65660307d56b75db4158d709785dd0f3c89c

    if detector == 'muon_veto':
        output_fullname = out_location + '/' + name + '_MV'
    elif detector == 'tpc':
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

<<<<<<< HEAD

    if json_file == "":
        print("No JSON file supplied.")
        return 1
=======
    # This query is used to find if this run has already processed this data
    # in the same way.  If so, quit.
    query = {'name'    : name,
             'detector' : detector,
             # This 'data' gets deleted later and only used for checking
             'data'    : {'$elemMatch': {'host'       : host,
                                         'type'       : 'processed',
                                         'pax_version': pax_version}}}
    doc = collection.find_one(query)  # Query DB
    if doc is not None:
        print("Already processed %s.  Clear first.  %s" % (name,
                                                           pax_version))
        return 1

    # Not processed this way already, so notify run DB we will
    doc = collection.find_one_and_update({'detector': detector, 'name': name},
                                         {'$push': {'data': datum}},
                                         return_document=ReturnDocument.AFTER)
>>>>>>> 0bbf65660307d56b75db4158d709785dd0f3c89c

    # if processing on OSG
    elif json_file != "":
        with open(json_file, "r") as doc_file:
            doc = json.load(doc_file)
        
    
    # Determine based on run DB what settings to use for processing.
<<<<<<< HEAD

    if doc['detector'] == 'muon_veto':
        pax_config = 'XENON1T_MV'
        decoder = 'BSON.DecodeZBSON'

    elif doc['detector'] == 'tpc':
        #decoder = 'Pickle.DecodeZPickle'
=======
    if doc['detector'] == 'muon_veto':
        pax_config = 'XENON1T_MV'
        decoder = 'BSON.DecodeZBSON'
    elif doc['detector'] == 'tpc':
        decoder = 'Pickle.DecodeZPickle'
>>>>>>> 0bbf65660307d56b75db4158d709785dd0f3c89c
        if doc['reader']['self_trigger']:
            pax_config = 'XENON1T'
        else:
            pax_config = 'XENON1T_LED'
<<<<<<< HEAD
    
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

=======
>>>>>>> 0bbf65660307d56b75db4158d709785dd0f3c89c

    # Try to process data.
    try:
        print('processing', name, in_location, pax_config)
<<<<<<< HEAD
        print('saving to', output_fullname)
        p = core.Processor(config_names=pax_config,
                           config_dict=config_dict)
        p.run()
=======
        pax_kwargs = dict(config_names=pax_config,
                          config_dict={'pax': {'input_name' : in_location,
                                               'output_name': output_fullname,
                                               'decoder_plugin': decoder},
                                       'DEFAULT': {'lock_breaking_timeout': 600},
                                       'Queues': {'event_block_size': 1,
                                                  'max_blocks_on_heap': 1000,
                                                  'timeout_after_sec': 600}})
        if ncpus > 1:
            parallel.multiprocess_locally(n_cpus=ncpus, **pax_kwargs)
        else:
            core.Processor(**pax_kwargs).run()
>>>>>>> 0bbf65660307d56b75db4158d709785dd0f3c89c

    except Exception as exception:
        # processing failed
        raise
        
class ProcessBatchQueue(Task):
    "Create and submit job submission script."

<<<<<<< HEAD
    def submit(self, in_location, host, pax_version, pax_hash, out_location,
               ncpus, disable_updates, json_file):
        '''Submission Script
        '''
=======
    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def each_run(self):
        if self.has_tag('donotprocess'):
            self.log.debug("Do not process tag found, skip processing")
            return

        if 'processor' not in self.run_doc or \
                'DEFAULT' not in self.run_doc['processor']:
            self.log.debug("processor or DEFAUT tag not in run_doc, skip processing")
            return

        processing_parameters = self.run_doc['processor']['DEFAULT']
        if 'gains' not in processing_parameters or \
            'electron_lifetime_liquid' not in processing_parameters:
            self.log.info("gains or e-lifetime not in run_doc, skip processing")
            return
>>>>>>> 0bbf65660307d56b75db4158d709785dd0f3c89c

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

<<<<<<< HEAD
            joblog_dir = "/xenon/ershockley/cax/{pax_version}/{name}/joblogs".format(name=name, pax_version=pax_version)

            if not os.path.exists(joblog_dir):
                os.mkdir(joblog_dir)
            
            outer_dag_file = outer_dag_dir + "/{name}_outer.dag".format(name=name)
            inner_dag_file = inner_dag_dir + "/{name}_inner.dag".format(name=name)
=======
        if self.run_doc['reader']['ini']['write_mode'] != 2:
            self.log.debug("write_mode != 2, skip processing")
            return

        # Get number of events in data set (not set for early runs <1000)
        events = self.run_doc.get('trigger', {}).get('events_built', 0)
>>>>>>> 0bbf65660307d56b75db4158d709785dd0f3c89c

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

<<<<<<< HEAD
        version = 'v%s' % pax.__version__
=======
            _process(self.run_doc['name'], have_raw['location'], thishost,
                     version, pax_hash, out_location,
                     self.run_doc['detector'],
                     ncpus)
>>>>>>> 0bbf65660307d56b75db4158d709785dd0f3c89c

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
                 'name'    : name,
                 "data" : {"$not" : {"$elemMatch" : {"host" : thishost,
                                                     "type" : "processed",
                                                     "pax_version" : version
                                                     }
                                     },
                           "$elemMatch" : {"host" : 'login',
                                           "type" : "raw"}
                           },
                 'reader.ini.write_mode' : 2,
                 'trigger.events_built' : {"$gt" : 0},
                 'processor.DEFAULT.gains' : {'$exists' : True},
                 'processor.DEFAULT.electron_lifetime_liquid' : {'$exists' : True},
                 'tags' : {"$not" : {'$elemMatch' : {'name' : 'donotprocess'}}},
                 }

        query = {'query' : json.dumps(query)}

        # initialize instance of api class
        API = api()
        doc = API.get_next_run(query)
    
        
        # Check if suitable run found (i.e. no other processing or error, etc)
        if doc is None:
            print("Run name " + name + " not suitable")
            return 1

        #if doc["reader"]["ini"]["write_mode"] != 2:
        #    return 1
        #if doc["trigger"]["events_built"] == 0:
        #    return 1

        #if self.has_tag('donotprocess'):
        #    self.log.debug("Do not process tag found")
        #    return

        #if 'processor' not in self.run_doc or 'DEFAULT' not in self.run_doc['processor']:
        #    return

        #processing_parameters = self.run_doc['processor']['DEFAULT']
        
        #if 'gains' not in processing_parameters or \
        #    'electron_lifetime_liquid' not in processing_parameters:
        #    self.log.debug('no gains or electron lifetime! skipping processing')
        #    return

        #if any( ( d['host'] == thishost and d['type'] == 'processed' and
        #          d['pax_version'] == version ) for d in doc['data'] ):
        #    print("Already processed %s.  Clear first.  %s" % (name,
        #                                                       version))
        #    return


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

            # If the location doesn't refer to here, skip
            if datum['host'] != thishost:
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
