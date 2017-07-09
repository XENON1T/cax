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
from bson import json_util
import re
import shlex
#from pymongo import ReturnDocument

from cax import qsub, config
from cax.task import Task
from cax.api import api

def verify():
    """Verify the file

    Now is nothing.  Could check number of events later?
    """
    return True


def _process(name, in_location, host, pax_version,
             out_location, ncpus=1, disable_updates=False, json_file="", detector='tpc'):
    """Called by another command.
    """
    print('Welcome to cax-process')

    if pax_version != 'v' + pax.__version__:
        print("This pax version is %s, not %s. Abort processing." % ("v" + pax.__version__, pax_version))
        sys.exit(1)
    # Import pax so can process the data
    from pax import core, parallel, configuration

    # Grab the Run DB if not passing json so we can query it
    if json_file == "":
        collection = config.mongo_collection()
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

        # New data location
        datum = {'host': host,
                 'type': 'processed',
                 'pax_hash': pax_hash,
                 'pax_version': pax_version,
                 'status': 'transferring',
                 'location': output_fullname + '.root',
                 'checksum': None,
                 'creation_time': datetime.datetime.utcnow(),
                 'creation_place': host}

        # Not processed this way already, so notify run DB we will
        doc = collection.find_one_and_update({'detector': detector, 'name': name},
                                            {'$push': {'data': datum}},
                                            return_document=ReturnDocument.AFTER)

    # if processing on OSG (given json file)
    else:
        with open(json_file, "r") as doc_file:
            doc = json.load(doc_file)

        detector = doc['detector']

        basedir = out_location
        if host == 'login':
            basedir = 'output'

        if detector == 'muon_veto':
            output_fullname = basedir + '/' + name + '_MV'
            pax_config = 'XENON1T_MV'
            decoder = 'BSON.DecodeZBSON'

        elif detector == 'tpc':
            output_fullname = basedir + '/' + name
            decoder = 'Pickle.DecodeZPickle'

            if doc['reader']['self_trigger']:
                pax_config = 'XENON1T'
            else:
                pax_config = 'XENON1T_LED'

    os.makedirs(basedir, exist_ok=True)

    # New data location
    datum = {'host': host,
             'type': 'processed',
             'pax_version': pax_version,
             'status': 'transferring',
             'location': output_fullname + '.root',
             'checksum': None,
             'creation_time': datetime.datetime.utcnow(),
             'creation_place': host}
    # Determine based on run DB what settings to use for processing.

    pax_db_call = True
    if json_file != "":
        pax_db_call = False
        
    config_dict = {'pax': {'input_name' : in_location,
                           'output_name': output_fullname,
                           'look_for_config_in_runs_db' : pax_db_call,
                           'decoder_plugin' : decoder
                           }}

    # if processing tpc data, need to pass the corrections
    # Only for tpc data since this overrides .ini configuration and screws up MV gains

    if detector == 'tpc':
        mongo_config = doc['processor']
        config_dict = configuration.combine_configs(mongo_config, config_dict)

    # Add run number and run name to the config_dict
    config_dict.setdefault('DEFAULT', {})
    config_dict['DEFAULT']['run_number'] = doc['number']
    config_dict['DEFAULT']['run_name'] = doc['name'] 

    if host == 'midway-login1' and ncpus > 1:
        config_dict['DEFAULT']['lock_breaking_timeout'] = 600

        config_dict.setdefault('Queues', {})
        config_dict['Queues']['event_block_size'] = 1
        config_dict['Queues']['max_blocks_on_heap'] = 1000
        config_dict['Queues']['timeout_after_sec'] = 600
        
    # Try to process data.
    try:
        print('processing', name, in_location, pax_config)
        print('saving to', output_fullname)
        
        pax_kwargs = dict(config_names=pax_config,
                          config_dict=config_dict)

        # allows for ncpus to be passed as argument to cax-process bash command

        if not isinstance(ncpus, int):
            ncpus = int(ncpus)
        
        if ncpus > 1:
            parallel.multiprocess_locally(n_cpus=ncpus, **pax_kwargs)
        else:
            core.Processor(**pax_kwargs).run()

    except Exception as exception:
        # Data processing failed.
        if json_file == "":
            datum['status'] = 'error'
            if config.DATABASE_LOG == True:
                collection.update(query, {'$set': {'data.$': datum}})
        raise

    if json_file == "":

        datum['status'] = 'verifying'
        if config.DATABASE_LOG == True:
            collection.update(query, {'$set': {'data.$': datum}})

        datum['checksum'] = checksumdir._filehash(datum['location'],
                                                  hashlib.sha512)
        if verify():
            datum['status'] = 'transferred'
        else:
            datum['status'] = 'error'

        if config.DATABASE_LOG == True:
            collection.update(query, {'$set': {'data.$': datum}})

        
class ProcessBatchQueue(Task):
    "Create and submit job submission script."

    def __init__(self, use_api=False, hurry=False):
        self.thishost = config.get_hostname()
        self.pax_version = 'v%s' % pax.__version__
        self.hurry = hurry

        query = {"data" : {"$not" : {"$elemMatch" : {"type" : "processed",
                                                     "pax_version" : self.pax_version,
                                                     "host" : self.thishost,
                                                     "$or" : [{"status" : "transferred"},
                                                              { "status" : "error"}
                                                             ]

                                                    }
                                     }
                           },
                 "$or" : [{'number' : {"$gte" : 6730}},
                          {"detector" : "muon_veto",
                           "end" : {"$gt" : (datetime.datetime.utcnow() - datetime.timedelta(days=15))}
                           }
                          ],
                 'reader.ini.write_mode' : 2,
                 'trigger.events_built' : {"$gt" : 0},
                 'processor.DEFAULT.gains' : {'$exists' : True},
                 'processor.DEFAULT.electron_lifetime_liquid' : {'$exists' : True},
                 'processor.DEFAULT.drift_velocity_liquid' : {'$exists' : True},
                 'processor.correction_versions': {'$exists': True},
                 'processor.WaveformSimulator': {'$exists': True},
                 'processor.NeuralNet|PosRecNeuralNet': {'$exists': True},
                 'tags' : {"$not" : {'$elemMatch' : {'name' : 'donotprocess'}}},
                 }


        # if using OSG processing then need raw data in rucio catalog
        # will check later if on UC_OSG_USERDISK
        if self.thishost == 'login':
            query["data"]["$elemMatch"] = {"host" : "rucio-catalogue", "type": "raw", "status" : "transferred"}
        # if not using OSG (midway most likely), need the raw data at that host
        else:
            query["data"]["$elemMatch"] = {"host" : self.thishost,
                                           "type" : "raw"}
            
        Task.__init__(self, query=query, use_api=use_api)
    
    def verify(self):
        """Verify processing worked"""
        return True  # yeah... TODO.

    def submit(self):
        '''Submission Script
        '''

        name = self.run_doc['name']
        number = self.run_doc['number']
        detector = self.run_doc['detector']

        MVsuffix = ""
        # if MV, append string to name in dag dir
        if detector == 'muon_veto':
            MVsuffix = "_MV"
            identifier = name
        elif detector == 'tpc':
            identifier = number
        else:
            raise ValueError("Detector is neither tpc nor MV")

        if self.thishost == 'login':
            # load in json for the dag_writer configuration
            dag_config = config.load_dag_config()
            dag_config['runlist'] = [identifier]
            dag_config['pax_version'] = self.pax_version
            dag_config['host'] = self.thishost
            dag_config['rush'] = self.hurry

            outer_dag_dir = "{logdir}/pax_{pax_version}/{name}{MVsuffix}/dags".format(logdir=dag_config['logdir'],
                                                                                      pax_version=self.pax_version,
                                                                                      name=name,
                                                                                      MVsuffix = MVsuffix
                                                                                     )
            inner_dag_dir = outer_dag_dir + "/inner_dags"

            if not os.path.exists(inner_dag_dir):
                os.makedirs(inner_dag_dir)

            joblog_dir = outer_dag_dir.replace('dags', 'joblogs')

            if not os.path.exists(joblog_dir):
                os.makedirs(joblog_dir)

            if detector == 'tpc':
                outer_dag_file = outer_dag_dir + "/{number}_outer.dag".format(number=number)

            elif detector == 'muon_veto':
                outer_dag_file = outer_dag_dir + "/{name}_MV_outer.dag".format(name=name)

            # if there are more than 10 rescue dags, there's clearly something wrong, so don't submit
            if self.count_rescues(outer_dag_dir) >= 10:
                self.log.info("10 or more rescue dags exist for Run %d. Skipping." % number)

                # register as an error to database if haven't already
                for d in self.run_doc['data']:
                    if d['host'] == self.thishost and d['type'] == 'processed' and d['pax_version'] == self.pax_version:
                        error_set = (d['status'] == 'error')
                        datum = d

                if datum is not None and not error_set:
                    updatum = datum.copy()
                    updatum['status'] = 'error'

                    API = api()
                    API.update_location(self.run_doc['_id'], datum, updatum)

                return

            qsub.submit_dag_job(outer_dag_file, dag_config)

        else:
            qsub.submit_job(self.thishost, script, name + "_" + self.pax_version)
                        
    def each_run(self):
        detector = self.run_doc['detector']

        # if OSG processing, check if raw data on stash
        # and check if too many dags running
        transferring = False

        if self.thishost == 'login':
            rucio_name = None
            for d in self.run_doc['data']:
                if d['host'] == 'rucio-catalogue':
                    rucio_name = d['location']
                if d['host'] == self.thishost and d['type'] == 'processed' and d['pax_version'] == self.pax_version:
                    transferring = (d['status']=='transferring')

            if rucio_name is None: # something wrong with query if this happens
                self.log.info("Run %d not in rucio catalogue." % run_doc['number'])
                return
            # if not on stash, skip this run. don't have logging here since I imagine this will happen
            # for quite a few runs
            else:
                if not self.is_on_stash(rucio_name) and not self.hurry:
                    self.log.info("Run %d not on stash RSE" % self.run_doc['number'])
                    return

            # if status is 'transferring' then see if the run is in the queue
            if transferring:
                id = self.run_doc['number'] if detector == 'tpc' else self.run_doc['name']
                if self.in_queue(id):
                    return

            # now check how many dags are running
            self.log.debug("%d dags currently running" % len(qsub.get_queue()))
            if len(qsub.get_queue()) > 29:
                self.log.info("Too many dags in queue, waiting 10 minutes")
                time.sleep(60*10)
                return

        disable_updates = not config.DATABASE_LOG

        have_processed, have_raw = self.local_data_finder()

        if have_processed:
            self.log.debug("Skipping %s already processed",
                           self.run_doc['name'])
            return

        # Get number of events in data set (not set for early runs <1000)
        events = self.run_doc.get('trigger', {}).get('events_built', 0)

        # Specify number of cores for pax multiprocess
        # OSG only uses 1 cpu, so 1 is default. Modified few lines later if on midway
        ncpus = 1

        # 1 CPU for small number of events (sometimes pax stalls
        # with too many CPU)
        if events > 1000 and self.thishost == 'midway-login1':
            ncpus = 4  # based on Figure 2 here https://xecluster.lngs.infn.it/dokuwiki/doku.php?id=xenon:xenon1t:shockley:performance#automatic_processing


        out_location = config.get_processing_dir(self.thishost, self.pax_version)
        os.makedirs(out_location, exist_ok = True)

        queue_list = qsub.get_queue(self.thishost)
        
        # Warning: No check for pax version here
        if self.run_doc['name'] in queue_list:
            self.log.debug("Skipping %s currently in queue",
                           self.run_doc['name'])
            return

        self.log.info("Processing %s with pax_%s, output to %s",
                      self.run_doc['name'], self.pax_version, out_location)

        if self.thishost != 'login':
            # this will break things, need to change have_raw['location']
            _process(self.run_doc['name'], have_raw['location'], self.thishost,
                     self.pax_version, out_location, detector, ncpus)

        else:
            MV = ""

            if detector == 'muon_veto':
                MV = "_MV"

            self.submit()

            time.sleep(2)

    def local_data_finder(self):
        have_processed = False
        have_raw = False
        # Iterate over data locations to know status
        for datum in self.run_doc['data']:

            # Is host known?
            if 'host' not in datum:
                continue

            # If the location doesn't refer to here, skip
            if datum['host'] != self.thishost:
                continue

            # Raw data must exist
            if datum['type'] == 'raw' and datum['status'] == 'transferred':
                have_raw = datum

            # Check if processed data already exists in DB
            if datum['type'] == 'processed':
                if self.pax_version == datum['pax_version'] and datum['status'] == 'transferred':
                    have_processed= True

        return have_processed, have_raw

    def count_rescues(self, dagdir):
        # returns the number of rescue dags
        return len([f for f in os.listdir(dagdir) if "rescue" in f])

    def is_on_stash(self, rucio_name):
        # checks if run with rucio_name is on stash
        out = subprocess.Popen(["rucio", "list-rules", rucio_name], stdout=subprocess.PIPE).stdout.read()
        out = out.decode("utf-8").split("\n")
        for line in out:
            line = re.sub(' +', ' ', line).split(" ")
            if len(line) > 4 and line[4] == "UC_OSG_USERDISK" and line[3][:2] == "OK":
                return True
        return False

    def in_queue(self, id):
        cmd = "condor_q -long -attributes DAGNodeName | grep xe1t_%s" % id
        args = shlex.split(cmd)
        out = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).stdout.read()
        out = out.decode('utf-8')
        if out == "":
            return False
        else:
            return True



# Arguments from process function: (name, in_location, host, pax_version,
#                                   out_location, ncpus):
def main():
    _process(*sys.argv[1:])
