
import logging
from json import loads

import pymongo
from bson.json_util import dumps

from cax import config
from cax.dag_prescript import clear_errors
from cax.api import api

class Task():
    def __init__(self, query = {}, use_api = False):
        # Grab the Run DB so we can query it

        if not use_api:
            self.collection = config.mongo_collection()

        self.log = logging.getLogger(self.__class__.__name__)
        self.run_doc = None
        self.untriggered_data = None
        self.use_api = use_api

        self.query = query

    def go(self, specify_run = None):
        """Run this periodically"""

        # argument can be run number or run name
        #TODO modify clear_errors so that it can handle run names instead of numbers
        if specify_run is not None:
            if isinstance(specify_run,int):
                self.query['number'] = specify_run

            elif isinstance(specify_run,str):
                self.query['name'] = specify_run

        # Get user-specified list of datasets
        datasets = config.get_dataset_list()

        # Collect all run document ids.  This has to be turned into a list
        # to avoid timeouts if a task takes too long.

        ids = self.collect_ids()
        if ids is None:
            self.log.info("Can't get run ids for some reason. Skipping")
            return


        if len(ids) == 0:
            self.log.info("Query matches no entry. Skipping.")
            return

        # Iterate over each run
        for id in ids:
            # Make sure up to date
            self.run_doc = self.get_rundoc(id)

            if self.run_doc is None:
                self.log.info("Problems getting rundoc for id %s. Skipping" % id)
                continue

            if 'data' not in self.run_doc:
                self.log.info('Data not in run_doc')
                continue

            # Operate on only user-specified datasets
            if datasets:
                if self.run_doc['name'] not in datasets:
                    continue

            # DAQ experts only:
            # Find location of untriggered DAQ data (if exists)
            self.untriggered_data = self.get_daq_buffer()

            self.each_run()

        self.shutdown()

    def each_run(self):
        for data_doc in self.run_doc['data']:
            self.log.debug('%s on %s' % (self.__class__.__name__,
                                         self.run_doc['number']))
            self.each_location(data_doc)

    def each_location(self, data_doc):
        raise NotImplementedError()

    def get_daq_buffer(self):
        for data_doc in self.run_doc['data']:
            if data_doc['type'] == 'untriggered':
                if data_doc['host'] == 'reader':
                    if config.get_hostname() == 'eb0':
                        return data_doc

        # Not found
        return None

    def give_error(self, message):
        """Report error to PagerDuty and log

        This calls peoples and issues a wide range of alarms, so use wisely.
        """
        santized_run_doc = self.run_doc.copy()
        santized_run_doc = loads(dumps(santized_run_doc))

        self.log.error(message)

    def has_tag(self, name):
        if 'tags' not in self.run_doc:
            return False

        for tag in self.run_doc['tags']:
            if name == tag['name']:
                return True
        return False

    def shutdown(self):
        """Runs at end and can be overloaded by subclasses
        """
        pass

    def collect_ids(self):
        # if not using API interface, do normal pymongo query which is faster
        if not self.use_api:
            try:
                ids = [doc['_id'] for doc in self.collection.find(self.query,
                                                                  projection=('_id'),
                                                                  sort=(('start', -1),))]
            except pymongo.errors.CursorNotFound:
                self.log.info("Cursor not found exception.  Skipping")
                return

        else: # slower but uses API which can be useful
            # initialize api instance
            API = api()
            ids = [doc['_id'] for doc in API.get_all_runs(self.query)]

        return ids


    def get_rundoc(self, id):
        if not self.use_api:
            try:
                rundoc = self.collection.find_one({'_id': id})
            except pymongo.errors.AutoReconnect:
                self.log.error("pymongo.errors.AutoReconnect, skipping...")
                return

        else:
            # initialize api
            API = api()
            # only want the first result,  mimics collection.find_one
            rundoc = API.get_all_runs({'_id' : id}, _id=id)[0]

        return rundoc