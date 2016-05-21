import logging
from cax import config
from bson.json_util import dumps
from json import loads


class Task():
    def __init__(self):
        # Grab the Run DB so we can query it
        self.collection = config.mongo_collection()
        self.log = logging.getLogger(self.__class__.__name__)
        self.run_doc = None
        self.untriggered_data = None

    def go(self):
        """Run this periodically"""

        # Get user-specified list of datasets
        datasets = config.get_dataset_list()

        # Collect all run document ids.  This has to be turned into a list
        # to avoid timeouts if a task takes too long.
        ids = [doc['_id'] for doc in self.collection.find({'detector': 'tpc',
                                             'number' : {"$gt": 400,
                                                         "$lt" : 450}})]

        # Iterate over each run
        for id in ids:
            # Make sure up to date
            self.run_doc = self.collection.find_one({'_id' : id})

            if 'data' not in self.run_doc:
                continue

            # DAQ experts only:
            # Find location of untriggered DAQ data (if exists)
            self.untriggered_data = self.get_daq_buffer()
 
            # Operate on only user-specified datasets
            if datasets:
                if self.run_doc['name'] not in datasets:
                    continue
          
            self.each_run()
            

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
