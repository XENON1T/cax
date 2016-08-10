import logging
from json import dumps, loads

from cax import api
from cax import config


class Task():
    def __init__(self):
        # Grab the Run DB so we can query it        
        self.log = logging.getLogger(self.__class__.__name__)
        self.run_doc = None
        self.untriggered_data = None
        self.api = api.api()
        
    def go(self, specify_run = None):
        """Run this periodically"""

        query = {'detector': 'tpc'}
        if specify_run is not None:
            query['number'] = specify_run

        # Get user-specified list of datasets
        datasets = config.get_dataset_list()
        
        # Iterate over each run matcing query
        self.run_doc = self.api.get_next_run(query)
        while self.run_doc is not None:

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

            # Continue until no runs remain
            self.run_doc = self.api.get_next_run(query)

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
