import logging
from cax import config, reporting
from bson.json_util import dumps
from json import loads

class Task():
    def __init__(self):
        # Grab the Run DB so we can query it
        self.collection = config.mongo_collection()
        self.log = logging.getLogger(self.__class__.__name__)
        self.run_doc = None
        self.raw_data = None


    def go(self):
        """Run this periodically"""

        for self.run_doc in self.collection.find({'detector': 'tpc'}):
            if 'data' not in self.run_doc:
                continue

            self.raw_data = self.get_daq_buffer()

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
                if data_doc['host'] == config.get_hostname():
                    return data_doc

        # Not found
        return None

    def give_error(self, message):
        """Report error to PagerDuty and log

        This calls peoples and issues a wide range of alarms, so use wisely.
        """
        santized_run_doc = self.run_doc.copy()
        santized_run_doc = loads(dumps(santized_run_doc))

        reporting.alarm(message,
                        santized_run_doc)

        self.log.error(message)
