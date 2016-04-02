import logging

from cax import config


class Task():
    def __init__(self):
        # Grab the Run DB so we can query it
        self.collection = config.mongo_collection()
        self.log = logging.getLogger(self.__class__.__name__)
        self.run_doc = None
        self.raw_data = None

        try:
            self.upload_options = config.upload_options()
        except LookupError as e:
            self.upload_options = []
            self.log.info("Unknown host: %s", config.get_hostname())
        else:
            self.log.info("Upload options: %s" % str(self.upload_options))

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
                return data_doc

        # Not found
        return None
