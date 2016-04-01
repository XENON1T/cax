import os

import pymongo

from . import checksum
from cax import config


class ClearDAQBuffer(checksum.CompareChecksums):
    "Perform a checksum on accessible data."

    def remove_untriggered(self):
        client = pymongo.MongoClient(self.raw_data['location'])
        print(self.raw_data['location'])

        db = client.untriggered
        db.authenticate('eb',
                        os.environ.get('MONGO_PASSWORD'))
        self.log.error('db.drop()')

    def each_run(self):
        values = self.get_checksums()
        if self.count(values) > 2 and self.raw_data:
            self.remove_untriggered()

class ClearFailedTransfer(checksum.CompareChecksums):
    "Perform a checksum on accessible data."

    def each_location(self, data_doc):
        if 'host' in data_doc and data_doc['host'] == config.get_hostname():
            print(data_doc)
