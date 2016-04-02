import os
import datetime

import pymongo

from . import checksum
from cax import config
from ..task import Task


class ClearDAQBuffer(checksum.CompareChecksums):
    "Perform a checksum on accessible data."

    def remove_untriggered(self):
        client = pymongo.MongoClient(self.raw_data['location'])

        db = client.untriggered
        db.authenticate('eb',
                        os.environ.get('MONGO_PASSWORD'))
        self.log.error('db.drop()')

    def each_run(self):
        values = self.get_checksums()
        if self.count(values) > 2 and self.raw_data:
            self.remove_untriggered()

class ClearFailedTransfer(Task):
    "Perform a checksum on accessible data."

    def each_location(self, data_doc):
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            return

        time = data_doc['creation_time']
        difference = datetime.datetime.utcnow() - time

        self.log.debug(difference)

        if difference > datetime.timedelta(days=1):
            self.give_error("Transfer lasting more than one day")

