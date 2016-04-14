import os
import datetime
import shutil

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
        self.log.debug('Dropping %s' % self.raw_data['collection'])
        db.drop_collection(self.raw_data['collection'])
        self.log.info('Dropped %s' % self.raw_data['collection'])
        
        self.log.debug(self.collection.update({'_id': self.run_doc['_id']},
                                              {'$pull': {'data' : self.raw_data}}))

    def each_run(self):
        values = self.get_checksums()
        if self.count(values) > 2 and self.raw_data:
            self.remove_untriggered()
        else:
            self.log.debug("Did not drop: %s" % str(self.raw_data))

class AlertFailedTransfer(checksum.CompareChecksums):
    "Alert if stale transfer."

    def each_location(self, data_doc):
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            return # Skip places where we can't locally access data

        if 'creation_time' not in data_doc:
            self.log.warning("No creation time for %s" % str(data_doc))
            return

        # How long has transfer been ongoing
        time = data_doc['creation_time']
        difference = datetime.datetime.utcnow() - time

        if data_doc["status"] == "transferred":
            return # Transfer went fine

        self.log.debug(difference)

        if difference > datetime.timedelta(days=1):  # If stale transfer
            self.give_error("Transfer lasting more than one day")

        if difference > datetime.timedelta(days=2):  # If stale transfer
            self.give_error("Transfer lasting more than one week, retry.")

            values = self.get_checksums()
            if self.count(values) > 2:
                self.log.info("Deleting %s" % data_doc['location'])
                shutil.rmtree(data_doc['location'])
                self.log.error('Deleted, notify run database.')

                resp = self.collection.update({'_id': self.run_doc['_id']},
                                              {'$pull': {'data' : data_doc}})
                self.log.error('Removed from run database.')
                self.log.debug(resp)