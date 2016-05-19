import os
import datetime
import shutil

import pymongo

from cax.tasks import checksum
from cax import config
from cax.task import Task


class ClearDAQBuffer(checksum.CompareChecksums):
    """Perform a checksum on accessible data."""

    def remove_untriggered(self):
        client = pymongo.MongoClient(self.raw_data['location'])
        db = client.untriggered
        try:
            db.authenticate('eb',
                            os.environ.get('MONGO_PASSWORD'))
        except pymongo.errors.ServerSelectionTimeoutError as e:
            self.log.error("Mongo error: %s" % str(e))
            return None

        self.log.debug('Dropping %s' % self.raw_data['collection'])
        try:
            db.drop_collection(self.raw_data['collection'])
        except pymongo.errors.OperationFailure:
            # This usually means some background operation is still running
            self.log.error("Mongo error: %s" % str(e))
            return None

        self.log.info('Dropped %s' % self.raw_data['collection'])
        
        self.log.debug(self.collection.update({'_id': self.run_doc['_id']},
                                              {'$pull': {'data': self.raw_data}}))

    def each_run(self):
        if self.check(warn=False) > 2 and self.raw_data:
            self.remove_untriggered()
        else:
            self.log.debug("Did not drop: %s" % str(self.raw_data))


class RetryStalledTransfer(checksum.CompareChecksums):
    """Alert if stale transfer."""

    # Do not overload this routine.
    each_run = Task.each_run

    def has_untriggered(self):
        for data_doc in self.run_doc['data']:
            if data_doc['type'] == 'untriggered':
                return True
        return False

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
            self.give_error("Transfer %s from run %d (%s) lasting more than "
                            "one day" % (data_doc['type'],
                                         self.run_doc['number'],
                                         self.run_doc['name']))

        if difference > datetime.timedelta(days=2):  # If stale transfer
            self.give_error("Transfer lasting more than two days, retry.")
            if self.check(warn=False) > 1 or self.has_untriggered():
                self.log.info("Deleting %s" % data_doc['location'])

                if os.path.isdir(data_doc['location']):
                    shutil.rmtree(data_doc['location'])
                    self.log.error('Deleted, notify run database.')
                elif os.path.isfile(data_doc['location']):
                    os.remove(data_doc['location'])
                else:
                    self.log.error('did not exist, notify run database.')

                resp = self.collection.update({'_id': self.run_doc['_id']},
                                               {'$pull': {'data' : data_doc}})
                self.log.error('Removed from run database.')
                self.log.debug(resp)

class RetryBadChecksumTransfer(checksum.CompareChecksums):
    """Alert if stale transfer."""

    # Do not overload this routine.
    each_run = Task.each_run

    def each_location(self, data_doc):
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            return # Skip places where we can't locally access data

        if data_doc["status"] != "transferred":
            return # Transfer went fine


        if self.get_main_checksum(data_doc['type']) != data_doc['checksum']:
            self.give_error("Bad checksum")
            if self.check(warn=False) > 1:
                self.log.info("Deleting %s" % data_doc['location'])

                if os.path.isdir(data_doc['location']):
                    shutil.rmtree(data_doc['location'])
                    self.log.error('Deleted, notify run database.')
                elif os.path.isfile(data_doc['location']):
                    os.remove(data_doc['location'])
                else:
                    self.log.error('did not exist, notify run database.')

                resp = self.collection.update({'_id': self.run_doc['_id']},
                                               {'$pull': {'data' : data_doc}})
                self.log.error('Removed from run database.')
                self.log.debug(resp)