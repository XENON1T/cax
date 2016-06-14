"""Logic for pruning data

The requirement to prune data can occur in a few cases.  Time outs on transfers
or failed checksums are an obvious case.  We also use this section for clearing
the DAQ buffer copy.
"""

import datetime
import os
import shutil

from cax import config
from cax.task import Task
from cax.tasks import checksum


class RetryStalledTransfer(checksum.CompareChecksums):
    """Alert if stale transfer.

    Inherits from the checksum task since we use checksums to know when we
    can delete data.
    """

    # Do not overload this routine from checksum inheritance.
    each_run = Task.each_run

    def has_untriggered(self):
        for data_doc in self.run_doc['data']:
            if data_doc['type'] == 'untriggered':
                return True
        return False

    def each_location(self, data_doc):
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            return  # Skip places where we can't locally access data

        if 'creation_time' not in data_doc:
            self.log.warning("No creation time for %s" % str(data_doc))
            return

        # How long since last update to file
        difference = self.check_file_age(data_doc['location'])
        timeoutseconds = 6 * 3600 # six hours
        
        if data_doc["status"] == "transferred":
            return  # Transfer went fine

        self.log.debug(difference)

        if difference > timeoutseconds:  # If stale transfer
            self.give_error("Transfer %s from run %d (%s) lasting more than "
                            "%i seconds" % (data_doc['type'],
                                            self.run_doc['number'],
                                            self.run_doc['name'],
                                            timeoutseconds))

        if difference > timeoutseconds or \
                        data_doc["status"] == 'error':  # If stale transfer
            self.give_error("Transfer lasting more than %i seconds "
                            "or errored, retry." % (timeoutseconds))

            self.log.info("Deleting %s" % data_doc['location'])

            if os.path.isdir(data_doc['location']):
                try:
                    shutil.rmtree(data_doc['location'])
                except FileNotFoundError:
                    self.log.warning("FileNotFoundError within %s" % data_doc['location'])
                self.log.info('Deleted, notify run database.')
            elif os.path.isfile(data_doc['location']):
                os.remove(data_doc['location'])
            else:
                self.log.error('did not exist, notify run database.')

            if config.DATABASE_LOG == True:
                resp = self.collection.update({'_id': self.run_doc['_id']},
                                              {'$pull': {'data': data_doc}})
            self.log.error('Removed from run database.')
            self.log.debug(resp)

        def check_file_age(path):
            # Path can be a file or a directory
            modtime = (datetime.datetime.now()-
                       datetime.datetime.strptime
                       (time.ctime(os.path.getmtime(path)),
                        "%a %b %d %H:%M:%S %Y")).total_seconds()

            for subdir, dirs, files in os.walk(path):
                if subdir == path:
                    continue

                sdirtime = CheckFileDir(subdir)
                if sdirtime < modtime:
                    modtime = sdirtime                    
            return (int)(modtime)
                                                                

class RetryBadChecksumTransfer(checksum.CompareChecksums):
    """Alert if stale transfer.

    Inherits from the checksum task since we use checksums to know when we
    can delete data.
    """

    # Do not overload this routine from checksum inheritance.
    each_run = Task.each_run

    def each_location(self, data_doc):
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            return  # Skip places where we can't locally access data

        if data_doc["status"] != "transferred":
            return

        if data_doc['checksum'] != self.get_main_checksum(**data_doc):
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

                if config.DATABASE_LOG == True:
                    resp = self.collection.update({'_id': self.run_doc['_id']},
                                                  {'$pull': {'data': data_doc}})

                self.log.error('Removed from run database.')
                self.log.debug(resp)
