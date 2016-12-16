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

    def each_location(self, data_doc):
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            return  # Skip places where we can't locally access data

        if 'creation_time' not in data_doc:
            self.log.warning("No creation time for %s" % str(data_doc))
            return

        # How long has transfer been ongoing
        try:
            time_modified = os.stat(data_doc['location']).st_mtime
        except FileNotFoundError:
            time_modified = 0
        time_modified = datetime.datetime.fromtimestamp(time_modified)
        time_made = data_doc['creation_time']
        difference = datetime.datetime.utcnow() - max(time_modified,
                                                      time_made)

        if data_doc["status"] == "transferred" or data_doc["status"] == "verifying":
            return  # Transfer went fine

        self.log.debug(difference)

        if difference > datetime.timedelta(hours=2):  # If stale transfer
            self.give_error("Transfer %s from run %d (%s) lasting more than "
                            "2 hours" % (data_doc['type'],
                                          self.run_doc['number'],
                                          self.run_doc['name']))

        if difference > datetime.timedelta(hours=24):
            self.give_error("Transfer lasting more than 24 hours, retry.")
            self.purge(data_doc)
            
        elif data_doc["status"] == 'error' and data_doc['host'] != 'xe1t-datamanager':
            self.give_error("Transfer or process errored, retry.")
            self.purge(data_doc)

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

        comparison = self.get_main_checksum(**data_doc)

        if comparison is None:
            return

        if data_doc['checksum'] != comparison:
            self.give_error("Bad checksum %d" % self.run_doc['number'])
            if self.check(data_doc['type'], warn=False) > 1:
                self.purge(data_doc)


class BufferPurger(checksum.CompareChecksums):
    """Purge buffer

    """

    # Do not overload this routine from checksum inheritance.
    each_run = Task.each_run

    def each_location(self, data_doc):
        """Check every location with data whether it should be purged.
        """
        # Skip places where we can't locally access data
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            return

        # Do not purge processed data
        # Warning: if you want to enable this here, need to add pax version checking in check()
        if data_doc['type'] == 'processed':
            self.log.debug("Do not purge processed data")
            return

        if data_doc['host'] == 'midway-login1':
            if self.run_doc['source']['type'] == "Kr83m" or self.run_doc['source']['type'] == "Rn220":
                self.log.debug("Do not purge %s data" % self.run_doc['source']['type'])
                return
        
        self.log.debug("Checking purge logic")

        # Only purge transfered data
        if data_doc["status"] != "transferred":
            self.log.debug("Not transfered")
            return

        # See if purge settings specified, otherwise don't purge
        if config.purge_settings() == None:
            self.log.debug("No purge settings")
            return

        # Require at least three copies of the data since we are deleting third.
        num_copies = self.check(data_doc['type'], warn=False)
        if num_copies < 3:
            self.log.debug("Not enough copies (%d)" % num_copies)
            return

        # The dt we require
        dt = datetime.timedelta(days=config.purge_settings())

        t0 = self.run_doc['start']
        t1 = datetime.datetime.utcnow()

        self.log.info(t1-t0 > dt)

        if t1 - t0 > dt:
            self.log.info("Purging %s" % data_doc['location'])
            self.purge(data_doc)
