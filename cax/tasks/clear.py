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

import pax

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

        # Some RunsDB entries are different format for some reason (#40)
        if type(time_made) is list:
            # Assume only one list entry that contains the time
            time_made = time_made[0]

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

        # Do not delete stalled or failed raw data transfers to recover with rsync 
        # (Warning: do not use scp, which may create nested directories)
        delete_data = (data_doc['type'] == 'processed' and 'v%s' % pax.__version__ == data_doc['pax_version'])

        if difference > datetime.timedelta(hours=24):
            self.give_error("Transfer lasting more than 24 hours, retry.")
            self.purge(data_doc, delete_data)
            
        elif data_doc["status"] == 'error' and data_doc['host'] != 'xe1t-datamanager':
            self.give_error("Transfer or process errored, retry.")
            self.purge(data_doc, delete_data)

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
            self.give_error("Bad checksum %d, %s, %s, %s" % (self.run_doc['number'], data_doc['host'], \
                            data_doc['type'], data_doc['pax_version']))

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

        # Do not purge processed data (use PurgeProcessed below)
        if data_doc['type'] == 'processed':
            self.log.debug("Do not purge processed data")
            return

        self.log.debug("Checking purge logic")

        # Only purge transfered data
        if data_doc["status"] != "transferred":
            self.log.debug("Not transfered")
            return

        # Require at least three copies of the data since we are deleting third.
        num_copies = self.check(data_doc['type'], warn=False)
        if num_copies < 3:
            self.log.debug("Not enough copies (%d)" % num_copies)
            return

        if self.check_purge_requirements():
            self.log.info("Purging %s" % data_doc['location'])
            self.purge(data_doc)
        else:
            self.log.debug("Not enough time elapsed")

    def check_purge_requirements(self):

        # See if purge settings specified, otherwise don't purge
        if config.purge_settings() == None:
            self.log.debug("No purge settings")
            return False

        # Hardcoded sources and tags to preserve on Midway (no longer needed from 18/03/2017)
        #if config.get_hostname() == 'midway-login1':
        #    if self.run_doc['source']['type'] == "Kr83m" or self.run_doc['source']['type'] == "AmBe" or self.run_doc['source']['type'] == "Rn220" or self.has_tag('_sciencerun0'):
        #        self.log.debug("Do not purge %s data" % self.run_doc['source']['type'])
        #        return False

        # Do not purge from Midway until processed
        # (no longer needed from 18/03/2017)
        #if config.get_hostname() == 'midway-login1' and not self.local_data_finder(config.get_hostname(), 'v%s' % pax.__version__):
        #    self.log.debug("Not yet processed")
        #    return

        # The dt we require
        dt = datetime.timedelta(days=config.purge_settings())

        t0 = self.run_doc['start']
        t1 = datetime.datetime.utcnow()

        purge_time_pass = t1-t0 > dt

        self.log.info('Purge time: now = %s, run = %s, dt = %s, pass = %d' % (str(t1), str(t0), str(dt), purge_time_pass))

        return purge_time_pass

    def local_data_finder(self, thishost, version):
        have_processed = False

        # Iterate over data locations to know status
        for datum in self.run_doc['data']:

            # Is host known?
            if 'host' not in datum:
                continue

            # If the location doesn't refer to here, skip
            if datum['host'] != thishost:
                continue

            # Check if processed data already exists in DB
            if datum['type'] == 'processed' and datum['status'] == 'transferred':
                if version == datum['pax_version']:
                    have_processed = True

        return have_processed


class PurgeProcessed(checksum.CompareChecksums):
    """
    Purge Processed root files
    """

    # Do not overload this routine from checksum inheritance.
    each_run = Task.each_run

    def each_location(self, data_doc):
        """
        Check every location with data whether it should be purged.
        """
        self.log.debug("Checking purge logic")

        # Skip places where we can't locally access data
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            return

        # See if purge settings specified, otherwise don't purge
        if not config.purge_version() or (config.purge_version() == None) :
            self.log.debug("No processed version specified for purge, skipping")
            return

        # Do not purge processed data
        if data_doc['type'] == 'raw':
           self.log.debug("Do not purge raw data")
           return

        # Check pax version of processed run
        if (data_doc['pax_version'] != config.purge_version()) :
            self.log.debug("Don't purge this version: %s" % (data_doc['pax_version']) )
            return

        # The purge data
        self.log.info("Purging %s" % data_doc['location'])
        self.purge(data_doc)
        
        return

