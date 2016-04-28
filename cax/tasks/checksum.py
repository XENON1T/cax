import checksumdir

from cax import config
from ..task import Task


class AddChecksum(Task):
    """Adds a checksum for each raw data copy on this host that doesn't yet have one"""

    def each_location(self, data_doc):
        # Only raw data waiting to be verified
        if data_doc['status'] != 'verifying' or data_doc['type'] != 'raw':
            self.log.debug('Location does not qualify')
            return

        # Require data be here
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            self.log.debug('Location not here')
            return

        value = checksumdir.dirhash(data_doc['location'],
                                    'sha512')

        data_doc['checksum'] = value
        data_doc['status'] = 'transferred'

        self.log.info("Adding a checksum to run %d" % self.run_doc['number'])
        self.collection.update({'_id': self.run_doc['_id'],
                                'data.host': data_doc['host']},
                               {'$set': {'data.$': data_doc}})


class CompareChecksums(Task):
    """Checks that the checksums for all raw data copies of each run are the same;
    gives an error if they are not.

    This Task is subclassed by other tasks that need to verify checksums first.
    """

    def count(self, checksums):
        """Takes list of checksums"""
        n = len(checksums)
        if n:
            for key, value in checksums.items():
                if value != list(checksums.values())[0]:
                    error = "Checksums error " \
                            "run %d" % self.run_doc['number']
                    self.give_error(error)

        self.log.debug("%d checksums agree" % n)
        return n

    def get_checksums(self):
        values = {}
        for data_doc in self.run_doc['data']:
            # Only look at transfered data
            if data_doc['status'] == 'transferred':
                # And require raw
                if data_doc['type'] == 'raw':
                    values[data_doc['host']] = data_doc['checksum']
        return values

    def each_run(self):
        self.count(self.get_checksums())
