import checksumdir

from cax import config
from ..task import Task


class AddChecksum(Task):
    "Perform a checksum on accessible data."

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

        self.log.info("Updating %d" % self.run_doc['number'])
        self.collection.update({'_id'      : self.run_doc['_id'],
                                'data.host': data_doc['host']},
                               {'$set': {'data.$': data_doc}})


class CompareChecksums(Task):
    "Perform a checksum on accessible data."

    def count(self, checksums):
        """Takes list of checksums"""
        n = len(checksums)
        if n:
            for key, value in checksums.items():
                if value != checksums.values()[0]:
                    self.log.error("Checksums error "
                                   "run %d" % self.run_doc['number'])
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
