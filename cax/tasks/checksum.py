import os
import checksumdir
import hashlib

from cax import config
from ..task import Task


class AddChecksum(Task):
    "Perform a checksum on accessible data."

    def each_location(self, data_doc):
        # Only raw data waiting to be verified
        if data_doc['status'] != 'verifying':
            self.log.debug('Location does not qualify')
            return

        # Require data be here
        if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
            self.log.debug('Location not here')
            return

        if os.path.isdir(data_doc['location']):
            value = checksumdir.dirhash(data_doc['location'],
                                        'sha512')
        else:
            value = checksumdir._filehash(data_doc['location'],
                                          hashlib.sha512)

        data_doc['checksum'] = value
        data_doc['status'] = 'transferred'

        self.log.info("Adding a checksum to run "
                      "%d %s" % (self.run_doc['number'],
                                 data_doc['type']))
        self.collection.update({'_id': self.run_doc['_id'],
                                'data.host': data_doc['host']},
                               {'$set': {'data.$': data_doc}})


class CompareChecksums(Task):
    "Perform a checksum on accessible data."

    def get_main_checksum(self, data_type='raw'):
        for data_doc in self.run_doc['data']:
            # Only look at transfered data
            if data_doc['status'] == 'transferred':
                if data_doc['type'] == data_type:
                    if data_doc['type'] == 'raw':
                        if data_doc['host'] == 'eb0':
                            return data_doc['checksum']
                    if data_doc['type'] == 'processed':
                        if data_doc['host'] == 'midway-login1':
                            return data_doc['checksum']
        return None


    def check(self,
              data_type='raw',
              warn=True):
        """Returns number of good locations"""
        n = 0

        master = self.get_main_checksum(data_type)

        for data_doc in self.run_doc['data']:
            if 'host' not in data_doc:
                continue

            # Only look at transfered data
            if data_doc['status'] != 'transferred':
                continue

            # And require raw
            if data_doc['type'] != data_type:
                continue

            if data_doc['checksum'] != master:
                if data_doc['host'] == config.get_hostname():
                    error = "Local checksum error " \
                            "run %d" % self.run_doc['number']
                    if warn: self.give_error(error)
            else:
                n += 1

        return n

    def each_run(self):
        self.log.debug("Checking raw checksums "
                      "run %d" % self.run_doc['number'])
        self.check('raw')

        self.log.debug("Checking processed checksums "
                      "run %d" % self.run_doc['number'])
        self.check('processed')

