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
                                'data': {'$elemMatch': { 'host': data_doc['host'],
                                                         'type': data_doc['type']}}},
                               {'$set': {'data.$': data_doc}})


class CompareChecksums(Task):
    "Perform a checksum on accessible data."

    def get_main_checksum(self, type='raw', pax_hash='', pax_version = '',
                          **kwargs):
        for data_doc in self.run_doc['data']:
            # Only look at transfered data
            if data_doc['status'] == 'transferred' and data_doc['type'] == type:
                if data_doc['type'] == 'raw' and data_doc['host'] == 'eb0':
                    return data_doc['checksum']

                if data_doc['type'] == 'processed' and \
                   data_doc['host'] == 'midway-login1' and \
                   data_doc['pax_version'] == pax_version:
                        return data_doc['checksum']

        self.log.info("Missing checksum within %d" % self.run_doc['number'])
        return None


    def check(self,
              data_type='raw',
              warn=True):
        """Returns number of good locations"""
        n = 0

        for data_doc in self.run_doc['data']:
            if 'host' not in data_doc:
                continue

            # Only look at transfered data
            if data_doc['status'] != 'transferred':
                continue

            if data_doc['type'] == 'untriggered':
                continue

            local_checksum = data_doc.get('checksum', 0)

            # Grab main checksum.
            if local_checksum != self.get_main_checksum(**data_doc):
                if data_doc['host'] == config.get_hostname():
                    error = "Local checksum error " \
                            "run %d" % self.run_doc['number']
                    if warn: self.give_error(error)
            else:
                n += 1

        return n

    def each_run(self):
        self.log.debug("Checking checksums "
                      "run %d" % self.run_doc['number'])
        self.check()

