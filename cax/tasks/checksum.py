"""Responsible for all checksum operations on data.
"""

import hashlib
import os

import checksumdir
import shutil
import subprocess
from zlib import adler32

from cax import config
from ..task import Task


class ChecksumMethods():
    """Implement own checksum methods"""
    
    def get_adler32( fname ):
        """Calcualte an Adler32 checksum in python
            Used for cross checks with Rucio
        """
        BLOCKSIZE=256*1024*1024
        asum = 1
        with open(fname, "rb") as f:
          while True:
            data = f.read(BLOCKSIZE)
            if not data:
                break
            asum = adler32(data, asum)
            if asum < 0:
                asum += 2**32

        return hex(asum)[2:10].zfill(8).lower()

class AddChecksum(Task):
    """Perform a checksum on accessible data.

    If no previous checksum present, then adds one.  Otherwise, confirms the
    checksum still is true.
    """

    def each_location(self, data_doc):
        # Only raw data waiting to be verified
        if data_doc['status'] != 'verifying' and data_doc['status'] != 'transferred':
            self.log.debug('Location does not qualify')
            return
        
        if data_doc['status'] == 'transferred' and \
           (config.get_hostname() == 'xe1t-datamanager' or config.get_hostname() == 'login'):
            return
        

        # Data must be hosted somewhere
        if 'host' not in data_doc:
            return

        # Data must be here locally
        if data_doc['host'] != config.get_hostname():

            # Special case of midway-srm accessible via POSIX on midway-login1
            if not (data_doc['host']  == "midway-srm" and config.get_hostname() == "midway-login1"):
                self.log.debug('Location not here')
                return

        # This status is given after checksumming
        status = 'transferred'

        # Find file and perform checksum
        if os.path.isdir(data_doc['location']):
            value = checksumdir.dirhash(data_doc['location'],
                                        'sha512')
        elif os.path.isfile(data_doc['location']):
            value = checksumdir._filehash(data_doc['location'],
                                          hashlib.sha512)
        else:
            # Data not actually found
            self.log.error("Location %s not found." % data_doc['location'])
            value = None
            status = 'error'

        if config.DATABASE_LOG:
            if data_doc['status'] == 'verifying':
                self.log.info("Adding a checksum to run "
                              "%d %s" % (self.run_doc['number'],
                                         data_doc['type']))
                self.collection.update({'_id' : self.run_doc['_id'],
                                        'data': {'$elemMatch': data_doc}},
                                       {'$set': {'data.$.status'  : status,
                                                 'data.$.checksum': value}})
            elif data_doc['checksum'] != value or status == 'error':
                self.log.info("Checksum fail "
                              "%d %s" % (self.run_doc['number'],
                                         data_doc['type']))
                self.collection.update({'_id' : self.run_doc['_id'],
                                        'data': {'$elemMatch': data_doc}},
                                       {'$set': {'data.$.checksumproblem': True}})



class CompareChecksums(Task):
    "Perform a checksum on accessible data."

    def get_main_checksum(self, type='raw', pax_version='', **kwargs):
        """Iterate over data locations and search for priviledged checksum
        """

        # These types of data and location provide master checksum
        master_checksums = (('raw', 'xe1t-datamanager', None),
                            ('raw', 'tegner-login-1', None),
                            ('processed', 'midway-login1', pax_version))

        for data_doc in self.run_doc['data']:

            # Only look at transfered data
            if data_doc['status'] == 'transferred' and data_doc['type'] == type:

                # Search key
                test = tuple((data_doc.get(key) for key in ('type',
                                                            'host',
                                                            'pax_version')))
                if test in master_checksums:
                    # If matched a master checksum location, return checksum
                    return data_doc['checksum']

        self.log.debug("Missing checksum within %d" % self.run_doc['number'])
        return None

    def check(self, type='raw',
              warn=True):
        """Returns number of verified data locations

        Return the number of sites that have the same checksum as the master
        site.
        """
        n = 0

        for data_doc in self.run_doc['data']:
            # Only look at transfered data that is not untriggered
            if 'host' not in data_doc or \
                            data_doc['status'] != 'transferred' or \
                            data_doc['type'] == 'untriggered' or \
                            data_doc['type'] != type or \
                            'checksum' not in data_doc:
                continue

            # Grab main checksum.
            if data_doc['checksum'] != self.get_main_checksum(**data_doc):

                if data_doc['host'] == config.get_hostname():
                    error = "Local checksum error " \
                            "run %d" % self.run_doc['number']
                    if warn:
                        self.give_error(error)
            else:
                n += 1

        return n

    def each_run(self):
        self.log.debug("Checking checksums "
                       "run %d" % self.run_doc['number'])
        self.check()

    def purge(self, data_doc):
        self.log.info("Deleting %s" % data_doc['location'])

        # Temporary hardcoded check for gfal-rm removal
        if config.get_hostname() == 'login' and 'raw' in data_doc['location']:
            config_original = config.get_config('login')
            server = config_original['hostname']
            if config.get_cert() == None:
                grid_cert = ''
            else:
                grid_cert = config.get_cert()

            full_command = "gfal-rm -v -r --cert %s " % grid_cert + \
                           server+data_doc['location']

            self.log.info(full_command)

            try:
                gfal_out = subprocess.check_output(full_command, stderr=subprocess.STDOUT, shell=True)

            except subprocess.CalledProcessError as gfal_exec:
                self.log.error(gfal_exec.output.rstrip().decode('ascii'))
                self.log.error("Error: gfal-rm status = %d\n" % gfal_exec.returncode)
                raise

            gfal_out_ascii = gfal_out.rstrip().decode('ascii')
            if "error" in gfal_out_ascii.lower(): # Some errors don't get caught above
                self.log.error(gfal_out_ascii)
                raise

            else:
                self.log.info(gfal_out_ascii) # To print timing

        # Default POSIX removal        
        else:
            if os.path.isdir(data_doc['location']):
                shutil.rmtree(data_doc['location'])
                self.log.info('Deleted, notify run database.')
            elif os.path.isfile(data_doc['location']):
                os.remove(data_doc['location'])
            else:
                self.log.error('did not exist, notify run database.')

        if config.DATABASE_LOG == True:
            resp = self.collection.update({'_id': self.run_doc['_id']},
                                          {'$pull': {'data': data_doc}})
            self.log.info('Removed from run database.')
            self.log.debug(resp)
