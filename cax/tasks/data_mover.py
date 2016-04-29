import datetime
import os
import logging

from paramiko import SSHClient, util
import scp

from cax import config
from cax.task import Task

import subprocess

class CopyBase(Task):

    def copy(self, datum_original, datum_destination, method):
        if method == 'scp':
            self.copySCP(datum_original, datum_destination)
        elif method == 'gfal-copy':
            self.copyGFAL(datum_original, datum_destination)
        else:
            print (method+" not implemented")
            raise NotImplementedError()

    """Copy data via GFAL function
    """
    def copyGFAL(self, datum_original, datum_destination):

        if datum_original['host'] == config.get_hostname():
            upload = True

            config_destination = config.get_config(datum_destination['host'])
            server = config_destination['hostname']

        elif datum_destination['host'] == config.get_hostname():
            upload = False # ie., download

            config_original = config.get_config(datum_original['host'])
            server = config_original['hostname']

        else:
            raise ValueError()

        if upload:
            logging.info("put: %s to %s" % (datum_original['location'],
                                            server+datum_destination['location']))

            dataset = datum_original['location'].split('/').pop()

            status = subprocess.call("time gfal-copy -f -r -n8 file://"+datum_original['location']+" "+server+datum_destination['location']+" lfn:/grid/xenon.biggrid.nl/xenon1t/"+dataset, shell=True)
            #print ("gfal status = ", status)

 
        else:
            logging.info("get: %s to %s" % (server+datum_original['location'],
                                            datum_destination['location']))
            raise NotImplementedError()


    def copySCP(self, datum_original, datum_destination):
    """Copy data via SCP function
    """

        util.log_to_file('ssh.log')
        ssh = SSHClient()
        ssh.load_system_host_keys()

        if datum_original['host'] == config.get_hostname():
            upload = True

            config_destination = config.get_config(datum_destination['host'])
            server = config_destination['hostname']
            username = config_destination['username']

        elif datum_destination['host'] == config.get_hostname():
            upload = False # ie., download

            config_original = config.get_config(datum_original['host'])
            server = config_original['hostname']
            username = config_original['username']

        else:
            raise ValueError()

        logging.info("connection to %s" % server)
        ssh.connect(server,
                    username=username)

        # SCPCLient takes a paramiko transport as its only argument
        client = scp.SCPClient(ssh.get_transport())

        if upload:
            logging.info("put: %s to %s" % (datum_original['location'],
                                            datum_destination['location']))
            client.put(datum_original['location'],
                       datum_destination['location'],
                       recursive=True)
        else:
            logging.info("get: %s to %s" % (datum_original['location'],
                                            datum_destination['location']))
            client.get(datum_original['location'],
                       datum_destination['location'],
                       recursive=True)

        client.close()

    def each_run(self):
        for data_type in ['raw', 'processed']:
            self.log.debug("%s" % data_type)
            self.do_possible_transfers(option_type=self.option_type,
                                       data_type=data_type)

    def do_possible_transfers(self,
                              option_type='upload',
                              data_type='raw'):
        """Determine candidate transfers
        """

        # Get the 'upload' or 'download' options.
        options = config.get_transfer_options(option_type)

        # If no options, can't do anything
        if options is None:
            return None, None

        # For this run, where do we have transfer access?
        for remote_host in options:
            self.log.debug(remote_host)

            method = config.get_config(remote_host)['receive'] 
            if not method:
                print ("Must specify receive method for "+remote_host)
                raise ValueError()

            there = False  # Is data remote?

            datum_here = None  # Information about data here
            datum_there = None # Information about data there

            # Iterate over data locations to know status
            for datum in self.run_doc['data']:

                # Is host known?
                if 'host' not in datum or datum['type'] != data_type:
                    continue

                transferred = (datum['status'] == 'transferred')

                # If the location refers to here
                if datum['host'] == config.get_hostname():
                    # If uploading, we should have data
                    if option_type == 'upload' and not transferred:
                        continue
                    datum_here = datum.copy()
                elif datum['host'] == remote_host:  # This the remote host?
                    # If downloading, they should have data
                    if option_type == 'download' and not transferred:
                        continue
                    datum_there = datum.copy()

            # Upload logic
            if option_type == 'upload' and datum_here and datum_there is None:
                self.copy_handshake(datum_here, remote_host, method)

            # Download logic
            if option_type == 'download' and datum_there and datum_here is None:
                self.copy_handshake(datum_there, config.get_hostname(), method)

    def copy_handshake(self, datum, destination, method='scp'):
        destination_config = config.get_config(destination)

        self.log.info("Transferring run %d to: %s" % (self.run_doc['number'],
                                                      destination))

        self.log.debug("Notifying run database")
        datum_new = {'type': datum['type'],
                     'host': destination,
                     'status': 'transferring',
                     'location': os.path.join(destination_config['directory'],
                                              self.run_doc['name'] +
                                              ('.root' if datum['type'] == 'processed' else '')),
                     'checksum': None,
                     'creation_time': datetime.datetime.utcnow(),
                     }

        if datum['type'] == 'processed':
            datum_new['pax_version'] = datum['pax_version']
            datum_new['pax_hash'] = datum['pax_hash']
            datum_new['creation_place'] = datum['creation_place']

        self.collection.update({'_id': self.run_doc['_id']},
                               {'$push': {'data': datum_new}})
        self.log.info('Starting '+method)

        try:
            self.copy(datum,datum_new,method)
            datum_new['status'] = 'verifying'
        except scp.SCPException as e:
            self.log.exception(e)
            datum_new['status'] = 'error'
        self.log.debug("Fake copy done, telling run database")

        self.collection.update({'_id': self.run_doc['_id'],
                                'data': {'$elemMatch': { 'host': datum_new['host'],
                                                         'type': datum_new['type']}}},
                               {'$set': {'data.$': datum_new}})
        self.log.info("Fake Transfer complete")



class CopyPush(CopyBase):
    """Copy data to there

    If the data is transfered to current host and does not exist at any other
    site (including transferring), then copy data there."""
    option_type = 'upload'

class CopyPull(CopyBase):
    """Copy data to here

    If data exists at a reachable host but not here, pull it.
    """
    option_type = 'download'

