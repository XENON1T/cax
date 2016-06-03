"""Handle copying data between sites.

This is one of the key tasks of 'cax' because it's responsible for moving
data between sites.  At present, it just does scp.
"""

import datetime
import logging
import os

import scp
from paramiko import SSHClient, util

from cax import config
from cax.task import Task


def copy(datum_original, datum_destination):
    """Wrap the SSH and SCP libraries.

    The inputs to this function are dictionaries that describe the data
    location. It also determines if this is an upload or download.  The
    locations are already defined by this point.
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
        upload = False  # ie., download

        config_original = config.get_config(datum_original['host'])
        server = config_original['hostname']
        username = config_original['username']
    else:
        raise ValueError()

    logging.info("Connection to %s" % server)
    ssh.connect(server,
                username=username,
                compress=True)

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


class SCPBase(Task):
    """Copy data via SCP base class
    """

    def each_run(self):
        for data_type in ['raw', 'processed']:
            self.log.debug("%s" % data_type)
            self.do_possible_transfers(option_type=self.option_type,
                                       data_type=data_type)

    def do_possible_transfers(self,
                              option_type='upload',
                              data_type='raw'):
        """Determine candidate transfers.
        :param option_type: 'upload' or 'download'
         :type str
        :param data_type: 'raw' or 'processed'
         :type str
        :return:
        """

        # Get the 'upload' or 'download' options.
        options = config.get_transfer_options(option_type)

        # If no options, can't do anything
        if options is None:
            return None, None

        # For this run, where do we have transfer access?
        for remote_host in options:
            self.log.debug(remote_host)

            datum_here = None  # Information about data here
            datum_there = None  # Information about data there

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
                self.copy_handshake(datum_here, remote_host)

            # Download logic
            if option_type == 'download' and datum_there and datum_here is None:
                self.copy_handshake(datum_there, config.get_hostname())

    def copy_handshake(self, datum, destination):
        """ Perform all the handshaking required with the run DB.
        :param datum: The dictionary data location describing data to be
                      transferred
         :type str
        :param destination:  The host name where data should go to.
         :type str
        :return:
        """

        # Get information about this destination
        destination_config = config.get_config(destination)

        self.log.info("Transferring run %d to: %s" % (self.run_doc['number'],
                                                      destination))

        # Determine where data should be copied to
        base_dir = destination_config['dir_%s' % datum['type']]
        if datum['type'] == 'processed':
            base_dir = os.path.join(base_dir,
                                    'pax_%s' % datum['pax_verison'])

        if not os.path.exists(base_dir):
            if destination != config.get_hostname():
                raise NotImplementedError("Cannot create directory on another "
                                          "machine.")

            # Recursively make directories
            os.makedirs(base_dir)

        # Directory or filename to be copied
        filename = datum['location'].split('/')[-1]

        self.log.debug("Notifying run database")
        datum_new = {'type'         : datum['type'],
                     'host'         : destination,
                     'status'       : 'transferring',
                     'location'     : os.path.join(base_dir,
                                                   filename),
                     'checksum'     : None,
                     'creation_time': datetime.datetime.utcnow(),
                     }

        if datum['type'] == 'processed':
            for variable in ('pax_version', 'pax_hash', 'creation_place'):
                datum_new = datum.get(variable)

        if config.DATABASE_LOG == True:
            self.collection.update({'_id': self.run_doc['_id']},
                                   {'$push': {'data': datum_new}})

        self.log.info('Starting SCP')

        try:  # try to copy
            copy(datum,
                 datum_new)
            status = 'verifying'
        except scp.SCPException as e:
            self.log.exception(e)
            status = 'error'
        self.log.debug("SCP done, telling run database")

        if config.DATABASE_LOG:
            self.collection.update({'_id' : self.run_doc['_id'],
                                    'data': {
                                        '$elemMatch': datum_new}},
                                   {'$set': {'data.$.status': status}})

        self.log.info("Transfer complete")


class SCPPush(SCPBase):
    """Copy data via SCP to there

    If the data is transfered to current host and does not exist at any other
    site (including transferring), then copy data there."""
    option_type = 'upload'


class SCPPull(SCPBase):
    """Copy data via SCP to here

    If data exists at a reachable host but not here, pull it.
    """
    option_type = 'download'
