import datetime
import logging
import os

import scp
from paramiko import SSHClient, util

from cax import config
from cax.task import Task


def copy(datum_original, datum_destination):
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

    logging.info("connection to %s" % server)
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


class RenameSingle(Task):
    def __init__(self, input, output):
        # Save filesnames to use
        self.input = input
        self.output = output

        # Perform base class initialization
        Task.__init__(self)

    def each_run(self):
       # For each data location, see if this filename in it
       for data_doc in self.run_doc['data']:
            # Is not local, skip
            if 'host' not in data_doc or data_doc['host'] != config.get_hostname():
                continue

            if data_doc['location'] != self.input:
                continue

            self.log.info("Moving %s to %s" % (self.input,
                                               self.output))
            os.rename(self.input,
                      self.output)

            if config.DATABASE_LOG == True:
                self.collection.update({'_id' : self.run_doc['_id'],
                                        'data': {'$elemMatch': data_doc}},
                                       {'$set': {'data.$.location': self.output}})
            break


