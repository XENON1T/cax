import os

from paramiko import SSHClient, util
import scp

from .. import config
from ..task import Task


def copy(f1, f2,
         server,
         username):
    util.log_to_file('ssh.log')
    ssh = SSHClient()
    ssh.load_system_host_keys()

    ssh.connect(server,
                username=username)

    # SCPCLient takes a paramiko transport as its only argument
    client = scp.SCPClient(ssh.get_transport())

    client.put(f1, f2,
               recursive=True)

    client.close()


class SCPPush(Task):
    "Perform a checksum on accessible data."

    def each_run(self):
        if self.upload_options is None:
            return

        # For this run, where can we upload to?
        for remote_host in self.upload_options:
            self.log.debug(remote_host)
            # Grab the configuration of this host
            remote_config = config.get_config(remote_host)

            there = False  # Is data remote?
            datum_here = None  # Information about data here

            # Iterate over data locations to know status
            for datum in self.run_doc['data']:
                # Is host known?
                if 'host' not in datum:
                    continue

                # If the location refers to here
                if datum['host'] == config.get_hostname():
                    # Was data transferred here?
                    if datum['status'] == 'transferred':
                        # If so, store info on it.
                        datum_here = datum
                elif datum['host'] == remote_host:  # This the remote host?
                    # Is the data already there (checked or not)?
                    if datum['status'] == 'transferred' or datum[
                        'status'] == 'verifying':
                        there = True

            if datum_here and not there:
                self.copy_handshake(datum_here, remote_config, remote_host)

    def copy_handshake(self, datum_here, remote_config, remote_host):
        self.log.info("Starting transfer of run %d to remove "
                      "site: %s" % (self.run_doc['number'], remote_host))

        self.log.debug("Notifying run database")
        datum_there = {'type'    : datum_here['type'],
                       'host'    : remote_host,
                       'status'  : 'transferring',
                       'location': os.path.join(remote_config['directory'],
                                                self.run_doc['name']),
                       'checksum': None}
        self.collection.update({'_id': self.run_doc['_id']},
                               {'$push': {'data': datum_there}})
        self.log.info('Starting SCP')
        try:
            copy(datum_here['location'],
                 remote_config['directory'],
                remote_config['hostname'],
                remote_config['username'])
            datum_there['status'] = 'verifying'
        except scp.SCPException as e:
            self.log.exception(e)
            datum_there['status'] = 'error'
        self.log.debug("SCP done, telling run database")

        self.collection.update({'_id'      : self.run_doc['_id'],
                                'data.host': datum_there['host']},
                               {'$set': {'data.$': datum_there}})
        self.log.info("Transfer complete")
