"""Handle copying data between sites.

This is one of the key tasks of 'cax' because it's responsible for moving
data between sites.  At present, it just does scp.
"""

import datetime
import logging
import os
import time

import scp
from paramiko import SSHClient, util

from cax import config
from cax.task import Task

import subprocess

class CopyBase(Task):

    def copy(self, datum_original, datum_destination, method, option_type):

        if option_type == 'upload':
            config_destination = config.get_config(datum_destination['host'])
            server = config_destination['hostname']
            username = config_destination['username']

        else:
            config_original = config.get_config(datum_original['host'])
            server = config_original['hostname']
            username = config_original['username']

        nstreams = 16

        # Determine method for remote site
        if method == 'scp':
            self.copySCP(datum_original, datum_destination, server, username, option_type)

        elif method == 'gfal-copy':
            self.copyGFAL(datum_original, datum_destination, server, option_type, nstreams)

        elif method == 'lcg-cp':
            self.copyLCGCP(datum_original, datum_destination, server, option_type, nstreams)

        else:
            print (method+" not implemented")
            raise NotImplementedError()

    def copyLCGCP(self, datum_original, datum_destination, server, option_type, nstreams):
        """Copy data via GFAL function
        WARNING: Only SRM<->Local implemented (not yet SRM<->SRM)
        """
        dataset = datum_original['location'].split('/').pop()

        # gfal-copy arguments:
        #   -n: number of streams (4 for now, but doesn't work on xe1t-datamanager so use lcg-cp instead)
        command_options = "-b -D srmv2 " # Currently for resolving Midway address, may not be needed for other sites
        command_options += "-n %d " % (nstreams)
        command = "time lcg-cr "+command_options

        status = -1

        if option_type == 'upload':
            logging.info(option_type+": %s to %s" % (datum_original['location'],
                                            server+datum_destination['location']))

            # Simultaneous LFC registration
            lfc_config = config.get_config("lfc")

            if not os.path.isdir(datum_original['location']):
                raise TypeError('{} is not a directory.'.format(datum_original['location']))

            for root, dirs, files in os.walk(datum_original['location'], topdown=True):
                for filename in files:
                    
                    # Warning: Processed data dir not implemented for LFC here
                    lfc_address = lfc_config['hostname']+lfc_config['dir_'+datum_original['type']]

                    full_command = command+ \
                                   "-d "+server+datum_destination['location']+"/"+filename+" "+ \
                                   "-l "+lfc_address+"/"+dataset+"/"+filename+" "+ \
                                   "file://"+datum_original['location']+"/"+filename
                    
                    self.log.info(full_command)

                    try:
                        lcg_out = subprocess.check_output(full_command, stderr=subprocess.STDOUT, shell=True)

                    except subprocess.CalledProcessError as lcg_exec:
                        self.log.error(lcg_exec.output.rstrip().decode('ascii'))
                        self.log.error("Error: lcg-cr status = %d\n" % lcg_exec.returncode)
                        raise

        else: # download
            logging.info(option_type+": %s to %s" % (server+datum_original['location'],
                                                     datum_destination['location']))
            raise NotImplementedError()

            #command = "time lcg-cp "+command_options 
            #full_command = command+ \
            #               server+datum_original['location']+" "+ \
            #               "file://"+datum_destination['location']


    def copyGFAL(self, datum_original, datum_destination, server, option_type, nstreams):
        """Copy data via GFAL function
        WARNING: Only SRM<->Local implemented (not yet SRM<->SRM)
        """
        dataset = datum_original['location'].split('/').pop()

        # gfal-copy arguments:
        #   -f: overwrite 
        #   -r: recursive
        #   -n: number of streams (4 for now, but doesn't work on xe1t-datamanager so use lcg-cp instead)
        command = "time gfal-copy -v -f -r -p -n %d " % nstreams

        status = -1

        if option_type == 'upload':
            logging.info(option_type+": %s to %s" % (datum_original['location'],
                                            server+datum_destination['location']))

            # Simultaneous LFC registration
            lfc_config = config.get_config("lfc")

            # Warning: Processed data dir not implemented for LFC here
            lfc_address = lfc_config['hostname']+lfc_config['dir_'+datum_original['type']]

            full_command = command+ \
                           "file://"+datum_original['location']+" "+ \
                           server+datum_destination['location']+" "+ \
                           lfc_address+"/"+dataset                  

        else: # download
            logging.info(option_type+": %s to %s" % (server+datum_original['location'],
                                                     datum_destination['location']))
 
            full_command = command+ \
                           server+datum_original['location']+" "+ \
                           "file://"+datum_destination['location']

        self.log.info(full_command)

        try:
            gfal_out = subprocess.check_output(full_command, stderr=subprocess.STDOUT, shell=True)

        except subprocess.CalledProcessError as gfal_exec:
            self.log.error(gfal_exec.output.rstrip().decode('ascii'))
            self.log.error("Error: gfal-copy status = %d\n" % gfal_exec.returncode)
            raise

        gfal_out_ascii = gfal_out.rstrip().decode('ascii')
        if "error" in gfal_out_ascii.lower(): # Some errors don't get caught above
            self.log.error(gfal_out_ascii)
            raise

        else:
            self.log.info(gfal_out_ascii) # To print timing
            

    def copySCP(self, datum_original, datum_destination, server, username, option_type):
        """Copy data via SCP function
        """
        util.log_to_file('ssh.log')
        ssh = SSHClient()
        ssh.load_system_host_keys()

        logging.info("connection to %s" % server)
        ssh.connect(server, 
                    username=username,
                    compress=True,
                    timeout=60)

        # SCPCLient takes a paramiko transport as its only argument
        client = scp.SCPClient(ssh.get_transport())

        logging.info(option_type+": %s to %s" % (datum_original['location'],
                                                 datum_destination['location']))
        
        if option_type == 'upload':
            client.put(datum_original['location'],
                       datum_destination['location'],
                       recursive=True)
        else:
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

        start = time.time()

        # For this run, where do we have transfer access?
        for remote_host in options:
            self.log.debug(remote_host)

            # Get transfer protocol
            method = config.get_config(remote_host)['method'] 
            if not method:
                print ("Must specify transfer protocol (method) for "+remote_host)
                raise

            datum_here, datum_there = self.local_data_finder(data_type,
                                                             option_type,
                                                             remote_host)

            # Upload logic
            if option_type == 'upload' and datum_here and datum_there is None:
                self.copy_handshake(datum_here, remote_host, method, option_type)
                break

            # Download logic
            if option_type == 'download' and datum_there and datum_here is None:
                self.copy_handshake(datum_there, config.get_hostname(), method, option_type)
                break

        dataset = None
        if datum_there is not None:
            dataset = datum_there['location'].split('/').pop()
        elif datum_here is not None:
            dataset = datum_here['location'].split('/').pop()

        if dataset is not None: # Not sure why it does this sometimes
            end = time.time()
            elapsed = end - start
            self.log.info(method+" "+option_type+" dataset "+dataset+" took %d seconds" % elapsed) 
     
    def local_data_finder(self, data_type, option_type, remote_host):
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

        return datum_here, datum_there

    def copy_handshake(self, datum, destination, method, option_type):
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

        self.log.info(option_type+"ing run %d to: %s" % (self.run_doc['number'],
                                                      destination))

        # Determine where data should be copied to
        base_dir = destination_config['dir_%s' % datum['type']]
        if base_dir is None:
            self.log.info("no directory specified for %s" % datum['type'])
            return

        if datum['type'] == 'processed':
            self.log.info(datum)
            base_dir = os.path.join(base_dir, 'pax_%s' % datum['pax_version'])

        # Check directory existence on local host for download only
        if option_type == 'download' and not os.path.exists(base_dir):
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
                datum_new[variable] = datum.get(variable)

        if config.DATABASE_LOG == True:
            result = self.collection.update_one({'_id': self.run_doc['_id'],
                                                 },
                                   {'$push': {'data': datum_new}})

            if result.matched_count == 0:
                self.log.error("Race condition!  Could not copy because another "
                               "process seemed to already start.")
                return

        self.log.info('Starting '+method)

        try:  # try to copy
            self.copy(datum, 
                      datum_new, 
                      method, 
                      option_type)
            status = 'verifying'

        except scp.SCPException as e:
            self.log.exception(e)
            status = 'error'

        # WARNING: This needs to be extended to catch gfal-copy errors
        except:
            self.log.exception("Unexpected copy error")
            status = 'error'

        self.log.debug(method+" done, telling run database")

        if config.DATABASE_LOG:
            self.collection.update({'_id' : self.run_doc['_id'],
                                    'data': {
                                        '$elemMatch': datum_new}},
                                   {'$set': {'data.$.status': status}})

        self.log.debug(method+" done, telling run database")

        self.log.info("End of "+option_type+"\n")

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

