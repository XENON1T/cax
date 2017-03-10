"""Handle copying data between sites.

This is one of the key tasks of 'cax' because it's responsible for moving
data between sites.  At present, it just does scp.
"""

import datetime
import logging
import os
import time
import shutil
import scp
from paramiko import SSHClient, util

import pax

from cax import config
from cax.task import Task
from cax import qsub

from cax.tasks.tsm_mover import TSMclient
from cax.tasks.rucio_mover import RucioBase, RucioRule, RucioDownload


import subprocess

class CopyBase(Task):

    def copy(self, datum_original, datum_destination, method, option_type, data_type):

        if option_type == 'upload':
            config_destination = config.get_config(datum_destination['host'])
            server = config_destination['hostname']
            username = config_destination['username']

        else:
            config_original = config.get_config(datum_original['host'])
            server = config_original['hostname']
            username = config_original['username']

        if config.nstream_settings() == None:
            nstreams = 1
        else:
            nstreams = config.nstream_settings()

        if config.get_cert() == None:
            grid_cert = ''
        else:
            grid_cert = config.get_cert()

        # Determine method for remote site
        if method == 'scp':
            self.copySCP(datum_original, datum_destination, server, username, option_type)

        elif method == 'rsync':
            self.copyRSYNC(datum_original, datum_destination, server, username, option_type, data_type)

        elif method == 'gfal-copy':
            self.copyGFAL(datum_original, datum_destination, server, option_type, nstreams, grid_cert)

        elif method == 'lcg-cp':
            self.copyLCGCP(datum_original, datum_destination, server, option_type, nstreams)

        elif method == 'rucio' and option_type == 'upload':
            self.rucio.copyRucio(datum_original, datum_destination, option_type)

        elif method == 'rucio' and option_type == 'download':
            self.ruciodw.DoDownload(datum_original, datum_destination, option_type)           

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


    def copyGFAL(self, datum_original, datum_destination, server, option_type, nstreams, grid_cert):
        """Copy data via GFAL function
        WARNING: Only SRM<->Local implemented (not yet SRM<->SRM)
        """
        dataset = datum_original['location'].split('/').pop()

        # gfal-copy arguments:
        #   -f: overwrite 
        #   -r: recursive
        #   -n: number of streams (4 for now, but doesn't work on xe1t-datamanager so use lcg-cp instead)
        #   -t: timeout in seconds
        #   -K: specify checksum algorithm
        # --cert: path to initialized GRID certificate (voms-proxy-init  -voms xenon.biggrid.nl -valid 168:00 -out user_cert)
        command = "time gfal-copy -v -f -r -p -t 32400 -K adler32 --cert %s -n %d " % (grid_cert, nstreams)

        status = -1

        if option_type == 'upload':
            logging.info(option_type+": %s to %s" % (datum_original['location'],
                                            server+datum_destination['location']))

            # Simultaneous LFC registration
            #lfc_config = config.get_config("lfc")

            # Warning: Processed data dir not implemented for LFC here
            #lfc_address = lfc_config['hostname']+lfc_config['dir_'+datum_original['type']]

            # Use GSIFTP address instead of POSIX from Stash (to avoid login node)
            if config.get_hostname() == 'login':
                config_original = config.get_config(datum_original['host'])
                server_original = config_original['hostname']
                full_command = command+ \
                           server_original+datum_original['location']+" "+ \
                           server+datum_destination['location'] #+" "+ \
                           #lfc_address+"/"+dataset                  

            # Use SRM address instead of POSIX from Midway (to avoid worker nodes)
            #elif config.get_hostname() == 'midway-login1':
            #    server_original = 'srm://srm1.rcc.uchicago.edu:8443/srm/v2/server?SFN='
            #    full_command = command+ \
            #               server_original+datum_original['location']+" "+ \
            #               server+datum_destination['location'] #+" "+ \
                           #lfc_address+"/"+dataset                  
           
            else:
                full_command = command+ \
                           "file://"+datum_original['location']+" "+ \
                           server+datum_destination['location'] #+" "+ \
                           #lfc_address+"/"+dataset                  

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
            
    def copyRSYNC(self, datum_original, datum_destination, server, username, option_type, data_type):
        """Copy data via rsync function
        """

        command = "time rsync -r --stats "

        if data_type is 'raw':
            command += "--append "

        status = -1

        if option_type == 'upload':
            logging.info(option_type+": %s to %s" % (datum_original['location'],
                                            server+datum_destination['location']))

            full_command = command+ \
                       datum_original['location']+" "+ \
                       username+"@"+server+":"+os.path.dirname(datum_destination['location'])

        else: # download
            logging.info(option_type+": %s to %s" % (server+datum_original['location'],
                                                     datum_destination['location']))

            full_command = command+ \
                           username+"@"+server+":"+datum_original['location']+" "+ \
                           os.path.dirname(datum_destination['location'])

        self.log.info(full_command)

        try:
            rsync_out = subprocess.check_output(full_command, stderr=subprocess.STDOUT, shell=True)

        except subprocess.CalledProcessError as rsync_exec:
            self.log.error(rsync_exec.output.rstrip().decode('ascii'))
            self.log.error("Error: rsync status = %d\n" % rsync_exec.returncode)
            raise

        rsync_out_ascii = rsync_out.rstrip().decode('ascii')
        if "error" in rsync_out_ascii.lower(): # Some errors don't get caught above
            self.log.error(rsync_out_ascii)
            raise

        else:
            self.log.info(rsync_out_ascii) # To print timing


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

        for data_type in config.get_config( config.get_hostname() )['data_type']:
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
        datum_there = None
        datum_here = None
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


            #Delete the old data base entry if rucio transfers are requested
            #and an old upload failed by a bad connection error.
            if method == "rucio" and datum_there != None and datum_there['status'] == 'RSEreupload' and config.DATABASE_LOG == True:
              self.log.info("Former upload of %s failed with error", datum_here['location'])
              self.log.info("[('Connection aborted.', BadStatusLine('',))] -> Delete runDB status and start again")
              
              self.collection.update({'_id': self.run_doc['_id']},
                                     {'$pull': {'data': datum_there}})

            #Upload logic for everything exepct tape
            if option_type == 'upload' and method != "tsm" and datum_here and (datum_there is None or                                                           datum_there['status'] == 'RSEreupload'):
                self.copy_handshake(datum_here, remote_host, method, option_type, data_type)
                break

            # Download logic for everything exepct tape
            if option_type == 'download' and datum_there and datum_here is None and method != "tsm":
                self.copy_handshake(datum_there, config.get_hostname(), method, option_type, data_type)
                break

            # Upload tsm:
            if option_type == 'upload' and datum_here and datum_there is None and method == "tsm":
                self.copy_tsm(datum_here, config.get_config(remote_host)['name'], method, option_type)
                break

            # Download tsm:
            if option_type == 'download' and datum_there and datum_here is None and method == "tsm":
                self.copy_tsm_download(datum_there, config.get_hostname(), method, option_type)
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

        version = 'v%s' % pax.__version__

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

                if datum['type'] == 'processed' and not version == datum['pax_version']:
                    continue 
                
                datum_here = datum.copy()

            elif datum['host'] == remote_host:  # This the remote host?
                # If downloading, they should have data
                if option_type == 'download' and not transferred:
                    continue

                if datum['type'] == 'processed' and not version == datum['pax_version']:
                    continue

                datum_there = datum.copy()

        return datum_here, datum_there

    def copy_tsm_download( self, datum, destination, method, option_type):
        """A dedicated download function for downloads from tape storage"""
        self.tsm = TSMclient()

        logging.info('Tape Backup to PDC STOCKHOLM (Download)')

        raw_data_location = datum['location']
        raw_data_filename = datum['location'].split('/')[-1]
        raw_data_path     = config.get_config( config.get_hostname() )['dir_raw']
        raw_data_tsm      = config.get_config( config.get_hostname() )['dir_tsm']
        logging.info("Raw data location @xe1t-datamanager: %s", raw_data_location)
        logging.info("Path to raw data: %s", raw_data_path)
        logging.info("Path to tsm data: %s", raw_data_tsm)
        logging.info("File/Folder for backup: %s", raw_data_filename)
        
        self.log.debug("Notifying run database")
        datum_new = {'type'         : datum['type'],
                     'host'         : destination,
                     'status'       : 'transferring',
                     'location'     : "n/a",
                     'checksum'     : None,
                     'creation_time': datetime.datetime.utcnow(),
                     }
        logging.info("new entry for rundb: %s", datum_new )
        


        if config.DATABASE_LOG == True:
            result = self.collection.update_one({'_id': self.run_doc['_id'],
                                                 },
                                   {'$push': {'data': datum_new}})

            if result.matched_count == 0:
                self.log.error("Race condition!  Could not copy because another "
                               "process seemed to already start.")
                return

        logging.info("Start tape download")

        #Sanity Check
        if self.tsm.check_client_installation() == False:
          logging.info("There is a problem with your dsmc client")
          return

        #Do download:
        tsm_download_result = self.tsm.download( raw_data_location, raw_data_path, raw_data_filename)
        if os.path.exists( raw_data_path + raw_data_filename ) == False:
          logging.info("Download to %s failed.", raw_data_path)
          if config.DATABASE_LOG:
            #Notify the database if something went wrong during the download:
            logging.info("Notifiy the runDB: error")
            self.collection.update({'_id' : self.run_doc['_id'],
                                  'data': {
                                        '$elemMatch': datum_new}},
                                   {'$set': {'data.$.status': "error",
                                             'data.$.location': "n/a",
                                             'data.$.checksum': "n/a",
                                             }
                                   })

        #Rename
        file_list = []
        for (dirpath, dirnames, filenames) in os.walk(raw_data_path + raw_data_filename):
          file_list.extend(filenames)
          break

        for i_file in file_list:
          path_old = raw_data_path + raw_data_filename + "/" + i_file
          path_new = raw_data_path + raw_data_filename + "/" + i_file[12:]
          if not os.path.exists(path_new):
              os.rename( path_old, path_new)

        #Do checksum and summarize it:
        checksum_after = self.tsm.get_checksum_folder( raw_data_path  + "/" + raw_data_filename )
        logging.info("Summary of the download for checksum comparison:")
        logging.info("Number of downloaded files: %s", tsm_download_result["tno_restored_objects"])
        logging.info("Transferred amount of data: %s", tsm_download_result["tno_restored_bytes"])
        logging.info("Network transfer rate: %s", tsm_download_result["tno_network_transfer_rate"])
        logging.info("Download time: %s", tsm_download_result["tno_data_transfer_time"])
        logging.info("Number of failed downloads: %s", tsm_download_result["tno_failed_objects"])
        logging.info("MD5 Hash (database entry): %s", datum['checksum'])
        logging.info("MD5 Hash (downloaded data): %s", checksum_after)


        if checksum_after == datum['checksum']:
          logging.info("The download/restore of the raw data set %s was [SUCCESSFUL]", raw_data_filename)
          logging.info("Raw data set located at: %s", raw_data_path + raw_data_filename)
        elif checksum_after != datum['checksum']:
          logging.info("The download/restore of the raw data set %s [FAILED]", raw_data_filename)
          logging.info("Checksums do not agree!")

        #Notifiy the database for final registration
        if checksum_after == datum['checksum']:

          if config.DATABASE_LOG:
            #Notify the database if everything was fine:
            logging.info("Notifiy the runDB: transferred")
            self.collection.update({'_id' : self.run_doc['_id'],
                                  'data': {
                                        '$elemMatch': datum_new}},
                                   {'$set': {'data.$.status': "transferred",
                                             'data.$.location': raw_data_path + raw_data_filename,
                                             'data.$.checksum': checksum_after,
                                             }
                                   })
          else:
            logging.info("Database is not notified")

        elif checksum_after != datum['checksum']:
          if config.DATABASE_LOG:
            #Notify the database if something went wrong during the download:
            logging.info("Notifiy the runDB: error")
            self.collection.update({'_id' : self.run_doc['_id'],
                                  'data': {
                                        '$elemMatch': datum_new}},
                                   {'$set': {'data.$.status': "error",
                                             'data.$.location': "n/a",
                                             'data.$.checksum': "n/a",
                                             }
                                   })
          else:
            logging.info("Database is not notified")

        return 0

    def copy_tsm(self, datum, destination, method, option_type):
        self.tsm = TSMclient()

        logging.info('Tape Backup to PDC STOCKHOLM')
        print( datum, destination, method, option_type)

        raw_data_location = datum['location']
        raw_data_filename = datum['location'].split('/')[-1]
        raw_data_path     = raw_data_location.replace( raw_data_filename, "")
        raw_data_tsm      = config.get_config( config.get_hostname() )['dir_tsm']
        logging.info("Raw data location @xe1t-datamanager: %s", raw_data_location)
        logging.info("Path to raw data: %s", raw_data_path)
        logging.info("Path to tsm data: %s", raw_data_tsm)
        logging.info("File/Folder for backup: %s", raw_data_filename)

        self.log.debug("Notifying run database")
        datum_new = {'type'         : datum['type'],
                     'host'         : destination,
                     'status'       : 'transferring',
                     'location'     : "n/a",
                     'checksum'     : None,
                     'creation_time': datetime.datetime.utcnow(),
                     }
        logging.info("new entry for rundb: %s", datum_new )

        if config.DATABASE_LOG == True:
            result = self.collection.update_one({'_id': self.run_doc['_id'],
                                                 },
                                   {'$push': {'data': datum_new}})

            if result.matched_count == 0:
                self.log.error("Race condition!  Could not copy because another "
                               "process seemed to already start.")
                return

        logging.info("Start tape upload")


        if self.tsm.check_client_installation() == False:
          logging.info("There is a problem with your dsmc client")
          if config.DATABASE_LOG:
            self.collection.update({'_id' : self.run_doc['_id'],
                                  'data': {
                                        '$elemMatch': datum_new}},
                                   {'$set': {'data.$.status': "error",
                                             'data.$.location': "n/a",
                                             'data.$.checksum': "n/a",
                                             }
                                   })
          return

        #Prepare a copy from raw data location to tsm location ( including renaming)
        checksum_before_raw = self.tsm.get_checksum_folder( raw_data_path+raw_data_filename )
        file_list = []
        for (dirpath, dirnames, filenames) in os.walk(raw_data_path+raw_data_filename):
          file_list.extend(filenames)
          break

        if not os.path.exists(raw_data_tsm + raw_data_filename):
          os.makedirs(raw_data_tsm + raw_data_filename)

        for i_file in file_list:
          path_old = raw_data_path + raw_data_filename + "/" + i_file
          path_new = raw_data_tsm + raw_data_filename + "/" + raw_data_filename + "_" + i_file
          if not os.path.exists(path_new):
              shutil.copy2(path_old, path_new)

        checksum_before_tsm = self.tsm.get_checksum_folder( raw_data_tsm + raw_data_filename )

        if checksum_before_raw != checksum_before_tsm:
          logging.info("Something went wrong during copy & rename")
          if config.DATABASE_LOG:
            self.collection.update({'_id' : self.run_doc['_id'],
                                  'data': {
                                        '$elemMatch': datum_new}},
                                   {'$set': {'data.$.status': "error",
                                             'data.$.location': "n/a",
                                             'data.$.checksum': "n/a",
                                             }
                                   })

          return
        elif checksum_before_raw == checksum_before_tsm:
          logging.info("Copy & rename: [succcessful] -> Checksums agree")


        tsm_upload_result = self.tsm.upload( raw_data_tsm + raw_data_filename )
        logging.info("Number of uploaded files: %s", tsm_upload_result["tno_backedup"])
        logging.info("Number of inspected files: %s", tsm_upload_result["tno_inspected"])
        logging.info("Number of failed files: %s", tsm_upload_result["tno_failed"])
        logging.info("Transferred amount of data: %s", tsm_upload_result["tno_bytes_transferred"])
        logging.info("Inspected amount of data: %s", tsm_upload_result["tno_bytes_inspected"])
        logging.info("Upload time: %s", tsm_upload_result["tno_data_transfer_time"])
        logging.info("Network transfer rate: %s", tsm_upload_result["tno_network_transfer_rate"])
        logging.info("MD5 Hash (raw data): %s", checksum_before_tsm)

        test_download = os.path.join(raw_data_tsm, "tsm_verify_download")
        #Make sure that temp. download directory exists:
        if not os.path.exists(test_download):
          os.makedirs(test_download)
        tsm_download_result = self.tsm.download( raw_data_tsm + raw_data_filename, test_download, raw_data_filename)
        if os.path.exists( raw_data_tsm + raw_data_filename ) == False:
          logging.info("Download to %s failed. Checksum will not match", test_download)

        checksum_after = self.tsm.get_checksum_folder( test_download  + "/" + raw_data_filename )
        logging.info("Summary of the download for checksum comparison:")
        logging.info("Number of downloaded files: %s", tsm_download_result["tno_restored_objects"])
        logging.info("Transferred amount of data: %s", tsm_download_result["tno_restored_bytes"])
        logging.info("Network transfer rate: %s", tsm_download_result["tno_network_transfer_rate"])
        logging.info("Download time: %s", tsm_download_result["tno_data_transfer_time"])
        logging.info("Number of failed downloads: %s", tsm_download_result["tno_failed_objects"])
        logging.info("MD5 Hash (raw data): %s", checksum_after)

        status = ""
        if checksum_before_tsm == checksum_after:
          logging.info("Upload to tape: [succcessful]")
          status = "transferred"
        else:
          logging.info("Upload to tape: [failed]")
          status = "error"

        ##Delete check folder
        shutil.rmtree(raw_data_tsm + raw_data_filename)
        shutil.rmtree(test_download + "/" + raw_data_filename)

        if config.DATABASE_LOG:
          self.collection.update({'_id' : self.run_doc['_id'],
                                  'data': {
                                        '$elemMatch': datum_new}},
                                   {'$set': {'data.$.status': status,
                                             'data.$.location': raw_data_tsm + raw_data_filename,
                                             'data.$.checksum': checksum_after,
                                             }
                                   })

        return 0

    def copy_handshake(self, datum, destination, method, option_type, data_type):
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
        if destination_config['dir_%s' % datum['type']] != None:
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
        else:
          base_dir = "none"
          
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

        if method == "rucio" and option_type == "upload":
            #Init the rucio module when method==rucio is requested
            self.log.info("Init rucio_mover module for Rucio transfers (upload)")
            self.rucio = RucioBase(self.run_doc)
            self.rucio.set_host( config.get_hostname() )
            self.rucio.set_remote_host( destination )
            #Sanity check for rucio client
            if self.rucio.sanity_checks() == False:
              logging.info("!!! <<The sanity checks fail>>  !!!")
              return 0
            #Add two further database entries for rucio related uploads
            datum_new['rse'] = []
            datum_new['location'] = "n/a"

        if method == "rucio" and option_type == "download":
            rucio_catalogue_config = config.get_config("rucio-catalogue")
            
            self.log.info("Init rucio_mover module for Rucio transfers (download)")
            #self.rucio = RucioBase(self.run_doc)
            #self.rucio.set_host( config.get_hostname() )
            #self.rucio.set_remote_host( destination )
            ##Sanity check for rucio client
            #if self.rucio.sanity_checks() == False:
              #logging.info("!!! <<The sanity checks fail>>  !!!")
              #return 0
            #Load and config the download module of rucio/ruciax
            self.ruciodw = RucioDownload()
            self.ruciodw.SetDatabaseEntry(self.run_doc)
            self.ruciodw.ExternalDatabaseEntry()
            self.ruciodw.SetDownloadConfig( rucio_catalogue_config, destination_config)
            datum_new['location'] = "n/a"
            

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
                      option_type, data_type)
            # Checksumming to follow on local site
            if method == 'scp' or method == 'rsync':
                status = 'verifying'

            # Cannot do cax-checksum on GRID sites, 
            # so assume gfal-copy/lcg-cp checksum is sufficient
            else:
                status = 'verifying'
                # TO DO: Manually copy checksum to DB entry here

        except scp.SCPException as e:
            self.log.exception(e)
            status = 'error'

        # WARNING: This needs to be extended to catch gfal-copy errors
        except:
            self.log.exception("Unexpected copy error")
            status = 'error'

        self.log.debug(method+" done, telling run database")

        if config.DATABASE_LOG:
          if method == "rucio" and option_type == "upload":
            logging.info("Following entries are added to the runDB:")
            logging.info("  * Status: %s", self.rucio.get_rucio_info()['status'] )
            logging.info("  * Location: %s", self.rucio.get_rucio_info()['location'] )
            logging.info("  * Checksum: %s", self.rucio.get_rucio_info()['checksum'] )
            logging.info("  * RSE: %s", self.rucio.get_rucio_info()['rse'] )
            
            self.collection.update({'_id' : self.run_doc['_id'],
                                    'data': {
                                    '$elemMatch': datum_new}},
                                 {'$set': {
                                      'data.$.status': self.rucio.get_rucio_info()['status'],
                                      'data.$.location': self.rucio.get_rucio_info()['location'],
                                      'data.$.checksum': self.rucio.get_rucio_info()['checksum'],
                                      'data.$.rse': self.rucio.get_rucio_info()['rse']
                                          }
                                })
          
          elif method == "rucio" and option_type == "download":
            logging.info("Following entries are added to the runDB:")
            logging.info("  * Status: %s", self.ruciodw.get_rucio_info()['status'] )
            logging.info("  * Location: %s", self.ruciodw.get_rucio_info()['location'] )
            
            self.collection.update({'_id' : self.run_doc['_id'],
                                    'data': {
                                    '$elemMatch': datum_new}},
                                 {'$set': {
                                      'data.$.status': self.ruciodw.get_rucio_info()['status'],
                                      'data.$.location': self.ruciodw.get_rucio_info()['location']
                                          }
                                })  

              
          else:
          #Fill the data if method is not rucio
            if config.DATABASE_LOG:  
              self.collection.update({'_id' : self.run_doc['_id'],
                                    'data': {
                                        '$elemMatch': datum_new}},
                                   {'$set': {
                                        'data.$.status': status
                                            }
                                   })
        

        if method == "rucio" and option_type == "upload":
          #Rucio 'side load' to set the transfer rules directly after the file upload
          if self.rucio.get_rucio_info()['status'] == "transferred":
            logging.info("Initiate the RucioRule for a first set of transfer rules")
            #Add: Outcome of the rucio transfers to the new database entry
            #     without read the runDB again.
            datum_new['status'] = self.rucio.get_rucio_info()['status'] 
            datum_new['location'] = self.rucio.get_rucio_info()['location']
            datum_new['checksum'] = self.rucio.get_rucio_info()['checksum']
            datum_new['rse'] = self.rucio.get_rucio_info()['rse']
            
            #Init the RucioRule module and set its runDB entry manually
            self.rucio_rule = RucioRule()
            self.rucio_rule.set_db_entry_manually( self.run_doc )
            #Perform the initial rule setting:
            self.rucio_rule.set_possible_rules(data_type=datum['type'], dbinfo = datum_new)
            logging.info("Status: transferred -> Transfer rules are set for %s", self.rucio.get_rucio_info()['rse'])
            
            # Commented out due to upload section (rucio_mover) option 3!
            # No need to delete single file rules manually after upload
            #let it sleep for 5 seconds:
            #logging.info("Sleep")
            #time.sleep(15)
            #logging.info("Sleep time finished ")
            #self.log.info("Delete individual rules of the uploaded files:")
            #for i_file in self.rucio.get_rucio_info()['file_list']:
              #i_location = "{iscope}:{ifile}".format( iscope=self.rucio.get_rucio_info()['scope_upload'],
                                                      #ifile=i_file.split("/")[-1] )
              
              #self.log.info("Time out for %s", i_location)
              #self.rucio_rule.update_rule( i_location, self.rucio.get_rucio_info()['rse'][0], "10" )
          else:
            logging.info("Something went wrong during the upload (error). No rules are set")

        elif method == "rucio" and option_type == "download":
          logging.info("<-- Finished download %s to location %s with status %s -->", datum['location'], self.ruciodw.get_rucio_info()['location'], self.ruciodw.get_rucio_info()['status'])

        logging.debug(method+" done, telling run database")

        logging.info("End of "+option_type+"\n")

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

