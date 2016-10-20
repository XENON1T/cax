"""Handle copying data between sites.

This is one of the key tasks of 'cax' because it's responsible for moving
data between sites.  At present, it just does scp.
"""

import datetime
import logging
import os
import time
import argcomplete
import hashlib
import json
import random
import requests
import signal
import socket
import subprocess
import sys
import tabulate
import time
import traceback
import datetime
import tarfile
import copy
import shutil
import checksumdir
import tempfile

import scp
from paramiko import SSHClient, util

from cax import config
from cax.task import Task



class TSMclient(Task):
    def __init__(self, rd):
        self.run_doc = rd
    
    def create_script(self, script):
        """Create script as temp file to be run on cluster"""
        fileobj = tempfile.NamedTemporaryFile(delete=False,
                                            suffix='.sh',
                                            mode='wt',
                                            buffering=1)
        fileobj.write(script)
        os.chmod(fileobj.name, 0o774)

        return fileobj
    
    def doTSM(self, upload_string ):
        #scname = "tsm_call_{id}".format(id=self.run_doc['name'] )
        #sc = "/tmp/{sc_name}.sh".format(sc_name=scname)
        
        sc = self.create_script( upload_string )
        execute = subprocess.Popen( ['sh', sc.name] , 
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, shell=False )
        stdout_value, stderr_value = execute.communicate()
      
        stdout_value = stdout_value.decode("utf-8")
        stdout_value = stdout_value.split("\n")
        
        #for i in range(0, len(stdout_value)):
          #if len(stdout_value[i]) == 0:
            #stdout_value.pop(i)
            
        #print( stdout_value)    

        return stdout_value, stderr_value
    
    def get_checksum_folder( self, raw_data_location ):
        return checksumdir.dirhash(raw_data_location, 'sha512')
    
    def get_checksum_list(self, raw_data_location):
        """Get a dictionary with filenames and their checksums"""
        filelist = []
        for (dirpath, dirnames, filenames) in os.walk( raw_data_location ):
            filelist.extend(filenames)
            break
        
        print(filelist)
        
        print( ha)
    
    def download(self, tape_source, dw_destination, raw_data_filename):
        """Download a folder from the tape storage"""
        
        script_download = self.tsm_commands("restore-path").format(path_tsm = tape_source,
                                                                   path_restore = dw_destination)
        
        logging.debug( script_download )
        
        msg_std, msg_err = self.doTSM( script_download   )
        
        tno_dict = {"tno_restored_objects": -1,
                    "tno_restored_bytes": -1,
                    "tno_failed_objects": -1,
                    "tno_data_transfer_time": -1,
                    "tno_network_transfer_rate": -1,
                    "tno_aggregate_transfer_rate": -1,
                    "tno_elapsed_processing_time": -1,
                    "tno_file_info": "",
                    }
        sub_dict = {}
            
        for i in msg_std:
          if i.find("Restoring") >= 0:
            j_restoring = i.split(" ")
            i_restoring = [x for x in j_restoring if x]
            j_dic = {}
            filename_local = i_restoring[4].replace(" ", "").split("/")[-1]
            j_dic["file_size"] = i_restoring[1].replace(" ", "")
            j_dic["file_path_tsm"] = i_restoring[2].replace(" ", "")
            j_dic["file_path_local"] = i_restoring[4].replace(" ", "")
            j_dic["file_status"] = i_restoring[5].replace(" ", "")
            sub_dict[filename_local] = j_dic
            
          if i.find("Total number of objects restored") >= 0:
            tno_dict["tno_restored_objects"] = i.split(":")[1].replace(" ", "")
          
          if i.find("Total number of bytes transferred:") >= 0:
            tno_dict["tno_restored_bytes"] = i.split(":")[1].replace(" ", "")
          
          if i.find("Total number of objects failed:") >= 0:
            tno_dict["tno_failed_objects"] = i.split(":")[1].replace(" ", "")
          
          if i.find("Data transfer time:") >= 0:
            tno_dict["tno_data_transfer_time"] = i.split(":")[1].replace(" ", "")
          
          if i.find("Network data transfer rate:") >= 0:
            tno_dict["tno_network_transfer_rate"] = i.split(":")[1].replace(" ", "")
          
          if i.find("Aggregate data transfer rate:") >= 0:    
            tno_dict["tno_aggregate_transfer_rate"] = i.split(":")[1].replace(" ", "")
          
          if i.find("Elapsed processing time:") >= 0:    
            tno_dict["tno_elapsed_processing_time"] = i.split(":")[1].replace(" ", "")
        
        tno_dict["tno_file_info"] = sub_dict
        return tno_dict
        
    def upload(self, raw_data_location):
    
        script_upload = self.tsm_commands("incr-upload-path").format(path=raw_data_location)
        
        logging.debug( script_upload )
                
        tno_dict = {
            "tno_inspected": -1,
            "tno_updated": -1,
            "tno_rebound": -1,
            "tno_deleted": -1,
            "tno_expired": -1,
            "tno_failed":-1,
            "tno_encrypted":-1,
            "tno_grew": -1,
            "tno_retries": -1,
            "tno_bytes_inspected": -1,
            "tno_bytes_transferred": -1,
            "tno_data_transfer_time":-1,
            "tno_network_transfer_rate": -1,
            "tno_aggregate_transfer_rate":-1,
            "tno_object_compressed":-1,
            "tno_total_data_reduction":-1,
            "tno_elapsed_processing_time":-1

            }
        
        
        msg_std, msg_err = self.doTSM( script_upload )
        
        for i in msg_std:

            if i.find("Total number of objects inspected:") >= 0:
              tno_dict['tno_inspected'] = int(i.split(":")[1])
            elif i.find("Total number of objects backed up:") >= 0:
              tno_dict['tno_backedup'] = int(i.split(":")[1])  
            elif i.find("Total number of objects updated:") >= 0:
              tno_dict['tno_updated'] = int(i.split(":")[1])
            elif i.find("Total number of objects rebound:") >= 0:
              tno_dict['tno_rebound'] = int(i.split(":")[1])
            elif i.find("Total number of objects deleted:") >= 0:
              tno_dict['tno_deleted'] = int(i.split(":")[1])
            elif i.find("Total number of objects expired:") >= 0:
              tno_dict['tno_expired'] = int(i.split(":")[1])
            elif i.find("Total number of objects failed:") >= 0:
              tno_dict['tno_failed'] = int(i.split(":")[1])
            elif i.find("Total number of objects encrypted:") >= 0:
              tno_dict['tno_encrypted'] = int(i.split(":")[1])
            elif i.find("Total number of objects grew:") >= 0:
              tno_dict['tno_grew'] = int(i.split(":")[1])
            elif i.find("Total number of retries:") >= 0:
              tno_dict['tno_retries'] = int(i.split(":")[1])
            elif i.find("Total number of bytes inspected:") >= 0:
              tno_dict['tno_bytes_inspected'] = i.split(":")[1].replace(" ", "")
            elif i.find("Total number of bytes transferred:") >= 0:
              tno_dict['tno_bytes_transferred'] = i.split(":")[1].replace(" ", "")
            elif i.find("Data transfer time:") >= 0:
              tno_dict['tno_data_transfer_time'] = i.split(":")[1].replace(" ", "")
            elif i.find("Network data transfer rate:") >= 0:
              tno_dict['tno_network_transfer_rate'] = i.split(":")[1].replace(" ", "")
            elif i.find("Aggregate data transfer rate:") >= 0:
              tno_dict['tno_aggregate_transfer_rate'] = i.split(":")[1].replace(" ", "")
            elif i.find("Objects compressed by:") >= 0:
              tno_dict['tno_object_compressed'] = i.split(":")[1].replace(" ", "")
            elif i.find("Total data reduction ratio:") >= 0:
              tno_dict['tno_total_data_reduction'] = i.split(":")[1].replace(" ", "")
            elif i.find("Elapsed processing time:") >= 0:
              tno_dict['tno_elapsed_processing_time'] = (i.split(":")[1].replace(" ", "") + ":" + 
                                                        i.split(":")[2].replace(" ", "") + ":" + 
                                                        i.split(":")[3].replace(" ", "") )
                                

        #print("readout: ", tno_dict )
        return tno_dict
        
        
    def copy_and_rename(self, source, destination):
        """Create a viratually copy in /tmp for upload"""
        pass

    def delete(self, path ):
        """Delete the given path including the sub-folders"""    
        pass

    def tsm_commands(self, method=None):
        
        host_xe1t_datamanager = """#!/bin/bash
echo "Basic Config"
source /home/xe1ttransfer/tsm_config/init_tsm.sh   
        """
        
        host_teger = """#!/bin/bash
echo "Basic Config But No TSM CLIENT
        """
        
        general = {"xe1t-datamanager":host_xe1t_datamanager,
                "tegner-login-1": host_teger}
        
        
        check_for_raw_data = """
dsmc query ba {path}    
        """
        
        check_method = """
echo "No method is selected: Do nothing"
        """
        
        incr_upload = """
dsmc incr {path}/
        """
        
        restore_path = """
dsmc rest {path_tsm}/ {path_restore}/ -su=yes
        """
        
        if method == "check-for-raw-data":
          return general[config.get_hostname()]+check_for_raw_data
        elif method == None:
          return general[config.get_hostname()]
        elif method == "incr-upload-path":
          return general[config.get_hostname()]+incr_upload
        elif method == "restore-path":
          return general[config.get_hostname()]+restore_path
        else:
          return general[config.get_hostname()]+check_method
        
    