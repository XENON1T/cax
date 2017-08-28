"""Handle copying data between sites.

This is one of the key tasks of 'cax' because it's responsible for moving
data between sites.  At present, it just does scp.
"""

import datetime
import logging
import os
import time
import hashlib
import json
import random
import requests
import signal
import socket
import subprocess
import sys
import time
import traceback
import datetime
import time
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

    def __init__(self):
        """init"""
    
    def check_client_installation(self):
        
        check_install = self.tsm_commands("check-installation")
    
        logging.debug( check_install )
        
        msg_std, msg_err = self.doTSM( check_install )
        
        client_info = ""
        server_info = ""
        for i in msg_std:
          if i.find("Client Version") >= 0:
            client_info = i
            logging.info("Client information: %s", client_info)
          if i.find("Server Version") >= 0:
            server_info = i
            logging.info("Server information: %s", server_info)
          if i.find("command not found") >= 0:
            client_info = "Client not Installed"
            server_info = "No Information"
        
        if client_info == "Client not Installed":
          return False
        else:
          return True        
    
    def delete_script(self, fileobj):
        """Delete script after submitting to cluster

        :param script_path: path to the script to be removed

        """
        fileobj.close()
    
    def create_script(self, script):
        """Create script as temp file to be run on cluster"""
        fileobj = tempfile.NamedTemporaryFile(delete=True,
                                            suffix='.sh',
                                            mode='wt',
                                            buffering=1)
        fileobj.write(script)
        os.chmod(fileobj.name, 0o774)

        return fileobj
    
    def doTSM(self, upload_string ):
        
        sc = self.create_script( upload_string )
        execute = subprocess.Popen( ['sh', sc.name] , 
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, shell=False )
        stdout_value, stderr_value = execute.communicate()
      
        stdout_value = stdout_value.decode("utf-8")
        stdout_value = stdout_value.split("\n")
        self.delete_script( sc )
        
        return stdout_value, stderr_value
    
    def get_checksum_folder( self, raw_data_location ):
        return checksumdir.dirhash(raw_data_location, 'sha512')
    
    def get_checksum_list(self, raw_data_location):
        """Get a dictionary with filenames and their checksums"""
        filelist = []
        for (dirpath, dirnames, filenames) in os.walk( raw_data_location ):
            filelist.extend(filenames)
            break
    
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
              tno_dict['tno_inspected'] = int(i.split(":")[1].replace(",", ""))
            elif i.find("Total number of objects backed up:") >= 0:
              tno_dict['tno_backedup'] = int(i.split(":")[1].replace(",", ""))  
            elif i.find("Total number of objects updated:") >= 0:
              tno_dict['tno_updated'] = int(i.split(":")[1].replace(",", ""))
            elif i.find("Total number of objects rebound:") >= 0:
              tno_dict['tno_rebound'] = int(i.split(":")[1].replace(",", ""))
            elif i.find("Total number of objects deleted:") >= 0:
              tno_dict['tno_deleted'] = int(i.split(":")[1].replace(",", ""))
            elif i.find("Total number of objects expired:") >= 0:
              tno_dict['tno_expired'] = int(i.split(":")[1].replace(",", ""))
            elif i.find("Total number of objects failed:") >= 0:
              tno_dict['tno_failed'] = int(i.split(":")[1].replace(",", ""))
            elif i.find("Total number of objects encrypted:") >= 0:
              tno_dict['tno_encrypted'] = int(i.split(":")[1].replace(",", ""))
            elif i.find("Total number of objects grew:") >= 0:
              tno_dict['tno_grew'] = int(i.split(":")[1].replace(",", ""))
            elif i.find("Total number of retries:") >= 0:
              tno_dict['tno_retries'] = int(i.split(":")[1].replace(",", ""))
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
                                

        return tno_dict
        
        
    def copy_and_rename(self, source, destination):
        """Create a viratually copy in /tmp for upload"""
        pass

    def delete(self, path ):
        """Delete the given path including the sub-folders"""    
        pass

    def tsm_commands(self, method=None):
        
        host_xe1t_datamanager = """#!/bin/bash
echo "Basic Config@xe1tdatamanager"
source /home/xe1ttransfer/tsm_config/init_tsm.sh   
        """
        
        host_teger = """#!/bin/bash
echo "Basic Config@Tegner"
export PATH="/cfs/klemming/projects/xenon/.adm/xenon-tsm/:$PATH"
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
dsmc rest {path_tsm}/ {path_restore}/ -followsymbolic=yes
        """
        
        check_install = """
dsmc
        """
        
        
        if method == "check-for-raw-data":
          return general[config.get_hostname()]+check_for_raw_data
        elif method == None:
          return general[config.get_hostname()]
        elif method == "incr-upload-path":
          return general[config.get_hostname()]+incr_upload
        elif method == "restore-path":
          return general[config.get_hostname()]+restore_path
        elif method == "check-installation":
          return general[config.get_hostname()]+check_install  
        else:
          return general[config.get_hostname()]+check_method

#Class: Add checksums for missing tsm-server entries in the runDB:
class AddTSMChecksum(Task):
    """Perform a checksum on accessible data at the tsm-server
       and add the checksum to the runDB.
       (Only in case the checksum is not yet added)
    """
    
    def variables(self):
        self.checksum_xe1t = 'no_checksum_xe1tdatam'
    
    def each_location(self, data_doc):
        #print("each location")
        hostname = config.get_hostname()
        destination = config.get_config("tsm-server")
        self.variables()
        
        if data_doc['host'] == "xe1t-datamanager":
            self.checksum_xe1t = data_doc['checksum']
            logging.info("Found checksum for xe1t-datamanger: %s", self.checksum_xe1t )
            return

        
        if destination['name'] == data_doc['host'] and data_doc['checksum'] == None and data_doc['status'] == 'transferred':
            """A dedicated function to add checksums to the database
               in case there are no checksums for tsm-server entries
               but the status says transferred
            """
            logging.info("There is a database entry for %s (transferred) but no checksum", data_doc['location'])
            
            #Init the TSMclient class:
            self.tsm = TSMclient()
            
            raw_data_location = data_doc['location']
            raw_data_filename = data_doc['location'].split('/')[-1]
            raw_data_path     = config.get_config( config.get_hostname() )['dir_raw']
            raw_data_tsm      = config.get_config( config.get_hostname() )['dir_tsm']
            tmp_data_path     = raw_data_tsm + "tmp_checksum_test/"
            logging.info("Raw data location @xe1t-datamanager: %s", raw_data_location)
            logging.info("Path to raw data: %s", raw_data_path)
            logging.info("Path to tsm data: %s", raw_data_tsm)
            logging.info("Path to temp. data: %s", tmp_data_path)
            logging.info("File/Folder for backup: %s", raw_data_filename)        
        
            #Sanity Check
            if self.tsm.check_client_installation() == False:
              logging.info("There is a problem with your dsmc client")
              return
            
            #Make sure that temp. download directory exists:
            if not os.path.exists(tmp_data_path):
                os.makedirs(tmp_data_path)
            
            #Download it to a temp directory
            dfolder = tmp_data_path  + "/" + raw_data_filename 
            if os.path.exists(dfolder):
              logging.info("Temp. directory %s already exists -> Delete it now", dfolder )
              shutil.rmtree(dfolder)
              
            tsm_download_result = self.tsm.download( raw_data_tsm + raw_data_filename, tmp_data_path, raw_data_filename)
            if os.path.exists( tmp_data_path + raw_data_filename ) == False:
              logging.info("Download to %s failed.", raw_data_path)
            
            #Do the checksum
            checksum_after = self.tsm.get_checksum_folder( tmp_data_path  + "/" + raw_data_filename )
            logging.info("Summary of the download for checksum comparison:")
            logging.info("Number of downloaded files: %s", tsm_download_result["tno_restored_objects"])
            logging.info("Transferred amount of data: %s", tsm_download_result["tno_restored_bytes"])
            logging.info("Network transfer rate: %s", tsm_download_result["tno_network_transfer_rate"])
            logging.info("Download time: %s", tsm_download_result["tno_data_transfer_time"])
            logging.info("Number of failed downloads: %s", tsm_download_result["tno_failed_objects"])        
            logging.info("MD5 Hash (database entry|TSM-SERVER): %s", data_doc['checksum'])
            logging.info("MD5 Hash (database entry|xe1t-datamanager): %s", self.checksum_xe1t)
            logging.info("MD5 Hash (downloaded data): %s", checksum_after)
            
            #Add to runDB and compare
            #if data_doc['checksum'] == None and self.checksum_xe1t == checksum_after:
            if data_doc['checksum'] == None and self.checksum_xe1t == "no_checksum_xe1tdatam":    
              logging.info("No checksum for database entry TSM-server")
              logging.info("Checksums for xe1t-datamanager is verfied")
              
              if config.DATABASE_LOG:
                logging.info("Notify the runDB to add checksum")
                self.collection.update({'_id' : self.run_doc['_id'],
                                        'data': {'$elemMatch': data_doc}},
                                       {'$set': {'data.$.checksum': checksum_after}})  
            
              #Delete from temp directory
            #if data_doc['checksum'] == None and self.checksum_xe1t == checksum_after:
              logging.info("Delete temp. directory for checksum verification: %s", dfolder)
              shutil.rmtree(dfolder)

        

#Class: Log-file analyser:
class TSMLogFileCheck():
    
    def __init__(self, f_folder = None):
        if f_folder != None:
          self.f_folder = f_folder
        else:
          self.f_folder = "/home/xe1ttransfer/tsm_log/"
        
        self.flist = self.init_logfiles_from_path( self.f_folder )        
        self.read_all_logfiles()
        
    
    def init_logfiles_from_path(self, path_to_logfiles):
      """Read the log-file path for logfiles:"""
      if path_to_logfiles == None:
        logging.info("No log file path is chosen")
        return 0
      
      filelist = []
      for (dirpath, dirnames, filenames) in os.walk(path_to_logfiles):
        filelist.extend(filenames)
        break
      if len(filelist) == 0:
        logging.info("Ups... Your chosen log file folder (%s) seems to be empty", path_to_logfiles)
        return 0
      
      return filelist
    
    def search_for_expression(self, logfile, expression ):
      ffile = open( logfile, 'r')
      is_in = False  
      for i_line in ffile:
        if i_line.find( expression ) >= 0:
          expression_position = i_line.find( expression )
          is_in = True
      return is_in
      
    def sort(self, sortkey=None):
      """Have a sort key for upload time and run time"""
      pass
    
    def read_logfile(self, logfile=None, search_expression=None ):
      if logfile == None:
        return 0, 0

      """Read single log file"""
      ffile = open( logfile, 'r')
      
      #Select log file for a search criterion:
      t = self.search_for_expression( logfile, search_expression)
      if t == False:
        return 0, 0
    
      #extract the when the upload started:
      #print( logfile.split(".")[0].split("_") )
      
      inf_str = logfile.split(".")[0].split("_")
      #print("a")
      length = len( inf_str )
      date_str = str(inf_str[length-2])
      time_str = str(inf_str[length-1])
      datetime_str = "{dd}_{tt}".format(dd=date_str,
                                        tt=time_str)
      #print(inf_str, length, date_str, time_str, datetime_str)
      
      nb_uploaded_files = 0
      nb_inspected_files = 0
      
      tr_amount_up = 0
      tr_amount_up_counted = False
      tr_amount_dw = 0
      
      tr_rate_up = 0
      tr_rate_up_counted = False
      tr_rate_dw = 0
      
      upload_time = 0
      download_time = 0
      total_time = 0
      dataset = ''
      
      dataset_time = ''
      
      for i in ffile:
        if i.find("Number of uploaded files:") >= 0:
          nb_uploaded_files = int(i[i.find("Number of uploaded files:"):].split(":")[1].replace(" ", ""))
        
        if i.find("Number of inspected files:") >= 0:
          nb_inspected_files = int(i[i.find("Number of inspected files:"):].split(":")[1].replace(" ", ""))
        
        if i.find("Upload time:") >= 0:
          print("TU: ", i)
          upload_time = i[i.find("Upload time:"):].split(":")[1].replace(" ", "").replace(",", "")
          upload_time = upload_time[:len(upload_time)-4]
        
        if i.find("Download time:") >= 0:
          print("TD: ", i)
          download_time = i[i.find("Download time:"):].split(":")[1].replace(" ", "").replace(",", "")
          download_time = download_time[:len(download_time)-4]
        
        if i.find("Transferred amount of data:") >= 0 and tr_amount_up_counted == False:
          tr_read = i[i.find("Transferred amount of data:"):].split(":")[1].replace(" ", "")
          tr_amount_up = tr_read[:len(tr_read)-3]
          tr_amount_unit = tr_read[len(tr_read)-3:].replace(" ", "")
          if tr_amount_unit.find("MB") >= 0:
            tr_amount_up = float(tr_amount_up)/1024.
          elif tr_amount_unit.find("KB") >= 0:
            tr_amount_up = float(tr_amount_up)/1024./1024.  
          elif tr_amount_unit.find("GB") >= 0:
            tr_amount_up = float(tr_amount_up)
          print("TUPAmount: ", tr_amount_up)  
          tr_amount_up_counted = True
          
        if i.find("Transferred amount of data:") >= 0 and tr_amount_up_counted == True:
          tr_read      = i[i.find("Transferred amount of data:"):].split(":")[1].replace(" ", "")
          tr_amount_dw = tr_read[:len(tr_read)-3]
          tr_amount_unit = tr_read[len(tr_read)-3:].replace(" ", "")
          if tr_amount_unit.find("MB") >= 0:
            tr_amount_dw = float(tr_amount_dw)/1024.
          elif tr_amount_unit.find("KB") >= 0:
            tr_amount_dw = float(tr_amount_dw)/1024./1024.  
          elif tr_amount_unit.find("GB") >= 0:
            tr_amount_dw = float(tr_amount_dw)
          print("TDWAmount: ", tr_amount_dw)
          
        if i.find("Network transfer rate:") >= 0 and tr_rate_up_counted == False:
          print("NTR up: ", i)
          tr_rate_up = i[i.find("Network transfer rate:"):].split(":")[1].replace(" ", "").replace(",", "")
          tr_rate_up = tr_rate_up[:len(tr_rate_up)-7]  
          tr_rate_up_counted = True
          
        if i.find("Network transfer rate:") >= 0 and tr_amount_up_counted == True:
          print("NTR dw: ", i)
          tr_rate_dw = i[i.find("Network transfer rate:"):].split(":")[1].replace(" ", "").replace(",", "")
          tr_rate_dw = tr_rate_dw[:len(tr_rate_dw)-7]   
 
        if i.find("tsm upload dataset") >= 0:
            position = int(i.split("[INFO]")[1].find("_"))
            beg_d = int(position-6)
            end_d = int(position+5)
            dataset = i.split("[INFO]")[1][beg_d:end_d]
            total_time = i.split("took")[1].replace(" ", "").replace("seconds", "")
            total_time = total_time[:len(total_time)-1]
        
        if i.find("File/Folder for backup:") >= 0:
            position = i.split("[INFO]")[1].split(":")[1]
            if position.find("_MV") >= 0:
              position = position.split("_MV")[0]
            dataset_time = position
      
      subinfo = {}
      subinfo['dataset_time'] = dataset
      subinfo['upload_time'] = datetime_str
      subinfo['nb_uploaded_files'] = nb_uploaded_files
      subinfo['nb_inspected_files'] = nb_inspected_files
      subinfo['tr_amount_up'] = tr_amount_up
      subinfo['tr_amount_dw'] = tr_amount_dw
      subinfo['tr_rate_up'] = tr_rate_up
      subinfo['tr_rate_dw'] = tr_rate_dw
      subinfo['total_time'] = total_time
      
      return dataset, subinfo
    
    def read_all_logfiles(self):
      """A function to read all logfile at the same time"""
      print( self.f_folder )
      import ROOT as root
      
      total_upload_time_per_dataset = 0
      total_upload_volume = 0
      
      fin = root.TFile("/home/xe1ttransfer/tsm_log/tsm_summary.root", "RECREATE")
      gr_upload_rate = root.TGraph()
      gr_upload_rate.SetName("upload_rate")
      
      gr_dwload_rate = root.TGraph()
      gr_dwload_rate.SetName("download_rate")

      gr_upAmount = root.TGraph()
      gr_upAmount.SetName("Upload_Amount")
      
      gr_dwAmount = root.TGraph()
      gr_dwAmount.SetName("Download_Amount")
      
      gr_total_time = root.TGraph()
      gr_total_time.SetName("total_time")
      
      gr_nb_uploaded_files = root.TGraph()
      gr_nb_uploaded_files.SetName("gr_nb_uploaded_files")
      
      i = 0
      for i_file in self.flist:
        #print(i_file)
        try:
          filename, info = self.read_logfile( self.f_folder + i_file, "Upload to tape: [succcessful]")
          total_upload_time_per_dataset += float(info['total_time'])
          total_upload_volume += float(info['tr_amount_up'])
          print(filename, info)
          
          dataset_time      = info['dataset_time']
          upload_time       = info['upload_time']
          
          upload_rate       = float(info['tr_rate_up'])
          dwload_rate       = float(info['tr_rate_dw'])
          upAmount          = float(info['tr_amount_up'])
          dwAmount          = float(info['tr_amount_dw'])
          total_time        = float(info['total_time'])
          nb_uploaded_files = float(info['nb_uploaded_files'])      
          nb_inspected_files = float(info['nb_inspected_files'])
          
          #Quick Summary calculation:
          if nb_uploaded_files > 0 and nb_inspected_files > 0:
            total_upload_time_per_dataset += total_time #sec
            total_upload_volume += upAmount             #GB
          
          x = time.strptime(upload_time,'%Y%m%d_%H%M%S')
          y = time.mktime( x )
          x_axis = y    #specify the x axis in seconds
          
          if nb_uploaded_files > 0 and nb_inspected_files > 0:
            gr_upload_rate.SetPoint(i, x_axis, upload_rate )
            gr_dwload_rate.SetPoint(i, x_axis, dwload_rate)
            gr_upAmount.SetPoint(i, x_axis, upAmount)
            gr_dwAmount.SetPoint(i, x_axis, dwAmount)
            gr_total_time.SetPoint(i, x_axis, total_time)
            gr_nb_uploaded_files.SetPoint(i, x_axis, nb_uploaded_files)
          
          print("-> Output: time vs. uploaded files", x_axis, nb_uploaded_files)
          i = i + 1
        except:
          pass
      
      gr_upload_rate.SetTitle("Upload Rate")
      gr_upload_rate.GetXaxis().SetTitle("time [s]")
      gr_upload_rate.GetYaxis().SetTitle("Upload Rate [kB/s]")
      gr_upload_rate.Write()
      
      gr_dwload_rate.SetTitle("Download Rate")
      gr_dwload_rate.GetXaxis().SetTitle("time [s]")
      gr_dwload_rate.GetYaxis().SetTitle("Download Rate [kB/s]")
      gr_dwload_rate.Write()
      
      gr_upAmount.SetTitle("Amount of uploaded data")
      gr_upAmount.GetXaxis().SetTitle("time [s]")
      gr_upAmount.GetYaxis().SetTitle("Upload [GB]")
      gr_upAmount.Write()
      
      gr_dwAmount.SetTitle("Amount of downloaded data")
      gr_dwAmount.GetXaxis().SetTitle("time [s]")
      gr_dwAmount.GetYaxis().SetTitle("Download [GB]")      
      gr_dwAmount.Write()
      
      gr_total_time.SetTitle("Total upload time")
      gr_total_time.GetXaxis().SetTitle("time [s]")
      gr_total_time.GetYaxis().SetTitle("UploadTime [s]")
      gr_total_time.Write()
      
      gr_nb_uploaded_files.SetTitle("Number of uploaded files")
      gr_nb_uploaded_files.GetXaxis().SetTitle("time [s]")
      gr_nb_uploaded_files.GetYaxis().SetTitle("Files [#]")  
      gr_nb_uploaded_files.Write()

      print("Total upload time: ", total_upload_time_per_dataset/60/60, 'hours')
      print("Total uploaded volume: ", total_upload_volume/1024, "TB")
        
        
class TSMDatabaseCheck(Task):
    """A class to cross check runDB information
       and grab tsm-server information via tsm query
    """
    def __init__(self):
        self.tsm = TSMclient()
    
    def each_location(self, data_doc):
        #print("each location")
        hostname = config.get_hostname()
        destination = config.get_config("tsm-server")
        
    def get_info(self, tsm_path):
        
        #Prepare path for tsm-server query:
        if tsm_path[-1] != "/":
            tsm_path+="/"
        
        logging.info("Query tsm-server information for path %s", tsm_path)
        
        #Query tsm-information by script:
        query_script = self.tsm.tsm_commands("check-for-raw-data").format(path = tsm_path)
        #logging.debug( query_script )
        
        msg_std, msg_err = self.tsm.doTSM( query_script   )
        
        #find read position:
        read_position = 0
        for key, line in enumerate(msg_std):
          if line.find("Accessing as node: XENON") >= 0:
            read_position = key + 3
        
        file_size_run = 0
        for key, line in enumerate(msg_std):
          if line.find("DEFAULT") >= 0:
            iline = line.split(" ")
            iline = list(filter(None, iline))
            file_size = float( iline[0].replace(",", "") )
            file_size_run += file_size/1024/1024
        
        return file_size_run
    
class TSMStatusCheck(Task):
    """Check TSM related entries and status messages

    This notifies the run database.
    """

    def __init__(self, db, status):
        self.run_doc = db
        self.status  = status

        Task.__init__(self)

    def each_run(self):
        # For each data location, see if this filename in it
        cnt = 0        
        for data_doc in self.run_doc['data']:
            # Is not local, skip
            #print(data_doc)
            if data_doc['host'] == "xe1t-datamanager":
              data_path_datamanager = data_doc['location']
              
            if data_doc['host'] != "tsm-server":
              continue
          
            #if data_doc['status'] == "transferred" and data_doc['checksum'] == None:
              #print( data_doc['location'], data_doc['status'] , data_doc['checksum'])
              
            if data_doc['status'] == self.status:
              if data_doc['checksum'] != None:
                cksum = "checksum: YES"
              else:
                cksum = "checksum: NO"  
              
              logging.info( "Run %s/%s at %s: Status: %s, Location: %s, %s", self.run_doc['number'], self.run_doc['name'], data_doc['host'], data_doc['status'], data_doc['location'],  cksum)        
 
        
    
