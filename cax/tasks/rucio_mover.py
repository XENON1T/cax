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
import tarfile
import copy
import shutil
import tempfile
import io
import locale
import json

import scp
from paramiko import SSHClient, util

from cax import config
from cax.task import Task



class RucioBase(Task):
    
    def __init__(self, rd):
      self.run_doc = rd
      self.return_rucio = {}
      
    def get_rucio_info(self):
      return self.return_rucio
  
    def set_host(self, host):
      self.host = host
      
    def set_remote_host(self, remote_host):
      self.remote_host = remote_host
    
    def list_rse_usage(self, rse_remote):
      """List the data usage at the rucio storage elements"""
      lirseusage = self.RucioCommandLine( self.host,
                                      "list-rse-usage",
                                      filelist = None,
                                      metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"],
                                                              rse_remote=rse_remote)
      
      logging.debug( lirseusage )
      
      rse_usage_summary = {'rse_usage_used': 'n/a',
                           'rse_usage_rse' : 'n/a',
                           'rse_usage_updatedat': 'n/a',
                           'rse_usage_source': 'n/a'}
      
      msg_std, msg_err = self.doRucio( lirseusage )
      for i in msg_std:
        if i.find("[RSE does not exist") >= 0:
          logging.info("RSE %s does not exist", rse_remote)
          break
        elif i.find("used:") >= 0:
          rse_usage_summary['rse_usage_used'] = i.split(":")[1].replace(" ", "")
        elif i.find("rse:") >= 0:  
          rse_usage_summary['rse_usage_rse'] = i.split(":")[1].replace(" ", "")
        elif i.find("updated_at:") >= 0:
          rse_usage_summary['rse_usage_updatedat'] = "{d}_{h}{m}{s}".format(d=i.split(":")[1].replace(" ", ""),
                                                                            h=i.split(":")[2].replace(" ", ""),
                                                                            m=i.split(":")[3].replace(" ", ""),
                                                                            s=i.split(":")[4].replace(" ", "")
                                                                            )
        elif i.find("source:") >= 0:
          rse_usage_summary['rse_usage_source'] = i.split(":")[1].replace(" ", "")
    
      return rse_usage_summary
    
    def list_rules(self, location, rse_remote):
      
      lirule = self.RucioCommandLine( self.host,
                                      "list-rules",
                                      filelist = None,
                                      metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"],
                                                              location=location,
                                                              rse_remote=rse_remote)
      
      logging.debug( lirule )
      
      rule_summary = {}
      msg_std, msg_err = self.doRucio( lirule )
      #for i in msg_std:
        #print(i)
      for i in msg_std:
        if i.find( rse_remote ) >= 0 and i.find(config.get_config( self.remote_host )["rucio_account"]) >= 0:
          i0 = i.split(" ")
          i1 = [x for x in i0 if x]
          rule_summary['rule_id']  = i1[0]
          rule_summary['account']  = i1[1]
          rule_summary['location'] = i1[2]
          rule_summary['status']   = i1[3]
          rule_summary['rse']      = i1[4]
          rule_summary['copies']   = i1[5]
          if len(i1) >= 7:
            rule_summary['expires'] = "{date}_{time}".format(date=i1[6], time=i1[7])
          elif len(i1) < 7:
            rule_summary['expires'] = "valid"
          break
        else:
          rule_summary['rule_id']  = "n/a"
          rule_summary['account']  = "n/a"
          rule_summary['location'] = "n/a"
          rule_summary['status']   = "n/a"
          rule_summary['rse']      = rse_remote
          rule_summary['copies']   = "n/a"
          rule_summary['expires']  = "valid"
      
      return rule_summary
    
    def delete_rule(self, location, rse_remote):
      
      rule_summary = self.list_rules( location, rse_remote )
      
      
      delrule = self.RucioCommandLine( self.host,
                                      "delete-rule",
                                      filelist = None,
                                      metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"],
                                                              ruleid = rule_summary['rule_id'])
      
      logging.debug( delrule )

      msg_std, msg_err = self.doRucio( delrule )
      
      i_rule_id = None
      i_rule_status = None
      i_rse     = None
      i_path    = None  
        
    
    def set_rule(self, location, rse_remote, lifetime = "-1"):
      """ A general approach to define a rule """
      
      #1) Check for rules: scope:did at rse  
      i_rule_id = None
      i_rule_status = None
      i_rse     = None
      i_path    = None
      i_expired = None
      i_rule_account = None
      
      rule_summary = self.list_rules( location, rse_remote)
      i_rule_id = rule_summary['rule_id']
      i_rse     = rule_summary['rse']
      i_expired = rule_summary['expires']
      i_rule_account = config.get_config( self.remote_host )["rucio_account"]
      if rule_summary['status'].find("OK") >= 0 and i_expired == "valid":
        #return array of files and file properties  
        files, file_info = self.list_files( location.split(":")[0] , location.split(":")[1] )
        #get all file loations for a single rse:
        pathlists = self.get_file_locations( location.split(":")[0] , files )
        #create a super string out of it
        super_string = []
        for i_n, i_f in enumerate(files):
          j_path     = pathlists[ i_f ][rse_remote]["path"]
          j_checksum = pathlists[ i_f ][rse_remote]["checksum"]
          j_name     = pathlists[ i_f ][rse_remote]["name"]
          j_str = "{name}|{checksum}|{path}".format( name=j_name,
                                                     checksum=j_checksum,
                                                     path=j_path)
          super_string.append( j_str )
              
        i_path = super_string
        logging.info("Status of transfer %s to RSE %s: OK", location, rse_remote)
        i_rule_status = "OK"
        i_expired = "valid"
        
      elif rule_summary['status'].find("REPLICATING") >= 0 and i_expired == "valid":
        logging.info("Status of transfer %s to RSE %s: REPLICATING", location, rse_remote)
        i_path = "n/a"
        i_rule_status = "REPLICATING"
        
      elif rule_summary['status'].find("STUCK") >= 0 and i_expired == "valid":
        logging.info("Status of transfer %s to RSE %s: STUCK", location, rse_remote)
        i_path = "n/a"
        i_rule_status = "STUCK"
        
      elif i_expired != "valid":
        i_path = "n/a"
        logging.info("Status of transfer %s to RSE %s: OK/REPLICATING/STUCK but rule is expired.", location, rse_remote)
        i_rule_status = "Expired"
            
      elif i_rule_id == "n/a":
        logging.info("No ruleID definied - We should create one!")
        
        if lifetime == "-1":
          trrule = self.RucioCommandLine( self.host,
                                          "add-rule",
                                          filelist = None,
                                          metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"],
                                                                  location=location,
                                                                  rse_remote=rse_remote)
        elif lifetime != "-1":
          trrule = self.RucioCommandLine( self.host,
                                          "add-rule-lifetime",
                                          filelist = None,
                                          metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"],
                                                                  location=location,
                                                                  rse_remote=rse_remote,
                                                                  dataset_lifetime=lifetime)
        
        logging.debug( trrule )
        print("old summary: ", rule_summary, location, rse_remote)
        msg_std, msg_err = self.doRucio( trrule )

        rule_summary_new = self.list_rules( location, rse_remote)
        print("new summary: ", rule_summary, location, rse_remote )
      
        for i in msg_std:
          if i.find("ERROR [Data identifier not found.") >= 0:
            logging.info("Error: Data identifier %s does not exists!", location)
            i_rule_id = "n/a"
            i_rse     = rse_remote
            i_path    = "n/a"
            i_rule_status = "n/a"
            i_expired = "n/a"
            i_rule_account = config.get_config( self.remote_host )["rucio_account"]
            break
          elif i.find("Details: There is insufficient quota") >= 0:
            logging.info("Error: There is insufficient quota on %s to fullfill the operation!", rse_remote)
            i_rule_id = "n/a"
            i_rse     = rse_remote
            i_path    = "n/a"
            i_rule_status = "Failed"
            i_expired = "n/a"
            i_rule_account = config.get_config( self.remote_host )["rucio_account"]
            break
          elif i.find("ERROR [The creation of the replication rule failed at this time. Please try again later.") >= 0:
            logging.info("Error: The creation of the replication rule failed at this time. Please try again later.")
            i_rule_id = "n/a"
            i_rse     = rse_remote
            i_path    = "n/a"
            i_rule_status = "Failed"
            i_expired = "n/a"
            i_rule_account = config.get_config( self.remote_host )["rucio_account"]
            break
          elif i.find("ERROR [The creation of the replication rule failed at this time. Please try again later.") >= 0:
            logging.info("Error: The creation of the replication rule failed at this time. Please try again later.")
            i_rule_id = "n/a"
            i_rse     = rse_remote
            i_path    = "n/a"
            i_rule_status = "Failed"
            i_expired = "n/a"
            i_rule_account = config.get_config( self.remote_host )["rucio_account"]
            break
          elif i.find("ERROR [A duplicate rule for this account, did, rse_expression, copies already exists.") >= 0:
            logging.info("Rule already exists for location=%s and RSE=%s", location, rse_remote)
            rule_summary = self.list_rules( location, rse_remote)
            print( rule_summary )


      #Gather information about transfer rules from above:
      rucio_rule_summary = {}
      rucio_rule_summary['rule_account'] = i_rule_account
      rucio_rule_summary['rule_id'] = i_rule_id
      rucio_rule_summary['rule_rse'] = i_rse
      rucio_rule_summary['rule_path'] = i_path
      rucio_rule_summary['rule_status'] = i_rule_status
      rucio_rule_summary['rule_expired'] = i_expired
      
      return rucio_rule_summary
        
    def list_rule(self):
      pass
    
    
    def check_rucio(self):
      """Check if rucio installed at the host"""

      host_installed = self.RucioCommandLine( self.host,
                                       "check-rucio-installation",
                                       filelist = None,
                                       metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"])
      
      logging.debug( host_installed )

      msg_std, msg_err = self.doRucio( host_installed )

      version = 0
      for i in msg_std:
        if i.find("rucio: command not found") >= 0:
          version = False
        if i.find("rucio") == 0 and len(i.split(" ")[0]) == 5:
          version = i.split(" ")[1]

      return version
      
    def check_rucio_account(self):
      """Check if the rucio account exists"""
      
      check_account = self.RucioCommandLine( self.host,
                                       "list-accounts",
                                       filelist = None,
                                       metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"])
      
      logging.debug( check_account )
      msg_std, msg_err = self.doRucio( check_account )
      
      account_exists = False
      for i in msg_std:
        if i.find( config.get_config( self.remote_host )["rucio_account"] ) >= 0:
          account_exists = True
                  
      return account_exists
    
    def is_transferred_to_location(self, location, ttype):
      tags_all = self.run_doc
    
      status = False
      for ifile in tags_all["data"]:
        if ifile["host"].find(location) == 0 and ifile["status"].find("transferred") == 0 and ifile["type"].find(ttype) == 0:         
          status = True
          break
        else:
          status = False
    
      return status

    def get_software_version(self, location, ttype):
      tags_all = self.run_doc
    
      for ifile in tags_all["data"]:
        if ifile["host"].find(location) == 0 and ifile["status"].find("transferred") == 0 and ifile["type"] == "processed":
          return ifile["pax_version"]
        else:
          return "None"
    
    def query_transfer_tags(self, location, ttype):
      #Gather transfer information
      tags_all = self.run_doc
      transfer_tags = []

      for ifile in tags_all["data"]:
        if ifile["host"].find(location) == 0 and ifile["status"].find("transferred") == 0 and ifile["type"].find(ttype) == 0:
          transfer_tags.append( ifile )
          
      #add the date of creation
      tag_created_at            = tags_all["start"]
      transfer_tag_created_at   = tag_created_at.date().strftime("%Y%m%d")[2:]+"_"+tag_created_at.time().strftime("%H%M")
      transfer_tags[0].update( {"created_at": transfer_tag_created_at} )
      
      return transfer_tags
    
    def query_meta_tags(self, location, ttype):
      """Gather meta tag information"""
      
      #data base entry according the run
      tags_all = self.run_doc
      meta_tags = []
      #print( tags_all )
      #Gather basic meta data:
      meta_tag_shifter      = tags_all["user"]
      meta_tag_name         = tags_all["name"]              #just an option, not used in meta data set (see w)
      meta_tag_source_type  = tags_all["source"]["type"]
      meta_tag_runnumber    = tags_all["number"]
      meta_tag_sub_detector = tags_all["detector"]
      meta_tag_trigger_events_built = tags_all["trigger"]["events_built"]
      tag_created_at        = tags_all["start"]
      meta_tag_created_at   = tag_created_at.date().strftime("%Y%m%d")[2:]+"_"+tag_created_at.time().strftime("%H%M")
      
      #define the muon veto short term 'mv' (hardcoded)
      if meta_tag_sub_detector == "muon_veto":
        meta_tag_sub_detector = "mv"
      
      w = { "phys_group": meta_tag_shifter,
            "provenance": meta_tag_sub_detector,
            "datatype" : meta_tag_source_type,
            "campaign"  : ("SR"+config.RUCIO_CAMPAIGN),
            "run_number": meta_tag_runnumber
            }
      
      meta_tags.append(w)
      
      #Add location based meta data:
      if self.is_transferred_to_location(location, ttype) == True and ttype == "raw":
        meta_tags[0].update( {"prod_step": "raw"} )
      elif self.is_transferred_to_location(location, ttype) == True and ttype == "processed":
        meta_tags[0].update( {"prod_step": "processed"} )
      else:
        meta_tags[0].update( {"prod_step": "undefined"} )
      
      meta_tags[0].update( {"version" : self.get_software_version( location, ttype)} )

      return meta_tags

    def create_script(self, script):
        """Create script as temp file to be run on cluster"""
        fileobj = tempfile.NamedTemporaryFile(delete=False,
                                            suffix='.sh',
                                            mode='wt',
                                            buffering=1)
        fileobj.write(script)
        os.chmod(fileobj.name, 0o774)

        return fileobj

    def delete_script(self, fileobj):
        """Delete script after submitting to cluster

        :param script_path: path to the script to be removed

        """
        fileobj.close()
        
    def get_python_version(self):
      """Check for the right python version. Assure that rucio is executed under python <=2.7"""
      sc = "/tmp/{sc_name}.sh".format(sc_name="python_check")
      upload_string = """#.bashrc
export PATH=/home/SHARED/anaconda3/bin:$PATH
source activate rucio_client
python -V
      """
      self.create_script( sc, upload_string )
      
      ex = subprocess.Popen(['sh', sc] , 
                            shell=True,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE
                            )
      out, err = ex.communicate()
      std = out
      
      print("Python Version: ",  out, err )
      
      self.delete_script( sc )
    
    def ping_rucio(self):
      rucio_version = False  
      ping_rucio = self.RucioCommandLine(self.host,
                                               "ping-rucio",
                                               filelist = None,
                                               metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"])
      
      logging.debug( ping_rucio )
      
      msg_std, msg_err = self.doRucio( ping_rucio )
      for i in msg_std:
        if i.find("ERROR") >= 0:
          rucio_version = False
        else:
          rucio_version = True
      
      return rucio_version
    
    def sanity_checks(self):
      logging.info("Sanity check module")
      #Do some test before rucio upload:
      if self.ping_rucio() == False:
        logging.info("Sanity check: Unable to ping Rucio server")
        return False
      
      #0)Check if rucio is available on the host:
      elif self.check_rucio() == False:
        logging.info("Sanity check: Check for you Rucio installation at %s", config.get_hostname() )
        return False
            
      #1) Check if the specified rucio account exists  
      elif self.check_rucio_account() == False:
        logging.info("Sanity check: The specified account %s does not exists in the current rucio list", config.get_config( self.remote_host )["rucio_account"] )
        logging.info("Sanity check: Use \'rucio-admin account list\' manually!")
        return False

      #3)Check if the requested RSE from the configuration file is registered
      #  to the Rucio catalogue.
      elif self.get_rucio_rse() not in self.get_rse_list():
        logging.info("Sanity check: Attention: Check your json configuration file: RSE %s does not exists!", self.get_rucio_rse() )
        return False
    
      logging.info("Sanity check: Rucio is ok")
      return True
    
    
    def check_scope(self, scope_name ):
      """Check if a certain scope already excists"""
      #Return True if excists else False
      
      scope_excists = False  
      check_scope_name = self.RucioCommandLine(self.host,
                                               "check-scope",
                                               filelist = None,
                                               metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"])
      
      logging.debug( check_scope_name )
      
      msg_std, msg_err = self.doRucio( check_scope_name )
      for i in msg_std:
        if i.find(scope_name) >= 0 and len(i) == len(scope_name):
          scope_excists = True
      
      logging.debug("The scope name %s exists: %s (message within check_scope)", scope_name, scope_excists )
      
      return scope_excists

    def get_rucio_rse(self):
        """Returns hostnames that the current host can upload or download to.
        transfer_kind: 'upload' or 'download'
        transfer_method: is specified and not None, return only hosts with which
                        we can work using this method (e.g. scp)
        """
        try:
            if config.get_config( self.remote_host )["method"] == "rucio" and config.get_config( self.remote_host )["rucio_upload_rse"] is not None:
                return config.get_config( self.remote_host )["rucio_upload_rse"]
        except LookupError:
            logging.info("RSE is not definied.")
            return []

    def get_rse_list(self):
      """Ask for a list of registered Rucio Storage Elements (RSEs)"""
      
      rse_list = self.RucioCommandLine( self.host,
                                       "list-rses",
                                       filelist = None,
                                       metakey  = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"])
      
      logging.debug( rse_list )
      
      msg_std, msg_err = self.doRucio( rse_list )
      rses = []
      for i in msg_std:
        rses.append(i)

      return rses
    
    def get_checksum(self, rscope, ifile):
        cksum = None
        
        checksum_name = self.RucioCommandLine(self.host, 
                                            "get-checksum", 
                                            filelist = None,
                                            metakey = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"],
                                                                   scope=rscope,
                                                                   dataset = ifile)
        logging.debug( checksum_name)     
        msg_std, msg_err = self.doRucio( checksum_name )
        for i in msg_std:
          if i.find("adler32") >= 0:
            logging.debug("Rucio (get-checksum): %s", i)
            cksum = i.split(":")[1][1:]
        
        
        return cksum

    def get_file_locations(self, rscope, ifilelist):
        
        rse_list = self.get_rse_list()
        
        file_location = {}
        for i_filename in ifilelist:
          file_location_rse = {}
          ii_filename = i_filename.split("/")[-1]
          checksum_name = self.RucioCommandLine(self.host, 
                                            "get-file-replicas", 
                                            filelist = None,
                                            metakey = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"],
                                                                   scope=rscope,
                                                                   dataset = ii_filename)
          logging.debug( checksum_name)     
          msg_std, msg_err = self.doRucio( checksum_name )


          for i in msg_std:
            for irse in rse_list:
              if i.find(irse) >= 0 and i.find("|") == 0:
                ii = i.split("|")
                for j in ii:
                  if j.find( irse ) >= 0:
                    file_location_sub = {}
                    j_end = j.split(":", 1)
                    a = j_end[0].replace(" ", "")
                    b = j_end[1].replace(" ", "")

                    file_location_sub['scope']    = ii[1].replace(" ", "")
                    file_location_sub['name']     = ii[2].replace(" ", "")
                    file_location_sub['size']     = ii[3].replace(" ", "")
                    file_location_sub['checksum'] = ii[4].replace(" ", "")
                    file_location_sub['path']     = b
                    
                    file_location_rse[ a ] = file_location_sub
          
          file_location[ii_filename] = file_location_rse
          
        return file_location
        
        

    def get_file_location(self, rscope, ifile):
        file_location = {}
        rse_list = self.get_rse_list()
        
        checksum_name = self.RucioCommandLine(self.host, 
                                            "get-file-replicas", 
                                            filelist = None,
                                            metakey = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"],
                                                                   scope=rscope,
                                                                   dataset = ifile)
        logging.debug( checksum_name)     
        msg_std, msg_err = self.doRucio( checksum_name )

        for i in msg_std:
          for irse in rse_list:
            if i.find(irse) >= 0:
              ii = i.split("|")
              for j in ii:
                if j.find( irse ) >= 0:
                  j_end = j.split(":", 1)
                  a = j_end[0].replace(" ", "")
                  b = j_end[1].replace(" ", "")  
                  file_location[ a ] = b
        
        return file_location

    def list_files(self, rscope, ifile):
        file_list = {}          #A list of file names without scope
        file_list_name = []     #A dictionary of with file names (key) and further information (value)
        
        listfile_name = self.RucioCommandLine(self.host, 
                                            "list-files", 
                                            filelist = None,
                                            metakey = None).format(rucio_account=config.get_config( self.remote_host )["rucio_account"],
                                                                   scope=rscope,
                                                                   dataset = ifile)
        logging.debug( listfile_name)     
        msg_std, msg_err = self.doRucio( listfile_name )
        
        for idx, val in enumerate(msg_std):
          sub_dict = {}
          if val.find("+-----------------------------") >= 0:
            continue    #sort out first and last line of output
          if val.find("|-----------------------------") >= 0:
            continue    #sort out middle lines of the output  
          if val.find("SCOPE:NAME") >= 0:
            continue    #sort out the head line
          if val.find("|") == -1:
            continue    #sort out last two lines
            
          file_list_name.append(val.split("|")[1].replace(" ", "").split(":")[1])
          sub_dict['name']     = val.split("|")[1].replace(" ", "").split(":")[1]
          sub_dict['guid']     = val.split("|")[2].replace(" ", "")
          sub_dict['checksum'] = val.split("|")[3].replace(" ", "").split(":")[1]
          sub_dict['size']     = val.split("|")[4].replace(" ", "")
          sub_dict['events']   = val.split("|")[5].replace(" ", "")
          file_list[val.split("|")[1].replace(" ", "").split(":")[1]] = sub_dict
        
        
        return file_list_name, file_list
    
    def doRucio(self, upload_string ):
      #scname = "rucio_call_{runnumber}".format(runnumber= self.run_doc['name'] )
      #sc = "/tmp/{sc_name}.sh".format(sc_name=scname)
      sc = self.create_script( upload_string )    
      execute = subprocess.Popen( ['sh', sc.name] , 
                                  stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT,
                                  shell=False,
                                  universal_newlines=False)
      stdout_value, stderr_value = execute.communicate()
      stdout_value = stdout_value.decode("utf-8")
      stdout_value = stdout_value.split("\n")
      stdout_value = list(filter(None, stdout_value)) # fastest way to remove '' from list
      self.delete_script(sc)
      return stdout_value, stderr_value
    
    def copyRucio(self, datum_original, datum_destination, option_type):
      """Copy data via Rucio function
      """
      
      
      #Attention: Just temp definition:
      exp_phase   = "x1t"
      science_run = "SR"+config.RUCIO_CAMPAIGN
      
      data_type = datum_original['type']
      rrse   = config.get_config( datum_destination['host'] )["rucio_upload_rse"]
      raccount = config.get_config( datum_destination['host'] )["rucio_account"]

            
      #if data_type == "raw":
      logging.info("Start raw data upload to rucio catalogue")
        
      dataset_name, datapath, files = self.get_input_files(option_type, data_type)
      meta_tags     = self.query_meta_tags( self.host, data_type)
      transfer_tags = self.query_transfer_tags( self.host, data_type)
      nb_files      = len( files )                                                #number of files to transfer
        
      logging.info("Dataset name: %s", dataset_name)
      logging.info("Data path: %s", datapath)
      logging.info("Files: %s", files)
      
      #Sanity check for the number of uploaded files: If zero files gathered -> error and abort!
      if len( files ) == 0:
        logging.info("The data path %s/%s does not exists on %s or does not contain any data",
                     datapath, dataset_name, self.host )
        self.return_rucio['checksum'] = "n/a"
        self.return_rucio['location'] = "n/a"
        self.return_rucio['rse']      = []
        self.return_rucio['status'] = "error"
        return
      
      #Create the data structure for upload:
      #-------------------------------------
          
      #0) Create the containter, dataset and tarfile name for the upload
      #-----------------------------------------------------------------
      date = transfer_tags[0]["created_at"][0:6]
      time = transfer_tags[0]["created_at"][7:12]
      
      #Sort: data (processed or raw) vs. mc
      if data_type == "raw" or data_type == "processed":
        gtype = "data"
      elif data_type == "mc":
        gtype = "mc"
      else:
        logging.info("Attention: No data_type (raw/processed/mc) definied")
        logging.info("Upload failed!")
        self.return_rucio['checksum'] = "n/a"
        self.return_rucio['location'] = "n/a"
        self.return_rucio['rse']      = []
        self.return_rucio['status'] = "RSEreupload"
        return 0
      logging.info("Upload type: %s", gtype)
      
      #Container:
      container_name = "{exp_phase}_{sr}_{data}_{time}_{subdetector}".format(exp_phase=exp_phase,
                                                                             sr=science_run, 
                                                                             data=date,
                                                                             time=time,
                                                                             subdetector=meta_tags[0]["provenance"] )

      over_container_name = "{exp_phase}_{sr}_{type}".format(exp_phase=exp_phase,
                                                      sr=science_run,
                                                      type=gtype)
        
      #Scopes:
      #basic scope: (eg. "xe1t_SR000")
      rscope_basic = "{exp_phase}_{sr}".format(exp_phase=exp_phase,
                                               sr=science_run)    
      
      #data upload scope: (eg. xe1t_SR000_160824_0125_tpc)
      rscope_upload = "{exp_phase}_{sr}_{date}_{time}_{subdetector}".format(exp_phase=exp_phase,
                                                                            sr=science_run, 
                                                                            date=date,
                                                                            time=time,
                                                                            subdetector=meta_tags[0]["provenance"] )
      
      #Dataset names:
      basic_dataset_raw = "raw"
      basic_dataset_proc = "processed"
      
      if data_type == "raw":
        dataset_name = basic_dataset_raw
      elif data_type == "processed":
        dataset_name = "{basic}_{paxV}".format(basic=basic_dataset_proc,
                                               paxV="pax_vX-X-X")
        
      fileRaw_name = dataset_name
      
      logging.info("Rucio - Scope (basic): %s", rscope_basic)
      logging.info("Rucio - Scope (upload): %s", rscope_upload)
      logging.info("Rucio - Science Run Container: %s", over_container_name)
      logging.info("Rucio - Container name: %s", container_name)
      logging.info("Rucio - Dataset name (depend on raw/processed upload type): %s", dataset_name)
      
      #0) Create the container and datasets:
      #-----------------------------------------------------------------

      #Create the scope: rscope_basic
      if self.check_scope( rscope_basic ) == False:
        add_scope = self.RucioCommandLine(self.host,
                                          "add-scope",
                                          filelist = None,
                                          metakey = None).format(rucio_account=raccount,
                                                                 scope=rscope_basic)
        logging.info( add_scope )
        msg_std, msg_err = self.doRucio( add_scope )
        for i in msg_std:
          logging.info("Rucio (add-scope - basic): %s", i)  
      else:
        logging.info("Scope %s already created", rscope_basic)
      
      #Create the scope: rscope_upload
      if self.check_scope( rscope_upload ) == False:
        add_scope = self.RucioCommandLine(self.host,
                                          "add-scope",
                                          filelist = None,
                                          metakey = None).format(rucio_account=raccount,
                                                                 scope=rscope_upload)
        logging.info( add_scope )
        msg_std, msg_err = self.doRucio( add_scope )
        for i in msg_std:
          logging.info("Rucio (add-scope - upload): %s", i)  
      else:
        logging.info("Scope %s already created", rscope_upload)

      #Create container: over_container_name in rscope_basic
      cmd_container_name = self.RucioCommandLine(self.host,
                                                   "add-container",
                                                   filelist=None,
                                                   metakey=None).format(rucio_account=raccount,
                                                                        scope=rscope_basic,
                                                                        container_name=over_container_name)
      logging.info( cmd_container_name )
      msg_std, msg_err = self.doRucio( cmd_container_name )
      for i in msg_std:
        logging.info("Rucio (add-container into scope %s): %s", rscope_basic, i)
      
      #Create container: container_name into rscope_basic
      cmd_container_name = self.RucioCommandLine(self.host,
                                                 "add-container",
                                                 filelist=None,
                                                 metakey=None).format(rucio_account=raccount,
                                                                      scope=rscope_basic,
                                                                      container_name=container_name)
      logging.info( cmd_container_name )
      msg_std, msg_err = self.doRucio( cmd_container_name )
      for i in msg_std:
        logging.info("Rucio (add-container into scope %s): %s", rscope_basic, i)
      
      #Create dataset into rscope_upload
      cmd_dataset_name = self.RucioCommandLine(self.host,
                                               "add-dataset",
                                               filelist = None,
                                               metakey = None).format(rucio_account=raccount,
                                                                      scope=rscope_upload,
                                                                      dataset=dataset_name)
      logging.info( cmd_dataset_name )
      msg_std, msg_err = self.doRucio( cmd_dataset_name )
      for i in msg_std:
        logging.info("Rucio (add-dataset into %s): %s", rscope_upload, i)
            
      #1) Upload the raw/processed data and set the meta tags:
      #-----------------------------------------------------------------    
      print("test")
      if data_type == "raw":
        upload_file_s = files
        print("testraw")
      elif data_type == "processed":
        upload_file_s = [files]
        print("testpr")
      else:
        logging.info("No files for upload are specified")
        return 0
      print("testafter")
      #1.1) Upload      
      upload_name = self.RucioCommandLine(self.host, 
                                          "upload-advanced", 
                                          filelist = upload_file_s,
                                          metakey = None).format(rucio_account=raccount,
                                                                 scope=rscope_upload,
                                                                 dataset=dataset_name,
                                                                 rse=rrse)
      logging.debug( upload_name )
      print(upload_name)
      msg_std, msg_err = self.doRucio( upload_name )
      for i in msg_std:
        logging.info("Rucio (upload-advanced): %s", i)
        
      for i in msg_std:
        if i.find("ERROR [The requested service is not available at the moment.") >= 0:
          logging.info("ERROR: Rucio service is not available")
          self.return_rucio['checksum'] = "n/a"
          self.return_rucio['location'] = "n/a"
          self.return_rucio['rse']      = []
          self.return_rucio['status'] = "RSEreupload"
          return
        elif i.find("ERROR [There is not enough quota left to fulfil the operation.") >= 0:
          logging.info("ERROR: Not enough quota left")
          self.return_rucio['checksum'] = "n/a"
          self.return_rucio['location'] = "n/a"
          self.return_rucio['rse']      = []
          self.return_rucio['status'] = "RSEquota"
          return
        elif i.find("ERROR [('Connection aborted.', BadStatusLine('',))]") >= 0:
          logging.info("ERROR: Connection aborted")
          self.return_rucio['checksum'] = "n/a"
          self.return_rucio['location'] = "n/a"
          self.return_rucio['rse']      = []
          self.return_rucio['status'] = "RSEreupload"
          return
        elif i.find("Details: Missing dependency : gfal2]") >= 0:
          logging.info("ERROR: Missing gfal2 dependency")
          self.return_rucio['checksum'] = "n/a"
          self.return_rucio['location'] = "n/a"
          self.return_rucio['rse']      = []
          self.return_rucio['status'] = "RSEreupload"
          return        
        elif i.find("SSL routines:SSL3_READ_BYTES:sslv3 alert certificate expired]") >= 0:
          logging.info("ERROR: Certificate expired")
          self.return_rucio['checksum'] = "n/a"
          self.return_rucio['location'] = "n/a"
          self.return_rucio['rse']      = []
          self.return_rucio['status'] = "RSEreupload"
          return  

        #Not yet sure if necessary:
        #elif i.find("ERROR [The file already exists.") >=0:
          #self.return_rucio['checksum'] = "n/a"
          #self.return_rucio['location'] = "n/a"
          #self.return_rucio['rse']      = []
          #self.return_rucio['status'] = False
          #return

      #1.2) Set meta tags
      for ifile in files:
        iifile = ifile.split("/")[-1]
        set_metadata_string = self.RucioCommandLine(self.host,
                                                    "set-metadata", 
                                                    filelist = None, 
                                                    metakey  = meta_tags).format(rucio_account=raccount,
                                                                                 scope=rscope_upload,
                                                                                 dataset=iifile)
        logging.debug( set_metadata_string )
        metadata_msg, metadata_err = self.doRucio( set_metadata_string )
        for i in metadata_msg:
          logging.info("Rucio (set-metadata): %s to file %s", i, ifile)

      #2) Attach the files to the data set:
      #-----------------------------------------------------------------
      attach_name = self.RucioCommandLine(self.host,
                                          "attach",
                                          filelist = files,
                                          metakey  = None).format(rucio_account=raccount,
                                                                  up_scope=rscope_upload,
                                                                  up_did=dataset_name,
                                                                  scope=rscope_upload
                                                                  )
      logging.debug(attach_name)
      msg_std, msg_err = self.doRucio( attach_name )
      for i in msg_std:
        logging.info("Rucio (attach): %s", i)
        

      #3) Set Meta tags to Data set
      #-----------------------------------------------------------------
      set_metadata_string = self.RucioCommandLine(self.host,
                                                  "set-metadata", 
                                                  filelist = None, 
                                                  metakey  = meta_tags).format(rucio_account=raccount,
                                                                               scope=rscope_upload,
                                                                               dataset=dataset_name)
      logging.debug( set_metadata_string )
      metadata_msg, metadata_err = self.doRucio( set_metadata_string )
      for i in metadata_msg:
        logging.info("Rucio (set-metadata): %s", i)
        
      #4) Attach the data set to containter
      #-----------------------------------------------------------------
      attach_name = self.RucioCommandLine(self.host,
                                          "attach-to-container", 
                                          filelist = None,
                                          metakey = None).format(rucio_account=raccount,
                                                                 scope_container=rscope_basic,
                                                                 container=container_name,
                                                                 up_scope=rscope_upload,
                                                                 up_did=dataset_name)
                                                                                        
      logging.debug(attach_name)
      msg_std, msg_err = self.doRucio( attach_name )
      for i in msg_std:
        logging.info("Rucio (attach-to-container): %s", i)
      
      #5) Attach the container to container
      #-----------------------------------------------------------------
      attach_name = self.RucioCommandLine(self.host,
                                          "attach-to-container", 
                                          filelist = None,
                                          metakey = None).format(rucio_account=raccount,
                                                                 scope_container=rscope_basic,
                                                                 container=over_container_name,
                                                                 up_scope=rscope_basic,
                                                                 up_did=container_name)
                                                                                        
      logging.debug(attach_name)
      msg_std, msg_err = self.doRucio( attach_name )
      for i in msg_std:
        logging.info("Rucio (attach-to-container): %s", i)   
      
      #6) Set Meta tags to container
      #-----------------------------------------------------------------
      set_metadata_string = self.RucioCommandLine(self.host,
                                                  "set-metadata", 
                                                  filelist = None, 
                                                  metakey  = meta_tags).format(rucio_account=raccount,
                                                                               scope=rscope_basic,
                                                                               dataset=container_name)
      logging.debug( set_metadata_string )
      metadata_msg, metadata_err = self.doRucio( set_metadata_string )
      for i in metadata_msg:
        logging.info("Rucio (set-metadata): %s", i)
      
      #7) Clean up /tmp/ and prepare to notify the data base:
      #-----------------------------------------------------------------
      file_locations = self.get_file_locations(rscope_upload, files)  
      entrance_rse = config.get_config( datum_destination['host'] )["rucio_upload_rse"]
      #print(file_locations)
      cnt_cksum = 0
      cksum_string = ''
      for i_file in files:
        for key_filename, value in file_locations.items():
          if i_file.find( key_filename ) >= 0  and entrance_rse in value:
            local_cksum = config.get_adler32( i_file )
            local_file  = i_file.split("/")[-1]
            rucio_cksum = value[entrance_rse]['checksum']
            rucio_file  = value[entrance_rse]['name']
            logging.info("checksum test: %s -> %s (local) and %s (rucio)", local_file, local_cksum, rucio_cksum)  
            
            #Compare by file name comparison:
            if local_file == rucio_file:
              logging.info("File names agree")
              cksum_string+=":"+rucio_cksum
              cnt_cksum+=1
            #Compare by checksums NEED A FIX  
            #if local_cksum == rucio_cksum:
              #logging.info("checksum: Agree")
              #cksum_string+=":"+rucio_cksum
              #cnt_cksum+=1
              
              
      if cnt_cksum == len( files ):
        #Update destination status: Here we go!
        self.return_rucio['checksum'] = cksum_string
        self.return_rucio['location'] = "{scope}:{filename}".format(scope=rscope_upload,
                                                                  filename=dataset_name)
        self.return_rucio['rse']      = [rrse]
        self.return_rucio['status'] = "transferred"
        logging.info("Upload status: transferred")
        return
      elif cnt_cksum < len( files ) or cnt_cksum > len( files ):
        self.return_rucio['checksum'] = "n/a"
        self.return_rucio['location'] = "{scope}:{filename}".format(scope=rscope_upload,
                                                                  filename=dataset_name)
        self.return_rucio['rse']      = []
        self.return_rucio['status'] = "RSEreupload"
        logging.info("Upload status: error - Files or checksums do not agree!")
        return
      
      #final return!
      return

      
    def each_run(self):
      
      #-2) Setup host and remote host:
      self.set_host( config.get_hostname() )
      self.set_remote_host()
      
      #-1) Only go on if method is rucio:
      if config.get_config( self.remote_host )["method"] != "rucio":
              
        #Do some test before rucio upload:
        #0)Check if rucio is available on the host:
        if self.check_rucio() == False:
            logging.info("Check for you Rucio installation at %s", self.host )
            return 
        
        if self.ping_rucio() == False:
            logging.info("Unable to ping Rucio server")
            return

        #1) Check if the specified rucio account exists  
        if self.check_rucio_account() == False:
            logging.info("The specified account %s does not exists in the current rucio list", config.get_config( self.remote_host )["rucio_account"] )
            logging.info("Use \'rucio-admin account list\' manually!")
            return 
        
        #3)Check if the requested RSE from the configuration file is registered
        #  to the Rucio catalogue.
        if self.get_rucio_rse() not in self.get_rse_list():
            logging.info("Attention: Check your json configuration file: RSE %s does not exists!", self.get_rucio_rse() )
            return 


        for data_type in config.get_config( config.get_hostname() )['data_type']:
            logging.debug("%s" % data_type)
            self.do_possible_transfers(option_type=self.option_type, data_type=data_type)
      
      else:
        logging.info("Nothing to do for rucio uploader")

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
        #if data_type == "raw":
        if data_type == "processed":    
          self.copyRucio( option_type, data_type )
        
    
    def RucioCommandLine(self, host, method, filelist=None, metakey=None ):
      """Define a general command line interface
         for Rucio calls
      """
      
      
      general_string_xe1tdata = """#!/bin/bash
export PATH=/home/xe1ttransfer/.local/bin:$PATH
export RUCIO_HOME=~/.local/rucio
export RUCIO_ACCOUNT={rucio_account}
      """
      
      general_string_tegner = """#!/bin/bash

#export PATH="/cfs/klemming/nobackup/b/bobau/ToolBox/TestEnv/Anaconda3/bin:$PATH"
source deactivate
source activate rucio_p2
export PATH=~/.local/bin:$PATH
cd /cfs/klemming/nobackup/b/bobau/ToolBox/gfal-tools
source /cfs/klemming/nobackup/b/bobau/ToolBox/gfal-tools/setup.sh
cd
export RUCIO_HOME=~/.local/rucio
export RUCIO_ACCOUNT={rucio_account}
echo "Rucio load"
      """

      general_string_stash = """#!/bin/bash
module load python/2.7
source /cvmfs/xenon.opensciencegrid.org/software/rucio-py27/setup_rucio_1_8_3.sh
export RUCIO_HOME=/cvmfs/xenon.opensciencegrid.org/software/rucio-py27/1.8.3/
export RUCIO_ACCOUNT={rucio_account}
      """

      general_string_midway = """#!/bin/bash
#set the environment path directly
export PATH=/project/lgrandi/anaconda3/envs/rucio_basic/bin:$PATH

#Set up the PATH and Rucio account information
export PATH=~/.local/bin:$PATH  
export RUCIO_HOME=~/.local/rucio
export RUCIO_ACCOUNT={rucio_account}

#Source gfal commands:
source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh
      """
      
      general = {
          "xe1t-datamanager": general_string_xe1tdata,
          "tegner-login-1": general_string_tegner,
          "midway-login1": general_string_midway,
          "login": general_string_stash
                 }
      
      upload_simple = """
cd {path_to_data}
rucio upload {dataset} --rse {rse}
      """
      
      upload_adv = """
\n
      """
      if filelist is not None and method == "upload-advanced":
        for ifile in filelist:
          upload_adv+="rucio upload --rse {rse} --scope {scope} " + str(ifile) + " \n"
      
      
      get_checksum = """
rucio get-metadata {scope}:{dataset}
"""
      
      get_metadata="""
rucio get-metadata {scope}:{dataset}      
      """
      
      set_metadata="""
      \n
      """
      if metakey is not None and method == "set-metadata":
        metakey_list = metakey[0].items()
        for i in metakey_list:
            set_metadata += "rucio set-metadata --did {scope}:{dataset} --key " + str(i[0]) + " --value " + str(i[1]) + "\n"

      add_container="""
rucio add-container {scope}:{container_name}      
      """
      
      add_dataset="""
rucio add-dataset {scope}:{dataset}      
      """
      
      attach="""
      """
      if filelist is not None and metakey == None:
        for ifile in filelist:
          attach+="rucio attach {up_scope}:{up_did} {scope}:" + ifile.split("/")[-1] + "\n"
      
      
      attach_to_container="""
      """
      if filelist is None and metakey == None:
          attach_to_container+="rucio attach {scope_container}:{container} {up_scope}:{up_did} \n"
      
      add_scope ="""
rucio-admin scope add --account {rucio_account} --scope {scope}
      """
      
      check_for_scope = """
rucio list-scopes
      """
      
      list_rses = """
rucio list-rses
      """
      
      check_rucio_installation= """
rucio --version
      """
      
      list_accounts ="""
rucio-admin account list
      """
      get_file_replicas = """
rucio list-file-replicas {scope}:{dataset}
      """
      
      add_rule = """
rucio add-rule {location} 1 {rse_remote}
      """

      add_rule_lifetime = """
rucio add-rule --lifetime {dataset_lifetime} {location} 1 {rse_remote}
      """
      
      
      list_rules = """
rucio list-rules {location}
      """
      ping_rucio = """
rucio ping
      """
      
      delete_rule = """
rucio delete-rule --acccount {account} {ruleid}
      """
      
      list_rse_usage = """
rucio list-rse-usage {rse_remote}
      """
    
      list_files = """
rucio list-files {scope}:{dataset}      
      """
      
      if method == "upload-simple":
          return general[host] + upload_simple
      elif method == "get-metadata":
          return general[host] + get_metadata
      elif method == "set-metadata":
          return general[host] + set_metadata
      elif method == "add-container":
          return general[host] + add_container
      elif method == "add-dataset":
          return general[host] + add_dataset
      elif method == "upload-advanced":
          return general[host] + upload_adv
      elif method == "attach":
          return general[host] + attach
      elif method == "attach-to-container":
          return general[host] + attach_to_container
      elif method == "add-scope":
          return general[host] + add_scope
      elif method == "check-scope":
          return general[host] + check_for_scope
      elif method == "get-checksum":
          return general[host] + get_checksum
      elif method == "list-rses":
          return general[host] + list_rses
      elif method == "check-rucio-installation":
          return general[host] + check_rucio_installation
      elif method == "list-accounts":
          return general[host] + list_accounts
      elif method == "get-file-replicas":
          return general[host] + get_file_replicas
      elif method == "list-files":
          return general[host] + list_files
      elif method == "add-rule":
          return general[host] + add_rule
      elif method == "add-rule-lifetime":
          return general[host] + add_rule_lifetime
      elif method == "list-rules":
          return general[host] + list_rules
      elif method == "ping-rucio":
          return general[host] + ping_rucio
      elif method == "delete-rule":
          return general[host] + delete_rule
      elif method == "list-rse-usage":
          return general[host] + list_rse_usage
      else:
          return 0
        
    def get_files(self, args):
        '''List files in a directory'''  
        files = []
        
        if args.count(':') == 1:
          logging.warning("{0} cannot be distinguished from scope:datasetname. Skipping it.".format(args))
        
        if os.path.isdir(args):
          dname, subdirs, fnames = os.walk(args).__next__()
          # Check if there are files in the directory
          if fnames:
            for fname in fnames:
              files.append(os.path.join(dname, fname))
                # No files, but subdirectories. Needed to be added one-by-one
                # Maybe change so we look through the subdirs and add those files?
          elif subdirs:
            raise Exception("Directory ({directory}) has no files in it. Please add subdirectories individually.".format(directory = dname))
          else:
            raise Exception("Directory ({directory}) is empty.".format(directory = dname))
        
        elif os.path.isfile(args):
          files.append(args)
        else:
          logging.warning('{0} is not a directory or file or does not exist'.format(args))
          return []
        return files

    def get_dataset(self, args):
        '''Parse helper for upload'''
        dsscope = None
        dsname = None

        for item in args:
            if item.count(':') == 1:
                if dsscope:
                    raise Exception("Only one dataset should be given")
                else:
                    dsscope, dsname = item.split(':')
        return dsscope, dsname
    
    def get_input_files(self, option_type, data_type):
      
      dataset_name = ""
      datapath     = ""
      files        = ""
      
      if config.RUCIO_UPLOAD is not None:
        is_file      = os.path.isfile( config.RUCIO_UPLOAD )
        is_folder    = os.path.isdir( config.RUCIO_UPLOAD )
        f_exists     = os.path.exists( config.RUCIO_UPLOAD )
        
        pfolder = config.RUCIO_UPLOAD
        
          #If you try to upload a file manually
        if is_file == True and is_folder == False and f_exists == True:
            logging.info("Upload of a file")
            pfolder = os.path.abspath(pfolder)
            
            dataset_name = pfolder.split("/")[-1]   #get dataset name for dataset creation
            datapath = pfolder[0:len(pfolder) - len(dataset_name) ]
            logging.info("Single data set: %s", dataset_name)
            files = [pfolder]
        
          #If you try to upload a folder manually
        elif is_file == False and is_folder == True and f_exists == True:
            logging.info("Upload of a folder")
            
            pfolder = os.path.abspath(pfolder)
            if pfolder[-1] == "/":
              pfolder = pfolder[:-1]
            
            dataset_name = pfolder.split("/")[-1]   #get dataset name for dataset creation
            datapath = pfolder[0:len(pfolder) - len(dataset_name) - 1 ]
            
            files = self.get_files( datapath + "/" + dataset_name )
            logging.info("Raw data set: %s", pfolder)
            for ifile in files:
              logging.info("Contained files: %s", ifile)
        
        elif f_exists == False:
          logging.info("No file")
          exit()
      
      elif config.RUCIO_UPLOAD is None:
        meta_tags     = self.query_meta_tags(self.host, data_type)
        #print( "meta tags: ", meta_tags )
        transfer_tags = self.query_transfer_tags(self.host, data_type)
        #print( "transfer tags: ", transfer_tags )
      
        logging.info("Meta tag selection: %s", meta_tags)
        logging.info("Transfer information %s (not used at the moment)", transfer_tags)  
        
        pfolder = os.path.abspath(transfer_tags[0]['location'])
        dataset_name = pfolder.split("/")[-1]   #get dataset name for dataset creation
        datapath = pfolder[0:len(pfolder) - len(dataset_name) ]
        
        #print("t: ", transfer_tags[0]['location'] )
        files = self.get_files( datapath + "/" + dataset_name )
        logging.info("Raw data set: %s", pfolder)
        for ifile in files:
          logging.info("Contained files: %s", ifile)
          
      return dataset_name, datapath, files



class RucioPush(RucioBase):
    """Copy data to there

    If the data is transfered to current host and does not exist at any other
    site (including transferring), then copy data there."""
    option_type = 'upload'
    

class RucioPull(RucioBase):
    """Copy data to here

    If data exists at a reachable host but not here, pull it.
    """
    option_type = 'download'
    
class RucioRule(Task):
    
    def __init__(self):
      """Init the RucioRule class to set 
         transfer rules or deletions independent of
         the upload or download
      """
      Task.__init__(self)
      
    def set_db_entry_manually(self, db):
      """This memberfunction allows a side load
         with external data base entry
      """
      self.run_doc = db
    
    def rule_definition(self):
      """Load the transfer rule definitions"""
      
      logging.info("Define the transfer rules")
      
      if config.RUCIO_RULE == None:
        return 0
      
      t = json.loads(open(config.RUCIO_RULE, 'r').read())
      
      #get run numbers
      run_nb = t[0]['run_nb'].split(",")
      runNB = []
      for i in run_nb:
        i = i.replace(" ", "")
        if i.find("-") >= 0:
          a = i.split("-")[0]
          b = i.split("-")[1]
          for j in range( int(a), int(b) ):
            runNB.append( j )
                  
        else:
          runNB.append( i )
      
      run_name = t[0]['run_name'].split(",")
      runNameList = []
      runNameRange = []
      for i in run_name:
        i = i.replace(" ", "")
        if i.find("-") >= 0:
          a = i.split("-")[0]
          b = i.split("-")[1]
          tmp_runname_list = [a, b]
          runNameRange.append( tmp_runname_list )
        else:
          runNameList.append( i )
        
      detector_type         = t[0]['detector_type']
      source_type           = t[0]['source_type']
      destination_rse       = t[0]['destination_rse']
      destination_livetime  = t[0]['destination_livetime']
      destination_condition = t[0]['destination_condition']
      remove_rse            = t[0]['remove_rse']

      dest_info = {
        'run_number': runNB,          
        'run_name_list': runNameList,
        'run_name_range': runNameRange,
        'destination_rse': destination_rse,
        'destination_livetime': destination_livetime,
        'destination_condition': destination_condition,
        'remove_rse': remove_rse
        }
      
      return dest_info
         
    def each_run(self):
      """Tell what to do for raw and processed data"""
      
      #load the transfer rules definitions for each data set:
      self.rule_definition()

      for data_type in config.get_config( config.get_hostname() )['data_type']:
         
         logging.info("Set rules for data type: %s" % data_type)
         self.set_possible_rules( data_type=data_type,
                                  dbinfo=None)
         
         self.del_possible_rules( data_type=data_type,
                                  dbinfo=None)
         
    def set_possible_rules(self, data_type, dbinfo ):
      '''Set Possible rules according a set of mandatory pre definitions'''
      logging.info("Set rules for data transfers")
      
      if dbinfo == None:
        there = self.get_rundb_entry( data_type )  
        logging.info("Side load DISABLED: set_possible_rules()")
        logging.info("Means: No previous do_possible_transfers() executed")
      
      #A side load of 
      if dbinfo != None:
        logging.info("Side load ENABLED")
        logging.info("Means: There was a previous upload by do_possible_transfers() executed")
        there = dbinfo
      
      # Get list of possible transfer destinations (RSE)           
      transfer_list = config.get_config("rucio-catalogue")['rucio_transfer']
      transfer_list.append( config.get_config("rucio-catalogue")['rucio_upload_rse'] ) 
      method = config.get_config("rucio-catalogue")['method']
      
      # Modify this list by rule_definition information:
      # NEED SOME MORE CODE # self.the_mighty_rule
      print("possible transfers: ", transfer_list)
      print("method: ", method)
      print("rucio-entry: ", there)
      print("run number: ", self.run_doc['number']) 
      print("run name: ", self.run_doc['name']) 
      print("run source: ", self.run_doc['source']['type'] ) 
      print("run detector: ", self.run_doc['detector']) 

      actual_run_number   = self.run_doc['number']
      actual_run_name     = self.run_doc['name']
      actual_run_source   = self.run_doc['source']['type']
      actual_run_detector = self.run_doc['detector']
      
      print( self.rule_definition() )
      
      
      
      ##test run numbers
      #if actual_run_number in self.rule_definition()['run_number'] and actual_run_number != 0:
        ##logging.info("No rules need to be changed for run number %s", actual_run_number)
        ##return
        #if self.rule_definition()['source'] != None:
          #if actual_run_source == self.rule_definition()['source']:
            #logging.info("Source specified and match")
            #tt_list = self.rule_definition()['destination_rse']
            #tt_life = self.rule_definition()['destination_livetime']
          
        #elif self.rule_definition()['detector'] != None:
          #if actual_run_detector == self.rule_definition()['detector']:
            #logging.info("Detector specified and match")  
            #tt_list = self.rule_definition()['destination_rse']
            #tt_life = self.rule_definition()['destination_livetime']
        
        #else:
          #logging.info("Deal with only run numbers")
          #tt_list = self.rule_definition()['destination_rse']
          #tt_life = self.rule_definition()['destination_livetime']
        
      #elif actual_run_number == 0:
        ##fix  
        #if actual_run_name in self.rule_definition()['run_name_list']:
          #tt_list = self.rule_definition()['destination_rse']
          #tt_life = self.rule_definition()['destination_livetime']
        
        #for i_l in self.rule_definition()['run_name_range']:
          #beg = i_l[0]
          #end = i_l[1]
      
      #elif self.rule_definition()['run_number'] == None:
        #logging.info("no run number")
      

      if there != None:
        self.rucio = RucioBase(self.run_doc)
        self.rucio.set_host( config.get_hostname() )
        self.rucio.set_remote_host( there['host'] )
        if self.rucio.sanity_checks() == False:
          return 0
        
        if there['status'] != "transferred":
          logging.info("RunDB status: %s - No need to create a transfer rule", there['status'])  
          return 0
        
        rucio_location = there['location']
              
        new_rses = []
        new_rses_path = []
        for i_rse in transfer_list:
          logging.info("Gather rule information for RSE %s", i_rse)
          print("Rucio location from runDB: ", rucio_location)
          
          #initiate the the transfer and add the rse + path to the runDB
          rule_result = self.rucio.set_rule( rucio_location, i_rse, "-1" )
          logging.info(" * Status rule/transfer: %s", rule_result['rule_status'])
          logging.info(" * RSE: %s", rule_result['rule_rse'])
          logging.info(" * RuleID: %s", rule_result['rule_id'])
          logging.info(" * Path: %s", rule_result['rule_path'])
          logging.info(" * Rule expires: %s", rule_result['rule_expired'] )
          logging.info(" * Rule account: %s", rule_result['rule_account'] )
                
          if rule_result['rule_status'] == "OK" and rule_result['rule_expired'] == "valid":
            new_rses.append( i_rse )
            new_rses_path.append(rule_result['rule_path'])
              
        if method == "rucio" and there['rse'] != new_rses:
          #Notify the runDB if there has been a change in the number of registered RSE
          if config.DATABASE_LOG:           
            #Delete old entry
            print("Old entry: ", there)
            self.collection.update({'_id': self.run_doc['_id']},
                                       {'$pull': {'data': there}})
                  
          #Add the modified one:
          there['rse'] = new_rses
                
          if config.DATABASE_LOG:  
            print("New entry: ", there)
            self.collection.update({'_id': self.run_doc['_id'],},
                                             {'$push': {'data': there}})
                
          logging.info("Updated database entry:")
          #for key, value in there.items():
            #logging.info( "DB: %s - %s", str(key), str(value) )   
        
        elif method == "rucio" and there['rse'] == new_rses:
          #Do not notify the runDB in case there is no change at the RSE information
          logging.info("No database update necessary for type %s", data_type)
      
      elif there == None:
        logging.info("There is no runDB information for rucio-catalogue available: SKIP")
      
    def del_possible_rules(self, data_type, dbinfo ):
      '''Delete possible rules according a set of mandatory pre definitions'''
      logging.info("Delete rules for data transfers.")
      
      if dbinfo == None:
        there = self.get_rundb_entry( data_type )  
        logging.info("Side load DISABLED: del_possible_rules()")
        logging.info("Means: No previous do_possible_transfers() executed")
      
      #A side load of 
      if dbinfo != None:
        logging.info("Side load ENABLED")
        logging.info("Means: There was a previous upload by do_possible_transfers() executed")
        there = dbinfo
      
      # Get list of possible transfer destinations (RSE)           
      transfer_list = config.get_config("rucio-catalogue")['rucio_transfer']
      transfer_list.append( config.get_config("rucio-catalogue")['rucio_upload_rse'] ) 
      method = config.get_config("rucio-catalogue")['method'] 
      print("sc: ", transfer_list )
      print("th: ", there)
      logging.info("Nothing yet done here to delete rules and files !<code>")
      # WRITE SOME CODE TO DELTE TRANFER RULES AND FILES
      # according to self.the_mighty_rule
      
      #Finish this once the RSE locations are fixed.
      #if there != None:
        #self.rucio = RucioBase(self.run_doc)
        #self.rucio.set_host( config.get_hostname() )
        #self.rucio.set_remote_host( there['host'] )
        #if self.rucio.sanity_checks() == False:
          #return 0
        
        #if there['status'] != "transferred":
          #logging.info("RunDB status: %s - No need to create a transfer rule", there['status'])  
          #return 0  
      
    def get_rundb_entry(self, data_type):
      """Get a specified runDB entry"""
      db_entry = None
      for i_data in self.run_doc['data']:
          
        if i_data['host'] != "rucio-catalogue":
          continue
        if i_data['type'] != data_type:
          continue
        
        db_entry = i_data
       
      return db_entry
         