#!/usr/bin/env python
from __future__ import print_function
import os
from cax.api import api
from cax import config
import json

"""
This class contains functions for writing inner and outer dag files. It takes a list of runs (integers) and pax_version as input.
"""

class dag_writer():

    def __init__(self, runlist, pax_version, logdir, reprocessing=False):
        self.runlist = runlist
        self.pax_version = pax_version
        self.logdir = logdir
        self.reprocessing = reprocessing
        self.default_uri = "gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm"
        self.host = "login"
        self.submitfile = "/home/ershockley/cax/osg_scripts/processing_template.submit"

    def get_run_doc(self, run_number):
        query = {"detector" : "tpc",
                 "number"   : run_number
                 }
        API = api()
        doc = API.get_next_run(query)
        return doc
    
    def write_json_file(self, doc):
        # take a run doc, write to json file
        name = doc["name"]
        
        json_file = "/xenon/ershockley/jsons/" + name + ".json"

        with open(json_file, "w") as f:
            json.dump(doc, f)
            
        return json_file
        
        
    def write_outer_dag(self, outer_dag):
        """
        Writes outer dag file that contains subdag for each run in runlist.
        """
        with open(outer_dag, "wt") as outer_dag_file:
            # loop over runs in list, write an outer dag that contains subdags for each rubn
            for run in self.runlist:
                # get run doc, various parameters needed to write dag
                doc = self.get_run_doc(run)
                rawdir = self.get_raw_dir(doc)
                run_name = doc["name"]

                # write inner dag and json file for this run 
                inner_dagname = "xe1t_" + str(run)
                inner_dagdir = outer_dag.replace(outer_dag.split("/")[-1], "inner_dags")
                inner_dagfile = inner_dagdir + "/" + str(run) + ".dag" 
                if not os.path.exists(inner_dagdir):
                    os.makedirs(inner_dagdir)
                    os.chmod(inner_dagdir, 0o777)
                outputdir = config.get_processing_dir('login',self.pax_version)
                json_file = self.write_json_file(doc)
                print(json_file)
                if not os.path.exists(os.path.join(self.logdir, self.pax_version, run_name, "joblogs")):
                    os.makedirs(os.path.join(self.logdir, self.pax_version, run_name, "joblogs"))
                self.write_submit_script(self.submitfile, self.logdir, json_file)
                self.write_inner_dag(run, inner_dagfile, outputdir, self.submitfile, json_file, doc) 

                # write inner dag info to outer dag
                outer_dag_file.write("SUBDAG EXTERNAL %s %s \n" % (inner_dagname, inner_dagfile))
                #outer_dag_file.write("RETRY %s 5\n" % inner_dagname)
                if self.reprocessing:
                    outer_dag_file.write("SCRIPT PRE %s /home/ershockley/cax/osg_scripts/pre_script.sh "
                                         "%s %s %s %s \n" % (inner_dagname, rawdir, self.pax_version, run, self.logdir))
                outer_dag_file.write("SCRIPT POST %s /home/ershockley/cax/osg_scripts/hadd_and_upload.sh "
                                     "%s %s $RETURN %s %s \n" % (inner_dagname, rawdir, self.pax_version, run, self.logdir))

    def write_inner_dag(self, run_number, inner_dag, outputdir, submitfile, jsonfile, doc, 
                        inputfilefilter = "XENON1T-", uri = "DEFAULT", muonveto = False):
        """
        Writes inner dag file that contains jobs for each zip file in a run
        """
        if uri == "DEFAULT":
            uri = self.default_uri
        rawdir = self.get_raw_dir(doc)
        
        i = 0
        with open(inner_dag, "wt") as inner_dag:
            for dir_name, subdir_list, file_list in os.walk(rawdir):
                if not muonveto and "MV" in dir_name:
                    continue
                if (self.runlist is not None and run_number not in self.runlist):
                    continue
                run_name = rawdir.split('/')[-1]
                for infile in file_list:
                    if inputfilefilter not in infile:
                        continue
                    filepath, file_extenstion = os.path.splitext(infile)
                    if file_extenstion != ".zip":
                        continue
                    zip_name = filepath.split("/")[-1]
                    outfile = zip_name + ".root"
                    infile_local = os.path.abspath(os.path.join(dir_name, infile))
                    infile = uri + infile_local
                    if not os.path.exists(os.path.join(outputdir, run_name)):
                        os.makedirs(os.path.join(outputdir, run_name))
                        os.chmod(os.path.join(outputdir, run_name), 0o777)

                    outfile = os.path.abspath(os.path.join(outputdir,
                                                           run_name, outfile))
                    outfile_full = uri + outfile
                    inner_dag.write("JOB %s.%d %s\n" % (run_number, i, submitfile))
                    inner_dag.write(("VARS %s.%d input_file=\"%s\" "
                                    "out_location=\"%s\" name=\"%s\" "
                                    "ncpus=\"1\" disable_updates=\"True\" "
                                    "host=\"login\" pax_version=\"%s\" "
                                    "pax_hash=\"n/a\" zip_name=\"%s\" "
                                    "json_file=\"%s\" \n") % (run_number, i, infile,
                                                              outfile_full, run_name,
                                                              self.pax_version, zip_name, jsonfile))
                    inner_dag.write("Retry %s.%d 12\n" % (run_number, i))
                    i += 1


    # for retrieving path to raw data. takes a run doc
    def get_raw_dir(self, doc):
        # loop over data locations, find the path for this host
        for entry in doc["data"]:
            if entry["host"] == self.host and entry["type"] == "raw":
                if entry["status"] != "transferred":
                    print("Run %d raw data is not transferred to %s yet!" % (run_number, host))
                    break
                path = entry["location"]
        return path        


    def write_submit_script(self, filename, logdir, json_file):
        template = """#!/bin/bash
executable = /home/ershockley/cax/osg_scripts/run_xenon.sh
universe = vanilla
Error = {logdir}/$(pax_version)/$(name)/$(zip_name)_$(cluster).err
Output  = {logdir}/$(pax_version)/$(name)/$(zip_name)_$(cluster).out
Log     = {logdir}/$(pax_version)/$(name)/joblogs/$(zip_name)_$(cluster).joblog

Requirements = ((OpSysAndVer == "CentOS6" || OpSysAndVer == "RedHat6" || OpSysAndVer == "SL6") && (GLIDEIN_ResourceName =!= "NPX")) && (GLIDEIN_ResourceName =!= "BU_ATLAS_Tier2")
request_cpus = $(ncpus)
transfer_input_files = /home/ershockley/user_cert, $(json_file)
transfer_output_files = ""
+WANT_RCC_ciconnect = True
when_to_transfer_output = ON_EXIT
# on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
transfer_executable = True
# periodic_release =  ((NumJobStarts < 5) && ((CurrentTime - EnteredCurrentStatus) > 600))
arguments = $(name) $(input_file) $(host) $(pax_version) $(pax_hash) $(out_location) $(ncpus) $(disable_updates) $(json_file)
queue 1
"""
        script = template.format(logdir = logdir, json_file = json_file)

        with open(filename, "w") as f:
            f.write(script)

            
