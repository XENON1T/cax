#!/usr/bin/env python
from __future__ import print_function
import os
from cax.api import api
from cax import config
import json
import time
import stat

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
        self.submitfile = os.path.join(logdir, "process.submit")
        
        self.outer_dag_template = """SPLICE {inner_dagname} {inner_dagfile}
JOB {inner_dagname}_noop1 {inner_dagfile} NOOP
JOB {inner_dagname}_noop2 {inner_dagfile} NOOP
SCRIPT PRE {inner_dagname}_noop1 /home/ershockley/cax/osg_scripts/pre_script.sh {rawdir} {pax_version} {number} {logdir}
SCRIPT POST {inner_dagname}_noop2 /home/ershockley/cax/osg_scripts/hadd_and_upload.sh {rawdir} {pax_version} {number} {logdir}
PARENT {inner_dagname}_noop1 CHILD {inner_dagname}
PARENT {inner_dagname} CHILD {inner_dagname}_noop2
"""

        self.inner_dag_template = """JOB {number}.{zip_i} {submit_file}
VARS {number}.{zip_i} input_file="{infile}" out_location="{outfile_full}" name="{run_name}" ncpus="1" disable_updates="True" host="login" pax_version="{pax_version}" pax_hash="n/a" zip_name="{zip_name}" json_file="{json_file}"
RETRY {number}.{zip_i} {n_retries}
"""

        self.final_script_template = """/home/ershockley/cax/osg_scripts/hadd_and_upload.sh {rawdir} {pax_version} {number} {logdir}
if [[ ! $? -eq 0 || ! $ex -eq 0 ]]; then
    ex=1
fi
sleep 2
"""

        self.write_submit_script(self.submitfile, self.logdir)

    def get_run_doc(self, run_number):
        query = {"detector" : "tpc",
                 "number"   : run_number
                 }
        API = api()
        doc = API.get_next_run(query)
        time.sleep(0.5)
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
        final_script = outer_dag.replace(".dag", "_final.sh")
        with open(outer_dag, "w") as outer_dag_file:
            with open(final_script, "w") as final_file:
                final_file.write("#!/bin/bash \n")
                # loop over runs in list, write an outer dag that contains subdags for each rubn
                run_counter = 0
                for run in self.runlist:
                    # get run doc, various parameters needed to write dag
                    doc = self.get_run_doc(run)

                    if doc is None:
                        print("Run %d does not exist" % run)
                        continue

                    # skip bad runs
                    if "data" not in doc.keys():
                        print("Skipping Run %d. No 'data' in run doc" % run)
                        continue

                    if "events_built" not in doc["trigger"] or doc["trigger"]["events_built"] < 1:
                        print("Skipping Run %d. No events" % run)
                        continue

                    # skip if already processed with this pax version
                    if any( [ entry["type"] == 'processed' and
                              entry['pax_version'] == self.pax_version and
                              entry['status'] == 'transferred'
                              for entry in doc["data"] if 'pax_version' in entry.keys() ]):
                        print("Skipping Run %d. Already processed with pax %s" %
                              (run, self.pax_version))
                        continue

                    #skip run if not present on stash (temporary)
                    if not any([entry["host"] == 'login' and entry["type"] == 'raw'
                                and entry["status"] == 'transferred' for entry in doc["data"]]):
                        print("Skipping Run %d. No raw data on login" % run)
                        continue

                    # skip if no gains in run doc
                    if "gains" not in doc["processor"]["DEFAULT"]:
                        print("Skipping Run %d. No gains in run doc" % run)
                        continue

                    # skip if no electron lifetime in run doc
                    if "electron_lifetime_liquid" not in doc["processor"]["DEFAULT"]:
                        print("Skipping Run %d. No e lifetime in run doc" % run)
                        continue

                    # skip if has donotprocess tag
                    if self.has_tag(doc, 'donotprocess'):
                        print("Skipping run %d. Has 'donotprocess' tag" % run)
                        continue

                    # skip if LED run before 3632
                    if not doc['reader']['self_trigger'] and run<3632:
                        print("Skipping run %d. LED run before 3632." % run)
                        continue


                    print("Adding run %d to dag" % run)
                    run_counter += 1

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
                    if not os.path.exists(os.path.join(self.logdir, self.pax_version, run_name, "joblogs")):
                        os.makedirs(os.path.join(self.logdir, self.pax_version, run_name, "joblogs"))
                    self.write_inner_dag(run, inner_dagfile, outputdir, self.submitfile, json_file, doc)

                    # write inner dag info to outer dag
                    outer_dag_file.write(self.outer_dag_template.format(inner_dagname = inner_dagname,
                                                                  inner_dagfile = inner_dagfile,
                                                                  rawdir = rawdir,
                                                                  pax_version = self.pax_version,
                                                                  number = run,
                                                                  logdir = self.logdir)
                                         )
                    final_file.write(self.final_script_template.format(rawdir=rawdir, pax_version=self.pax_version,
                                                                                        number = run, logdir = self.logdir))

                final_file.write("exit $ex")
            os.chmod(final_script, stat.S_IXUSR | stat.S_IRUSR )
            outer_dag_file.write("FINAL final_node %s NOOP \n" % self.submitfile)
            outer_dag_file.write("SCRIPT POST final_node %s \n" % final_script)

        print("\n%d Run(s) written to %s" % (run_counter, outer_dag))

    def write_inner_dag(self, run_number, inner_dag, outputdir, submitfile, jsonfile, doc, n_retries = 10, inputfilefilter = "XENON1T-", uri = "DEFAULT", muonveto = False):
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
                    inner_dag.write(self.inner_dag_template.format(number=run_number, zip_i=i, submit_file = submitfile,
                                                                   infile=infile, outfile_full=outfile_full, run_name=run_name,
                                                                   pax_version = self.pax_version, zip_name=zip_name,
                                                                   json_file=jsonfile, n_retries=n_retries))
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


    def write_submit_script(self, filename, logdir):
        template = """#!/bin/bash
executable = /home/ershockley/cax/osg_scripts/run_xenon.sh
universe = vanilla
Error = {logdir}/$(pax_version)/$(name)/$(zip_name)_$(cluster).log
Output  = {logdir}/$(pax_version)/$(name)/$(zip_name)_$(cluster).log
Log     = {logdir}/$(pax_version)/$(name)/joblogs/$(zip_name)_$(cluster).joblog

Requirements = ((OpSysAndVer == "CentOS6" || OpSysAndVer == "RedHat6" || OpSysAndVer == "SL6") && (GLIDEIN_ResourceName =!= "NPX")) && (GLIDEIN_ResourceName =!= "BU_ATLAS_Tier2")
request_cpus = $(ncpus)
request_memory = 2GB
transfer_input_files = /home/ershockley/user_cert, $(json_file)
transfer_output_files = ""
+WANT_RCC_ciconnect = True
when_to_transfer_output = ON_EXIT
# on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
transfer_executable = True
periodic_remove =  ((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > (60*60*6)))
#periodic_release = (JobStatus == 5) && (HoldReason == 3) && (NumJobStarts < 5) && ( (CurrentTime - EnteredCurrentStatus) > $RANDOM_INTEGER(60, 1800, 30) )
#periodic_remove = (NumJobStarts > 4)
arguments = $(name) $(input_file) $(host) $(pax_version) $(pax_hash) $(out_location) $(ncpus) $(disable_updates) $(json_file)
queue 1
"""
        script = template.format(logdir = logdir)

        with open(filename, "w") as f:
            f.write(script)


    def has_tag(self, doc, name):
        if 'tags' not in doc:
            return False

        for tag in doc['tags']:
            if name == tag['name']:
                return True
        return False
