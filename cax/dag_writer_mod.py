#!/usr/bin/env python
from __future__ import print_function
import os
from cax.api import api
from cax import config
import json
import time
import stat
import subprocess
"""
This class contains functions for writing inner and outer dag files. It takes a list of runs (integers) and pax_version as input.
"""

class dag_writer():

    def __init__(self, runlist, pax_version, logdir, reprocessing=False, n_retries=10, RCC_only = False, lyon_only=False):
        self.runlist = runlist
        self.pax_version = pax_version
        self.logdir = logdir
        self.reprocessing = reprocessing
        self.ci_uri = "gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm"
        self.midway_uri = "gsiftp://sdm06.rcc.uchicago.edu:2811"
        self.host = "login"
        self.n_retries = n_retries
        self.RCC_only = RCC_only
        self.lyon_only = lyon_only
        
        self.outer_dag_template = """SPLICE {inner_dagname} {inner_dagfile}
JOB {inner_dagname}_noop1 {inner_dagfile} NOOP
JOB {inner_dagname}_noop2 {inner_dagfile} NOOP
SCRIPT PRE {inner_dagname}_noop1 /home/ershockley/cax/osg_scripts/pre_script.sh {run_name} {pax_version} {number} {logdir}
SCRIPT POST {inner_dagname}_noop2 /home/ershockley/cax/osg_scripts/hadd_and_upload.sh {run_name} {pax_version} {number} {logdir} {n_zips}
PARENT {inner_dagname}_noop1 CHILD {inner_dagname}
PARENT {inner_dagname} CHILD {inner_dagname}_noop2
"""

        self.inner_dag_template = """JOB {number}.{zip_i} {submit_file}
VARS {number}.{zip_i} input_file="{infile}" out_location="{outfile_full}" name="{run_name}" ncpus="1" disable_updates="True" host="login" pax_version="{pax_version}" pax_hash="n/a" zip_name="{zip_name}" json_file="{json_file}" on_rucio="{on_rucio}" rse = "{rucio_rse}"
RETRY {number}.{zip_i} {n_retries}
"""

        self.final_script_template = """/home/ershockley/cax/osg_scripts/hadd_and_upload.sh {run_name} {pax_version} {number} {logdir} {n_zips}
if [[ ! $? -eq 0 || ! $ex -eq 0 ]]; then
    ex=1
fi
sleep 2
"""

        if RCC_only and lyon_only:
            raise ValueError("Trying to use RCC only and Lyon only")

        if reprocessing:
            self.submitfile = os.path.join(logdir, "process.submit")
            self.write_submit_script(self.submitfile, logdir)

            if not RCC_only:
                self.submitfile_lyon = os.path.join(logdir, "process_lyon.submit")
                self.write_submit_script(self.submitfile_lyon, logdir, use_lyon=True)

            elif not lyon_only:
                self.submitfile_rcc = os.path.join(logdir, "process_rcc.submit")
                self.write_submit_script(self.submitfile_rcc, logdir)

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
                              entry['status'] == 'transferred' and
                              entry['host'] == 'login'
                              for entry in doc["data"] if 'pax_version' in entry.keys() ]):
                        print("Skipping Run %d. Already processed with pax %s" %
                              (run, self.pax_version))
                        continue

                    # check if raw data is in chicago (stash, rucio, or midway)
                    on_rucio = False
                    on_stash = False
                    on_stash_local = False
                    on_midway = False
                    on_nikhef = False
                    on_lyon = False
                    
                    if any( [entry['type'] == 'raw' and entry['status']=='transferred' and
                             entry['host'] == 'midway-login1' for entry in doc['data'] ]):
                        on_midway = True

                    rawdir = self.get_raw_dir(doc)
                    if rawdir is not None:
                        if os.path.exists(rawdir):
                            on_stash_local = True

                    if not on_stash_local:
                        #print("Skipping Run %d. No raw data on login" % run)
                        #continue
                        # if not on stash local, check rucio catalogue
                        if not any([entry["host"] == 'rucio-catalogue' and entry["type"] == 'raw'
                                and entry["status"] == 'transferred' for entry in doc["data"]]):
                            print(" Run %d data not on stash or rucio-catalogue" % run)
                            #continue
                        else:
                            if any(["UC_OSG_USERDISK" in entry['rse'] for entry in doc['data'] if 'rse' in entry] ):
                                on_stash = True
                            if any(["CCIN2P3_USERDISK" in entry['rse'] for entry in doc['data'] if 'rse' in entry]):
                                on_lyon = True
                            if any(["NIKHEF_USERDISK" in entry['rse'] for entry in doc['data'] if 'rse' in entry]):
                                on_nikhef = True

                    on_rucio = (on_stash or on_lyon or on_nikhef)

                    if not (on_stash_local or on_rucio):
                        print("Run %d not in Chicago or rucio. Skipping" % run)
                        continue

                    rucio_rse = None
                    if on_stash_local:
                        rawdata_loc = "login"
                    else:
                        if on_rucio:
                            rawdata_loc = "rucio-catalogue"
                            if on_stash and not self.lyon_only:
                                rucio_rse = "UC_OSG_USERDISK"
                            elif on_lyon:
                                rucio_rse = "CCIN2P3_USERDISK"
                            elif on_nikhef:
                                rucio_rse = "NIKHEF_USERDISK"

                        elif on_midway:
                            rawdata_loc = "midway-login1"
                        else:
                            print("Error: no rawdata loc defined for run %d. Skipping" % run)
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

                    #with open("/home/ershockley/already_dagged_642.txt", "a") as book:
                    #    book.write("%i\n" % run)
                        

                    run_name = doc["name"]

                    # write inner dag and json file for this run
                    inner_dagname = "xe1t_" + str(run)
                    inner_dagdir = outer_dag.replace(outer_dag.split("/")[-1], "inner_dags")
                    inner_dagfile = inner_dagdir + "/" + str(run) + "_%s.dag" % self.pax_version
                    if not os.path.exists(inner_dagdir):
                        os.makedirs(inner_dagdir)
                        os.chmod(inner_dagdir, 0o777)
                    outputdir = config.get_processing_dir('login',self.pax_version)
                    json_file = self.write_json_file(doc)
                    if not os.path.exists(os.path.join(self.logdir, "pax_%s" % self.pax_version, run_name, "joblogs")):
                        os.makedirs(os.path.join(self.logdir, "pax_%s" % self.pax_version, run_name, "joblogs"))


                    # if not in chicago but in lyon or nikhef, use lyon for processing
                    use_lyon = False
                    if not on_stash and not on_stash_local:
                        if (on_lyon or on_nikhef) and (not self.RCC_only):
                            use_lyon = True
                        elif self.RCC_only:
                            raise ValueError("RCC only option passed, but can't process run %d on RCC yet." % run)


                    if not self.reprocessing:
                        dagdir = outer_dag.rsplit('/',1)[0]
                        if not use_lyon:
                            self.submitfile = dagdir + "/process.submit"
                            self.write_submit_script(self.submitfile, self.logdir)
                        elif use_lyon:
                            self.submitfile_lyon = dagdir = "/process_lyon.submit"
                            self.write_submit_script(self.submitfile_lyon, self.logdir)


                    submitfile = self.submitfile

                    if use_lyon or ((on_lyon or on_nikhef) and self.lyon_only):
                        submitfile = self.submitfile_lyon

                    if self.RCC_only:
                        submitfile = self.submitfile_rcc

                    self.write_inner_dag(run, inner_dagfile, outputdir, submitfile, json_file, doc, rawdata_loc,
                                         rucio_rse=rucio_rse, n_retries=self.n_retries)

                    # write inner dag info to outer dag
                    outer_dag_file.write(self.outer_dag_template.format(inner_dagname = inner_dagname,
                                                                        inner_dagfile = inner_dagfile,
                                                                        run_name = run_name,
                                                                        pax_version = self.pax_version,
                                                                        number = run,
                                                                        logdir = self.logdir,
                                                                        n_zips = self.n_zips)
                                         )

                    final_file.write(self.final_script_template.format(run_name=run_name, pax_version=self.pax_version,
                                                                       number = run, logdir = self.logdir, n_zips = self.n_zips))

                final_file.write("exit $ex")
            os.chmod(final_script, stat.S_IXUSR | stat.S_IRUSR )
            if not self.reprocessing:
                outer_dag_file.write("FINAL final_node %s NOOP \n" % self.submitfile)
                outer_dag_file.write("SCRIPT POST final_node %s \n" % final_script)

        print("\n%d Run(s) written to %s" % (run_counter, outer_dag))

    def write_inner_dag(self, run_number, inner_dag, outputdir, submitfile, jsonfile, doc, rawdata_loc, rucio_rse = None,
                        n_retries = 10, inputfilefilter = "XENON1T-", muonveto = False):

        """
        Writes inner dag file that contains jobs for each zip file in a run
        """

        # if raw data on stash, get zips directly from raw directory
        if rawdata_loc == "rucio-catalogue":
            if rucio_rse is None:
                print("Error: no RSE provided")
                sys.exit(1)
            on_rucio = True
        else:
            on_rucio = False

        if rawdata_loc == "login":
            rawdir = self.get_raw_dir(doc)

            i = 0
            with open(inner_dag, "w") as inner_dag:
                run_name = doc["name"]

                for dir_name, subdir_list, file_list in os.walk(rawdir):
                    if not muonveto and "MV" in dir_name:
                        continue
                    if (self.runlist is not None and run_number not in self.runlist):
                        continue
                    #run_name = rawdir.split('/')[-1]
                    zip_counter = 0
                    for infile in file_list:
                        if inputfilefilter not in infile:
                            continue
                        filepath, file_extenstion = os.path.splitext(infile)
                        if file_extenstion != ".zip":
                            continue
                        zip_counter += 1
                        zip_name = filepath.split("/")[-1]
                        outfile = zip_name + ".root"
                        infile_local = os.path.abspath(os.path.join(dir_name, infile))
                        infile = self.ci_uri + infile_local
                        if not os.path.exists(os.path.join(outputdir, run_name)):
                            os.makedirs(os.path.join(outputdir, run_name))
                            os.chmod(os.path.join(outputdir, run_name), 0o777)

                        outfile = os.path.abspath(os.path.join(outputdir,
                                                               run_name, outfile))
                        outfile_full = self.ci_uri + outfile
                        inner_dag.write(self.inner_dag_template.format(number=run_number, zip_i=i, submit_file = submitfile,
                                                                       infile=infile, outfile_full=outfile_full, run_name=run_name,
                                                                       pax_version = self.pax_version, zip_name=zip_name,
                                                                       json_file=jsonfile, n_retries=n_retries, on_rucio=on_rucio,
                                                                       rucio_rse = rucio_rse))
                        i += 1
            self.n_zips = zip_counter

        # if using rucio transfer, get from making a rucio call
        elif rawdata_loc == "rucio-catalogue":
            i = 0
            with open(inner_dag, "w") as inner_dag:
                run_name = doc["name"]
                rucio_scope = None
                for datum in doc["data"]:
                    if datum["host"] == "rucio-catalogue":
                        rucio_scope = datum["location"]

                if rucio_scope is None:
                    print("Run not in rucio catalog. Check RunsDB")
                    return

                if (self.runlist is not None and run_number not in self.runlist):
                    print("Run not in runlist")
                    return

                time.sleep(0.75)
                zip_list = self.rucio_getzips(rucio_scope)
                self.n_zips = len(zip_list)
                for zip in zip_list:
                    zip_name, file_extenstion = os.path.splitext(zip)
                    if file_extenstion != ".zip":
                        continue
                    outfile = zip_name + ".root"
                    infile = rucio_scope.replace("raw", zip)
                    if not os.path.exists(os.path.join(outputdir, run_name)):
                        os.makedirs(os.path.join(outputdir, run_name))
                        os.chmod(os.path.join(outputdir, run_name), 0o777)

                    outfile = os.path.abspath(os.path.join(outputdir,
                                                           run_name, outfile))
                    outfile_full = self.ci_uri + outfile
                    inner_dag.write(self.inner_dag_template.format(number=run_number, zip_i=i, submit_file=submitfile,
                                                                   infile=infile, outfile_full=outfile_full,
                                                                   run_name=run_name,
                                                                   pax_version=self.pax_version, zip_name=zip_name,
                                                                   json_file=jsonfile, n_retries=n_retries,
                                                                   on_rucio=on_rucio, rucio_rse = rucio_rse))
                    i += 1

        elif rawdata_loc == 'midway-login1':
            i = 0
            with open(inner_dag, "w") as inner_dag:
                run_name = doc["name"]
                midway_location = None
                for datum in doc["data"]:
                    if datum["host"] == "midway-login1" and datum['type'] == 'raw':
                        midway_location = datum["location"]

                if midway_location is None:
                    print("Run not on midway. Check RunsDB")
                    return

                if (self.runlist is not None and run_number not in self.runlist):
                    print("Run not in runlist")
                    return

                time.sleep(1)
                zip_list = self.midway_getzips(midway_location)
                self.n_zips = len(zip_list)
                for zip in zip_list:
                    zip_name, file_extenstion = os.path.splitext(zip)
                    if file_extenstion != ".zip":
                        continue
                    outfile = zip_name + ".root"
                    infile = self.midway_uri + midway_location + "/" + zip
                    if not os.path.exists(os.path.join(outputdir, run_name)):
                        os.makedirs(os.path.join(outputdir, run_name))
                        os.chmod(os.path.join(outputdir, run_name), 0o777)

                    outfile = os.path.abspath(os.path.join(outputdir,
                                                           run_name, outfile))
                    outfile_full = self.ci_uri + outfile
                    inner_dag.write(self.inner_dag_template.format(number=run_number, zip_i=i, submit_file=submitfile,
                                                                   infile=infile, outfile_full=outfile_full,
                                                                   run_name=run_name,
                                                                   pax_version=self.pax_version, zip_name=zip_name,
                                                                   json_file=jsonfile, n_retries=n_retries,
                                                                   on_rucio=on_rucio, rucio_rse = rucio_rse))
                    i += 1


    # for retrieving path to raw data. takes a run doc
    def get_raw_dir(self, doc):
        # loop over data locations, find the path for this host
        path = None
        for entry in doc["data"]:
            if entry["host"] == self.host and entry["type"] == "raw":
                if entry["status"] != "transferred":
                    print("Run %d raw data is not transferred to %s yet!" % (doc['number'], self.host))
                    return None
                path = entry["location"]
        return path        


    def write_submit_script(self, filename, logdir, use_lyon=False):
        # two different templates for if we use lyon or not
        template = """#!/bin/bash
executable = /home/ershockley/cax/osg_scripts/run_xenon.sh
universe = vanilla
Error = {logdir}/pax_$(pax_version)/$(name)/$(zip_name)_$(cluster).log
Output  = {logdir}/pax_$(pax_version)/$(name)/$(zip_name)_$(cluster).log
Log     = {logdir}/pax_$(pax_version)/$(name)/joblogs/$(zip_name)_$(cluster).joblog

Requirements = {requirements}
request_cpus = $(ncpus)
request_memory = 1900MB
request_disk = 3GB
transfer_input_files = /home/ershockley/user_cert, $(json_file)
transfer_output_files = ""
+WANT_RCC_ciconnect = True
when_to_transfer_output = ON_EXIT
# on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
transfer_executable = True
periodic_remove =  ((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > (60*60*10)))
#periodic_release = (JobStatus == 5) && (HoldReason == 3) && (NumJobStarts < 5) && ( (CurrentTime - EnteredCurrentStatus) > $RANDOM_INTEGER(60, 1800, 30) )
#periodic_remove = (NumJobStarts > 4)
arguments = $(name) $(input_file) $(host) $(pax_version) $(pax_hash) $(out_location) $(ncpus) $(disable_updates) $(json_file) $(on_rucio) $(rse)
queue 1
"""
        # can't use RCC_only and Lyon
        if self.RCC_only and use_lyon:
            raise ValueError("Trying to use both RCC only and Lyon")

        if not use_lyon:
            requirements = '(HAS_CVMFS_xenon_opensciencegrid_org)' \
                           ' && (((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName1) || (RCC_Factory == "ciconnect")) ' \
                           '&& ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName2) || (RCC_Factory == "ciconnect")) ' \
                           '&& ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName3)  || (RCC_Factory == "ciconnect")) ' \
                           '&& ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName4) || (RCC_Factory == "ciconnect"))) ' \
                           '&& (OSGVO_OS_STRING == "RHEL 6" || RCC_Factory == "ciconnect")'
        
        elif use_lyon:
            requirements = '(HAS_CVMFS_xenon_opensciencegrid_org) ' \
                           '&& (((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName1) || (RCC_Factory =="ciconnect"))' \
                           ' && ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName2) || (RCC_Factory == "ciconnect"))' \
                           ' && ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName3)  || (RCC_Factory == "ciconnect"))' \
                           ' && ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName4) || (RCC_Factory == "ciconnect")))' \
                           ' && (OSGVO_OS_STRING == "RHEL 6" || RCC_Factory == "ciconnect") ' \
                           '&& (GLIDEIN_Site == "CCIN2P3")'

        if self.RCC_only:
            requirements = '(HAS_CVMFS_xenon_opensciencegrid_org)' \
                           '&& (OSGVO_OS_STRING == "RHEL 6" || RCC_Factory == "ciconnect") ' \
                           '&& (VC3_GLIDEIN_VERSION == "0.9.4") \n' \
                           '+MATCH_APF_QUEUE=XENON1T'



        script = template.format(logdir=logdir, requirements=requirements)

        with open(filename, "w") as f:
            f.write(script)


    def has_tag(self, doc, name):
        if 'tags' not in doc:
            return False

        for tag in doc['tags']:
            if name == tag['name']:
                return True
        return False

    def rucio_getzips(self, dataset):
        """returns list of zip files from rucio call"""
        out = subprocess.Popen(["rucio", "-a", "ershockley", "list-file-replicas", dataset], stdout=subprocess.PIPE).stdout.read()
        out = str(out).split("\\n")
        files = set([l.split(" ")[3] for l in out if '---' not in l and 'x1t' in l])
        zip_files = sorted([f for f in files if f.startswith('XENON1T')])
        return zip_files


    def midway_getzips(self, midway_path):
        uri = self.midway_uri
        out = subprocess.Popen(["gfal-ls","--cert", "/home/ershockley/user_cert", uri + midway_path], stdout=subprocess.PIPE).stdout.read()
        out = str(out.decode("utf-8")).split("\n")
        zips = [l for l in out if l.startswith('XENON')]
        return zips
