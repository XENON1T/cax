#!/usr/bin/env python
from __future__ import print_function
import os
from cax.api import api
from cax import config
import json
import time
import stat
import subprocess
import re
"""
This class contains functions for writing inner and outer dag files. It takes a list of runs (integers) and pax_version as input.
"""

class dag_writer():

    def __init__(self, runlist, pax_version, logdir, reprocessing=False, n_retries=10, RCC_only=False, lyon_only=False):
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
SCRIPT PRE {inner_dagname}_noop1 /home/ershockley/cax/osg_scripts/pre_script.sh {run_name} {pax_version} {number} {logdir} {detector}
SCRIPT POST {inner_dagname}_noop2 /home/ershockley/cax/osg_scripts/hadd_and_upload.sh {run_name} {pax_version} {number} {logdir} {n_zips} {detector}
PARENT {inner_dagname}_noop1 CHILD {inner_dagname}
PARENT {inner_dagname} CHILD {inner_dagname}_noop2
"""

        self.inner_dag_template = """JOB {number}.{zip_i} {submit_file}
VARS {number}.{zip_i} input_file="{infile}" out_location="{outfile_full}" name="{run_name}" ncpus="1" disable_updates="True" host="login" pax_version="{pax_version}" pax_hash="n/a" zip_name="{zip_name}" json_file="{json_file}" on_rucio="{on_rucio}" dirname="{dirname}"
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
            self.write_submit_script(self.submitfile, self.logdir)

    def get_run_doc(self, run_id):
        if isinstance(run_id, int):
            identifier = 'number'
            detector = 'tpc'
        elif isinstance(run_id, str):
            identifier = 'name'
            detector = 'muon_veto'

        query = {identifier  : run_id,
                 'detector' : detector}
        API = api()
        doc = API.get_next_run(query)
        time.sleep(0.1)
        return doc
    
    def write_json_file(self, doc):
        # take a run doc, write to json file

        name = doc["name"]

        if doc['detector'] == 'muon_veto':
            json_file = "/xenon/ershockley/jsons/" + name + "_MV.json"

        elif doc['detector'] == 'tpc':
            json_file = "/xenon/ershockley/jsons/" + name + ".json"

        with open(json_file, "w") as f:
            # fix doc so that all '|' become '.' in json
            fixed_doc = self.FixKeys(doc)
            json.dump(fixed_doc, f)
            
        return json_file
        
        
    def write_outer_dag(self, outer_dag):
        """
        Writes outer dag file that contains subdag for each run in runlist.
        """
        final_script = outer_dag.replace(".dag", "_final.sh")
        with open(outer_dag, "w") as outer_dag_file:
            with open(final_script, "w") as final_file:
                final_file.write("#!/bin/bash \n")
                # loop over runs in list, write an outer dag that contains subdags for each run
                run_counter = 0
                for run in self.runlist:
                    # get run doc, various parameters needed to write dag
                    doc = self.get_run_doc(run)

                    if doc is None:
                        print("Run %s does not exist" % run)
                        continue

                    # skip bad runs
                    if "data" not in doc.keys():
                        print("Skipping Run %s. No 'data' in run doc" % run)
                        continue

                    if "events_built" not in doc["trigger"] or doc["trigger"]["events_built"] < 1:
                        print("Skipping Run %s. No events" % run)
                        continue

                    # skip if already processed with this pax version
                    if any( [ entry["type"] == 'processed' and
                              entry['pax_version'] == self.pax_version and
                              entry['status'] == 'transferred' and
                              entry['host'] == 'login'
                              for entry in doc["data"] if 'pax_version' in entry.keys() ]):
                        print("Skipping Run %s. Already processed with pax %s" %
                              (run, self.pax_version))
                        continue

                    # check if raw data is in chicago (stash, rucio, or midway)
                    on_rucio = False
                    on_stash_local = False
                    on_midway = False
                    rucio_name = None
                    
                    if any( [entry['type'] == 'raw' and entry['status']=='transferred' and
                             entry['host'] == 'midway-login1' for entry in doc['data'] ]):
                        on_midway = True

                    rawdir = self.get_raw_dir(doc)
                    if rawdir is not None:
                        if os.path.exists(rawdir):
                            on_stash_local = True


                    for d in doc['data']:
                        if d['host'] == 'rucio-catalogue' and d['status'] == 'transferred':
                            rucio_name = d['location']
                            on_rucio = True

                    on_stash_rucio = False
                    if rucio_name is not None:
                        on_stash_rucio = self.is_on_stash(rucio_name)

                    if not on_stash_local and not on_stash_rucio:
                        print(rucio_name)
                        print("Run %s not in Chicago. Skipping" % run)
                        continue
                            

                    if on_stash_local:
                        rawdata_loc = "login"
                    else:
                        if on_stash_rucio:
                            rawdata_loc = "rucio-catalogue"
                        elif on_midway:
                            rawdata_loc = "midway-login1"
                        elif on_rucio:
                            rawdata_loc = "rucio-catalogue"
                        else:
                            print("Error: no rawdata loc defined for run %s. Skipping" % run)
                            continue

                    # skip if no gains in run doc
                    if "gains" not in doc["processor"]["DEFAULT"]:
                        print("Skipping Run %s. No gains in run doc" % run)
                        continue

                    # skip if no electron lifetime in run doc
                    if "electron_lifetime_liquid" not in doc["processor"]["DEFAULT"]:
                        print("Skipping Run %s. No e lifetime in run doc" % run)
                        continue

                    # skip if has donotprocess tag
                    if self.has_tag(doc, 'donotprocess'):
                        print("Skipping run %s. Has 'donotprocess' tag" % run)
                        continue

                    # skip if LED run before 3632
                    if isinstance(run, int) and not doc['reader']['self_trigger'] and run<3632:
                        print("Skipping run %s. LED run before 3632." % run)
                        continue

                    print("Adding run %s to dag" % run)
                    run_counter += 1

                    #with open("/home/ershockley/already_dagged_642.txt", "a") as book:
                    #    book.write("%i\n" % run)
                        

                    run_name = doc["name"]
                    run_number = doc['number']

                    if doc['detector'] == 'muon_veto':
                        muon_veto = True
                        MV = "_MV"
                    else:
                        muon_veto = False
                        MV = ""

                    # write inner dag and json file for this run
                    inner_dagname = "xe1t_" + str(run)
                    inner_dagdir = outer_dag.replace(outer_dag.split("/")[-1], "inner_dags")
                    inner_dagfile = inner_dagdir + "/" + str(run) + "_%s.dag" % self.pax_version
                    if not os.path.exists(inner_dagdir):
                        os.makedirs(inner_dagdir)
                        os.chmod(inner_dagdir, 0o777)

                    outputdir = config.get_processing_dir('login',self.pax_version)

                    json_file = self.write_json_file(doc)

                    os.makedirs(os.path.join(self.logdir, "pax_%s" % self.pax_version, run_name + MV, "joblogs"),
                                exist_ok=True)


                    if not self.reprocessing:
                        dagdir = outer_dag.rsplit('/',1)[0]
                        self.submitfile = dagdir + "/process.submit"
                        self.write_submit_script(self.submitfile, self.logdir)

                    self.write_inner_dag(run, inner_dagfile, outputdir, self.submitfile, json_file, doc,
                                         rawdata_loc, n_retries=self.n_retries, muonveto=muon_veto)

                    # write inner dag info to outer dag
                    outer_dag_file.write(self.outer_dag_template.format(inner_dagname = inner_dagname,
                                                                        inner_dagfile = inner_dagfile,
                                                                        run_name = run_name,
                                                                        pax_version = self.pax_version,
                                                                        number = run_number,
                                                                        logdir = self.logdir,
                                                                        n_zips = self.n_zips,
                                                                        detector = doc['detector'])
                                         )

                    final_file.write(self.final_script_template.format(run_name=run_name, pax_version=self.pax_version,
                                                                       number = run, logdir = self.logdir, n_zips = self.n_zips))

                final_file.write("exit $ex")
            os.chmod(final_script, stat.S_IXUSR | stat.S_IRUSR )

            # write final script, but don't execute in dag. only inside cax if more than 10 rescues.
            # see cax/tasks/process.py
            if not self.reprocessing:
                outer_dag_file.write("FINAL final_node %s NOOP \n" % self.submitfile)
                outer_dag_file.write("SCRIPT POST final_node %s \n" % final_script)

        print("\n%d Run(s) written to %s" % (run_counter, outer_dag))

    def write_inner_dag(self, run_id, inner_dag, outputdir, submitfile, jsonfile, doc, rawdata_loc,
                        n_retries = 10, inputfilefilter = "XENON1T-", muonveto = False):

        """
        Writes inner dag file that contains jobs for each zip file in a run
        """

        # if raw data on stash, get zips directly from raw directory
        if rawdata_loc == "rucio-catalogue":
            on_rucio = True
        else:
            on_rucio = False

        # if mv run, need to append _MV to names in some places
        if muonveto:
            MV = "_MV"
        else:
            MV = ""

        if rawdata_loc == "login":
            rawdir = self.get_raw_dir(doc)
            i = 0
            with open(inner_dag, "w") as inner_dag:
                run_name = doc["name"]

                for dir_name, subdir_list, file_list in os.walk(rawdir):
                    if not muonveto and "MV" in dir_name:
                        continue
                    if (self.runlist is not None and run_id not in self.runlist):
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
                        if not os.path.exists(os.path.join(outputdir, run_name + MV)):
                            os.makedirs(os.path.join(outputdir, run_name + MV))
                            os.chmod(os.path.join(outputdir, run_name + MV), 0o777)

                        outfile = os.path.abspath(os.path.join(outputdir,
                                                               run_name + MV, outfile))
                        outfile_full = self.ci_uri + outfile
                        inner_dag.write(self.inner_dag_template.format(number=run_id, zip_i=i, submit_file = submitfile,
                                                                       infile=infile, outfile_full=outfile_full, run_name=run_name,
                                                                       pax_version = self.pax_version, zip_name=zip_name,
                                                                       json_file=jsonfile, n_retries=n_retries, on_rucio=on_rucio,
                                                                       dirname = run_name + MV))
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

                if (self.runlist is not None and run_id not in self.runlist):
                    print("Run not in runlist")
                    return

                time.sleep(0.5)
                zip_list = self.rucio_getzips(rucio_scope)
                self.n_zips = len(zip_list)
                for zip in zip_list:
                    zip_name, file_extenstion = os.path.splitext(zip)
                    if file_extenstion != ".zip":
                        continue
                    outfile = zip_name + ".root"
                    infile = rucio_scope.replace("raw", zip)
                    if not os.path.exists(os.path.join(outputdir, run_name + MV)):
                        os.makedirs(os.path.join(outputdir, run_name + MV))
                        os.chmod(os.path.join(outputdir, run_name + MV), 0o777)

                    outfile = os.path.abspath(os.path.join(outputdir,
                                                           run_name + MV, outfile))
                    outfile_full = self.ci_uri + outfile
                    inner_dag.write(self.inner_dag_template.format(number=run_id, zip_i=i, submit_file=submitfile,
                                                                   infile=infile, outfile_full=outfile_full,
                                                                   run_name=run_name,
                                                                   pax_version=self.pax_version, zip_name=zip_name,
                                                                   json_file=jsonfile, n_retries=n_retries,
                                                                   on_rucio=on_rucio, dirname = run_name + MV))
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

                if (self.runlist is not None and run_id not in self.runlist):
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
                    if not os.path.exists(os.path.join(outputdir, run_name + MV)):
                        os.makedirs(os.path.join(outputdir, run_name + MV))
                        os.chmod(os.path.join(outputdir, run_name + MV), 0o777)

                    outfile = os.path.abspath(os.path.join(outputdir,
                                                           run_name, outfile))
                    outfile_full = self.ci_uri + outfile
                    inner_dag.write(self.inner_dag_template.format(number=run_id, zip_i=i, submit_file=submitfile,
                                                                   infile=infile, outfile_full=outfile_full,
                                                                   run_name=run_name,
                                                                   pax_version=self.pax_version, zip_name=zip_name,
                                                                   json_file=jsonfile, n_retries=n_retries,
                                                                   on_rucio=on_rucio, dirname = run_name + MV))
                    i += 1


    # for retrieving path to raw data. takes a run doc
    def get_raw_dir(self, doc):
        # loop over data locations, find the path for this host
        path = None
        for entry in doc["data"]:
            if entry["host"] == self.host and entry["type"] == "raw":
                if entry["status"] != "transferred":
                    print("Run %s raw data is not transferred to %s yet!" % (doc['name'], self.host))
                    return None
                path = entry["location"]
        return path        


    def write_submit_script(self, filename, logdir):

        template = """#!/bin/bash
executable = /home/ershockley/cax/osg_scripts/run_xenon.sh
universe = vanilla
Error = {logdir}/pax_$(pax_version)/$(dirname)/$(zip_name)_$(cluster).log
Output  = {logdir}/pax_$(pax_version)/$(dirname)/$(zip_name)_$(cluster).log
Log     = {logdir}/pax_$(pax_version)/$(dirname)/joblogs/$(zip_name)_$(cluster).joblog

Requirements = (HAS_CVMFS_xenon_opensciencegrid_org) && (((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName1) || (RCC_Factory == "ciconnect")) && ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName2) || (RCC_Factory == "ciconnect")) && ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName3)  || (RCC_Factory == "ciconnect")) && ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName4) || (RCC_Factory == "ciconnect"))) && (OSGVO_OS_STRING == "RHEL 6" || RCC_Factory == "ciconnect") && (GLIDEIN_ResourceName =!= "Comet") 
request_cpus = $(ncpus)
request_memory = 1900MB
request_disk = 3GB
transfer_input_files = /home/ershockley/user_cert, $(json_file)
transfer_output_files = ""
+WANT_RCC_ciconnect = True
+ProjectName = "xenon1t" 
+AccountingGroup = "group_opportunistic.xenon1t.processing"
when_to_transfer_output = ON_EXIT
# on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
transfer_executable = True
periodic_remove =  ((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > (60*60*12)))
#periodic_release = (JobStatus == 5) && (HoldReason == 3) && (NumJobStarts < 5) && ( (CurrentTime - EnteredCurrentStatus) > $RANDOM_INTEGER(60, 1800, 30) )
#periodic_remove = (NumJobStarts > 4)
arguments = $(name) $(input_file) $(host) $(pax_version) $(pax_hash) $(out_location) $(ncpus) $(disable_updates) $(json_file) $(on_rucio)
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

    def is_on_stash(self, rucio_name):
        # checks if run with rucio_name is on stash
        out = subprocess.Popen(["rucio", "list-rules", rucio_name], stdout=subprocess.PIPE).stdout.read()
        out = out.decode("utf-8").split("\n")
        for line in out:
            line = re.sub(' +', ' ', line).split(" ")
            if len(line) > 4 and line[4] == "UC_OSG_USERDISK" and line[3][:2] == "OK":
                return True
        return False

    def FixKeys(self, dictionary):
        for key, value in dictionary.items():
            if type(value) in [type(dict())]:
                dictionary[key] = self.FixKeys(value)
            if '|' in key:
                dictionary[key.replace('|', '.')] = dictionary.pop(key)
        return dictionary

# for excluding syracuse
# && (GLIDEIN_Site =!= "SU-OG") && (GLIDEIN_ResourceName =!= "SU-OG-CE1") && (GLIDEIN_ResourceName =!= "SU-OG-CE")