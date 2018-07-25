#!/usr/bin/env python
from __future__ import print_function
import os
import stat
from cax.api import api
from cax import config
import json
import time
import subprocess
import re
import pwd

"""
This class contains functions for writing inner and outer dag files. It takes a list of runs (integers) 
and pax_version as input.
"""

ci_uri = "gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm"
midway_uri = "gsiftp://sdm06.rcc.uchicago.edu:2811"
dcache_uri = "gsiftp://xenon-gridftp.grid.uchicago.edu:2811/xenon/"

euro_sites = {"processing": ["NIKHEF-ELPROD", "CCIN2P3",
                             "WEIZMANN-LCG2", "INFN-T1"],
              "rucio": ["NIKHEF_USERDISK", "CCIN2P3_USERDISK",
                        "WEIZMANN_USERDISK", "CNAF_USERDISK",
                       	"CNAF_TAPE_USERDISK", "SURFSARA_USERDISK"]}

default_run_config = {"exclude_sites": [],
                      "specify_sites": []}


# get grid cert path from cax json
GRID_CERT = config.get_config()['grid_cert']

# one dir up from usual so that we can access the osg_scripts easily
#CAX_DIR = os.path.dirname(os.path.dirname(__file__))
CAX_DIR = os.path.expanduser("~") + "/cax"

class dag_writer():

    def __init__(self, config):
        self.config = config
        self.outer_dag_template = self.outerdag_template()
        self.inner_dag_template = self.innerdag_template()

        if any(site in config['exclude_sites'] for site in config['specify_sites']):
            raise ValueError("You can't both specify and exclude a site. Check config")

        if config['rush'] and config['use_midway']:
            raise ValueError("You can't rush and use midway. Change config and try again")

    def get_run_doc(self, run_id):
        if isinstance(run_id, int):
            identifier = 'number'
            detector = 'tpc'
        elif isinstance(run_id, str):
            identifier = 'name'
            detector = 'muon_veto'

        else:
            raise ValueError("identifier is neither int nor string")

        query = {identifier  : run_id,
                 'detector' : detector,
                 "data": {"$not": {"$elemMatch": {"type": "processed",
                                                  "pax_version": self.config['pax_version'],
                                                  "host": self.config['host'],
                                                  "$or": [{"status": "transferred"},
                                                          {"status": "transferring"}
                                                          ]
                                                  }
                                   }
                          },
                 "$or": [{"$and" : [{'number': {"$gte": 2000}},
                                    {'processor.DEFAULT.gains': {'$exists': True}},
#                                    {'processor.DEFAULT.electron_lifetime_liquid': {'$exists': True}},
                                    {'processor.DEFAULT.drift_velocity_liquid': {'$exists': True}},
                                    {'processor.correction_versions': {'$exists': True}},
#                                    {'processor.WaveformSimulator': {'$exists': True}},
                                    {'processor.NeuralNet|PosRecNeuralNet': {'$exists': True}},
                                    ]
                          },
                         {"detector": "muon_veto"}
                         ],
                 'reader.ini.write_mode': 2,
                 'trigger.events_built': {"$gt": 0},
                 'tags': {"$not": {'$elemMatch': {'name': 'donotprocess'}}},
                 }

        query = {'query': json.dumps(query)}
        API = api()
        doc = API.get_next_run(query)
        time.sleep(0.1)
        return doc
    
    def write_json_file(self, doc):
        # take a run doc, write to json file

        name = doc["name"]

        if doc['detector'] == 'muon_veto':
            json_file = self.config['logdir'] + '/pax_%s/'%self.config['pax_version'] + name + "_MV/" + name + "_MV.json"

        elif doc['detector'] == 'tpc':
            json_file = self.config['logdir'] + '/pax_%s/'%self.config['pax_version'] + name + "/" + name + ".json"

        with open(json_file, "w") as f:
            # fix doc so that all '|' become '.' in json
            fixed_doc = self.FixKeys(doc)
            json.dump(fixed_doc, f)
        if os.stat(json_file).st_uid == os.getuid():
            os.chmod(json_file, 0o777)
        return json_file
        
        
    def write_outer_dag(self, outer_dag):
        """
        Writes outer dag file that contains subdag for each run in runlist.
        """
        c = self.config

        with open(outer_dag, "w") as outer_dag_file:
            # loop over runs in list, write an outer dag that contains subdags for each run
            run_counter = 0
            for run in c['runlist']:
                # get run doc, various parameters needed to write dag

                doc = self.get_run_doc(run)

                if doc is None:
                    print("Run %s cannot be processed. Check DB." % run)
                    continue

                # skip if LED run before 3632
                if isinstance(run, int) and not doc['reader']['self_trigger'] and run<3632:
                    print("Skipping run %s. LED run before 3632." % run)
                    continue

                if doc['detector'] == 'muon_veto':
                    muon_veto = True
                    MV = "_MV"
                else:
                    muon_veto = False
                    MV = ""


                # check if raw data is in chicago (stash, rucio, or midway)
                on_rucio = False
                on_stash_local = False
                on_midway = False
                rucio_name = None
                rses = None

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

                    run_name = doc["name"]
                    run_number = doc['number']


                # run config to be used in submit script below
                run_config = default_run_config.copy()

                # use rucio if possible, else download from stash or midway
                ### TEMPORARY ###
               # on_rucio = False
                if on_rucio:
                    rawdata_loc = 'rucio-catalogue'
                    rses = self.get_rses(rucio_name)
                    if len(rses) < 1:
                        print("Run %s is not on any RSE. Skipping" % run)
                        continue

                    if not any(site in rses for site in (euro_sites['rucio'] + ['UC_OSG_USERDISK'])):
                        if on_stash_local:
                            rawdata_loc = "login"
                        elif on_midway:
                            rawdata_loc = "midway"
                        else:
                            print("Raw data for run %s is not at a necessary rucio end point for processing" % run)
                            continue

                    if not any(site in rses for site in euro_sites['rucio']):
                        run_config['exclude_sites'] = euro_sites['processing']

                    elif 'UC_OSG_USERDISK' not in rses:
                        if not c['rush']:
                            print("Run %s not on UC_OSG_USERDISK" % run)
                            continue
                        else:
                            run_config['specify_sites'] = euro_sites['processing']

                else:
                    if on_stash_local:
                        rawdata_loc = "login"

                    elif on_midway:
                        rawdata_loc = "midway-login1"

                    else:
                        print("Error: no rawdata loc defined for run %s. Skipping" % run)
                        continue

                # write inner dag and json file for this run
                inner_dagname = "xe1t_" + str(run)
                
                basedir = c['logdir'] + '/pax_%s/' % c['pax_version'] + run_name + MV
                inner_dagdir = basedir + '/dags'
                inner_dagfile = inner_dagdir + "/" + str(run) + "_%s.dag" % c['pax_version']
                os.makedirs(inner_dagdir, exist_ok=True )
                if os.stat(basedir).st_uid == os.getuid():
                    os.chmod(inner_dagdir, 0o777)

                # where the zip files will get copied - NOT where the final merged file will be
                outputdir = config.get_processed_zip_dir(c['host'], c['pax_version'])

                json_file = self.write_json_file(doc)
                os.makedirs(os.path.join(basedir, "joblogs"), exist_ok=True)
                submitfile = inner_dagdir + "/process.submit"

                self.write_submit_script(submitfile, run_config)

                self.write_inner_dag(run, inner_dagfile, outputdir, submitfile, json_file, doc,
                                     rawdata_loc, muonveto=muon_veto)

                if self.n_zips < 1:
                    print("There are no zip files for run %s for some reason. Skipping" % run)
                    continue

                print("Adding run %s to dag" % run)
                run_counter += 1

                # write inner dag info to outer dag
                outer_dag_file.write(self.outer_dag_template.format(inner_dagname = inner_dagname,
                                                                    inner_dagfile = inner_dagfile,
                                                                    run_name = run_name,
                                                                    pax_version = c['pax_version'],
                                                                    number = run_number,
                                                                    logdir = c['logdir'],
                                                                    n_zips = self.n_zips,
                                                                    detector = doc['detector'])
                                     )


                # change permissions for this run if it is owned by me                
                if os.stat(basedir).st_uid == os.getuid():
                    os.chmod(basedir, 0o777)
                    for root, subdirs, files in os.walk(basedir):
                        for d in subdirs:
                            path = os.path.join(root, d)
                            os.chmod(path, 0o777)
                    for f in files:
                        path = os.path.join(root, f)
                        os.chmod(path, 0o777)

        print("\n%d Run(s) written to %s" % (run_counter, outer_dag))
        return run_counter

    def write_inner_dag(self, run_id, inner_dag, outputdir, submitfile, jsonfile, doc, rawdata_loc,
                        n_retries=5, inputfilefilter = "XENON1T-", muonveto = False):

        """
        Writes inner dag file that contains jobs for each zip file in a run
        """

        c = self.config

        # update n_retries if it was passed to config
        n_retries = c['retries'] if 'retries' in c else n_retries

        # which uri to use for output files
        uri = ci_uri

        if 'xenon_dcache' in outputdir:
            uri = dcache_uri
            outputdir = outputdir.replace('/xenon_dcache/', '')

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
                    if (c['runlist'] is not None and run_id not in c['runlist']):
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
                        infile = ci_uri + infile_local
                        if not os.path.exists(os.path.join(outputdir, run_name + MV)):
                            os.makedirs(os.path.join(outputdir, run_name + MV))
                            os.chmod(os.path.join(outputdir, run_name + MV), 0o777)

                        outfile = os.path.join(outputdir, run_name + MV, outfile)
                        outfile_full = uri + outfile
                        
                        inner_dag.write(self.inner_dag_template.format(number=run_id, zip_i=i, submit_file = submitfile,
                                                                       infile=infile, outfile_full=outfile_full, run_name=run_name,
                                                                       pax_version = c['pax_version'], zip_name=zip_name,
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

                if not os.path.exists(os.path.join(outputdir, run_name + MV)):
                    if uri == dcache_uri:
                        gfal_mkdir(dcache_uri, os.path.join(outputdir, run_name + MV))
                        # else:
                        #    os.makedirs(os.path.join(outputdir, run_name + MV))
                        #    os.chmod(os.path.join(outputdir, run_name + MV), 0o777)

                if (c['runlist'] is not None and run_id not in c['runlist']):
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

                    outfile = os.path.join(outputdir, run_name + MV, outfile)
                    #os.path.abspath(os.path.join(outputdir, run_name + MV, outfile))
                    outfile_full = uri + outfile
                    inner_dag.write(self.inner_dag_template.format(number=run_id, zip_i=i, submit_file=submitfile,
                                                                   infile=infile, outfile_full=outfile_full,
                                                                   run_name=run_name,
                                                                   pax_version=c['pax_version'], zip_name=zip_name,
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

                if (c['runlist'] is not None and run_id not in c['runlist']):
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
                    infile = midway_uri + midway_location + "/" + zip
                    if not os.path.exists(os.path.join(outputdir, run_name + MV)):
                        if uri == dcache_uri:
                            gfal_mkdir(dcache_uri, outputdir.replace('/xenon_dcache/', ''))
                        else:
                            os.makedirs(os.path.join(outputdir, run_name + MV))
                            os.chmod(os.path.join(outputdir, run_name + MV), 0o777)

                    outfile = os.path.abspath(os.path.join(outputdir,
                                                           run_name, outfile))
                    outfile_full = uri + outfile
                    inner_dag.write(self.inner_dag_template.format(number=run_id, zip_i=i, submit_file=submitfile,
                                                                   infile=infile, outfile_full=outfile_full,
                                                                   run_name=run_name,
                                                                   pax_version=c['pax_version'], zip_name=zip_name,
                                                                   json_file=jsonfile, n_retries=n_retries,
                                                                   on_rucio=on_rucio, dirname = run_name + MV))
                    i += 1


    # for retrieving path to raw data. takes a run doc
    def get_raw_dir(self, doc):
        # loop over data locations, find the path for this host
        path = None
        for entry in doc["data"]:
            if entry["host"] == self.config['host'] and entry["type"] == "raw":
                if entry["status"] != "transferred":
                    print("Run %s raw data is not transferred to %s yet!" % (doc['name'], self.config['host']))
                    return None
                path = entry["location"]
        return path        


    def write_submit_script(self, filename, run_config):
        # merge configs
        run_config['exclude_sites'] = list(set(run_config['exclude_sites'] + self.config['exclude_sites']))

        run_config['specify_sites'] = [site for site in (run_config['specify_sites'] + self.config['specify_sites'])
                                       if site not in run_config['exclude_sites']]

        run_config['specify_sites'] = list(set(run_config['specify_sites']))



        template = """#!/bin/bash
executable = {cax_dir}/osg_scripts/run_xenon.sh
universe = vanilla
Error = {logdir}/pax_$(pax_version)/$(dirname)/$(zip_name).log
Output  = {logdir}/pax_$(pax_version)/$(dirname)/$(zip_name).log
Log     = {logdir}/pax_$(pax_version)/$(dirname)/joblogs/$(zip_name)_$(cluster).joblog

Requirements = {requirements}
request_cpus = $(ncpus)
request_memory = 1900MB
request_disk = 3GB
transfer_input_files = $(json_file), {cax_dir}/osg_scripts/determine_rse.py
transfer_output_files = ""
x509userproxy = {cert}
+WANT_RCC_ciconnect = True
+ProjectName = "xenon1t" 
+AccountingGroup = "group_opportunistic.xenon1t.processing"
when_to_transfer_output = ON_EXIT
transfer_executable = True

periodic_remove =  ((JobStatus == 2) && ((CurrentTime - EnteredCurrentStatus) > (60*60*8)))
arguments = $(name) $(input_file) $(host) $(pax_version) $(pax_hash) $(out_location) $(ncpus) $(disable_updates) $(json_file) $(on_rucio)
queue 1
"""

        if self.config['use_midway']:
            requirements = '(HAS_CVMFS_xenon_opensciencegrid_org)' \
                           '&& (OSGVO_OS_STRING == "RHEL 6" || RCC_Factory == "ciconnect") ' \
                           '&& (VC3_GLIDEIN_VERSION == "0.9.4") \n' \
                           '+MATCH_APF_QUEUE=XENON1T'

        else:
            requirements = "(HAS_CVMFS_xenon_opensciencegrid_org)" \
                           "&& (((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName1) " \
                                    "|| (RCC_Factory == \"ciconnect\") || (GLIDEIN_Site == \"MWT2-COREOS\")) " \
                                "&& ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName2) " \
                                    "|| (RCC_Factory == \"ciconnect\") || (GLIDEIN_Site == \"MWT2-COREOS\")) " \
                                "&& ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName3)  " \
                                    "|| (RCC_Factory == \"ciconnect\") || (GLIDEIN_Site == \"MWT2-COREOS\")) " \
                               "&& ((TARGET.GLIDEIN_ResourceName =!= MY.MachineAttrGLIDEIN_ResourceName4) " \
                                     "|| (RCC_Factory == \"ciconnect\") || (GLIDEIN_Site == \"MWT2-COREOS\"))) " \
                           "&& (OSGVO_OS_STRING == \"RHEL 6\" || RCC_Factory == \"ciconnect\")"

            for site in run_config['exclude_sites']:
                requirements += " && (GLIDEIN_ResourceName =!= \"{site}\")".format(site=site)

            if len(run_config['specify_sites']) > 0:
                specify_string = " && ("
                for i, site in enumerate(run_config['specify_sites']):
                    if i==0:
                        specify_string += "(GLIDEIN_ResourceName == \"{site}\")".format(site=site)
                    else:
                        specify_string += " || (GLIDEIN_ResourceName == \"{site}\")".format(site=site)
                specify_string += ")"
                requirements += specify_string

        script = template.format(logdir = self.config['logdir'],
                                 requirements = requirements,
                                 cax_dir = CAX_DIR,
                                 cert = GRID_CERT)

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
        out = subprocess.Popen(["rucio", "list-file-replicas", dataset], stdout=subprocess.PIPE).stdout.read()
        out = str(out).split("\\n")
        files = set([l.split(" ")[3] for l in out if '---' not in l and 'x1t' in l])
        zip_files = sorted([f for f in files if f.startswith('XENON1T')])
        return zip_files


    def midway_getzips(self, midway_path):
        uri = midway_uri
        out = subprocess.Popen(["gfal-ls","--cert", GRID_CERT, uri + midway_path], stdout=subprocess.PIPE).stdout.read()
        out = str(out.decode("utf-8")).split("\n")
        zips = [l for l in out if l.startswith('XENON')]
        return zips

    def get_rses(self, rucio_name):
        # checks if run with rucio_name is on stash
        out = subprocess.Popen(["rucio", "list-rules", rucio_name], stdout=subprocess.PIPE).stdout.read()
        out = out.decode("utf-8").split("\n")
        rses = []
        for line in out:
            line = re.sub(' +', ' ', line).split(" ")
            if len(line) > 4 and line[3][:2] == "OK":
                rses.append(line[4])
        if len(rses) < 1:
            #raise AssertionError("Problem finding rucio rses")
            print("Problem finding rucio rses")
        return rses

    def FixKeys(self, dictionary):
        for key, value in dictionary.items():
            if type(value) in [type(dict())]:
                dictionary[key] = self.FixKeys(value)
            if '|' in key:
                dictionary[key.replace('|', '.')] = dictionary.pop(key)
        return dictionary

    def outerdag_template(self):
        return """SPLICE {inner_dagname} {inner_dagfile}
JOB {inner_dagname}_noop1 {inner_dagfile} NOOP
JOB {inner_dagname}_noop2 {inner_dagfile} NOOP
SCRIPT PRE {inner_dagname}_noop1 %s/osg_scripts/pre_script.sh {run_name} {pax_version} {number} {logdir} {detector}
SCRIPT POST {inner_dagname}_noop2 %s/osg_scripts/hadd_and_upload.sh {run_name} {pax_version} {number} {logdir} {n_zips} {detector}
PARENT {inner_dagname}_noop1 CHILD {inner_dagname}
PARENT {inner_dagname} CHILD {inner_dagname}_noop2
""" % (CAX_DIR, CAX_DIR)

    def innerdag_template(self):
        return """JOB {number}.{zip_i} {submit_file}
VARS {number}.{zip_i} input_file="{infile}" out_location="{outfile_full}" name="{run_name}" ncpus="1" disable_updates="True" host="login" pax_version="{pax_version}" pax_hash="n/a" zip_name="{zip_name}" json_file="{json_file}" on_rucio="{on_rucio}" dirname="{dirname}"
RETRY {number}.{zip_i} {n_retries}
"""

def gfal_mkdir(uri, path, p=True):
    path = "-p "*p + os.path.join(uri, path)
    cmd = "gfal-mkdir %s" % path
    cmd = cmd.split(" ")
    #print(cmd)
    subprocess.Popen(cmd).communicate()

def gfal_rm(uri, path, r=True):
    path = "-r "*r + os.path.join(uri, path)
    cmd = "gfal-rm %s" % path
    cmd = cmd.split(" ")
    #print(cmd)
    subprocess.Popen(cmd).communicate()
