# OSG User Guide for Xenon Collaboration


## UChicago CI Connect

We have set up UChicago CI Connect to submit jobs to OSG through the UChicago MWT2 infrastructre. CI Connect is a generalized version of the OSG Connect service which was originally developed for research labs lacking experience with the grid. It allows for access to the OSG through an the UChicago MWT2 HTCondor pool. 

### Account Setup

Using your CNetID you can [sign up for a CI connect account](https://ci-connect.uchicago.edu/signup). Once your account is approved and you are added to the Xenon VO and group on CI Connect you can access the submit host at `login.ci-connect.uchicago.edu`. 

## High-Throughput Computing and HTCondor

CI Connect uses [High-Throughput Condor](http://research.cs.wisc.edu/htcondor/manual/v8.5/index.html) (HTCondor or historically Condor) as a job scheduler. Most campus clusters, like RCC, or High-Performance Computing centers, like NERSC, use PBS or one of its derivatives (SLURM and LSF, for example). HTCondor, as the name suggests, is based on the [high-throughput computing model](https://en.wikipedia.org/wiki/High-throughput_computing). In this model, is based on one assumes that the resources are indepdent of each other and distributed (either across multiple local servers or across servers around the world), tasks require a single CPU core (can be scaled to single nodes in newer HTCondor versions), and that every job is independent of the other. By contrast, the [high-performance computing model](https://en.wikipedia.org/wiki/High-performance_computing) assumes that in most cases a given job requires an aggregate of multiple CPU cores and/or nodes working together/

### Job Submission in HTCondor

Job submission in HTCondor is different than in PBS and its derivatives. In PBS, etc. one submits a bash-like script, which defines the job resource requirements and the commands that should be executed in a single script. HTCondor separates the job execution script or program from the submission file. A job submission file is a bash-like script that defines the job requirements. In broad terms the submit file defines which files should be transferred through the HTCondor file transfer mechanism (only useful for small files), what the executable is, what the arguments to the executable are, what the job requirements are, and how many of this type to queue. 

A sample submit file is given below:

```
#!/bin/bash
# executable is the location of the executable
executable = /path/to/executable

# universe is into which queue the job should be submitted into
universe = vanilla

# Location where log files should be put. The variable 
# `$(cluster)` is the unqiue ID assigned by HTCondor
# to the job. 
Error = /path/to/log/files/std_error_$(cluster)_log
Output  = /path/to/log/files/std_output_$(cluster)_log
Log     = /path/to/log/files/condor_job_$(cluster)_log

# Job requirements 
# Example: `(GLIDEIN_ResourceName =!= "NPX")` means that the 
#          advertised resource name cannot match "NPX"
# Example: `(OpSysAndVer == "CentOS6" || OpSysAndVer == "RedHat6" || OpSysAndVer == "SL6")` 
#          means that the advertised `OpSysAndVer` 
#          (or operation system and version) is RHEL6 or one of this derivaties
Requirements = ((OpSysAndVer == "CentOS6" || OpSysAndVer == "RedHat6" || OpSysAndVer == "SL6") && (GLIDEIN_ResourceName =!= "NPX"))

# Special condition to get access to the 
# Friends of MWT2 queue available from
# login.ci-connect.uchicago.edu
+WANT_RCC_ciconnect = True
# `request_cpus` tells HTCondor how many CPU cores
# this jobs needs, default value is 1
request_cpus = $(ncpus)
# `request_memory` tells HTCondor how much 
# memory (in MB) are needed. 
# Here 2 GB are requested, which is the
# default for OSG. 
request_memory = 2048 
# `request_disk` tells HTCondor how much 
# memory (in KB) are needed.
# Here 1 GB of disk are requested. 
request_disk = 1048576
# `transfer_input_files` defines which files should
# be transfered to the remote worker from the
# local machine
transfer_input_files = /home/briedel/user_cert
# `transfer_output_files` similar to `transfer_input_files`
# this allows one to define which files should be transfered
# back to the local host
transfer_output_files = ""
# `when_to_transfer_output` defines when the output should be transferred
when_to_transfer_output = ON_EXIT
# Whether or not to transfer executable to remote location
transfer_executable = True
# Defines possible arguments to the executable defined above
arguments = <job_name> <input_file> <host> <pax_version> <pax_hash> <output_file> <number of cpus> <disable database update>
# Defines the end of the jb submission file and tells HTCondor
# to queue a job. The number, in this example, 1
# signifies how many copies of the job should be submitted.
# For example, the line `queue 5` will submit 5 copies
# of the job.
queue 1
```

The `executable` can be any piece of code that can be run in the shell, for example, compiled binary, python script, or bash script. For OSG jobs, it is recommended to use a bash script as they typically require the transfer of input or output data, and setting up an environment.

To submit a your submit file to the HTCondor file simple execute

`condor_submit /path/to/your/submit/file`


### Job Monitoring in HTCondor

There are several ways to monitor a job in HTCondor. The most general is the HTCondor `qstat` replacement: `condor_q`. `condor_q` provides more options than `qstat`. To check only your jobs, either running, queued, or held, one can use:

`condor_q <optional username> <optional job id>`

To see only running jobs:

`condor_q -r <optional username> <optional job id>`

To see only idle jobs:

`condor_q -constraint 'JobStatus =?= 1 && Owner =?= "<username>"'`

To see held jobs:

`condor_q -constraint 'JobStatus =?= 5 && Owner =?= "<username>"'`

For a list of `JobStatus` codes see, [here](http://pages.cs.wisc.edu/~adesmet/status.html). 

Jobs will not run because the requirements are too restrictive. To figure out if this is the case you can use this command:

`condor_q <job id> -better-analyze -pool osg-flock.grid.iu.edu`


## Jobs on OSG

Getting jobs running with HTCondor and OSG has a fairly step learning curve. The two main issues that a user faces are the distribution of software and transfer of input and/or output data. 

### CVMFS

To distribute the Xenon software, we have established an external OASIS CVMFS repository for Xenon. It is available at OSG sites under `/cvmfs/xenon.opensciencegrid.org/`. To make changes to repository, please contact Patrick de Perio or Evan Shockley.

### Data Transfer

Note: This tutorial is under the assumption that your files are not in the Rucio catalog. If they are, please read the Rucio guide.

To transfer data in and out for the job, we recommend using Grid File Access Libray (gfal). gfal is a packages that allows users to use the different grid file transfer protocols, mainly SRM and GridFTP, under one unified interface. Using gfal requires having a grid certificate that is registered with the Xenon VO, and the Uniform Resource Identifier (URI) of the input and/or output file. 

Instructions on how to obtain a grid certificate can be found [here](https://twiki.grid.iu.edu/bin/view/Documentation/CertificateGetWeb). After the grid certificate is approved and issued, one can register with the VO [here](https://wiki.biggrid.nl/biggrid/index.php/Xenon).

The URI of a file can be most readily described as the GridFTP/SRM/POSIX-like URL of the file. A sample file URI is `gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm/xenon/briedel_transfers/raw/160804_1333/XENON1T-1932-000001000-000001999-000001000.zip`. It is split into three parts. The first portion defines the transfer protocol, i.e. `srm://` or `gsiftp://`. The second portion provides the server and port the file should be transferred from, i.e. `gridftp.grid.uchicago.edu:2811` or `srm1.rcc.uchicago.edu:8443`. The final portion, i.e. `/cephfs/srm/xenon/briedel_transfers/raw/160804_1333/XENON1T-1932-000001000-000001999-000001000.zip` is the POSIX path of the file on the GridFTP/SRM server. To access a local file or define where the file should be put on the local file system the URI becomes, for example, `file:///path/to/file/on/local/system`.

Putting all of this together some example gfal commands are:

To transfer files to the worker node from remote server, i.e. transfer input files:

`gfal --cert /path/to/user_cert -T 36000 -t 36000 --checksum md5 gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm/xenon/briedel_transfers/raw/160804_1333/XENON1T-1932-000004000-000004999-000001000.zip file:///path/to/local/work/input/directory/`

To transfer files from worker to remote server, i.e. transfer output files:

`gfal-copy --cert /path/to/user_cert -T 36000 -t 36000 -f --checksum md5 file:///path/to/local/work/input/directory/example_file.root gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm/xenon/path/to/output/file/folder/example_file_renamed.root`

`--cert` points gfal to the certificate file that is generated by running `grid-proxy-init` or `voms-proxy-init`. `-T` and `-t` increase the time out time fom the default 1800 seconds to 36000 seconds to accomodate large file transfers. `-f` forces to overwrite the destination file. `--checksum md5` checks that the MD5 checksum of the transfered file is consisted with original file. Other options for `--checksum` is `ADLER32` or `CRC32`.


### Sample Executable

In general, a executable for an OSG consists of three main parts:

1. Transfering input data and, if necessary, executable with dependencies, to the worker node
2. Running code
3. Transfering output data

A generic sample executable using CVMFS would be:

```
#!/bin/bash

# Takes 2 arguments:
# 1 - Input file URI
# 2 - Output file URI


source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh
export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/

start_dir=$PWD
if [ "${OSG_WN_TMP}" == "" ];
then
    OSG_WN_TMP=$PWD
fi

mkdir $PWD/tmp/
work_dir=`mktemp -d --tmpdir=$PWD/tmp/`
echo $work_dir
cd ${work_dir}

gfal-copy --cert ${start_dir}/user_cert -T 36000 -t 36000 --checksum md5 $1 file://${work_dir}

mkdir ${work_dir}/output

# pseudo code
source environment

./script.sh  ${work_dir}/input.file ${work_dir}/output/output.file

# pseudo code
clean ennvironment

gfal-copy --cert ${start_dir}/user_cert -T 36000 -t 36000 --checksum md5 file://${work_dir}/output/output.file $2
```

Below is a sample bash script that acts the executable for processing raw data with `pax` using `cax-process`.   

```
#!/bin/bash

## Takes 8 arguments:

# 1 - name of run being processed
# 2 - uri of input raw data file
# 3 - host
# 4 - pax version
# 5 - pax hash
# 6 - uri for output root file
# 7 - number of cpus used to process
# 8 - disable_updates  

# Printing hostname and environment
# Good to have for debugging, in case a site causes issues
# and needs to blacklisted or to test whether to 
# whitelist a site again. 
echo $HOSTNAME
env

###
# Transfering Input
###

# Testing whether gfal is available
which gfal-copy > /dev/null 2>&1
if [[ $? -eq 1 ]];
then
    # if gfal is not available, we get it out of cvmfs
    source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh
    export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
    export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
else
    # Not ideal but some sites don't have the gfal config and plug directories =(                                                                                          
    if [[ ! -d /etc/gfal2.d ]];
    then
        if [[ -z $OSG_LOCATION ]]; then
          OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
        fi
        export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
    fi
    if [[ ! -d /usr/lib64/gfal2-plugins/ ]];
    then
        if [[ -z $OSG_LOCATION ]]; then
          OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
        fi
        export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
    fi
fi

# Creating directories to work in
start_dir=$PWD
if [ "${OSG_WN_TMP}" == "" ];
then
    OSG_WN_TMP=$PWD
fi

mkdir $PWD/tmp/
work_dir=`mktemp -d --tmpdir=$PWD/tmp/`
echo $work_dir
cd ${work_dir}

# loop and use gfal-copy before pax gets loaded to avoid
# gfal using wrong python version/libraries    

input_file=$2
processing_path=${work_dir}/$1
mkdir $processing_path

cd ${processing_path}
pwd
cd ${work_dir}

gfal-copy --cert ${start_dir}/user_cert -T 36000 -t 36000 --checksum md5 $2 file://${processing_path}

###
# Processing
###

# Getting Xenon CVMFS environment

export PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/bin:$PATH
cd /cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/evan-testing/
# Checking whether /cvmfs/xenon.opensciencegrid.org exists
if [[ $? -ne 0 ]];
then 
    exit 255
fi 
# Loading environment
source activate evan-testing

# API Mongo DB keys
export API_USER='ci-connect'
export API_KEY=5ac3ed84c1ed8210c84f4d70f194161a64758e29

# Getting to work space
mkdir $start_dir/output/
echo "output directory: ${start_dir}/output"
cd $start_dir
# Doing the work
echo 'Processing...'
cax-process $1 $processing_path $3 $4 $5 output $7 $8
# Making sure that cax-process didn't fail 
if [[ $? -ne 0 ]];
then 
    exit 255
fi

###
# Transfering Output
###

pwd
ls output
echo ${start_dir}/output/$1.root
# Cleaning out environment, so we can transfer back out
source deactivate

# Setting up the environment 
source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh
export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
# Not ideal but some sites don't have the gfal config and plug directories =(
if [[ ! -d /etc/gfal2.d ]];
then
    OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
    export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
fi
if [[ ! -d /usr/lib64/gfal2-plugins/ ]];
then
    OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
    export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
fi

# Transfering data out
gfal-copy --cert ${start_dir}/user_cert -T 36000 -t 36000 -f --checksum md5 file://${start_dir}/output/$1.root $6 
if [[ $? -ne 0 ]];
then 
    exit 255
fi
# Cleaning up
rm -rf $work_dir
rm -rf ${start_dir}/output
```

### Best Practices on OSG with HTCondor

The volatile and distributed nature of OSG resouces make it hard to run jobs with "non-standard" resource requirements, either in terms of number of CPUs, RAM, disk space, or run time. In general, we recommend the following:

* Input File size: < 10 GB
* Output File size: < 10 GB
* Run time: 10 min to 12 hours
* CPU: preferably 1 core, up to 8 core
* RAM: preferably 2 GB/core, up to 24 GB/core

### Friends of MWT2

We have added a special "queue" for Xenon1T. This will provide about 15% of the MWT2 pool towards users in designted VOs, at the moment this is Xenon and SPT. 

### Large Processing Needs - DAG and DAGMan

In a typical HTCondor works flow one deals with with multiple 1000s, 10000s, and even 100000s of jobs that may have inter-dependencies. The management, monitoring, and bookkeeping of such a large number of jobs, especially when there are job inter-dependencies, can be a daunting task for any user. Besides this there is also a job scheduler (as with any piece of software) stability aspect to keep in mind. With the large user base of OSG Connect and CI Connect, and each user submitting jobs, submitting a large batch of jobs into the HTCondor queue can lead to stability issues with scheduling jobs or accepting new jobs. To relief the user from the job management, monitoring, and bookkeeping task as well as allow the user to submit large batches of jobs in more manageable increments, HTCondor includes [Directed Acyclic Graph Manager (DAGMan)](http://research.cs.wisc.edu/htcondor/manual/latest/2_10DAGMan_Applications.html). 

Setting up a Directed Acyclic Graph (DAG) in HTCondor is a straightforward task. The most basic DAG file consists of

```
JOB xenon.0 xenon.submit
JOB xenon.1 xenon.submit
JOB xenon.2 xenon.submit
JOB xenon.3 xenon.submit
...
```

In the above example, job is defined through the `JOB <unqiue job ID> /path/to/your/submit/file` sequence. The `<unique job ID>`, `xenon.0`, `xenon.1`, and `xenon.2` in the above example, only has to be unqiue to the DAG itself and can be reused.

In a normal processing situation there is different input parameters for every job, for example every job has a different input and output file. In a DAG file, these input variables can are defined through the `VARS <unqiue job ID> <sequence of input variables>`. This changes the above example to:

```
JOB xenon.0 xenon.submit
VARS xenon.0 inputfile="/path/to/input/file0.zip" outputfile="/path/to/output/file0.root"
JOB xenon.1 xenon.submit
VARS xenon.1 inputfile="/path/to/input/file1.zip" outputfile="/path/to/output/file1.root"
JOB xenon.2 xenon.submit
VARS xenon.2 inputfile="/path/to/input/file2.zip" outputfile="/path/to/output/file2.zip"
JOB xenon.3 xenon.submit
VARS xenon.3 inputfile="/path/to/input/file3.zip" outputfile="/path/to/output/file3.zip"
...
```

Note: The `""` around the variable values are required, even if assigning numerical values.

Adding these input variables also requires changes to the submit file.  In the above example submit file, the hard coded input and output file are replaced by bash-like variables. The line 

```
arguments = <job_name> <input_file> <host> <pax_version> <pax_hash> <output_file> <number of cpus> <disable database update>
```

changes to

```
arguments = <job_name> $(inputfile) <host> <pax_version> <pax_hash> $(outputfile) <number of cpus> <disable database update>
```

A job maybe evicted randomly without the job actually failing or a job may fail at one site and not another because of the somewhat volatile nature of OSG resources. For this reason we recommend retrying a job multiple times before considering it failed. To add the retry mechanism to a DAG file, the line `Retry <unqiue job ID> <number of retries>` is added, such that

```
JOB xenon.0 xenon.submit
VARS xenon.0 inputfile="/path/to/input/file0.zip" outputfile="/path/to/output/file0.root"
Retry xenon.0 5
JOB xenon.1 xenon.submit
VARS xenon.1 inputfile="/path/to/input/file1.zip" outputfile="/path/to/output/file1.root"
Retry xenon.1 5
JOB xenon.2 xenon.submit
VARS xenon.2 inputfile="/path/to/input/file2.zip" outputfile="/path/to/output/file2.zip"
Retry xenon.2 5
JOB xenon.3 xenon.submit
VARS xenon.3 inputfile="/path/to/input/file3.zip" outputfile="/path/to/output/file3.zip"
Retry xenon.3 5
...
```

retries every job at least 5 times before considering it failed.

To define job inter-dependencies in a DAG, one needs to add the line `PARENT <list of unqiue job ID parents> CHILD <list of unique job IDs of children>`. For example, `xenon.1` and `xenon.2` depend on `xenon.0` to finish, this would require to add `PARENT xenon.0 CHILD xenon.1 xenon.2` to the DAG file, such that the DAG file looks like:

```
JOB xenon.0 xenon.submit
VARS xenon.0 inputfile="/path/to/input/file0.zip" outputfile="/path/to/output/file0.root"
Retry xenon.0 5
JOB xenon.1 xenon.submit
VARS xenon.1 inputfile="/path/to/input/file1.zip" outputfile="/path/to/output/file1.root"
Retry xenon.1 5
JOB xenon.2 xenon.submit
VARS xenon.2 inputfile="/path/to/input/file2.zip" outputfile="/path/to/output/file2.zip"
Retry xenon.2 5
JOB xenon.3 xenon.submit
VARS xenon.3 inputfile="/path/to/input/file3.zip" outputfile="/path/to/output/file3.zip"
Retry xenon.3 5
PARENT xenon.0 CHILD xenon.1 xenon.2
...
```

Similarly we can say add the requirement that `xenon.3` depends on `xenon.1` and `xenon.2` finishing successfully. Adding the line `PARENT xenon.1 xenon.2 CHILD xenon.3` will accomplish this:


```
JOB xenon.0 xenon.submit
VARS xenon.0 inputfile="/path/to/input/file0.zip" outputfile="/path/to/output/file0.root"
Retry xenon.0 5
JOB xenon.1 xenon.submit
VARS xenon.1 inputfile="/path/to/input/file1.zip" outputfile="/path/to/output/file1.root"
Retry xenon.1 5
JOB xenon.2 xenon.submit
VARS xenon.2 inputfile="/path/to/input/file2.zip" outputfile="/path/to/output/file2.zip"
Retry xenon.2 5
JOB xenon.3 xenon.submit
VARS xenon.3 inputfile="/path/to/input/file3.zip" outputfile="/path/to/output/file3.zip"
Retry xenon.3 5
PARENT xenon.0 CHILD xenon.1 xenon.2
PARENT xenon.1 xenon.2 CHILD xenon.3
...
```

The order of the `JOB`, `VAR`, `Retry`, and `PARENT/CHILD` in the file does not matter. The ordering in the example is personal preference. For more complicated example for defining job inter-dependency, [see the documentation for DAGMan](http://research.cs.wisc.edu/htcondor/manual/latest/2_10DAGMan_Applications.html). 

To submit a DAGman to the HTCondor queue 

`condor_submit_dag /path/to/your/dag/file`

One can add additional configuration parameters to a DAGman submission either through the command line, use `condor_submit_dag -help` to see the options, or adding a config file. To add a config file, 

`condor_submit_dag -config /path/to/dagman/config/file /path/to/your/dag/file`

where an example DAGman config file is:

```
DAGMAN_MAX_JOBS_SUBMITTED=30000
DAGMAN_MAX_SUBMITS_PER_INTERVAL=10000
DAGMAN_USER_LOG_SCAN_INTERVAL=1
```

In the above example:

* `DAGMAN_MAX_JOBS_SUBMITTED`: Sets the maximum number of jobs this DAG can have in the queue, i.e. 30000. 
* `DAGMAN_MAX_SUBMITS_PER_INTERVAL`: Sets how many jobs can be submitted per interval, i.e. 10000.
* `DAGMAN_USER_LOG_SCAN_INTERVAL`: How often a job submission interval occurs in seconds, i.e. every second.


DAGMan's job monitoring abilities allow it to produce a "rescue" DAG. The "rescue" DAG contains all the jobs failed after the requested number of retries or the DAGman jobs that had not yet completed when the DAG was removed from the queue. The "rescue" DAG follows the naming pattern of `<dag_file_name>.rescueXXX` where "XXX" is simply a counter starting at 1 or in this case 001. To resubmit the jobs that failed one simply has to submit the DAG file again, i.e. `condor_submit_dag -config /path/to/dagman/config/file /path/to/your/dag/file`.

