#!/bin/bash

## takes 8 arguments: ##

  # 1 - name of run being processed
  # 2 - uri of input raw data file
  # 3 - host
  # 4 - pax version
  # 5 - pax hash
  # 6 - uri for output root file
  # 7 - number of cpus used to process
  # 8 - disable_updates  

echo $HOSTNAME
env
df -h


which gfal-copy > /dev/null 2>&1
if [[ $? -eq 1 ]];
then
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


start_dir=$PWD
if [ "${OSG_WN_TMP}" == "" ];
then
    OSG_WN_TMP=$PWD
fi
#/bin/env 
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
#gfal-copy --cert ${start_dir}/user_cert ${input_files[index]} file://${work_dir}/$input_filename

gfal-copy --cert ${start_dir}/user_cert -T 36000 -t 36000 --checksum md5 $2 file://${processing_path}

#module load pax/evan-testing
export PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/bin:$PATH
cd /cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/evan-testing/
if [[ $? -ne 0 ]];
then 
    exit 255
fi 
source activate evan-testing

#export MONGO_PASSWORD=luxstinks
export API_USER='ci-connect'
export API_KEY=5ac3ed84c1ed8210c84f4d70f194161a64758e29

mkdir $start_dir/output/
echo "output directory: ${start_dir}/output"
cd $start_dir
echo 'Processing...'
cax-process $1 $processing_path $3 $4 $5 output $7 $8
if [[ $? -ne 0 ]];
then 
    exit 255
fi
# mv ${processing_path}/*.root $start_dir/output/
pwd
ls output
echo ${start_dir}/output/$1.root
source deactivate

source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh
export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
# else
    # Not ideal but some sites don't have the gfal config and plug directories =(                                                                                          
if [[ ! -d /etc//etc/gfal2.d ]];
then
    OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
    export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
fi
if [[ ! -d /usr/lib64/gfal2-plugins/ ]];
then
    OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
    export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
fi
# fi

gfal-copy --cert ${start_dir}/user_cert -T 36000 -t 36000 -f --checksum md5 file://${start_dir}/output/$1.root $6 
if [[ $? -ne 0 ]];
then 
    exit 255
fi
# 'gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm/xenon/ershockley/processed/'
rm -rf $work_dir
rm -rf ${start_dir}/output
