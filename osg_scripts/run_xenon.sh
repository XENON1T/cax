#!/bin/bash

## takes 11 arguments: ##

  # 1 - name of run being processed
  # 2 - uri of input raw data file
  # 3 - host
  # 4 - pax version
  # 5 - pax hash
  # 6 - uri for output root file
  # 7 - number of cpus used to process
  # 8 - disable_updates  
  # 9 - json file
  # 10 - on rucio? (True or False)
  # 11 - rucio rse

echo $@
echo
echo $HOSTNAME
echo
echo $LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/pax_$4_OSG/lib:$LD_LIBRARY_PATH
export GFAL2_GRIDFTP_DEBUG=1
echo $LD_LIBRARY_PATH
# df -h
echo 
env | grep -i glidein 
echo

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

echo "start dir is $start_dir. Here's whats inside"
ls -l *user_cert*
ls -l  *.json

json_file=$(ls *json)

echo ""
if [ "${OSG_WN_TMP}" == "" ];
then
    OSG_WN_TMP=$PWD
fi
#/bin/env 
mkdir $PWD/tmp/
work_dir=`mktemp -d --tmpdir=$PWD/tmp/`
echo $work_dir
cd ${work_dir}
export XDG_CACHE_HOME=${work_dir}/.cache
export XDG_CONFIG_HOME=${work_dir}/.config
# $XDG_DATA_DIRS
# loop and use gfal-copy before pax gets loaded to avoid
# gfal using wrong python version/libraries    

input_file=$2
rawdata_path=${work_dir}/$1
mkdir $rawdata_path

cd ${rawdata_path}
pwd
cd ${work_dir}
#gfal-copy --cert ${start_dir}/user_cert ${input_files[index]} file://${work_dir}/$input_filename

#gfal-copy --cert ${start_dir}/user_cert -T 36000 -t 36000 --checksum md5 $2 file://${rawdata_path}

# setup rucio commands

echo ${10}

if [[ ${10} == 'True' ]]; then

    sleep $[ ( $RANDOM % 1200 )  + 1 ]s
    echo "Performing rucio download"
    unset X509_USER_KEY
    unset X509_USER_CERT
    source /cvmfs/xenon.opensciencegrid.org/software/rucio-py26/setup_rucio_1_8_3.sh
    export RUCIO_HOME=/cvmfs/xenon.opensciencegrid.org/software/rucio-py26/1.8.3/rucio/
    export RUCIO_ACCOUNT=ershockley
    export X509_USER_PROXY=${start_dir}/user_cert

    env | grep X509

    if [[ -z ${11} ]]; then
	echo "rucio -T 18000 download $2 --no-subdir --dir ${rawdata_path} --rse UC_OSG_USERDISK"
	download="rucio -T 18000 download $2 --no-subdir --dir ${rawdata_path} --rse UC_OSG_USERDISK"
	#rucio -T 18000 download $2 --no-subdir --dir ${rawdata_path} --rse UC_OSG_USERDISK

    else 
	echo "rucio -T 18000 download $2 --no-subdir --dir ${rawdata_path} --rse ${11}"
	download="rucio -T 18000 download $2 --no-subdir --dir ${rawdata_path} --rse ${11}"
    fi
    #rucio --certificate ${start_dir}/user_cert download $2 --no-subdir --dir ${rawdata_path}
fi

if [[ ${10} == 'False' ]]; then 
    #sleep $[ ( $RANDOM % 600 )  + 1 ]s
    echo "Performing gfal copy"
    download="gfal-copy -v -f -p -t 3600 -T 3600 -K md5 --cert ${start_dir}/user_cert $2 file://${rawdata_path}"

fi
# perform the download
echo "($download) || (sleep 60s && $download) || (sleep 120s && $download)"
($download) || (sleep $[ ( $RANDOM % 60 )  + 1 ]s && $download) || (sleep $[ ( $RANDOM % 120 )  + 1 ]s && $download) || (sleep $[ ( $RANDOM % 180 )  + 1 ]s && $download) || (sleep $[ ( $RANDOM % 240 )  + 1 ]s && $download) || exit 1

export PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/bin:$PATH
cd /cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/pax_$4_OSG/
#cd /cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/evan-testing/
if [[ $? -ne 0 ]];
then 
    exit 255
fi 
source activate pax_$4_OSG
echo $PYTHONPATH
#export LD_LIBRARY_PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/evan-testing/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/pax_$4_OSG/lib:$LD_LIBRARY_PATH
export API_USER='ci-connect'
export API_KEY=5ac3ed84c1ed8210c84f4d70f194161a64758e29

mkdir $start_dir/output/
echo "output directory: ${start_dir}/output"
cd $start_dir
echo 'Processing...'

stash_loc=$6

echo "cax-process $1 $rawdata_path $3 $4 output $7 $8 $start_dir/${json_file}" 
cax-process $1 $rawdata_path $3 $4 output $7 $8 ${start_dir}/${json_file}

if [[ $? -ne 0 ]];
then 
    echo "exiting with status 255"
    exit 255
fi
# mv ${rawdata_path}/*.root $start_dir/output/
pwd
ls output
echo ${start_dir}/output/$1.root
#out_file=${start_dir}/output/$1.root
source deactivate

source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el6-x86_64/setup.sh
export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
# else
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
# fi


echo "---- Test line ----"
echo "Processing done, here's what's inside the output directory:"
outfile=$(ls ${start_dir}/output/)
echo "outfile: $outfile"
echo "Arg 6: $6"
echo "time gfal-copy --cert ${outfile} -T 36000 -t 36000 -f -p --checksum md5 file://${out_file} ${stash_loc}"
time gfal-copy --cert ${start_dir}/user_cert -T 36000 -t 36000 -f -p --checksum md5 file://${start_dir}/output/${outfile} ${stash_loc} 

if [[ $? -ne 0 ]];
then 
    echo "exiting with status 255"
    exit 255
fi
# 'gsiftp://gridftp.grid.uchicago.edu:2811/cephfs/srm/xenon/ershockley/processed/'
rm -rf $work_dir
rm -rf ${start_dir}/output
