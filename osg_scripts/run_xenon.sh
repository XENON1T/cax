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
env | grep -i OSG
echo

echo "RUCIO SOURCE SCRIPT"
cat /cvmfs/xenon.opensciencegrid.org/software/rucio-py26/setup_rucio_1_8_3.sh
echo

osg_software="/cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/3.3.26/el6-x86_64/"
start_dir=$PWD

export input_file=$2
export pax_version=$4
jobuuid=`uuidgen`

get_rse () {
	echo "start dir is $start_dir. Here's whats inside"
	ls -l 
	if ls ${start_dir}/*cert* &> /dev/null; then export X509_USER_PROXY=$(ls ${start_dir}/*cert*); fi
	if ls ${start_dir}/*proxy* &> /dev/null; then export X509_USER_PROXY=$(ls ${start_dir}/*proxy*); fi

	echo "Using this proxy: $X509_USER_PROXY"
	unset X509_USER_KEY
	unset X509_USER_CERT
	source /cvmfs/xenon.opensciencegrid.org/software/rucio-py26/setup_rucio_1_8_3.sh
	# source $osg_software/setup.sh					#ALE: remove this line and uncomment above to restore.
	# export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
	# export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
	export RUCIO_HOME=/cvmfs/xenon.opensciencegrid.org/software/rucio-py26/1.8.3/rucio/
	export RUCIO_ACCOUNT=xenon-analysis

	# set GLIDEIN_Country variable if not already
	if [[ -z "$GLIDEIN_Country" ]]
	then
		export GLIDEIN_Country="US"
	fi

	if [[ -e ${start_dir}/determine_rse.py ]]
	then
		echo "python ${start_dir}/determine_rse.py $input_file $GLIDEIN_Country" 
		rse=$(python ${start_dir}/determine_rse.py $input_file $GLIDEIN_Country)
	if [[ $? -ne 0 ]]; then exit 255; fi

	else
		echo "Can't find determine_rse.py script" 
		exit 255
	fi
	export filesize=$(rucio stat $input_file | grep "bytes" | awk '{print $2}')

}

curl_moni () {

	echo "${_condor_GLIDEIN_Site}"
	if [[ -z $GLIDEIN_ClusterId ]]; then 
		GLIDEIN_ClusterId=$(cat $_CONDOR_SCRATCH_DIR/.job.ad | awk '$1=="ClusterId" {print $3}')
	fi
	if [[ -z $GLIDEIN_ProcessId ]]; then
		GLIDEIN_ProcessId=$(cat $_CONDOR_SCRATCH_DIR/.job.ad | awk '$1=="ProcId" {print $3}')
	fi
	if [[ "${_condor_GLIDEIN_Site}" == "\"ruc.ciconnect@uct2-gk.mwt2.org/condor\"" ]]; then
		export OSG_SITE_NAME="MWT2"
	fi
	if [[ "${_condor_GLIDEIN_Site}" == "\"ruc.ciconnect@iut2-gk.mwt2.org/condor\"" ]]; then
		export OSG_SITE_NAME="MWT2"
	fi
	if [[ -z $OSG_SITE_NAME ]]; then
		if [[ -n $GLIDEIN_ResourceName ]]; then
			export OSG_SITE_NAME=$GLIDEIN_ResourceName
		else
			export OSG_SITE_NAME=$HOSTNAME
		fi
		# export OSG_SITE_NAME=$HOSTNAME
	fi
	get_rse
	echo "Moni step $1 with ID $GLIDEIN_ClusterId"
	echo "Moni step $1 at $OSG_SITE_NAME"
	message="{\"job_id\" : \"${GLIDEIN_ClusterId}_${GLIDEIN_ProcessId}_${jobuuid}\", \"type\" : \"x1t-event\",
			  \"job_progress\" : \"$1\", \"executable\" : \"run_xenon.sh\",
			  \"input_filename\": \"$input_file\", \"computing_center\" : \"$OSG_SITE_NAME\",
			  \"pax_version\": \"$pax_version\", \"filesize\": $filesize,
			  \"storage_origin\": \"$rse\", \"timestamp\" : $(date +%s%3N)}"
	echo "Message $message"
	# curl -H "Content-Type: application/json" -X POST -I --retry 5 --retry-delay 0 --retry-max-time 20 -d "$message" http://xenon-logstash.mwt2.org:8080/
	# curl -v -s --retry 5 --retry-delay 0 --retry-max-time 20 --connect-timeout 5 -H "Content-Type: application/json" -X POST -d "$message" http://xenon-logstash.mwt2.org:8080/
	curl -v -s --retry 5 --retry-delay 0 --retry-max-time 20 --connect-timeout 5 -H "Content-Type: application/json" -X POST -d "$message" http://192.170.227.205:8080/
}

#source $osg_software/el6-x86_64/setup.sh			#ALE: maybe this is an error so I canged as below, please uncomment to restore
source $osg_software/setup.sh					#ALE: remove this line and uncomment above to restore.
export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/

# which gfal-copy > /dev/null 2>&1
# if [[ $? -eq 1 ]];
# then
#     source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/current/el6-x86_64/setup.sh
#     export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
#     export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
# else
#     # Not ideal but some sites don't have the gfal config and plug directories =(                                                                                          
#     if [[ ! -d /etc/gfal2.d ]];
#     then
#         if [[ -z $OSG_LOCATION ]]; then
#           OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/current/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
#         fi
#         export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
#     fi
#     if [[ ! -d /usr/lib64/gfal2-plugins/ ]];
#     then
#         if [[ -z $OSG_LOCATION ]]; then
#           OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/current/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
#         fi
#         export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
#     fi
# fi

# if [[ $GLIDEIN_ResourceName = "IN2P3-CC" ]] || [[ $GLIDEIN_ResourcesName = "INFN-T1" ]]
# then
#     source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/current/el6-x86_64/setup.sh
#     export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
#     export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
# fi

echo "start dir is $start_dir. Here's whats inside"
ls -l 

if ls ${start_dir}/*cert* &> /dev/null; then export X509_USER_PROXY=${start_dir}/$(ls *cert*); fi
if ls ${start_dir}/*proxy* &> /dev/null; then export X509_USER_PROXY=${start_dir}/$(ls *proxy*); fi

echo "Using this proxy: $X509_USER_PROXY"
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

# setup rucio commands

echo ${10}

(curl_moni "start downloading") || (curl_moni "start downloading")

if [[ ${10} == 'True' ]]; then

	#sleep $[ ( $RANDOM % 1200 )  + 1 ]s
	echo "Performing rucio download"
	unset X509_USER_KEY
	unset X509_USER_CERT
	source /cvmfs/xenon.opensciencegrid.org/software/rucio-py26/setup_rucio_1_8_3.sh
	export RUCIO_HOME=/cvmfs/xenon.opensciencegrid.org/software/rucio-py26/1.8.3/rucio/
	export RUCIO_ACCOUNT=xenon-analysis

	# set GLIDEIN_Country variable if not already
	if [[ -z "$GLIDEIN_Country" ]]
	then
		export GLIDEIN_Country="US"
	fi

	if [[ -e ${start_dir}/determine_rse.py ]]
	then
		echo "python ${start_dir}/determine_rse.py $2 $GLIDEIN_Country" 
		rse=$(python ${start_dir}/determine_rse.py $2 $GLIDEIN_Country)
	if [[ $? -ne 0 ]]; then exit 255; fi

	else
		echo "Can't find determine_rse.py script" 
		exit 255
	fi

	echo "rucio -T 18000 download $2 --no-subdir --dir ${rawdata_path} --rse $rse"
	download="rucio -v -T 18000 download $2 --no-subdir --dir ${rawdata_path} --rse $rse"

fi

if [[ ${10} == 'False' ]]; then 
	#sleep $[ ( $RANDOM % 600 )  + 1 ]s
	echo "Performing gfal copy"
	download="gfal-copy -v -f -p -t 3600 -T 3600 -K md5 --cert ${start_dir}/user_cert $2 file://${rawdata_path}"

fi
# perform the download
echo "($download) || (sleep 60s && $download) || (sleep 120s && $download)"
($download) || (sleep $[ ( $RANDOM % 60 )  + 1 ]s && $download) || (sleep $[ ( $RANDOM % 120 )  + 1 ]s && $download) || exit 1 #(sleep $[ ( $RANDOM % 180 )  + 1 ]s && $download) || (sleep $[ ( $RANDOM % 240 )  + 1 ]s && $download) || exit 1

export PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/bin:$PATH
cd /cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/pax_$4_OSG/

if [[ $? -ne 0 ]];
then 
	exit 255
fi 

source activate pax_$4_OSG
echo $PYTHONPATH

export LD_LIBRARY_PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/pax_$4_OSG/lib:$LD_LIBRARY_PATH
export API_USER='ci-connect'
export API_KEY=5ac3ed84c1ed8210c84f4d70f194161a64758e29

(curl_moni "end downloading") || (curl_moni "end downloading")

mkdir $start_dir/output/
echo "output directory: ${start_dir}/output"
cd $start_dir
echo 'Processing...'

stash_loc=$6

(curl_moni "start processing") || (curl_moni "start processing")

echo "cax-process $1 $rawdata_path $3 $4 output $7 $8 $start_dir/${json_file}" 
cax-process $1 $rawdata_path $3 $4 output $7 $8 ${start_dir}/${json_file}

if [[ $? -ne 0 ]];
then 
	echo "exiting with status 255"
	exit 255
fi

pwd
ls output
echo ${start_dir}/output/$1.root

source deactivate


#source $osg_software/el6-x86_64/setup.sh  		#ALE: this seems an error uncomment this to restore.
source $osg_software/setup.sh  				#ALE: remove this line to restore.
export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
# export GFAL_CONFIG_DIR=/cvmfs/xenon.opensciencegrid.org/software/etc/gfal2.d/
export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/

(curl_moni "end processing") || (curl_moni "end processing")

# else
	# Not ideal but some sites don't have the gfal config and plug directories =(                                                                                          
# if [[ ! -d /etc/gfal2.d ]];
# then
#     OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/3.3.25/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
#     export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
# fi
# if [[ ! -d /usr/lib64/gfal2-plugins/ ]];
# then
#     OSG_LOCATION=`source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/3.3.25/el6-x86_64/setup.sh; env | grep OSG_LOCATION | cut -f 2 -d=`
#     export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/
# fi
# fi

echo "---- Test line ----"
echo "Processing done, here's what's inside the output directory:"
outfile=$(ls ${start_dir}/output/)
echo "-----"
ls ${start_dir}/output/*.root
echo "-----"
echo "outfile: $outfile"
echo "Arg 6: $stash_loc"
echo $GFAL_CONFIG_DIR
echo $GFAL_PLUGIN_DIR
export GFAL2_GRIDFTP_CHECKSUM_CALC_TIMEOUT=36000
export GFAL2_GRIDFTP_TIMEOUT=36000
export GFAL2_GRIDFTP_OPERATION_TIMEOUT=400
echo $(which gfal-copy)

if ls ${start_dir}/*cert* &> /dev/null; then export X509_USER_PROXY=${start_dir}/$(ls *cert*); fi
if ls ${start_dir}/*proxy* &> /dev/null; then export X509_USER_PROXY=${start_dir}/$(ls *proxy*); fi

echo "Using this proxy: $X509_USER_PROXY"

echo $X509_USER_PROXY
unset X509_USER_KEY
unset X509_USER_CERT
# echo "time gfal-copy --cert ${outfile} -T 36000 -t 36000 -f -p --checksum md5 file://${out_file} ${stash_loc}"
upload_cmd="gfal-copy -v -T 36000 -t 36000 -f -p --checksum md5 file://${start_dir}/output/${outfile} ${stash_loc}" 

upload ()
{
  gfal-copy -v -T 36000 -t 36000 -f -p --checksum md5 file://${start_dir}/output/${outfile} ${stash_loc}
}

echo $upload_cmd

(curl_moni "start uploading") || (curl_moni "start uploading")

(upload) || (sleep 30s && upload) || (sleep 60s && upload) || (echo "upload failed" && exit 255)
if [[ $? -ne 0 ]];
then
	echo "upload failed" 
	exit 255
fi

(curl_moni "end uploading") || (curl_moni "end uploading")

rm -rf $work_dir
rm -rf ${start_dir}/output


