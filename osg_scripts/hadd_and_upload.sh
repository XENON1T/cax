#!/bin/bash

# 1: run name
# 2: pax version
# 3: run number
# 4: general log directory
# 5: number of zips
# 6: detector

# make tmp directory and go there
workdir=$(mktemp -d)
cd $workdir
run="${1##*/}"
post_log=$4/pax_$2/$run/POST_LOG
if [[ $6 == 'muon_veto' ]]; then
    post_log=$4/pax_$2/${run}_MV/POST_LOG  
fi

echo "------ Start of post script ---------" >> $post_log
date >> $post_log

echo $@ >> $post_log

if [[ $6 == "muon_veto" ]]
then
    rootfile=/xenon/xenon1t_processed/pax_$2/$1_MV.root
else
    rootfile=/xenon/xenon1t_processed/pax_$2/$1.root
fi

source activate pax_$2_OSG
source $HOME/cax/osg_scripts/setup_rucio.sh

#export PYTHONPATH=/xenon/cax:$PYTHONPATH

#cax_dir=/xenon/cax
cax_dir=$HOME/cax

#if [[ ! -e $rootfile ]]; then
rawdir=$1

# save exit status of pipe
set -o pipefail

	#echo "python ${cax_dir}/osg_scripts/upload.py $1 $5 $2 $6" >> $post_log 2>&1	
python ${cax_dir}/osg_scripts/upload.py $1 $5 $2 $6 >> $post_log 2>&1

if [[ $? -ne 0 ]]; then
    echo "hadd or DB stuff failed, exiting" >> $post_log
    exit 1
#fi
fi	  

if [[ $2 == 'v6.8.0' ]]; then
    echo "SKIPPING TRANSFERS FOR v6.8.0"
    exit 0
fi

# transfer to midway

echo "Beginning cax transfer to midway" >> $post_log

if [[ $6 == 'tpc' ]]; then
    cax --once --run $3 --api --config ${cax_dir}/cax/cax_transfer.json >> $post_log 2>&1
fi

if [[ $6 == 'muon_veto' ]]; then
    cax --once --name $1 --api  --config ${cax_dir}/cax/cax_transfer.json >> $post_log 2>&1
fi

if [[ $? -ne 0 ]]; then
    exit 1
fi

# need to get the rucio did for this run
did=$(${cax_dir}/osg_scripts/get_rucio_did.py $3)

if [[ $? -ne 0 ]]; then
    echo "Error finding rucio did"
    exit 1
fi


# submit massive-cax job to verify transfer
if [[ $6 == 'tpc' ]]; then
    echo "Submitting massive cax job on midway for tpc run $3" >> $post_log 
    echo "ssh ershockley@midway2-login1.rcc.uchicago.edu -o StrictHostKeyChecking=no '/project/lgrandi/general_scripts/verify_stash_transfers.sh $3 $2 $did $6'" >> $post_log 2>&1
    ssh ershockley@midway2-login1.rcc.uchicago.edu -o StrictHostKeyChecking=no "/project/lgrandi/general_scripts/verify_stash_transfers.sh $3 $2 $did $6" >> $post_log 2>&1

    ex=$?
fi

if [[ $6 == 'muon_veto' ]]; then
    echo "Submitting massive cax job on midway for MV run $1" >> $post_log
    echo "ssh ershockley@midway2-login1.rcc.uchicago.edu -o StrictHostKeyChecking=no '/project/lgrandi/general_scripts/verify_stash_transfers.sh $1 $2 $did $6'" >> $post_log 2>&1
    ssh ershockley@midway2-login1.rcc.uchicago.edu -o StrictHostKeyChecking=no "/project/lgrandi/general_scripts/verify_stash_transfers.sh $0 $2 $did $6" >> $post_log 2>&1

    ex=$?
fi

echo "exiting with status $ex" >> $post_log

exit $ex
