#!/bin/bash

# 1: run name
# 2: pax version
# 3: run number
# 4: general log directory
# 5: number of zips
# 6: detector

run="${1##*/}"
post_log=$4/pax_$2/$run/POST_LOG
if [[ $6 == 'muon_veto' ]]; then
    post_log=$4/pax_$2/${run}_MV/POST_LOG  
fi

echo "------ Start of post script ---------" >> $post_log
date >> $post_log

echo $@ >> $post_log

if [[ -e /xenon/xenon1t_processed/pax_$2/$1.root ]]; then
    echo "Processing done! Don't need to run post script. Exit 0."
    exit 0
fi

rawdir=$1

source activate pax_$2_OSG

#cd /home/ershockley/cax/
#export PYTHONPATH=/home/ershockley/cax/lib/python3.4/site-packages/:$PYTHONPATH
#python setup.py install --prefix ${PWD}

# save exit status of pipe
set -o pipefail

#if [[ ! -e /xenon/xenon1t_processed/pax_$2/$1.root ]]; then
echo "python /home/ershockley/cax/osg_scripts/upload.py $1 $5 $2 $6"
python /home/ershockley/cax/osg_scripts/upload.py $1 $5 $2 $6 >> $post_log 2>&1

if [[ $? -ne 0 ]]; then
    echo "hadd or DB stuff failed, exiting"
    exit 1
fi  
#fi
# transfer to midway

echo "Beginning cax transfer to midway" >> $post_log
#cax --once --run $3 --config /home/ershockley/cax/cax/cax_transfer.json >> $post_log 2>&1

if [[ $6 == 'tpc' ]]; then
    cax --once --run $3 --config /home/ershockley/cax/cax/cax_transfer.json >> $post_log 2>&1
fi

if [[ $6 == 'muon_veto' ]]; then
    cax --once --name $1 --config /home/ershockley/cax/cax/cax_transfer.json >> $post_log 2>&1
fi

if [[ $? -ne 0 ]]; then
    exit 1
fi

# need to get the rucio did for this run
did=$(/home/ershockley/cax/osg_scripts/get_rucio_did.py $3)

if [[ $? -ne 0 ]]; then
    echo "Error finding rucio did"
    did=NONE
fi


# submit massive-cax job to verify transfer
if [[ $6 == 'tpc' ]]; then
    echo "Submitting massive cax job on midway for tpc run $3" >> $post_log 
    echo "ssh mklinton@midway-login1.rcc.uchicago.edu '/project/lgrandi/general_scripts/verify_stash_transfers.sh $3 $2 $did $6'" >> $post_log 2>&1
    ssh mklinton@midway-login1.rcc.uchicago.edu "/project/lgrandi/general_scripts/verify_stash_transfers.sh $3 $2 $did $6" >> $post_log 2>&1

    ex=$?
fi

if [[ $6 == 'muon_veto' ]]; then
    echo "Submitting massive cax job on midway for MV run $1" >> $post_log
    echo "ssh mklinton@midway-login1.rcc.uchicago.edu '/project/lgrandi/general_scripts/verify_stash_transfers.sh $1 $2 $did'" >> $post_log 2>&1
    ssh mklinton@midway-login1.rcc.uchicago.edu "/project/lgrandi/general_scripts/verify_stash_transfers.sh $1 $2 $did" >> $post_log 2>&1

    ex=$?
fi

echo "exiting with status $ex" >> $post_log

exit $ex