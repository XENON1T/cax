#!/bin/bash

# 1: run name
# 2: pax version
# 3: run number
# 4: general log directory
# 5: number of zips

run="${1##*/}"
post_log=$4/$2/$run/POST_LOG
echo "------ Start of post script ---------" >> $post_log
date >> $post_log

echo $@ >> $post_log

if [[ -e /xenon/xenon1t_processed/pax_$2/$1.root ]]; then
    echo "Processing done! Don't need to run prescript. Exit 0."
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
python /home/ershockley/cax/osg_scripts/upload.py $1 $5 $2 >> $post_log 2>&1

if [[ $? -ne 0 ]]; then
    exit 1
fi  
#fi
# transfer to midway
echo "Beginning cax transfer to midway" >> $post_log
cax --once --run $3 --config /home/ershockley/cax/cax/cax_transfer.json >> $post_log 2>&1

if [[ $? -ne 0 ]]; then
    exit 1
fi

# submit massive-cax job to verify transfer
echo "Submitting massive cax job on midway for run $3" >> $post_log 
ssh tunnell@midway-login1.rcc.uchicago.edu "/home/tunnell/verify_stash_transfers.sh $3 $2" >> $post_log 2>&1

ex=$?

echo "exiting with status $ex" >> $post_log

exit $ex