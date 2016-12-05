#!/bin/bash

# 1: raw directory
# 2: pax version
# 3: Job exit code
# 4: run number

#/xenon/ershockley/organize_errors_single.sh $1 $2
run="${1##*/}"
post_log=/xenon/ershockley/cax/$2/$run/POST_LOG
echo $@ #>> $post_log

rawdir=$1
run="${1##*/}" # crazy bash expression that extracts run name
echo $run #>> $post_log
echo "Job exit code is $3" #>> $post_log

#if [[ $3 -ne 0 ]]; then 
#    exit 255
#fi
#source activate evan-testing >> $post_log

python /home/ershockley/cax/osg_scripts/upload.py $1 $2 # >> $post_log
    
# transfer to midway
/home/ershockley/cax/bin/cax --once --run $4 --config /home/ershockley/cax/cax/cax_transfer.json #>> $post_log