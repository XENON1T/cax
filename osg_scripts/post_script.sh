#!/bin/bash

# 1: raw directory
# 2: pax version
# 3: Job exit code
# 4: run number
# 5: log directory

run="${1##*/}"
post_log=$5/pax_$2/$run/POST_LOG

if [[ $4 -eq "-1004" ]]; then
    echo "Job did not run. Skipping post script" >> $post_log
    exit 1
fi

echo "/home/ershockley/cax/osg_scripts/hadd_and_upload.sh $1 $2 $3 $4" >> $post_log
/home/ershockley/cax/osg_scripts/hadd_and_upload.sh $1 $2 $3 $4 >> $post_log