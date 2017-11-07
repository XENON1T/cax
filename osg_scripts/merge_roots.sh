#!/bin/bash

# 1: path to directory containing processed zips
# 2: path where we want to put the merged root file
proc_zip_dir=$1
proc_merged_dir=$2

run="${proc_zip_dir##*/}" # crazy bash expression that extracts run name

echo "    -- Merge Roots --"
echo "1: $1"
echo "2: $2"
echo $run 
#cd $procdir

source deactivate
source activate mc
source /cvmfs/xenon.opensciencegrid.org/software/mc_old_setup.sh

#echo "hadd_mod -d -f ${proc_merged_dir}/$run.root ${proc_zip_dir}/XENON*root"
hadd_mod -d -f ${proc_merged_dir}/$run.root ${proc_zip_dir}/XENON*root

if [[ ! $? -eq 0 ]]; then
    exit 1
fi

cd ..
#rm -rf $procdir

source deactivate

echo " --  end merge roots  -- "