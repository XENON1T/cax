  History
=======

5.0.4 (2017-02-17)
-------------------
* Implement command line arguments for:
  * Run tag (e.g. "_sciencerun0")
  * Cluster partition (e.g. "xenon1t", "kicp")
  * Number of CPUs per job 
    
5.0.3 (2017-02-16)
-------------------
* Fix bug in run range in massive cax

5.0.3 (2017-02-16)
-------------------
* Switch to rsync for processed data
* Fix drift velocity function bug (#79)

5.0.2 (2017-02-16)
-------------------
* Allow for massive cax to have stop range

5.0.1 (2017-02-16)
-------------------
* Make proximity trees

5.0.0 (2017-02-14)
-------------------
* First Ruciax merge
* Revert to checksumming "verifying" only
* Fix: only modify RunsDB if actually deleting data
* Add check of pax version when deleting processed data
* Switch back to xenon1t partition

4.11.6 (2017-02-13)
-------------------

* Switch to kicp partition
* Copy only processed files, not raw, and fix version check
* Disable iterative mode to always work on old runs

4.11.5 (2017-02-10)
-------------------

* Gains for acquisition monitor

4.11.4 (2017-02-08)
-------------------

* Tweaks for getting the slow control interface to be used for gains.

4.11.3 (2017-02-08)
-------------------

* Gains can depend on time (#77)
* Drift velicity (#75)
* Tweaks for _sciencerun0 data

4.11.2 (2016-12-27)
-------------------
* Decrease purge threshold to 5 days (from 25)
* Allow purge of Rn220, disallow AmBe
* Do not purge until processed
* Switch processed dir back to /project (from /project2)
* Request 9 cores per job (instead of 4)
* Specify 8 cores for pax (instead of 4), 1 extra for I/O worker

4.11.1 (2016-12-22)
-------------------
* Switch to /project2 space on Midway

4.11.0 (2016-12-21)
------------------
* Switch to rsync for data transfers (instead of scp)
  
4.10.6 (2016-12-20)
------------------
* Compute gains at Midway to speed it up (Closes #50)

4.10.5 (2016-12-06)
------------------
* Added tape backup upload (TSM) to master branch

4.10.4 (1026-12-02)
-------------------
* Fix hax logging bug (was preventing all minitree creation)
* Delay error'ed purging on xe1t-datamanager
* Reduce massive-cax job submission delay back to 1 second

4.10.3 (1026-11-29)
-------------------
* Fix pax input type for MV 
* Increase pax max_blocks_on_heap
  
4.10.2 (1026-11-23)
-------------------
* Adjust setpermission for PDC and Midway

4.10.1 (2016-11-21)
-------------------
* Add other treemakers to process_hax

4.10.0 (2016-11-21)
-------------------
* Do not table scan run database

4.9.1 (2016-11-21)
------------------
* Increase delay between job submission to 10 s

4.9.0 (2016-11-15)
------------------
* Increase timeout for pax processing
* Purge Rn220+Kr83m only on Midway
* Disable iterative mode for massive-cax (for now)
  
4.8.1 (2016-11-09)
------------------                                                                           
* Fix batch queue checking for MV jobs
  
4.8.0 (2016-11-07)
------------------
* Do not purge Kr83m and Rn220 
  
4.7.1 (2016-11-03)
------------------
* Fix bug in queue checking 
   
4.7.0 (2016-11-03)
------------------
*  hax minitree production
   
4.5.2 (2016-11-01)
------------------
* Reduce pax queue block size in batch processing #51 

4.5.1 (2016-10-31)
------------------

* Fix pax queue size configuration option
 
4.5.0 (2016-10-31)
------------------

* Remove gfal environment setup (may be clashing with pax) 
* Remove "once" functionality from massive-cax (strange error with "watch", and hanging without "watch")
      
4.4.16 (2016-10-26)
-------------------

* Revert to file:// instead of SRM address for Midway gfal-copy upload
* Remove extraenous AddChecksum's and put ProcessBatchQueue at the end
* Reduce max_queue_blocks from 100 to 50 (otherwise AmBe hits memory limit)

 
4.4.15 (2016-10-11)
-------------------

* Do not purge processed files
* Consider only same file type when counting copies
* Remove hardcoded midway-srm treatment

4.4.14 (2016-10-10)
-------------------

* Hardcode Midway SRM address for gfal-copy uploads.
* Switch back to Stash round-robin address.
* Fix missing "dir_processed" key error.

4.4.13 (2016-10-10)
-------------------

* Change from Stash to Nikhef for Midway GRID upload
* Specify ADLER32 checksum for gfal-copy
* Skip "verifying" stage for GRID transfers (assume gfal-copy checksum is sufficient)
  
4.4.12 (2016-10-06)
-------------------

* Change Stash GSIFTP site to round-robin address 
  
4.4.11 (2016-10-02)
-------------------

* Change raw directory on nikhef-srm

4.4.10 (2016-10-02)
-------------------

* Fix variable name for GSIFTP source server 

4.4.9 (2016-10-02)
------------------

* Extend gfal-copy time even more (to 9 hours)
* Should cover typical 40 GB file and slow 3 MB/s speed
* Use GSIFTP address of Stash (login) as source with gfal-copy
* Upload to nikhef-srm from Stash (login)
* Switch from lcg-cp to gfal-copy in cax.json

4.4.8 (2016-09-29)
------------------

* Purge using gfal-rm on Stash
  
4.4.7 (2016-09-29)
------------------

* Pass GRID certificate for worker nodes.
* Change raw directory for Stash GRID copy.
  
4.4.6 (2016-09-28)
------------------

* Load GRID tools within job on Midway
	
4.4.5 (2016-09-28)
------------------

* Switch Stash SRM address to gsiftp address
* Allow specification in cax.json for number of streams in GRID copy
* Increase gfal-copy timeout  to 3 hours (-t 10800)
* Disable LFC registration (Rucio should pick up the bookkeeping later)

4.4.4 (2016-09-26)
------------------

* Gains less than 1 are set exactly to zero.


4.4.3 (2016-09-23)
------------------

* Another bug fix (forgot a :)
  
4.4.2 (2016-09-23)
------------------

* Bug fix (commented wrong line in job script in previous commit)

4.4.1 (2016-09-23)
------------------

* Command-line option to specify one run or starting run with massive-cax
* Hardcoded (commented out) option to switch to Midway sandyb (public) partition
* Check queue in all partitions on Midway instead of just xenon1t
* Add "login" (Stash/ci-connect) to Midway upload option (remove Midway from Stash download)
* Do not recheck checksums on Stash ("login"), too slow since no batch queue for massive-cax

4.4.0 (2016-09-20)
------------------

* Verify that data is still there (#42)

4.3.13 (2016-09-01)
------------------

*  MV processing support

4.3.12 (2016-08-31)
------------------

*  Add command line options --once and --config for massive-cax
 
4.3.11 (2016-08-17)
------------------

* Bug fix: Job name should have pax version, not cax version

4.3.10 (2016-08-17)
------------------

* Temporarily disable 'sacct' call (seems to be broken on Midway after restart) 

4.3.8 (2016-08-17)
------------------

* Bug fix: check for actual version number in queue list instead of "head"

4.3.7 (2016-08-17)
------------------

* Reduce allowed number of jobs in queue to 500 (since we only have 28*16=448 cores)

4.3.6 (2016-08-12)
------------------

* Change path to Anaconda Installation at PDC

4.3.5 (2016-08-10)
------------------

* Process after copy.


4.3.4 (2016-08-09)
------------------

* Have LED mode have gains equal to 1, which is the same as XENON1T_LED.ini.

4.3.3 (2016-08-04)
------------------

* Fix permissions at PDC when new folder for new processed versions of data (#35).


4.3.2 (2016-08-02)
------------------

* Forgot to remove line about batch queue statistics that only works at Midway.


4.3.1 (2016-08-02)
------------------

* Forgot to update HISTORY.rst

4.3.0 (2016-08-02)
------------------

* Gains derived from HV measurements using HTTP interface (#34).
* Statistics at end of job on usage.


4.2.0 (2016-07-25)
------------------

* Slow control variables defined in hax added to rundoc.

4.1.3 (2016-07-21)
------------------

* Allow more jobs at Midway

4.1.2 (2016-07-21)
------------------

* Error if default gains

4.1.1 (2016-07-20)
------------------

* Create output directory prior to pax processing
  
4.1.0 (2016-07-20)
------------------

* Gains support (#32).

4.0.4 (2016-07-15)
------------------

* Create and use run sub-directory for logs
* Fix bug where "[]" in cax.json is not treated as "None"
  
4.0.3 (2016-07-12)
------------------

* Yet more PDC changes

4.0.2 (2016-07-12)
------------------

* Changes for Stockholm PDC

4.0.1 (2016-07-12)
------------------

* Forgot to switch environment outside of test environment

4.0.0 (2016-07-12)
------------------

* cax now operates by sending jobs to the batch queue for every run (See PR #30).

3.0.7 (2016-06-30)
------------------

* Only send email notifications for failed jobs 
  
3.0.6 (2016-06-29)
------------------

* Remove Nikhef ability to pull new data


3.0.5 (2016-06-28)
------------------

* Switch queue check command for public nodes on Midway

3.0.4 (2016-06-27)
------------------

* Switch to public nodes on Midway for next mass reprocessing
* Increase 1 CPU processing threshold to 1000 events (https://github.com/XENON1T/pax/issues/390)
  
3.0.2 (2016-06-23)
------------------

* Change all cax.json  entries from xenon1t-daq to xe1t-datamanager


3.0.1 (2016-06-23)
------------------

* Checksum comes from xe1t-datamanager

3.0.0 (2016-06-23)
------------------

* Grid copy functionality
* Use datamanager machine at LNGS.

2.2.6 (2016-06-18)
------------------

* Remove race condition check since didn't work


2.2.7 (2016-06-18)
------------------

* Raise timeout for deleting to 24 hours


2.2.6 (2016-06-18)
------------------

* Tune race condition logic (make stricter)


2.2.5 (2016-06-18)
------------------

* Log exceptions then reraise

2.2.4 (2016-06-17)
------------------

* Fix bug/typo in logic checking that data location doesn't already exist before transferring processed data.

2.2.3 (2016-06-17)
------------------

* Do not retransfer processed data now also checks pax_version because otherwise would stop after any version
* Execute one candidate transfer instead of all candidate transfers so it can recheck next time task is executed what candidates are


2.2.2 (2016-06-16)
------------------

* Avoid race condition if two cax running with copies.


2.2.1 (2016-06-16)
------------------

* Purity is float and not sympy float type.  Otherwise, MongoDB doesn't understand it.


2.2.0 (2016-06-15)
------------------

* Generalized purification evolution function in run database

2.1.8 (2016-06-15)
------------------

* Handle reconnect signal from Mongo if LNGS connection unstable.


2.1.7 (2016-06-15)
------------------

* Handle modified times even if file does not exist

2.1.6 (2016-06-14)
------------------

* Check modified times before deleting data for timeout

2.1.5 (2016-06-09)
------------------

* Catch FileNotFoundError when deleting files, then warn.

2.1.4 (2016-06-08)
------------------

* Process with pax 5.0

2.1.3 (2016-06-08)
------------------

* Revert PROCESSING_DIR to separate directories for each job
  
2.1.2 (2016-06-08)
------------------

* Stockholm grabs data from LNGSx

2.1.1 (2016-06-08)
------------------

* Fix bug in parameter manipulation for lifetime fit.

2.1.0 (2016-06-07)
------------------

* Add electron lifetime support

2.0.3 (2016-06-07)
------------------

* When task looks for runs, have it only return the _id then fetch that id later.  Helps with timeouts.

2.0.2 (2016-06-07)
------------------

* If task timeout of mongo find, have it skip that task.

2.0.1 (2016-06-06)
------------------

* Remove mv command for logs from job, doesn't work with new {processing_dir}. Keep them all in same location for now.

* Uncomment submit command for automatic processing

2.0.0 (2016-06-06)
------------------

* Use different folder for raw and root data

* Add cax-mv, cax-rm, cax-stray

* Don't need to clear DAQ buffer anymore in cax.

* Cleanup and fixes related to processing.

* Transfer bug that made bad element in data location list

* Specify the partition in qsub.py

* filesystem.py: Add a class to ask for the status of a file or folder

1.2.0 (2016-5-26)
------------------

* Retry if errored instead of waiting two days.

1.1.2 (2016-5-26)
------------------

* Specify log level on command line.

1.1.1 (2016-5-26)
------------------

* Version number only in file log, not screen

1.1.0 (2016-5-26)
------------------

* Add release support
* Add version number to log output

1.0.0 (2016-5-26)
------------------

* Initial stable release
* SCP support for transfer
* Checksumming
* Retry failed transfers if checksum fails or timeout
* Processing on batch queue

0.1.0 (2016-1-22)
------------------

* Initial release
