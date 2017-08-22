#from cax.dag_writer_mod import dag_writer
from cax.dag_writer import dag_writer
import numpy as np
from make_runlist import make_runlist
from pax import __version__
import os

#os.environ["PYTHONPATH"]="/xenon/cax:"+os.environ["PYTHONPATH"]

#runlist=['170331_1249'] #MUV to reprocess
runlist=list(np.arange(3410, 3421)) + list(np.arange(3446, 3458)) + [3443]
runlist = [int(run) for run in runlist]
print(runlist)

config = { 'runlist' : runlist,
           'pax_version' :'v' +  __version__,
           'logdir' : '/scratch/processing',
           'retries' : 9,
           'specify_sites' : [],
 	   "exclude_sites": ["Comet","WEIZMANN-LCG2","IN2P3-CC"], 
           'host' : 'login',
           'use_midway' : False, # this overrides the specify and exclude sites above,
           'rush' : True # processes as quickly as possible, submits to euro sites before raw data gets to stash
           }


dag = dag_writer(config)

#this name has to be changed in case one wants to do reprocessing
#dag.write_outer_dag('/scratch/processing/pax6.8.0_Rn220_PreReprocessingTest.dag')
dag.write_outer_dag('/scratch/processing/katrina_runs.dag')
#dag.write_outer_dag('/scratch/processing/pax6.8.0_notOSG_run_11912.dag')
#dag.write_outer_dag('/scratch/processing/pax6.6.2_MuV_run_170331_1249.dag')
#dag.write_outer_dag('/scratch/processing/forced_processing_666_old_kr85m_runs.dag')


