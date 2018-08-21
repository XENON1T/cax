from cax.dag_writer import dag_writer
import numpy as np
import pandas as pd
from make_runlist import make_runlist
from pax import __version__
import os

#runlist = np.load('v610_runs.npz')['runs'].astype(int)
#runlist = [int(r) for r in runlist]
#df = pd.read_csv('/home/ershockley/DBstuff/reprocess_180807/reprocess_these.csv')
#runlist = [int(r) for r in df.number.values]
runlist = [13976,13977,13978,13979,13980,13981,13991,13992,13993,13994]
#print(runlist)

config = { 'runlist' : runlist,
           'pax_version' :'v' +  __version__,
           'logdir' : '/scratch/processing',
           'retries' : 9,
           'specify_sites' : [],
 	   "exclude_sites": [], 
           'host' : 'login',
           'use_midway' : False, # this overrides the specify and exclude sites above,
           'rush' : True # processes as quickly as possible, submits to euro sites before raw data gets to stash
           }


dag = dag_writer(config)

#this name has to be changed in case one wants to do reprocessing
dag.write_outer_dag('/home/ershockley/send_to_diego.dag')
