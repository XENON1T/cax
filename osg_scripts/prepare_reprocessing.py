#from cax.dag_writer_mod import dag_writer
from cax.dag_writer import dag_writer
import numpy as np
from make_runlist import make_runlist
from pax import __version__


#runlist = make_runlist()
with open("/home/ershockley/murra.csv") as f:
    for num, line in enumerate(f.readlines()):
        line = line.split(',')
        if num == 0:
            continue
        for col in [4,5]:
            if line[col] != "":
                runlist.append(int(line[col]))

runlist = sorted(runlist)




config = { 'runlist' : runlist,
           'pax_version' : __version__,
           'logdir' : '/scratch/processing',
           'retries' : 9,
           'specify_sites' : [],
           'exclude_sites' : ['Comet'],
           'host' : 'login',
           'use_midway' : False, # this overrides the specify and exclude sites above,
           'rush' : True # processes as quickly as possible, submits to euro sites before raw data gets to stash
           }


dag = dag_writer(config)
dag.write_outer_dag('/scratch/processing/murra.dag')

