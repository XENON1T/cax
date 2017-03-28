from cax.dag_writer import dag_writer
import numpy as np
import pymongo
from get_gains import get_runs_with_gains
from get_sciencerun_list import get_science_runlist
from make_runlist import make_runlist

runlist = make_runlist()

print(len(runlist))

# divide into n dags of Y runs
runs_per_dag = 1500
n_dags = int(np.ceil(len(runlist)/runs_per_dag))

for i in range(n_dags):
    j = int(i*np.ceil(runs_per_dag))
    sublist = runlist[j:j+runs_per_dag]

    logdir = "/xenon/ershockley/reprocessing"
    dag = dag_writer(sublist, "v6.6.2", logdir, reprocessing = True, n_retries=9)
    dag.write_outer_dag(logdir + "/662_finishSR0_%i.dag" % i)
