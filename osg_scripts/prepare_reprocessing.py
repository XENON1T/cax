from cax.dag_writer import dag_writer
import numpy as np
import pymongo
from get_gains import get_runs_with_gains
from get_sciencerun_list import get_science_runlist
from make_runlist import make_runlist

runlist = make_runlist()

#Background: 4883-4893
#AmBe: 4457-4466
#Kr83m: 4649-4659
#Rn220: 5675, 5702, 5773, 5781, 5783, 5788, 5789, 5792

#runlist = list(range(4883, 4894)) + list(range(4457, 4467)) + list(range(4649,4660)) + [5675, 5702, 5773, 5781, 5783, 5788, 5789, 5792] 

#runlist = list(np.arange(5824,5831)) + list(np.arange(5838, 5841))

# read a file to check which runs have been written to a dag yet. Sometimes useful.
#already_dagged = []
#with open("/home/ershockley/already_dagged_642.txt") as f:
#    for line in f.readlines():
#        run = int(line)
#        already_dagged.append(run)

#runlist = [run for run in runlist if run not in already_dagged]
#runlist = list(reversed(runlist))
print(len(runlist))

# divide into n dags of Y runs
runs_per_dag = 500
n_dags = int(np.ceil(len(runlist)/runs_per_dag))

for i in range(n_dags):
    j = int(i*np.ceil(runs_per_dag))
    sublist = runlist[j:j+runs_per_dag]

    logdir = "/xenon/ershockley/reprocessing"
    dag = dag_writer(sublist, "v6.4.2", logdir, reprocessing = True, n_retries=9)
    dag.write_outer_dag(logdir + "/642_mar7_%i.dag" % i)
