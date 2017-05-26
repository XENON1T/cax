#from cax.dag_writer_mod import dag_writer
from cax.dag_writer import dag_writer
import numpy as np
from make_runlist import make_runlist

runlist = [9837, 9838, 9839]
# from Zach 6378-6730
#runlist = [6734]
#with open("/home/ershockley/murra.csv") as f:
#    for num, line in enumerate(f.readlines()):
#        line = line.split(',')
#        if num == 0:
#            continue
#        for col in [4,5]:
#            if line[col] != "":
#                runlist.append(int(line[col]))
#runlist = sorted(runlist)
print(len(runlist))
runlist = sorted(runlist)

#runlist = np.arange(6378, 6731)

#list1 = runlist[:np.ceil(len(runlist)/2)]
#list2 = runlist[np.ceil(len(runlist)/2):]

logdir = "/scratch/processing"
dag = dag_writer(runlist, "v6.6.5", logdir, reprocessing = True, n_retries=9)
dag.write_outer_dag(logdir + "/665_NG.dag")
