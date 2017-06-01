#from cax.dag_writer_mod import dag_writer
from cax.dag_writer import dag_writer
import numpy as np
from make_runlist import make_runlist

# [9989, 9990, 9991, 9992,9993,9994,9995,9996,9997,9998,9999,
# 10000,10001,10033,10034,10035,10036,10037,10038,10039,10040,10041,10042] THIS WAS THE FIRST HALF

runlist =  [10069,10070,10071,10072,10073,10074,10075,10076,10077,10078,10079,10080,10081,
            10082,10083,10084,10085,10086,10087,10088,10089]

#

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


logdir = "/scratch/hotspot"
dag = dag_writer(runlist, "v6.6.5", logdir, reprocessing = True, n_retries=9)
dag.write_outer_dag(logdir + "/665_HOTSPOT_SECONDPART.dag")
