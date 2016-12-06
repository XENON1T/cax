from cax.dag_writer import dag_writer

runlist = range(2099, 2110)
logdir = "/xenon/ershockley/reprocessing2"
dag = dag_writer(runlist, "v6.1.1", logdir, reprocessing = True)

dag.write_outer_dag(logdir + "/outer.dag")
