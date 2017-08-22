""" Access the cluster.
    Easy to use functions to make use of the cluster facilities.
    This checks the available slots on the requested queue, creates the
    scripts to submit, submits the jobs, and cleans up afterwards.

    Example usage::

        >>> import qsub
        >>> qsub.submit_job('touch /data/hisparc/test', 'job_1', 'express')

"""
import logging
import os
from cax import config, __file__
import subprocess
import tempfile
from distutils.spawn import find_executable
import shutil


def which(program):
    """Check if a command line program is available

    An Exception is raised if the program is not available.

    :param program: name or program to check for, e.g. 'wget'.

    """
    path = find_executable(program)
    if not path:
        raise Exception('The program %s is not available.' % program)



def submit_job(script, name='', extra=''):
    """Submit a job

    :param script: contents of the script to run.
    :param name: name for the job.
    :param extra: optional extra arguments for the sbatch command.

    """
    fileobj = create_script(script)

    host = config.get_hostname()
    
    #Different submit command for using OSG
    if host == 'login':
        which('condor_submit_dag')

        # Effect of the arguments for condor_submit:
        # http://research.cs.wisc.edu/htcondor/manual/v7.6/condor_submit.html
        submit_command = ('condor_submit_dag {extra} {script}'
                          .format(script=fileobj.name,
                                  extra=extra))

    else:
        which('sbatch')

        # Effect of the arguments for sbatch:
        # http://slurm.schedmd.com/sbatch.html

        submit_command = ('sbatch {extra} {script}'
                          .format(script=fileobj.name,
                                  extra=extra))

    logging.info('submit job:\n %s' % submit_command)

    try:
        result = subprocess.check_output(submit_command,
                                         stderr=subprocess.STDOUT,
                                         shell=True,
                                         timeout=120)
    except subprocess.TimeoutExpired as e:
        logging.error("Process timeout")
    except Exception as e:
        logging.exception(e)

    delete_script(fileobj)

def submit_dag_job(outer_dag, dag_config):

    from cax.dag_writer import dag_writer

    which('condor_submit_dag')

    # check if outer dag exists
    if not os.path.isfile(outer_dag):
        print("No OUTER dag file exists, writing one now")
        DAG = dag_writer(dag_config)

        # this next line writes the dag and returns the number of runs to
        # make sure there wasn't an error (typically from buggy MV runs)
        n_runs = DAG.write_outer_dag(outer_dag)

        if n_runs < 1:
            print("No runs written to dag, so removing")
            os.remove(outer_dag)
            return


    caxdir = os.path.dirname(os.path.dirname(__file__)) 
    submit_command = ('condor_submit_dag -config {caxdir}/osg_scripts/dag_config {script}'.format(caxdir=caxdir, script=outer_dag))

    logging.info("submit job:\n %s" % submit_command)

    try:
        result = subprocess.check_output(submit_command,
                                         stderr=subprocess.STDOUT,
                                         shell=True,
                                         timeout=120)
    except subprocess.TimeoutExpired as e:
        logging.error("Process timeout")
    except Exception as e:
        logging.exception(e)

def create_script(script):
    """Create script as temp file to be run on cluster"""
    fileobj = tempfile.NamedTemporaryFile(delete=False,
                                          suffix='.sh',
                                          mode='wt',
                                          buffering=1)
    fileobj.write(script)
    os.chmod(fileobj.name, 0o774)

    return fileobj


def delete_script(fileobj):
    """Delete script after submitting to cluster

    :param script_path: path to the script to be removed

    """
    fileobj.close()


def get_number_in_queue(host=config.get_hostname(), partition=''):
    return len(get_queue(host, partition))


def get_queue(host=config.get_hostname(), partition=''):
    """Get list of jobs in queue"""

    if host != "login":
        if host == "midway-login1":
            args = {'partition': 'xenon1t',
                    'user' : 'tunnell'}
        elif host == 'tegner-login-1':
            args = {'partition': 'main',
                    'user' : 'bobau'}

        else:
            raise ValueError()

        command = 'squeue --user={user} -o "%.30j"'.format(**args)




    else:
        command = "condor_q ershockley | grep dagman | awk '{print $1}'"

    try:
        queue = subprocess.check_output(command,
                                        shell=True,
                                        timeout=120)
    except subprocess.TimeoutExpired as e:
        logging.error("Process timeout")
        return []
    except Exception as e:
        logging.exception(e)
        return []

    queue_list = queue.rstrip().decode('ascii').split()
    if len(queue_list) > 1:
        if host != 'login':
            return queue_list[1:]
        else:
            return queue_list
    return []


