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
from cax import config
import subprocess
import tempfile
from distutils.spawn import find_executable


def which(program):
    """Check if a command line program is available

    An Exception is raised if the program is not available.

    :param program: name or program to check for, e.g. 'wget'.

    """
    path = find_executable(program)
    if not path:
        raise Exception('The program %s is not available.' % program)



def submit_job(host,script, name, extra=''):
    """Submit a job

    :param script: contents of the script to run.
    :param name: name for the job.
    :param extra: optional extra arguments for the sbatch command.

    """
    fileobj = create_script(script)

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

        submit_command = ('sbatch -J {name} {extra} {script}'
                          .format(name=name, script=fileobj.name,
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

def submit_dag_job(run_number, outer_dag, inner_dag, outputdir, submitscript, paxversion, json_file):
    
    from cax.dag_writer import dag_writer

    which('condor_submit_dag')

    # create submit file, which in turn is used by dag file.
    submitfileobj = create_script(submitscript)

    # check if inner dag file exists already
    if not os.path.isfile(inner_dag):
        print("No INNER dag file exists, writing one now")
        # create inner dag file. In creation of instance, must put run number in a list
        DAG = dag_writer([run_number], paxversion)
        DAG.write_inner_dag(run_number, inner_dag, outputdir, submitfileobj.name, json_file)
      
    # now check if outer dag exists
    if not os.path.isfile(outer_dag):
        print("No OUTER dag file exists, writing one now")
        DAG = dag_writer([run_number], paxversion)
        DAG.write_outer_dag(outer_dag, inner_dag)

    submit_command = ('condor_submit_dag {script}'.format(script=outer_dag))

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


def get_number_in_queue(host=config.get_hostname()):
    return len(get_queue(host))


def get_queue(host=config.get_hostname()):
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

        command = 'squeue --partition={partition} --user={user} -o "%.30j"'.format(**args)
    
    else:
        command = 'condor_q ershockley -format "%d\n" ClusterID'

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
        return queue_list[1:]
    return []

