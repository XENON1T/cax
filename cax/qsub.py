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
import subprocess
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

    script_path, script_name = create_script(script, name)

    #Different submit command for using OSG
    if host == 'login':
        which('condor_submit')
        
        # Effect of the arguments for condor_submit:                                                                                                            
        # http://research.cs.wisc.edu/htcondor/manual/v7.6/condor_submit.html                                                                                   
        submit_command = ('condor_submit {extra} {script}'
                          .format(name=name, script=script_path,
                                  extra=extra))

    else:
        which('sbatch')
        
        # Effect of the arguments for sbatch:
        # http://slurm.schedmd.com/sbatch.html
        submit_command = ('sbatch -J {name} {extra} {script}'
                          .format(name=name, script=script_path,
                                  extra=extra))
        
        
        

    logging.info('submit job:\n %s' % submit_command)
    result = subprocess.check_output(submit_command,
                                     stderr=subprocess.STDOUT,
                                     shell=True)
    logging.info(result)
        
#    delete_script(script_path)
        

def create_script(script, name):
    """Create script as temp file to be run on cluster"""

    script_name = 'batch_queue_job_{name}.sh'.format(name=name)
    script_path = os.path.join('/tmp', script_name)

    with open(script_path, 'w') as script_file:
        script_file.write(script)
    os.chmod(script_path, 0o774)

    return script_path, script_name


def delete_script(script_path):
    """Delete script after submitting to cluster

    :param script_path: path to the script to be removed

    """
    os.remove(script_path)


def get_queue(host):
    """Get list of jobs in queue"""

    if host == "midway-login1":
        partition='xenon1t'
    if host == "tegner-login-1":
        partition='main'
    
    if host == "midway-login1" or host=="tegner-login-1":   
        queue = subprocess.check_output("squeue --partition=" + partition + " -o \"\%.30j\"", shell=True)
    else:
        logging.error("Host %s not implemented in get_queue()" % host)
        return ''

    queue_list = queue.rstrip().decode('ascii')
    return queue_list
