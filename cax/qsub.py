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


def submit_job(script, extra=''):
    """Submit a job

    :param script: contents of the script to run.
    :param name: name for the job.
    :param extra: optional extra arguments for the sbatch command.

    """

    which('sbatch')
    fileobj = create_script(script)

    # Effect of the arguments for sbatch:
    # http://slurm.schedmd.com/sbatch.html
    sbatch = ('sbatch {extra} {script}'
              .format(script=fileobj.name,
                      extra=extra))
    try:
        result = subprocess.check_output(sbatch,
                                     stderr=subprocess.STDOUT,
                                     shell=True,
                                     timeout=120)
        logging.info(result)
    except subprocess.TimeoutExpired as e:
        logging.error("Process timeout")
    except Exception as e:
        logging.exception(e)

    

    delete_script(fileobj)


def create_script(script):
    """Create script as temp file to be run on cluster"""
    fileobj = tempfile.NamedTemporaryFile(delete=True,
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
    # print (len(get_queue(host, partition)), host, partition)
    return len(get_queue(host, partition))


def get_queue(host=config.get_hostname(), partition=''):
    """Get list of jobs in queue"""

    if host == "midway-login1":
        args = {'partition': 'sandyb',
                'user' : config.get_user()}
    elif host == 'tegner-login-1':
        args = {'partition': 'main',
                'user' : 'bobau'}
    else:
        return []

    if partition == '':
        command = 'squeue --user={user} -o "%.30j"'.format(**args)

    else:
        args['partition'] = partition
        command = 'squeue --partition={partition} --user={user} -o "%.30j"'.format(**args)

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
