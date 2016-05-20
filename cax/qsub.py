""" Access the cluster.

    Easy to use functions to make use of the cluster facilities.
    This checks the available slots on the requested queue, creates the
    scripts to submit, submits the jobs, and cleans up afterwards.

    Example usage::

        >>> import qsub
        >>> qsub.submit_job('touch /data/hisparc/test', 'job_1', 'express')

"""
import os
import logging
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

def submit_job(script, name, extra=''):
    """Submit a job

    :param script: contents of the script to run.
    :param name: name for the job.
    :param extra: optional extra arguments for the sbatch command.

    """
    
    which('sbatch')
    script_path, script_name = create_script(script, name)

    # Effect of the arguments for sbatch:
    # http://slurm.schedmd.com/sbatch.html
    sbatch = ('sbatch -J {name} {extra} {script}'
            .format(name=script_name, script=script_path,
                    extra=extra))

    result = subprocess.check_output(sbatch,
                                     stderr=subprocess.STDOUT,
                                     shell=True)
    logging.info(result)

    delete_script(script_path)


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
