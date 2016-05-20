""" Access the cluster.

    Easy to use functions to make use of the cluster facilities.
    This checks the available slots on the requested queue, creates the
    scripts to submit, submits the jobs, and cleans up afterwards.

    Example usage::

        >>> from sapphire import qsub
        >>> qsub.check_queue('long')
        340
        >>> qsub.submit_job('touch /data/hisparc/test', 'job_1', 'express')

"""
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


def check_queue(queue):
    """Check for available job slots on the selected queue for current user

    Maximum numbers from:
    ``qstat -Q -f | grep -e Queue: -e max_user_queuable -e max_queuable``.
    Note that some queues also have global maximum number of jobs.

    :param queue: queue name for which to check current number of job
                  slots in use.
    :return: number of available slots.

    """
    which('qstat')
    all_jobs = int(subprocess.check_output('qstat {queue} | '
                                           'grep " [QR] " | wc -l'
                                           .format(queue=queue), shell=True))
    user_jobs = int(subprocess.check_output('qstat -u $USER {queue} | '
                                            'grep " [QR] " | wc -l'
                                            .format(queue=queue), shell=True))

    if queue == 'express':
        return 2 - user_jobs
    elif queue == 'short':
        return 1000 - user_jobs
    elif queue == 'generic':
        return min(2000 - user_jobs, 4000 - all_jobs)
    elif queue == 'long':
        return min(500 - user_jobs, 1000 - all_jobs)
    else:
        raise KeyError('Unknown queue name: {queue}'.format(queue=queue))


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

    result = subprocess.check_output(sbatch, stderr=subprocess.STDOUT,
                                     shell=True)
    print(result)

    delete_script(script_path)


def create_script(script, name):
    """Create script as temp file to be run on cluster"""

    script_name = '{name}.sh'.format(name=name)
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
