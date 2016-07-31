"""Utility functions for quering slurm"""

import subprocess

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

def get_queued_jobs(user):
    """Returns a dictionary of user slurm jobs"""

    f_squeue = open('squeue.tmp', 'w')
    subprocess.Popen(['squeue', '-o', '%.18i %.15T', '--user', user], stdout=f_squeue).wait()
    f_squeue.close()

    jobs = {}
    f_squeue = open('squeue.tmp', 'r')
    for line in f_squeue:
        line = line.strip()
        parts = [p for p in line.split()]
        if parts[0] == 'JOBID':
            continue
        else:
            jobs[int(parts[0].split('.')[0])] = parts[1]

    f_squeue.close()
    return jobs
