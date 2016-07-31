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
    with open('squeue.tmp', 'r') as f_squeue:
        # skip the first line
        for _ in xrange(1):
            next(f_squeue)

        for line in f_squeue:
            parts = [p for p in line.strip().split()]
            jobs[int(parts[0].split('.')[0])] = parts[1]

    return jobs
