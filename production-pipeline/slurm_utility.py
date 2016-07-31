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

def get_job_stats(jobid):
    """Returns a dictionary of job stats"""

    jobid = str(jobid)
    f_squeue = open('sjob.tmp', 'w')
    subprocess.Popen(['sacct', '-o', 'JobID, STATE, ELAPSED, CPUTimeRaw, MaxRSS, MaxVMSize, AllocCPU, Reserved', '--job', jobid], stdout=f_squeue).wait()
    f_squeue.close()

    job_stats = {'state': 'NA', 'Elapsed': 0, 'CPUTime': 0, 'CpuEff': 0, 'MaxRSS': 0, 'MaxVMSize': 0, 'Reserved': ''}

    with open('sjob.tmp', 'r') as f_squeue:
        # skip first two lines
        lines = f_squeue.readlines()[2:]

        job_stats['state'] = lines[0].split()[1]

        if job_stats['state'] == 'COMPLETED':

            # get info from first line
            parts = [p for p in lines[0].strip().split()]

            job_stats['Elapsed'] = time_in_seconds(parts[2])
            job_stats['CPUTime'] = int(parts[3])
            job_stats['CpuEff'] = job_stats['CPUTime']/(job_stats['Elapsed']*int(parts[-2]))
            job_stats['Reserved'] = parts[-1]

            if len(lines) > 1:
                for line in lines[1:]:
                    parts = [p for p in line.strip().split()]
                    job_stats['MaxRSS'] = max(job_stats['MaxRSS'], parts[4])
                    job_stats['MaxVMSize'] = max(job_stats['MaxVMSize'], parts[5])
            else:
                job_stats['MaxRSS'] = parts[4]
                job_stats['MaxVMSize'] = parts[5]


    return job_stats

def time_in_seconds(time):
    """Converts time string to seconds"""

    days = 0
    if time.find('-') > 0:
        days = int(time.split('-')[0])
        time = time[time.find('-')+1:-1]

    time = time.split(':')

    return days*3600*24 + int(time[0])*3600 + int(time[1])*60 + int(time[2])
