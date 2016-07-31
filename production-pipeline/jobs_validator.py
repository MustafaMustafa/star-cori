#!/bin/usr/python
"""Daemon to watch jobs in slurm queue and validate output of completed jobs"""

import sys
import time
import logging
import load_configuration
import slurm_utility
from MongoDbUtil import MongoDbUtil
from StatsHeartbeat import StatsHeartbeat

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

# pylint: disable=C0103
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(module)s : %(message)s')
__logger = logging.getLogger(__name__)
# pylint: enable=C0103

def main():
    """To be used in CLI mode"""

    args = load_configuration.get_args(__doc__)

    jobs_validator(args.configuration)

def jobs_validator(config_file):
    """Daemon to watch jobs in slurm queue and validate output of completed jobs"""

    config = load_configuration.load_configuration(config_file)
    config = load_configuration.affix_production_tag(config, ['db_collection', 'db_production_files_collection'])

    database = MongoDbUtil('admin', db_server=config['db_server'], db_name=config['db_name']).database()

    # spawn a stats heartbeat
    accum_stats = {'completed': 0, 'failed': 0}
    stats = {'total_in_queue': 0, 'running': 0, 'pending': 0, 'failed': 0, 'completing': 0, 'unknown': 0}

    stats_heartbeat = StatsHeartbeat(config['heartbeat_interval'],
                                     database[config['db_collection']],
                                     accum_stats, stats)
    logging.info("Heartbeat daemon spawned")

    # loop over queued jobs and update status
    files_coll = database[config['db_production_files_collection']]

    while True:

        slurm_jobs = slurm_utility.get_queued_jobs(config['slurm_user'])
        stats = {'total_in_queue': len(slurm_jobs), 'running': 0, 'pending': 0, 'failed': 0, 'completing': 0, 'unknown': 0}

        for job in files_coll.find({'$or': [{'status': 'PENDING'}, {'status': 'RUNNING'}]}):

            #job is still in queue, update info
            if job['slurm_id'] in slurm_jobs:

                state = slurm_jobs[job['slurm_id']]
                if state == 'PENDING':
                    stats['pending'] += 1
                elif state == 'RUNNING':
                    stats['running'] += 1
                    if state != job['state']:
                        job['state'] = 'RUNNING'
                        files_coll.update_one({'_id':job['_id']}, {'$set': job}, upsert=False)
                elif state == 'COMPLETING':
                    stats['completing'] += 1
                else:
                    stats['unknown'] += 1

            #job is out of queue, check status
            else:

                job_stats = slurm_utility.get_job_stats(job['slurm_id'])
                state = job_stats['state']
                if state == 'COMPLETED':
                    job['state'] = 'COMPLETED'
                    job['Elapsed'] = job_stats['Elapsed']
                    job['CPUTime'] = job_stats['CPUTime']
                    job['CpuEff'] = job_stats['CpuEff']
                    job['MaxRSS'] = job_stats['MaxRSS']
                    job['MaxVMSize'] = job_stats['MaxVMSize']
                    files_coll.update_one({'_id':job['_id']}, {'$set': job}, upsert=False)
                    accum_stats['completed'] += 1
                elif state == 'FAILED':
                    job['failed'] = 'FAILED'
                    files_coll.update_one({'_id':job['_id']}, {'$set': job}, upsert=False)
                    accum_stats['failed'] += 1
                    stats['failed'] += 1
                else:
                    stats['unknown'] += 1

        stats_heartbeat.accum_stats = accum_stats
        stats_heartbeat.stats = stats

        time.sleep(config['recheck_sleep_interval'])

if __name__ == '__main__':
    main()
