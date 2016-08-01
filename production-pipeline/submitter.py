#!/bin/usr/python
"""Daemon to re/submit jobs according to files state in DB"""

import sys
import time
import logging
import load_configuration
import pymongo
from MongoDbUtil import MongoDbUtil
from StatsHeartbeat import StatsHeartbeat
from StarSubmitSlurmEngine import StarSubmitSlurmEngine

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

# pylint: disable=C0103
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(module)s : %(message)s')
__logger = logging.getLogger(__name__)
# pylint: enable=C0103

def main():
    """To be used in CLI mode"""

    args = load_configuration.get_args(__doc__)

    submitter(args.configuration)

def submitter(config_file):
    """re/submit jobs according to files state in DB"""

    config = load_configuration.load_configuration(config_file)
    config = load_configuration.affix_production_tag(config, ['db_collection',
                                                              'db_production_files_collection',
                                                              'db_jobs_validator_collection'])

    database = MongoDbUtil('admin', db_server=config['db_server'], db_name=config['db_name']).database()

    # spawn a stats heartbeat
    stats_heartbeat = StatsHeartbeat(config['heartbeat_interval'],
                                     database[config['db_collection']],
                                     accum_stats={'ever_submitted': 0},
                                     stats={'to_submit': 0, 'submitted': 0, 'resubmitted':0})
    logging.info("Heartbeat daemon spawned")

    # main work - loop over production files collection and submit jobs
    star_submit_engine = StarSubmitSlurmEngine(config)
    files_coll = database[config['db_production_files_collection']]
    jobs_coll = database['db_jobs_validator_collection']

    while True:

        stats = {'to_submit': 0, 'submitted': 0, 'resubmitted':0}

        empty_job_slots = config['max_jobs_in_queue'] - number_of_jobs_in_queue(jobs_coll)

        for daq in files_coll.find({'$or': [{'submitted': 0}, {'$where': 'this.failed == this.submitted'}]}):

            if daq['failed'] == config['max_resubmissions']:
                continue

            if empty_job_slots:
                updated_daq = star_submit_engine.process_job(daq)
                updated_daq['submitted'] += 1
                updated_daq['status'] = 'PENDING'

                stats['submitted'] += 1
                stats_heartbeat.accum_stats['ever_submitted'] += 1

                if daq['failed']:
                    updated_daq['failed'] += 1
                    stats['resubmitted'] += 1

                files_coll.update_one({'_id':daq['_id']}, {'$set': updated_daq}, upsert=False)
                empty_job_slots -= 1
            else:
                stats['to_submit'] += 1

        stats_heartbeat.stats = stats

        time.sleep(config['submit_sleep_interval'])

def number_of_jobs_in_queue(coll):
    """return number of queued jobs"""

    if coll.count():
        last_doc = coll.find().skip(coll.count()-1)[0]
        return last_doc['stats']['total_in_queue']
    else:
        return 0


if __name__ == '__main__':
    main()
