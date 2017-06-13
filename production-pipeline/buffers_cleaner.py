#!/bin/usr/python
"""Daemon to clean pipline input and ouput buffers"""

import sys
import os
import time
import logging
import tarfile
import load_configuration
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

    buffers_cleaner(args.configuration)

def buffers_cleaner(config_file):
    """Daemon to clean pipline input and ouput buffers"""

    config = load_configuration.load_configuration(config_file)
    config = load_configuration.affix_production_tag(config, ['db_collection', 'db_production_files_collection'])

    database = MongoDbUtil('admin', db_server=config['db_server'], db_name=config['db_name']).database()

    # spawn a stats heartbeat
    stats_heartbeat = StatsHeartbeat(config['heartbeat_interval'],
                                     database[config['db_collection']],
                                     accum_stats={'cleaned_prod': 0},
                                     stats={'missing_prod': 0, 'tar_failed': 0})
    logging.info("Heartbeat daemon spawned")

    files_coll = database[config['db_production_files_collection']]
    while True:
        stats = {'dsts_on_disk': 0, 'missing_prod': 0, 'tar_failed': 0}

        for job in files_coll.find({'status': 'COMPLETED'}):

            # check all output files
            if not check_prod_files(job):
                stats['missing_prod'] += 1
                continue

            tFile = tarfile.open('%s/%s.cori.tar'%(config['tar_dir'], job['basename']), 'w')

            for prod_file in job['production_files']:
                tFile.addfile(tarfile.TarInfo(os.path.basename(prod_file)), prod_file)

            tFile.addfile(tarfile.TarInfo(os.path.basename(job['log'])), job['log'])
            tFile.addfile(tarfile.TarInfo(os.path.basename(job['err'])), job['err'])
            eventCheck_file = os.path.join(os.path.dirname(job['log']), '%s.nEventsCheck.yaml'%job['basename'])
            tFile.addfile(tarfile.TarInfo(os.path.dirname(eventCheck_file)), eventCheck_file)

            try:
                tFile.close()
            except:
                stats['tar_failed'] += 1
                continue

            os.remove(job['daq_path'])
            for prod_file in job['production_files']:
                os.remove(prod_file)
            stats_heartbeat.accum_stats['cleaned_prod'] += 1

            job['status'] = 'TARBALLED'
            files_coll.update_one({'_id':job['_id']}, {'$set': job}, upsert=False)

        stats_heartbeat.stats = stats
        time.sleep(config['recheck_sleep_interval'])

def check_prod_files(job):

    for prod_file in job['production_files']:
        if not os.path.exists(prod_file):
            return False

    return True

if __name__ == '__main__':
    main()
