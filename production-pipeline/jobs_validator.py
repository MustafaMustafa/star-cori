#!/bin/usr/python
"""Daemon to watch jobs in slurm queue and validate output of completed jobs"""

import sys
import time
import logging
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

    jobs_validator(args.configuration)

def jobs_validator(config_file):
    """Daemon to watch jobs in slurm queue and validate output of completed jobs"""

    config = load_configuration.load_configuration(config_file)
    config = load_configuration.affix_production_tag(config, ['db_collection', 'db_production_files_collection'])

    database = MongoDbUtil('admin', db_server=config['db_server'], db_name=config['db_name']).database()

    # spawn a stats heartbeat
    accum_stats = {'completed': 0, 'failed': 0}
    stats = {'total_in_queue': 0, 'running': 0, 'pending': 0, 'failed': 0}

    stats_heartbeat = StatsHeartbeat(config['heartbeat_interval'],
                                     database[config['db_collection']],
                                     accum_stats, stats)
    logging.info("Heartbeat daemon spawned")

    while True:
        time.sleep(config['recheck_sleep_interval'])

if __name__ == '__main__':
    main()
