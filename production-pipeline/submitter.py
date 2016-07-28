#!/bin/usr/python
"""Daemon to re/submit jobs according to files state in DB"""

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

    submitter(args.configuration)

def submitter(config_file):
    """re/submit jobs according to files state in DB"""

    config = load_configuration.load_configuration(config_file)
    config = load_configuration.affix_production_tag(config, ['db_collection', 'db_production_files_collection'])

    database = MongoDbUtil('admin', db_server=config['db_server'], db_name=config['db_name']).database()

    # spawn a stats heartbeat
    accum_stats = {'submitted': 0}
    stats = {'pending': 0}

    stats_heartbeat = StatsHeartbeat(config['heartbeat_interval'],
                                     database[config['db_collection']],
                                     accum_stats, stats)
    logging.info("Heartbeat daemon spawned")

    while True:
        time.sleep(config['submit_sleep_interval'])

if __name__ == '__main__':
    main()
