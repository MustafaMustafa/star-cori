#!/bin/usr/python
"""Daemon to merge output files (MuDst/PicoDst)"""

import sys
import time
import logging
from MongoDbUtil import MongoDbUtil
from StatsHeartbeat import StatsHeartbeat
from load_configuration import get_args
from load_configuration import load_configuration

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

# pylint: disable=C0103
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(module)s : %(message)s')
__logger = logging.getLogger(__name__)
# pylint: enable=C0103

def main():
    """Daemon to watch jobs in slurm queue and validate output of completed jobs"""

    args = get_args(__doc__)
    config = load_configuration(args.configuration)

    database = MongoDbUtil('admin', db_server=config['db_server'], db_name=config['db_name']).database()

    # spawn a stats heartbeat
    accum_stats = {'merged': 0, 'failed': 0}
    stats = {'merging': 0, 'failed': 0}

    stats_heartbeat = StatsHeartbeat(config['heartbeat_interval'],
                                     database[config['db_collection']],
                                     accum_stats, stats)
    logging.info("Heartbeat daemon spawned")

    while True:
        time.sleep(config['recheck_sleep_interval'])
if __name__ == '__main__':
    main()
