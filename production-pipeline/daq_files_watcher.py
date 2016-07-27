#!/bin/usr/python
"""Daemon to watch for daq files at a specific path and populate the DB """

import sys
import os
import time
import datetime
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
    """Daemon to watch for daq files at a specific path and populate the DB """

    args = get_args(__doc__)
    config = load_configuration(args.configuration)

    database = MongoDbUtil('admin', db_server=config['db_server'], db_name=config['db_name']).database()

    # spawn a stats heartbeat
    accum_stats = {'total_files_seen': 0}
    stats = {'total_files_on_disk': 0}

    stats_heartbeat = StatsHeartbeat(config['heartbeat_interval'],
                                     database[config['db_collection']],
                                     accum_stats, stats)
    __logger.info("Heartbeat daemon spawned")

    # crawl over files
    while True:
        crawl_disk(database[config['db_production_files_collection']], config['daq_files_path'], stats_heartbeat.accum_stats, stats_heartbeat.stats)
        time.sleep(config['crawl_disk_every'])

#pylint: disable-msg=too-many-locals
def crawl_disk(files_coll, daqs_path, accum_stats, stats):
    """Crawls over the disk and updates the DB"""

    number_files_on_disk = 0
    number_new_files = 0
    for dirname, _, filenames in os.walk(daqs_path):

        for filename in filenames:
            if filename.find('.daq') < 0:
                continue
            else:
                basename = filename[0:filename.find('.daq')]

            number_files_on_disk += 1
            db_search = files_coll.find({'basename' : basename})
            if not db_search.count():
                number_new_files += 1

                with open(os.path.join(dirname, '%s.mrk'%basename), 'r') as f_mrk:
                    tmp = f_mrk.readlines()
                    if len(tmp) > 1:
                        __logger.error('Markup file %s.mrk has more than one line skipping ...', filename)
                    else:
                        number_of_events = int(tmp[0].rstrip())


                path = os.path.join(dirname, filename)
                timestamp = datetime.datetime.utcfromtimestamp(os.path.getmtime(path))
                day, runnumber = get_day_and_number(filename)
                doc = {'basename' : basename,
                       'daq_path' : path,
                       'daq_timestamp' : timestamp,
                       'day': day,
                       'runnumber': runnumber,
                       'number_of_submissions': 0,
                       'number_of_events': number_of_events}
                files_coll.insert(doc)

    accum_stats['total_files_seen'] = accum_stats['total_files_seen'] + number_new_files
    stats['total_files_on_disk'] = number_files_on_disk

    __logger.info("Added %i new daq file(s) to DB", number_new_files)
#pylint: enable-msg=too-many-locals

def get_day_and_number(basename):
    """Return day and runnumber"""

    idx = basename.find('_raw')

    if idx:
        runnumber = int(basename[idx-8:idx])
        day = int((runnumber%1e6)/1e3)
    else:
        day = -1
        runnumber = -1

    return day, runnumber

if __name__ == '__main__':
    main()
