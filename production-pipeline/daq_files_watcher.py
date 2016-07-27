#!/bin/usr/python
"""Daemon to watch for daq files at a specific path and populate the DB """

import argparse
import sys
import os
import time
import datetime
# import threading
import logging
import yaml
from MongoDbUtil import MongoDbUtil
from StatsHeartbeat import StatsHeartbeat

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

# global variables
# pylint: disable=C0103
__global_parameters = {'verbose' : False,
                       'db_server': '',
                       'db_name': '',
                       'db_collection': '',
                       'db_production_files_collection': '',
                       'daq_files_path' : '',
                       'crawl_disk_every' : 900,
                       'heartbeat' : True,
                       'heartbeat_interval' : 15}

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(module)s : %(message)s')
__logger = logging.getLogger(__name__)
# pylint: enable=C0103

def main():
    """Daemon to watch for daq files at a specific path and populate the DB """

    args = get_args()
    load_configuration(args.configuration)

    database = MongoDbUtil('admin', db_server=__global_parameters['db_server'], db_name=__global_parameters['db_name']).database()

    # spawn a stats heartbeat
    accum_stats = {'total_files_seen': 0}
    stats = {'total_files_on_disk': 0}

    stats_heartbeat = StatsHeartbeat(__global_parameters['heartbeat_interval'],
                                     database[__global_parameters['db_collection']],
                                     accum_stats, stats, __global_parameters['verbose'])
    __logger.info("Heartbeat daemon spawned")

    # crawl over files
    while True:
        crawl_disk(database[__global_parameters['db_production_files_collection']], stats_heartbeat.accum_stats, stats_heartbeat.stats)
        time.sleep(__global_parameters['crawl_disk_every'])

def get_args():
    """Parses command line arguments """
    parser = argparse.ArgumentParser(description="Daemon to watch for daq files at a specific path and populate the DB")
    required = parser.add_argument_group('required arguments')
    required.add_argument('-c', '--configuration', help='configuration file', action='store', type=str)
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true', default=False)

    args = parser.parse_args()

    if not args.configuration:
        __logger.error("Need configuration file")
        __logger.info("Usage: %s -c configuration.yaml", sys.argv[0])
        exit(1)

    __global_parameters['verbose'] = args.verbose

    return args

def load_configuration(configuration_file):
    """Load parameters from configuration file """

    __logger.info("-------------------------------------------------------------------------")
    # open configuration file
    if os.path.exists(configuration_file):
        __logger.info("Loading configuration file %s", configuration_file)
    else:
        __logger.error("Configuration file %s doesn't exist!", configuration_file)
        exit(1)

    conf_file = file(configuration_file, 'r')
    parameters = yaml.load(conf_file)
    conf_file.close()

    # set db parameters
    set_config_parameter(parameters, 'db_server')
    set_config_parameter(parameters, 'db_name')
    set_config_parameter(parameters, 'db_collection')
    set_config_parameter(parameters, 'db_production_files_collection')

    # set daq files directory path
    set_config_parameter(parameters, 'daq_files_path')

    if not os.path.isdir(__global_parameters['daq_files_path']):
        __logger.error("Path %s does not exist or is not a directory!", __global_parameters['daq_files_path'])
        exit(1)

    # set disk search periodicity
    set_config_parameter(parameters, 'crawl_disk_every', 'seconds')

    # set heartbeat parameters
    set_config_parameter(parameters, 'heartbeat')
    if __global_parameters['heartbeat']:
        set_config_parameter(parameters, 'heartbeat_interval', 'seconds')
    else:
        __logger.info("Heart beat disabled in configuration file")

    __logger.info("Done loading configuration")
    __logger.info("-------------------------------------------------------------------------")

def set_config_parameter(parameters, parameter_name, units=''):
    """ To set global parameters if the it exists, exists otherwise """

    if parameter_name in parameters:
        __global_parameters[parameter_name] = parameters[parameter_name]
        __logger.info("%s: %s %s", parameter_name, __global_parameters[parameter_name], units)
    else:
        __logger.error("%s is not set in the configuration file", parameter_name)
        exit(1)

#pylint: disable-msg=too-many-locals
def crawl_disk(files_coll, accum_stats, stats):
    """Crawls over the disk and updates the DB"""

    number_files_on_disk = 0
    number_new_files = 0
    for dirname, _, filenames in os.walk(__global_parameters['daq_files_path']):

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
