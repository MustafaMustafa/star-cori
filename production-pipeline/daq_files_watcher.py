#!/bin/usr/python
"""Daemon to watch for daq files at a specific path and populate the DB """

import logging
import argparse
import sys
import os
import time
import datetime
import threading
import yaml
from MongoDbUtil import MongoDbUtil

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
                       'heartbeat_interval' : 15,
                       'files_stats' : {}}

def main():
    """Daemon to watch for daq files at a specific path and populate the DB """
    log_format = '%(asctime)-15s %(levelname)s: %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)

    args = get_args()
    load_configuration(args.configuration)

    database = MongoDbUtil('admin', db_server=__global_parameters['db_server'], db_name=__global_parameters['db_name']).database()
    init_stats(database[__global_parameters['db_collection']])

    if __global_parameters['heartbeat']:
        heartbeat_thread = threading.Thread(target=heartbeat, args=(database[__global_parameters['db_collection']],))
        heartbeat_thread.setDaemon(True)
        heartbeat_thread.start()
        logging.info("Heartbeat daemon spawned")

    while True:
        crawl_disk(database[__global_parameters['db_production_files_collection']])
        time.sleep(__global_parameters['crawl_disk_every'])

def get_args():
    """Parses command line arguments """
    parser = argparse.ArgumentParser(description="Daemon to watch for daq files at a specific path and populate the DB")
    required = parser.add_argument_group('required arguments')
    required.add_argument('-c', '--configuration', help='configuration file', action='store', type=str)
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true', default=False)

    args = parser.parse_args()

    if not args.configuration:
        logging.error("Need configuration file")
        logging.info("Usage: %s -c configuration.yaml", sys.argv[0])
        exit(1)

    __global_parameters['verbose'] = args.verbose

    return args

def load_configuration(configuration_file):
    """Load parameters from configuration file """

    logging.info("-------------------------------------------------------------------------")
    # open configuration file
    if os.path.exists(configuration_file):
        logging.info("Loading configuration file %s", configuration_file)
    else:
        logging.error("Configuration file %s doesn't exist!", configuration_file)
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
        logging.error("Path %s does not exist or is not a directory!", __global_parameters['daq_files_path'])
        exit(1)

    # set disk search periodicity
    set_config_parameter(parameters, 'crawl_disk_every', 'seconds')

    # set heartbeat parameters
    set_config_parameter(parameters, 'heartbeat')
    if __global_parameters['heartbeat']:
        set_config_parameter(parameters, 'heartbeat_interval', 'seconds')
    else:
        logging.info("Heart beat disabled in configuration file")

    logging.info("Done loading configuration")
    logging.info("-------------------------------------------------------------------------")

def set_config_parameter(parameters, parameter_name, units=''):
    """ To set global parameters if the it exists, exists otherwise """

    if parameter_name in parameters:
        __global_parameters[parameter_name] = parameters[parameter_name]
        logging.info("%s: %s %s", parameter_name, __global_parameters[parameter_name], units)
    else:
        logging.error("%s is not set in the configuration file", parameter_name)
        exit(1)

def init_stats(hearbeat_coll):
    """Intialize stats from DB latest record"""

    logging.info("Initializing variables from DB ...")
    if hearbeat_coll.count():
        last_doc = hearbeat_coll.find().skip(hearbeat_coll.count()-1)[0]
        __global_parameters['files_stats']['numberOfFilesOnDisk'] = last_doc['numberOfFilesOnDisk']
        __global_parameters['files_stats']['totalNumberOfFilesSeen'] = last_doc['totalNumberOfFilesSeen']
    else:
        __global_parameters['files_stats']['numberOfFilesOnDisk'] = 0
        __global_parameters['files_stats']['totalNumberOfFilesSeen'] = 0

    logging.info("Number of files on disk according to DB = %i", __global_parameters['files_stats']['numberOfFilesOnDisk'])
    logging.info("Number of files ever seen according to DB = %i", __global_parameters['files_stats']['totalNumberOfFilesSeen'])


def heartbeat(hb_coll):
    """Send a heartbeat to DB """

    while __global_parameters['heartbeat']:
        entry = {'numberOfFilesOnDisk': __global_parameters['files_stats']['numberOfFilesOnDisk'],
                 'totalNumberOfFilesSeen' : __global_parameters['files_stats']['totalNumberOfFilesSeen'],
                 'date' : datetime.datetime.utcnow()}
        hb_coll.insert(entry)
        if __global_parameters['verbose']:
            logging.info("heartbeat: %i files on disk, %i total files seen", entry['numberOfFilesOnDisk'], entry['totalNumberOfFilesSeen'])
        time.sleep(__global_parameters['heartbeat_interval'])

def crawl_disk(files_coll):
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
                path = os.path.join(dirname, filename)
                timestamp = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(os.path.getmtime(path)))
                day, runnumber = get_day_and_number(filename)
                doc = {'basename' : basename,
                       'daq_path' : path,
                       'daq_timestamp' : timestamp,
                       'day': day,
                       'runnumber': runnumber}
                files_coll.insert(doc)

    __global_parameters['files_stats']['numberOfFilesOnDisk'] = number_files_on_disk
    __global_parameters['files_stats']['totalNumberOfFilesSeen'] += number_new_files

    logging.info("Added %i new daq files to DB", number_new_files)

def get_day_and_number(baseName):
    """Return day and runnumber"""

    idx = baseName.find('_raw')

    if idx:
        runnumber = int(baseName[idx-8:idx])
        day = int((runnumber%1e6)/1e3)
    else:
        day = -1
        runnumber = -1

    return day, runnumber

if __name__ == '__main__':
    main()
