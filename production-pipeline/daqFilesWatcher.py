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
from mongoDbUtil import mongoDbUtil

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

# global variables
__verbose = False
__daq_files_path = ''
__beat_your_heart = True
__heartbeat_interval = 15
__files_stats = {}

def main():
    """Daemon to watch for daq files at a specific path and populate the DB """
    log_format = '%(asctime)-15s %(levelname)s: %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)

    args = get_args()
    load_configuration(args.configuration)

    data_base = mongoDbUtil('admin').db
    init()

    heartbeat_thread = threading.Thread(target=heartbeat, args=(data_base['daqFilesWatcher'],))
    heartbeat_thread.setDaemon(True)
    heartbeat_thread.start()

    # This is just to keep the main thread running
    while True:
        time.sleep(1e4)

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

    global __verbose
    __verbose = args.verbose

    return args

def load_configuration(configuration_file):
    """Load parameters from configuration file """

    # open configuration file
    if os.path.exists(configuration_file):
        logging.info("Loading configuration file ...")
    else:
        logging.error("Configuration file %s doesn't exist!", configuration_file)
        exit(1)

    conf_file = file(configuration_file, 'r')
    parameters = yaml.load(conf_file)
    conf_file.close()

    # set daq files directory path
    global __daq_files_path
    __daq_files_path = parameters['daq_files_path']

    if os.path.isdir(__daq_files_path):
        logging.info("Set to watch %s", __daq_files_path)
    else:
        logging.error("Path %s does not exist or is not a directory!", __daq_files_path)
        # exit(1)

    # set heartbeat parameters
    global __beat_your_heart
    global __heartbeat_interval
    if 'heartbeat' in parameters and parameters['heartbeat'] == 'True':
        __beat_your_heart = True

        if 'heartbeat_interval' in parameters:
            __heartbeat_interval = int(parameters['heartbeat_interval'])

        logging.info("Heartbeat interval set to %i seconds", __heartbeat_interval)
    else:
        logging.info("Heart beat disabled in configuration file")
        __beat_your_heart = False

def init():
    """Intialize variables from DB """
    __files_stats['numberOfFilesOnDisk'] = 0
    __files_stats['totalNumberOfFilesSeen'] = 0

def heartbeat(hb_coll):
    """Send a heartbeat to DB """

    while __beat_your_heart:
        entry = {'numberOfFilesOnDisk': __files_stats['numberOfFilesOnDisk'],
                 'totalNumberOfFilesSeen' : __files_stats['totalNumberOfFilesSeen'],
                 'date' : datetime.datetime.utcnow()}
        hb_coll.insert(entry)
        time.sleep(__heartbeat_interval)

if __name__ == '__main__':
    main()
