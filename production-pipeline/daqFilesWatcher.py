#!/bin/usr/python
__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

"""
Daemon to watch for daq files at a specific path and populate the DB
"""

import logging
import argparse
import sys
import yaml
import os
from mongoDbUtil import mongoDbUtil

# global variables
__verbose = False
__daq_files_path = ''

def main():
    logFormat = '%(asctime)-15s %(levelname)s: %(message)s'
    logging.basicConfig(level=logging.INFO, format=logFormat)

    args = get_args()
    load_configuration(args.configuration)

    db = mongoDbUtil('admin').db

def get_args():
    parser = argparse.ArgumentParser(description="Daemon to watch for daq files at a specific path and populate the DB")
    required = parser.add_argument_group('required arguments')
    required.add_argument('-c','--configuration', help='configuration file', action = 'store', type=str)
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action = 'store_true', default = False)

    args = parser.parse_args()

    if not args.configuration:
        logging.error("Need configuration file")
        logging.info("Usage: %s -c configuration.yaml"%sys.argv[0])
        exit(1)

    global __verbose
    __verbose = args.verbose

    return args

def load_configuration(configuration_file):

    if os.path.exists(configuration_file):
        logging.info("Loading configuration file ...")
    else:
        logging.error("Configuration file %s doesn't exist!"%configuration_file)
        exit(1)

    f = file(configuration_file,'r')
    parameters = yaml.load(f)
    f.close()

    __daq_files_path = parameters['daq_files_path']

    if os.path.isdir(__daq_files_path):
        logging.info("Set to watch %s"%__daq_files_path)
    else:
        logging.error("Path %s does not exist or is not a directory!"%__daq_files_path)
        exit(1)

if __name__ == '__main__':
    main()
