"""Utility functions to load configuration files"""

import os
import sys
import argparse
import logging
import yaml

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

def load_configuration(configuration_file):
    """Load parameters from configuration file and return dictionary"""

    logging.info("-------------------------------------------------------------------------")

    if os.path.exists(configuration_file):
        logging.info("Loading configuration file %s", configuration_file)
    else:
        logging.error("Configuration file %s doesn't exist!", configuration_file)
        exit(1)

    conf_file = open(configuration_file, 'r')
    parameters = yaml.load(conf_file)
    conf_file.close()

    for key in parameters:
        logging.info('%s: %s', key, parameters[key])
        if key.find('path') > 0:
            if not os.path.isdir(parameters[key]):
                logging.error("Path %s does not exist or is not a directory!", parameters[key])
                exit(1)

    logging.info("Done loading configuration")
    logging.info("-------------------------------------------------------------------------")

    return parameters

def get_args(brief):
    """Parses command line arguments """
    parser = argparse.ArgumentParser(description=brief)
    required = parser.add_argument_group('required arguments')
    required.add_argument('-c', '--configuration', help='configuration file', action='store', type=str)
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true', default=False)

    args = parser.parse_args()

    if not args.configuration:
        logging.error("Need configuration file")
        logging.info("Usage: %s -c configuration.yaml", sys.argv[0])
        exit(1)

    return args

def affix_production_tag(parameters, collections):
    """Affix production tag to db collections"""

    for coll in collections:
        parameters[coll] = '%s_%s'%(parameters[coll], parameters['production_tag'])

    return parameters
