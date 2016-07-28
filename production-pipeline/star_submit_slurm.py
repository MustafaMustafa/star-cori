#!/usr/bin/python
"""a script to create slurm batch files to submit jobs to process a file list """

import os
import sys
import argparse
import logging
import yaml
from StarSubmitSlurmEngine import StarSubmitSlurmEngine

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

# global variables
# pylint: disable=C0103
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)-15s - %(levelname)s - %(module)s : %(message)s')
logger = logging.getLogger(__name__)
# pylint: enable=C0103

def main():
    """a script to create slurm batch files to submit jobs to process a file list """

    args = get_args()
    submission_paramaters = load_configuration(args.configuration)
    submission_paramaters['submit'] = args.submit

    star_submit_engine = StarSubmitSlurmEngine(submission_paramaters)

    file_list = get_list_of_files(args.list)

    for in_file in file_list:

        day, runnumber = get_day_and_number(in_file[0])
        job_parameters = {}
        job_parameters['daq_path'] = in_file[0]
        job_parameters['number_of_events'] = in_file[1]
        job_parameters['day'] = day
        job_parameters['runnumber'] = runnumber

        job_parameters = star_submit_engine.process_job(job_parameters)

        if args.submit:
            logger.info("Submitted batch job %i", job_parameters['slurm_id'])

    # with open('%s/%s.daqSplit.yaml'%(log_dir, base_name), 'w') as outfile:
        # yaml.dump(job_parameters, outfile)

def get_args():
    """Gets command line arguments"""

    parser = argparse.ArgumentParser(description="a script to create batch files for star data production using slurm")
    required = parser.add_argument_group('required arguments')
    required.add_argument('-c', '--configuration', help='configuration file', action='store', type=str)
    required.add_argument('-l', '--list', help='file containing list of files to process', action='store', type=str)
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true', default=False)
    parser.add_argument('-s', '--submit', help='submit jobs. Default is to create sbatch files only', action='store_true', default=False)

    args = parser.parse_args()

    if not args.list or not args.configuration:
        logger.info("Need list of daq files and a configuration file")
        logger.info("Usage: %s -l daq.list -c configuration.yaml", sys.argv[0])
        exit(1)

    return args

def load_configuration(configuration_file):
    """Loads configuration file into a yaml object and returns it"""

    if not os.path.exists(configuration_file):
        logger.error("Configuration file %s doesn't exist", configuration_file)
        exit(1)

    f_tmp = open(configuration_file, 'r')
    parameters = yaml.load(f_tmp)
    f_tmp.close()

    return parameters

def get_list_of_files(input_list):
    """Loads list of files"""

    f_in = open(input_list, 'r')
    lines = f_in.readlines()
    f_in.close()
    lines[:] = map(str.rstrip, lines)

    file_list = []
    for line in lines:
        tmp = str(line).split('::')
        file_list.append((str(tmp[0]), int(tmp[1])))

    return file_list

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
