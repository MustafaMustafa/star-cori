"""A class to create star prepare and submit jobs sbatch files"""

import os
import binascii
import subprocess
import math
import custom_logger

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"

# pylint: disable=too-many-instance-attributes
# The number of attributes is adequate for this class

# global variables
# pylint: disable=C0103
logger = custom_logger.get_logger(__name__)

def mkdir(dirname):
    """mkdir if directory doesn't exist"""

    if not os.path.isdir(dirname):
        subprocess.call(['mkdir', '-p', dirname])

class StarSubmitSlurmEngine(object):
    """A class to create prepare submission enivornment and submit jobs sbatch files"""

    def __init__(self, parameters):

        self.__submit = parameters['submit']
        self.__shifter_img = parameters['shifter_img']
        self.__production_chain = parameters['production_chain']
        self.__queue = parameters['queue']
        self.__cputime_per_event = parameters['cputime_per_event']
        self.__production_dir = parameters['production_dir']
        self.__stdout_dir = parameters['stdout_dir']
        self.__stderr_dir = parameters['stderr_dir']
        self.__sbatch_dir = parameters['sbatch_dir']
        self.__production_file_extensions = parameters['extensions']
        self.__clean_scratch = parameters['clean_scratch']
        self.__submission_id = binascii.hexlify(os.urandom(16))
        self.__job_index = -1

    def process_job(self, job_parameters):
        """ Prepare job directories, call make_sbatch file, submit job"""

        self.__job_index += 1
        job_parameters['submission_idx'] = '%s_%i'%(self.__submission_id, self.__job_index)

        queue, number_of_cores, estimated_running_time = self.__determine_running_parameters(job_parameters['number_of_events'])
        job_parameters['queue'] = queue
        job_parameters['number_of_cores'] = number_of_cores
        job_parameters['estimated_running_time'] = estimated_running_time

        job_parameters['production_dir'] = '%s/%i/%i'%(self.__production_dir, job_parameters['day'], job_parameters['runnumber'])
        job_parameters['log_dir'] = '%s/%i/%i'%(self.__stdout_dir, job_parameters['day'], job_parameters['runnumber'])
        job_parameters['err_dir'] = '%s/%i/%i'%(self.__stdout_dir, job_parameters['day'], job_parameters['runnumber'])

        mkdir(job_parameters['production_dir'])
        mkdir(job_parameters['log_dir'])
        mkdir(job_parameters['err_dir'])

        #pylint: disable-msg=too-many-format-args
        command = 'shifter ./usr/lib64/openmpi-1.10/bin/mpirun --tag-output'
        job_parameters['command'] = '%s -n %i runBfcChainMpi.o 1 %i \"%s\" \"%s\"'.format(command, number_of_cores,
                                                                                          job_parameters['number_of_events'], self.__production_chain, job_parameters['daq_file'])

        job_parameters['sbatch_filename'] = self.__make_sbatch_file(job_parameters)

        if self.__submit:
            output = subprocess.Popen(['sbatch', job_parameters['sbatch_filename']], stdout=subprocess.PIPE).communicate()[0]
            jobid = [int(s) for s in output.split() if s.isdigit()]
            job_parameters['slurm_id'] = jobid[0]

        return job_parameters

    def __determine_running_parameters(self, number_of_events):
        """Determines job running queue and number of cores per job

           returns queue, number_of_processes for this job and estimated running time"""

        totaltime = self.__cputime_per_event * number_of_events
        number_of_cores = math.ceil(float(totaltime)/float(self.__queue['max_running_time']))

        if number_of_cores > self.__queue['max_number_of_cores']:
            logger.error('%i events is too high to run on a single node. Limiting to max number of cores/node = %i',
                         number_of_events, self.__queue['max_number_of_cores'])
            number_of_cores = self.__queue['max_number_of_cores']

        estimated_running_time = math.ceil(float(totaltime)/float(number_of_cores))

        return self.__queue['name'], number_of_cores, estimated_running_time

    def __make_sbatch_file(self, job_parameters):
        """Create production/sbatch/log/err directories and make sbatch file

           return sbatch filename"""

        job_scratch = job_parameters['submission_idx']
        sbatch_filename = '%s/sched%s.sbatch'%(self.__sbatch_dir, job_parameters['submission_idx'])
        sbatch_file = open(sbatch_filename, 'w')
        sbatch_file.write('#!/bin/bash'+'\n')
        sbatch_file.write('#SBATCH --image=%s'%self.__shifter_img+'\n')
        sbatch_file.write('#SBATCH --ntasks=%i'%job_parameters['number_of_cores']+'\n')
        sbatch_file.write('#SBATCH --partition=%s'%job_parameters['queue']+'\n')
        sbatch_file.write('#SBATCH --output=%s/%s.log'%(job_parameters['log_dir'], job_parameters['submission_idx'])+'\n')
        sbatch_file.write('#SBATCH --error=%s/%s.err'%(job_parameters['err_dir'], job_parameters['submission_idx'])+'\n')
        sbatch_file.write('#SBATCH --time=%s'%job_parameters['estimated_running_time']+'\n')
        sbatch_file.write('\n')
        sbatch_file.write('#Prepare environment and run job...\n')
        sbatch_file.write('module load shifter\n')
        sbatch_file.write('cd $SCRATCH\n')
        sbatch_file.write('mkdir %s\n'%job_scratch)
        sbatch_file.write('cd  %s\n'%job_scratch)
        sbatch_file.write('%s\n'%job_parameters['command'])
        sbatch_file.write('\n')

        sbatch_file.write('#Copy back output files...\n')
        for ext in self.__production_file_extensions:
            sbatch_file.write('cp -p %s.%s %s\n'%(job_parameters['base_name'], ext, job_parameters['production_dir']))
        sbatch_file.write('\n')

        if self.__clean_scratch:
            sbatch_file.write('#Clean up...\n')
            sbatch_file.write('cd ../\n')
            sbatch_file.write('rm -r -f %s\n'%job_scratch)

        sbatch_file.close()

        return sbatch_filename