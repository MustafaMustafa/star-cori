"""A class to create star prepare and submit jobs sbatch files"""

import os
import binascii
import subprocess
import math
import logging

__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"


def mkdir(dirname):
    """mkdir if directory doesn't exist"""

    if not os.path.isdir(dirname):
        subprocess.call(['mkdir', '-p', dirname])

# pylint: disable=too-many-instance-attributes
# The number of attributes is adequate for this class
class StarSubmitSlurmEngine(object):
    """A class to create prepare submission enivornment and submit jobs sbatch files"""

    def __init__(self, parameters):

        self.__submit = parameters['submit']
        self.__shifter_img = parameters['shifter_img']
        self.__stardb_payload = parameters['stardb_payload']
        self.__production_chain = parameters['production_chain']
        self.__queue = parameters['queue']
        self.__cputime_per_event = parameters['cputime_per_event']
        self.__production_dir = os.path.abspath(parameters['production_dir'])
        self.__stdout_dir = os.path.abspath(parameters['stdout_dir'])
        self.__stderr_dir = os.path.abspath(parameters['stderr_dir'])
        self.__sbatch_dir = os.path.abspath(parameters['sbatch_dir'])
        self.__mpi_binary = os.path.abspath(parameters['mpi_binary'])
        self.__production_file_extensions = parameters['extensions']
        self.__submission_id = binascii.hexlify(os.urandom(16)).decode('utf-8')
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
        job_parameters['log'] = '%s/%i/%i'%(self.__stdout_dir, job_parameters['day'], job_parameters['runnumber'])
        job_parameters['err'] = '%s/%i/%i'%(self.__stderr_dir, job_parameters['day'], job_parameters['runnumber'])
        job_parameters['sbatch'] = '%s/%i/%i'%(self.__sbatch_dir, job_parameters['day'], job_parameters['runnumber'])

        mkdir(job_parameters['production_dir'])
        mkdir(job_parameters['log'])
        mkdir(job_parameters['err'])
        mkdir(job_parameters['sbatch'])

        job_parameters['log'] += '/%s.log'%job_parameters['submission_idx']
        job_parameters['err'] += '/%s.err'%job_parameters['submission_idx']
        job_parameters['sbatch'] += '/%s.sbatch'%job_parameters['submission_idx']

        #pylint: disable-msg=too-many-format-args
        command = '/usr/lib64/openmpi-1.10/bin/mpirun --tag-output'
        job_parameters['command'] = '%s -n %i runBfcChainMpi.o 1 %i \"%s\" \"%s\"'%(command, number_of_cores,
                                                                                    job_parameters['number_of_events'],
                                                                                    self.__production_chain,
                                                                                    os.path.abspath(job_parameters['daq_path']))
        #pylint: enable-msg=too-many-format-args

        self.__make_sbatch_file(job_parameters)

        if self.__submit:
            output = subprocess.Popen(['sbatch', job_parameters['sbatch']], stdout=subprocess.PIPE).communicate()[0]
            if output:
                jobid = [int(s) for s in output.split() if s.isdigit()]
                job_parameters['slurm_id'] = int(jobid[0])
            else:
                job_parameters['slurm_id'] = -1
        else:
            job_parameters['slurm_id'] = -1

        return job_parameters

    def __determine_running_parameters(self, number_of_events):
        """Determines job running queue and number of cores per job

           returns queue, number_of_processes for this job and estimated running time"""

        totaltime = self.__cputime_per_event * number_of_events
        number_of_cores = math.ceil(float(totaltime)/float(self.__queue['max_running_time']))

        if number_of_cores > self.__queue['max_number_of_cores']:
            logging.warning('%i events is too high to run on a single node. Limiting to max number of cores/node = %i',
                            number_of_events, self.__queue['max_number_of_cores'])
            number_of_cores = self.__queue['max_number_of_cores']

        time_per_core = math.ceil(float(totaltime)/float(number_of_cores)) + 15*60
        minutes, seconds = divmod(time_per_core, 60)
        hours, minutes = divmod(minutes, 60)
        estimated_running_time = '%d:%d:%d'%(hours, minutes, seconds)

        return self.__queue['name'], number_of_cores, estimated_running_time

    def __make_sbatch_file(self, job_parameters):
        """Create production/sbatch/log/err directories and make sbatch file"""

        scratch_dir = os.getenv('SCRATCH', 'FALSE')
        stardb_dir_name = os.path.basename(os.path.normpath(self.__stardb_payload))

        if scratch_dir == 'FALSE':
            logging.error('$SCRATCH is not defined. Cannot proceed!')
            exit(1)

        memory_point = 0
        job_scratch = job_parameters['submission_idx']
        sbatch_file = open(job_parameters['sbatch'], 'w')
        sbatch_file.write('#!/bin/bash'+'\n')
        sbatch_file.write('#SBATCH --image=%s'%self.__shifter_img+'\n')
        sbatch_file.write('#SBATCH --volume=%s/%s:/mnt:perNodeCache=size=100G\n'%(scratch_dir, job_parameters['submission_idx']))
        sbatch_file.write('#SBATCH --ntasks=%i'%(job_parameters['number_of_cores']*self.__queue['threads_per_bfc'])+'\n')
        sbatch_file.write('#SBATCH --partition=%s'%job_parameters['queue']+'\n')
        sbatch_file.write('#SBATCH --output=%s'%job_parameters['log']+'\n')
        sbatch_file.write('#SBATCH --error=%s'%job_parameters['err']+'\n')
        sbatch_file.write('#SBATCH --time=%s'%job_parameters['estimated_running_time']+'\n')
        sbatch_file.write('\n')
        sbatch_file.write('echo "Memory point %i"\nfree -g\n\n'%memory_point)
        memory_point += 1
        sbatch_file.write('module load shifter\n')
        sbatch_file.write('shifter /bin/csh <<EOF\n')
        sbatch_file.write('source /usr/local/star/group/templates/cshrc\n')
        sbatch_file.write('source /usr/local/star/group/templates/enable_scl\n')
        sbatch_file.write('\n')
        sbatch_file.write('#Prepare DB...\n')
        sbatch_file.write('cd /mnt\n')
        sbatch_file.write('echo "DB cp time is : "\n')
        sbatch_file.write('time cp -r -p %s .\n'%self.__stardb_payload)
        sbatch_file.write('/usr/bin/mysqld_safe --defaults-file=/mnt/%s/my.cnf --skip-grant-tables &\n'%stardb_dir_name)
        sbatch_file.write('setenv DB_SERVER_LOCAL_CONFIG "/mnt/%s/dbLoadBalancerLocalConfig_generic.xml"\n'%stardb_dir_name)
        sbatch_file.write('sleep 30\n')
        sbatch_file.write('\n')
        sbatch_file.write('echo "Memory point %i"\nfree -g\n\n'%memory_point)
        memory_point += 1
        sbatch_file.write('#Run job...\n')
        sbatch_file.write('cp %s .'%self.__mpi_binary+'\n')
        sbatch_file.write('%s\n'%job_parameters['command'])
        sbatch_file.write('\n')

        if 'MuDst.root' in self.__production_file_extensions:
            sbatch_file.write('\n#Merge MuDst files...\n')
            sbatch_file.write('ls *.MuDst.root > tmp.MuDst.list\n')
            sbatch_file.write('sort tmp.MuDst.list -o tmp.MuDst.list\n')
            sbatch_file.write('root4star -l -b -q -x lMuDst.C \'Hadd.C++(\"%s.MuDst.root\", \"@tmp.MuDst.list\")\''%job_parameters['basename']+'\n')
            sbatch_file.write('\n')
            sbatch_file.write('#Check production...\n')
            sbatch_file.write('root4star -l -b -q -x \'checkProduction.C(\"%s.MuDst.root\", %i)\''%(job_parameters['basename'], job_parameters['number_of_events']))
            sbatch_file.write('\n')

        sbatch_file.write('#Copy back output files...\n')
        for ext in self.__production_file_extensions:
            if ext == 'MuDst.root':
                sbatch_file.write('cp -p %s.nEventsCheck.yaml %s\n'%(job_parameters['basename'], os.path.dirname(job_parameters['log'])))
                sbatch_file.write('cp -p %s.%s %s\n'%(job_parameters['basename'], ext, job_parameters['production_dir']))
            else:
                sbatch_file.write('cp -p *.%s %s\n'%(ext, job_parameters['production_dir']))

        sbatch_file.write('EOF\n')
        sbatch_file.write('echo "Memory point %i"\nfree -g\n'%memory_point)
        sbatch_file.close()
# pylint: enable=too-many-instance-attributes
