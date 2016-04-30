#!/usr/bin/python
__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"
""" a script to create batch files for star data production using slurm """

import os
import json
import subprocess
import argparse
import binascii
import math

SUBMISSION_ID = ''
JOB_IDX = -1
CPU_PER_EVENT = 50
QUEUE_LIMIT = 11*60*60
IMAGE = 'docker:mmustafa/sl64_sl16c:v1_pdsf'
CHAIN = 'DbV20150316 P2014a pxlHit istHit btof mtd mtdCalib BEmcChkStat CorrX OSpaceZ2 OGridLeak3D -hitfilt'

OUT_DIR = '/project/projectdirs/starprod/rnc/mustafa/cori_test/prod'
SBATCH_DIR = './sbatch'
STDOUT_DIR = './log'
STDERR_DIR = './err'
DAQ_SPLIT_LOG = './daq_split_log'

VERBOSE = False
SUBMIT = False

def get_args():
    parser = argparse.ArgumentParser(description="a script to create batch files for star data production using slurm")
    parser.add_argument('-l', '--list', help='file containting list of files to process', action = 'store', type=str)
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action = 'store_true', default = False)
    parser.add_argument('-s', '--submit', help='submit jobs. Default is to create sbatch files only', action = 'store_true', default = False)

    args = parser.parse_args()

    if not args.list:
        print "Need list of files to process. Usage:\n"
        exit(1)

    global VERBOSE
    global SUBMIT
    VERBOSE = args.verbose
    SUBMIT = args.submit

    return args

def get_list_of_files(input_list):

    file = open(input_list,'r')
    lines = file.readlines()
    file.close()
    lines[:] = map(str.rstrip,lines)

    file_list = []
    for line in lines:
        p = line.split('::')
        file_list.append((str(p[0]), int(p[1])))

    return file_list

def get_time(star_evt=0, end_evt=0, total_time=0):
    if total_time == 0:
        total_time = (end_evt-star_evt) * CPU_PER_EVENT
    m, s = divmod(total_time, 60)
    h, m = divmod(m, 60)

    return "%i:%i:%i"%(h,m,s)

def get_basename(file):
    return file[file.find('st_'):file.find('.')]
                 
def get_runnumber(baseName):
    idx = baseName.find('_raw')                                                                                                              
    if idx > 0:  
        return int(baseName[idx-8:idx])
    else:        
        return -1
                 
def get_daynumber(runnumber):
    if(runnumber<1e7): return 0
                 
    return int((runnumber%1e6)/1e3)

def check_submission_env():
    
    errors = 0

    submission_dir = os.getcwd()
    if not os.path.isdir(SBATCH_DIR):
        print "ERROR: SBATCH DIR = %s doesn't exist!"%SBATCH_DIR
        errors += 1

    global STDOUT_DIR
    if not os.path.isdir(STDOUT_DIR):
        print "ERROR: LOG DIR = %s doesn't exist!"%STDOUT_DIR
        errors += 1
    elif STDOUT_DIR[0] != '/':
        if STDOUT_DIR[0] == '.':
            STDOUT_DIR = STDOUT_DIR[2:]
        STDOUT_DIR = submission_dir + '/' + STDOUT_DIR

    global STDERR_DIR
    if not os.path.isdir(STDERR_DIR):
        print "ERROR: ERR DIR = %s doesn't exist!"%STDERR_DIR
        errors += 1
    elif STDERR_DIR[0] != '/':
        if STDERR_DIR[0] == '.':
            STDERR_DIR = STDERR_DIR[2:]
        STDERR_DIR = submission_dir + '/' + STDERR_DIR

    if not os.path.isdir(OUT_DIR):
        print "ERROR: OUT DIR = %s doesn't exist!"%OUT_DIR
        errors += 1

    if not os.path.isdir(DAQ_SPLIT_LOG):
        print "ERROR: DAQ SPLIT DIR = %s doesn't exist!"%DAQ_SPLIT_LOG
        errors += 1
        
    return errors == 0


def make_sbatch_file(file, star_evt, end_evt, sub_idx):

    base_name = get_basename(file)
    runnumber = get_runnumber(base_name)
    day = get_daynumber(runnumber)
    out_dir = '%s/%i/%i/%i'%(OUT_DIR,day,runnumber,sub_idx)
    scratch_dir = '%s_%i'%(SUBMISSION_ID,JOB_IDX)
    log_dir = '%s/%i/%i'%(STDOUT_DIR,day,runnumber)
    err_dir = '%s/%i/%i'%(STDERR_DIR,day,runnumber)
    subprocess.call(['mkdir','-p',log_dir])
    subprocess.call(['mkdir','-p',err_dir])

    sbatch_filename = '%s/sched%s_%i.sbatch'%(SBATCH_DIR,SUBMISSION_ID,JOB_IDX)
    sbatch_file = open(sbatch_filename,'w')
    sbatch_file.write('#!/bin/bash'+'\n')
    sbatch_file.write('#SBATCH --image=%s'%IMAGE+'\n')
    sbatch_file.write('#SBATCH --nodes=1'+'\n')
    sbatch_file.write('#SBATCH --partition=regular'+'\n')
    sbatch_file.write('#SBATCH --output=%s/%s_%i.log'%(log_dir,SUBMISSION_ID,JOB_IDX)+'\n')
    sbatch_file.write('#SBATCH --error=%s/%s_%i.err'%(err_dir,SUBMISSION_ID,JOB_IDX)+'\n')
    sbatch_file.write('#SBATCH --time=%s'%get_time(star_evt,end_evt)+'\n')
    sbatch_file.write('\n')
    sbatch_file.write('module load shifter\n')
    sbatch_file.write('cd $SCRATCH\n')
    sbatch_file.write('mkdir %s\n'%scratch_dir)
    sbatch_file.write('cd  %s\n'%scratch_dir)
    sbatch_file.write("srun -n 1 shifter /bin/csh -c \"source /usr/local/star/group/templates/cshrc; root4star -l -b -q "+
                      "'bfc.C(%i,%i,\\\"%s\\\",\\\"%s\\\")'\""%(star_evt,end_evt,CHAIN,file))
    sbatch_file.write('\n')
    sbatch_file.write('mkdir -p %s\n'%out_dir)
    sbatch_file.write('cp -p %s.*.root %s\n'%(base_name,out_dir))
    sbatch_file.write('rm -r -f %s\n'%scratch_dir)
    sbatch_file.close()

    return sbatch_filename

def process_file(file):
    total_time = file[1] * CPU_PER_EVENT
    number_jobs = int(math.ceil(float(total_time)/float(QUEUE_LIMIT)))
    events_per_job = file[1] / number_jobs

    base_name = get_basename(file[0])
    log_json = {}
    log_json['file'] = '%s.daq'%base_name
    log_json['number_events'] = '%i'%file[1]
    log_json['number_jobs'] = '%i'%number_jobs
    log_json['total_time'] = '%s'%get_time(total_time=total_time)
    log_json['sub_files'] = []

    if VERBOSE:
        print "%s.daq -- %i job(s)"%(base_name, number_jobs)

    global JOB_IDX
    for jj in xrange(0,number_jobs):
        start_evt = 1 + jj * events_per_job
        end_evt = (jj+1) * events_per_job
        JOB_IDX += 1
        sub_file = '%s_%i:%i:%i'%(SUBMISSION_ID,JOB_IDX, start_evt, end_evt)

        sbatch_filename = make_sbatch_file(file[0], start_evt,end_evt, jj)
        if SUBMIT:
            output = subprocess.Popen(['sbatch', sbatch_filename], stdout=subprocess.PIPE).communicate()[0]
            jobid = [int(s) for s in output.split() if s.isdigit()]
            sub_file += ':%i'%jobid[0]
            if VERBOSE:
                print "Submitted job: %i"%jobid[0]

        log_json['sub_files'].append(sub_file)

    runnumber = get_runnumber(base_name)
    day = get_daynumber(runnumber)
    log_dir = '%s/%s/%s'%(DAQ_SPLIT_LOG,day,runnumber)
    subprocess.call(['mkdir','-p', log_dir])
    with open('%s/%s.daqSplit.log'%(log_dir,base_name), 'w') as outfile:
        json.dump(log_json, outfile)


def main():
    global SUBMISSION_ID
    SUBMISSION_ID = binascii.hexlify(os.urandom(16))
    args = get_args()

    if not check_submission_env():
        exit(1)


    # create DAQ split log directory for this submission
    global DAQ_SPLIT_LOG
    os.mkdir('%s/%s'%(DAQ_SPLIT_LOG,SUBMISSION_ID))
    DAQ_SPLIT_LOG += '/%s'%SUBMISSION_ID

    file_list = get_list_of_files(args.list)
    process_file(file_list[0])

if __name__ == '__main__':
    main()
