#!/usr/bin/python
__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"
""" a script to create batch files for star data production using slurm """

import os
import sys
import argparse
import binascii
import math

SUBMISSION_ID = ''
JOB_IDX = -1
CPU_PER_EVENT = 50
QUEUE_LIMIT = 10*60*60
IMAGE = 'docker:mmustafa/sl64_sl16c:v1_pdsf'
CHAIN = 'DbV20150316 P2014a pxlHit istHit btof mtd mtdCalib BEmcChkStat CorrX OSpaceZ2 OGridLeak3D -hitfilt'
OUTDIR = '/project/projectdirs/starprod/rnc/mustafa/cori_test/prod'
SBATCH_DIR = './sbatch'
LOG_DIR = './log'
ERR_DIR = './err'

def get_args():
    parser = argparse.ArgumentParser(description="a script to create batch files for star data production using slurm")
    parser.add_argument('--list', help='file containting list of files to process', action = 'store', type=str)

    args = parser.parse_args()

    if not args.list:
        print "Need list of files to process. Usage:\n"
        exit(1)

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

def get_time(star_evt, end_evt):
    total = (end_evt-star_evt) * CPU_PER_EVENT
    m, s = divmod(total, 60)
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

def make_sbatch_file(file, star_evt, end_evt, sub_idx):

    base_name = get_basename(file)
    runnumber = get_runnumber(base_name)
    day = get_daynumber(runnumber)
    out_dir = '%s/%i/%i/%i'%(OUTDIR,day,runnumber,sub_idx)
    scratch_dir = '%s_%i'%(SUBMISSION_ID,JOB_IDX)

    sbatch_file = open("%s/sched%s_%i.sbatch"%(SBATCH_DIR,SUBMISSION_ID,JOB_IDX),'w')
    sbatch_file.write('#!/bin/bash'+'\n')
    sbatch_file.write('#SBATCH --image=%s'%IMAGE+'\n')
    sbatch_file.write('#SBATCH --nodes=1'+'\n')
    sbatch_file.write('#SBATCH --partition=regular'+'\n')
    sbatch_file.write('#SBATCH --output=%s/%s_%i.log'%(LOG_DIR,SUBMISSION_ID,JOB_IDX)+'\n')
    sbatch_file.write('#SBATCH --error=%s/%s_%i.err'%(ERR_DIR,SUBMISSION_ID,JOB_IDX)+'\n')
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

def process_file(file):
    total_time = file[1] * CPU_PER_EVENT
    number_jobs = int(math.ceil(float(total_time)/float(QUEUE_LIMIT)))
    events_per_job = file[1] / number_jobs

    global JOB_IDX
    for jj in xrange(0,number_jobs):
        start_evt = 1 + jj * events_per_job
        end_evt = (jj+1) * events_per_job
        JOB_IDX += 1
        print "%s_%i %i %i"%(SUBMISSION_ID,JOB_IDX,start_evt,end_evt)
        make_sbatch_file(file[0], start_evt,end_evt, jj)


def main():
    global SUBMISSION_ID
    SUBMISSION_ID = binascii.hexlify(os.urandom(16))
    args = get_args()

    file_list = get_list_of_files(args.list)
    process_file(file_list[0])

if __name__ == '__main__':
    main()
