#!/usr/bin/env python3
''' needs those module
on Cori:
  module load python/3.5-anaconda
on PDSF:
  module load python/3.4.3

'''

import sys
(va,vb,vc,vd,ve)=sys.version_info ;
if va!=3:
   print('needs python3, have you executed\n\n module load python/3.5-anaconda \n\n aborting .py script')
   exit(1)
assert(va==3)  # needes Python3

import os
sys.path.append(os.path.abspath("../pdsfVaria/"))
from ShellCmdwTimeout import ShellCmdwTimeout

from pprint import pprint
import datetime
import argparse
parser = argparse.ArgumentParser()

par_shell_timeout=20 # in seconds
par_num_top_jobs=8


#----------------------------------------------------------
def setup_parser():
   # parser.add_argument("-x", dest="x",help="x")
   #parser.add_argument('-trigger','-t', action='append', dest="trigIdL", help="trigId, accepts many")
   parser.add_argument("-j","--jobid", dest="jobId",type=int,help="job id",default=4806302)

   parser.add_argument("-a", dest="allJobs",help="all users jobs", action="store_true", default=False)

#----------------------------------------------------------
def  supplementArgs(argD):
   # prep trig list
   jobD={}
   if argD['jobId']:
      jobD[argD['jobId']]={}
   return jobD

#----------------------------------------------------------
def getJobPriority(jobD):
   qosD={}
   print('get priority for jobs',jobD)

   '''sprio|head
          JOBID   PRIORITY        AGE  FAIRSHARE        QOS
        4123654      64802          2          1      64800
   '''
   for id in jobD:
      cmd="sprio | grep %d"%id
      #print('cmd=',cmd)
      task=ShellCmdwTimeout(cmd, timeout = par_shell_timeout)
      if task.ret:
         print('failed  %d, cmd=%s, skip'%(task.ret,cmd))
         return qosD
      dataL=task.stdout[0].split()
      #print('a=',dataL)
      jobD[id]['prior']=dataL[1]
      qos=dataL[4]
      jobD[id]['qos']=qos
      if not qos in qosD: qosD[qos]=0

   return qosD

#----------------------------------------------------------
def getTopJobs(qosD):
   #print('get top jobs for QOS=',qosD.keys())
   ''' sprio | head
          JOBID   PRIORITY        AGE  FAIRSHARE        QOS
        4123654      64817         17          1      64800
        4123851      64850         50          1      64800
   '''

   for qos in qosD.keys():
      cmd="sprio | grep %s| sort -k2 -r |nl  |head -n%d"%(qos,par_num_top_jobs)
      #print('cmd=',cmd)
      task=ShellCmdwTimeout(cmd, timeout = par_shell_timeout)
      if task.ret:
         print('failed  %d, cmd=%s, skip'%(task.ret,cmd))
         return
      dataL=task.stdout

      #print('b',dataL)
      s=0
      n=0
      for line in dataL:
         if len(line) <5: continue
         prio=line.split()[2]
         #print(prio,line)
         s=s+int(prio)
         n=n+1
      avr=s/n
      #print('qos=',qos,'avr=',avr)
      qosD[qos]=avr

#----------------------------------------------------------
def  estWaitTime(jobD,qosD):
   Tnow=datetime.datetime.now()
   for job in jobD:
      qos=jobD[job]['qos']
      prior=float(jobD[job]['prior'])
      topPrior=qosD[qos]
      delT=topPrior-prior
      DT=datetime.timedelta(minutes=delT)
      T1=Tnow+DT
      #print(job,prior,delT,' waitT=',DT,' startT=',T1)
      jobD[job]['waitT']=DT
      jobD[job]['startT']=T1
#----------------------------------------------------------
if __name__ == "__main__":
   setup_parser()
   args=parser.parse_args()
   #print('M:args',args)

   argD=vars(args)
   jobD=supplementArgs(argD)

   qosD=getJobPriority(jobD)
   if len(qosD) <=0: exit(1)
   getTopJobs(qosD)
   estWaitTime(jobD,qosD)

   #print("out=");   pprint(jobD)

   # nice printout
   print("\nswhen predicts wait time based on top %d jobs"%par_num_top_jobs)
   print("jobId   : wait time      ;  projected start")
   for job in jobD:
      print(job,":",jobD[job]['waitT'],';  ',jobD[job]['startT'])
  

'''
The ‘swhen’ wait time estimate is very primitive. 
Each SLURM job is assigned a priority at the submission time - higher is better.
Job is placed in the queue based on priority - its position will change as other jobs are being added.
Priority increases by 1 every minute.
‘swhen’ computes average priority of the  8 jobs on the top of priority list (to be executed ’soon’).
Next, it subtract your job priority from the average top-8 priority  - the difference is your wait time in minutes.
‘swhen’ ignores the  sizes of jobs in front of you. Some jobs need 5 nodes for 1 hour , other may need 5000 nodes for 20 hours.  It works well when your predicted wait time is in days - assume 1 hour error.

'''
