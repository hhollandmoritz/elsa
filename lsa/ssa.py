#!/usr/bin/env python
# submit jobs in $1 while total cores<300, total mem<300G
# current limit is 299
# current mem limit is 300G

import os, sys, shutil, subprocess, argparse, fnmatch, time
core_max=63
mem_max=300
uname="$USER"

def mem_size(mem):
  if mem[-1:]=='M':
    return int(mem[:-1])/1000
  elif mem[-1:]=='G':
    return int(mem[:-1])
  else:
    return -1

def peek_current(uname):
  tmp=subprocess.Popen("squeue -u %s --Format='JobID,Partition,Name,UserID,StateCompact,TimeUsed,TimeLimit,NumNodes,NumCPUs,MinMemory' | awk '{print $10}'" % uname, shell=True, stdout=subprocess.PIPE).communicate()
  mems=tmp[0].split('\n')
  #print >>sys.stderr, mems
  #print mems
  tmp=subprocess.Popen("squeue -u %s --Format='JobID,Partition,Name,UserID,StateCompact,TimeUsed,TimeLimit,NumNodes,NumCPUs,MinMemory' | awk '{print $9}'" % uname, shell=True, stdout=subprocess.PIPE).communicate()
  cores=tmp[0].split('\n')
  #print >>sys.stderr, cores
  #print cores
  # sessions is ST column in slurm
  tmp=subprocess.Popen("squeue -u %s --Format='JobID,Partition,Name,UserID,StateCompact,TimeUsed,TimeLimit,NumNodes,NumCPUs,MinMemory' | awk '{print $5}'" % uname, shell=True, stdout=subprocess.PIPE).communicate()
  sessions=tmp[0].split('\n') # session is job status column
  #print sessions
  #print >>sys.stderr, sessions
  
  #print 'I am here'
  hskip=5
  tskip=1
  total_core=0
  total_mem=0
  #print queue
  #print len(sessions)
  #print range(hskip+1,len(sessions)-tskip)
  #assert len(mems) == len(cores)
  
  for i in range(hskip+1,len(sessions)-tskip):
    #print 'sessions loop'
    #print i
    if sessions[i]!='R':
      #print >>sys.stderr, sessions[i]
      #print 'Insufficient number of jobs are running - waiting'
      #continue
      return True # if job is not running, just wait
  
  #print 'Calculating Cores'
  for i in range(hskip+1,len(cores)-tskip):
    #print cores[i]
    total_core+=int(cores[i])
    #print 'total core'
    #print total_core
  
  #print 'total core'
  #print total_core
  
  #print 'Calculating memory'
  for i in range(hskip+1,len(mems)-tskip):
    if mem_size(mems[i])>0:
      #print 'mem size'
      #print mem_size(mems[i])
      total_mem+=mem_size(mems[i])
      #print 'total mem'
      #print total_mem
    else: #Let's just wait
      #print 'I am in else'
      total_core=core_max
      total_mem=mem_max
      break
  #print 'total memory'
  #print total_mem
  
  #print 'I am at the end'
  #print total_mem>=mem_max
  #print total_core>=core_max
  full_status=(total_core>=core_max) or (total_mem>=mem_max)
  #print full_status
  return full_status

def submit( pbsFile ):
  tmp=subprocess.Popen("sbatch %s" % (pbsFile), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
  submitted=True
  if tmp[1] == '':
    print tmp[0]
    print pbsFile, "submitted"
  else:
    print tmp[1]
    print pbsFile, "error"
    submitted=False
  return submitted

def main():

  parser = argparse.ArgumentParser(description="MCB Queue Checking and Submission Tool")
  parser.add_argument("slurmFile", metavar="slurmFile", help="single slurm file to be submitted")
  arg_namespace = parser.parse_args()
  slurmFile = vars(arg_namespace)['slurmFile']
  pbsFiles = set([slurmFile])

  #print pbsFiles

  while( len(pbsFiles) != 0 ):
    if peek_current(uname):  #full
      time.sleep(1)
    else:
      pbsFile=pbsFiles.pop()
      print len(pbsFiles)
      if not submit(pbsFile):
        pbsFiles.add(pbsFile)

if __name__=="__main__":
  main()
