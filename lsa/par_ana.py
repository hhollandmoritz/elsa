#!/usr/bin/env python
#par_ana -- parallel computation of LSA and LAA analysis

#License: BSD

#Copyright (c) 2008 Li Charles Xia
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions
#are met:
#1. Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
#2. Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#3. The name of the author may not be used to endorse or promote products
#   derived from this software without specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
#IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
#OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
#IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
#INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
#NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
#THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
#THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#public libs
import sys, csv, re, os, time, argparse, string, tempfile, subprocess
#numeric libs
import numpy as np
import scipy as sp
try:
  #debug import
  import lsalib
except ImportError:
  #install import
  from lsa import lsalib
  #np.seterr(all='raise')

#assuming input of form "xxxxx %single_input %single_output xxxxxx", only % to be replaced by single line file
#multiline file format:
#headline
#content1
#content2
#...
#convert to
#file1:
#headline
#content1
#file2:
#headline
#content2
#...
#single result file format
#result_file1
#headline
#result1-1
#result1-2
#...
#result_file2
#headline
#result2-1
#result2-2
#...
#combine back to
#result_file
#headline
#result1-1
#result1-2
#...
#result2-1
#result2-2
#...

#input arguments:
#multiInput, multiOutput, singleCmd

print >>sys.stderr, "Example: par_ana ARISA20.txt ARISA20.lsa 'lsa_compute %s %s -e ARISA20.txt -s 127 -r 1 -p theo' $PWD"
print >>sys.stderr, "Example: par_ana ARISA20.txt ARISA20.la 'la_compute %s ARISA20.laq %s -s 127 -r 1 -p 1000' $PWD"

def get_content(file, hasHeader=False):
  i=0
  header=None
  content=[]
  pline=None #last line that start with #
  for line in file:
    if line[0] == '#': 
      i+=1
      pline=line #keep track of the last # line
    elif (i==0) & hasHeader: #keep header, which could be non # line but only one line
      pline=line
      i+=1
    else:   #not empty, to something
      #if pline != None:
      #  header=pline.rstrip('\n') 
      #print line.rstrip('\n')
      content.append(line.rstrip('\n'))
  header=pline.rstrip('\n') #use last comment line or first line as header if hasHeader==True
  return (header, content)

def gen_singles(multiInput, multiOutput, workDir):
  header, content=get_content(multiInput)
  #print multiInput.name, header
  multiname=multiInput.name
  i=1
  singles=[]
  results=[]
  ends=[]
  for line in content:
    singlename=multiname+".%d" % i
    tmp=open(os.path.join(workDir, singlename), "w")
    print >>tmp, header.rstrip('\n')
    print >>tmp, line.rstrip('\n')
    tmp.close()
    singles.append(tmp.name) 
    results.append(tmp.name+".tmp") 
    ends.append(tmp.name+".end")
    i+=1

  return list(reversed(singles)), list(reversed(results)), list(reversed(ends))

PBS_PREAMBLE = """#!/bin/bash
#SBATCH --job-name=%s 
## Dropped PBS -S and PBS -j oe because these are handled differently in slurm  
#SBATCH -o %s
#SBATCH -t 0-299:00:00 # set to 299 hours may want to change depending on job limits in system
#SBATCH -ntasks=1
#SBATCH -cpus-per-task=1
#SBATCH -mem=%d000M"""
vmem=12

def gen_pbs(singleFile, singleCmd, workDir, singleEnd, vmem):
  singleResult=singleFile+".tmp"
  singlePBS=open(singleFile+".slurm", "w")
  print >>singlePBS, PBS_PREAMBLE % (os.path.basename(singleFile), os.path.basename(singleFile)+".log", vmem)
  print >>singlePBS, "cd %s" % workDir
  print >>singlePBS, singleCmd % (singleFile, singleResult)
  print >>singlePBS, "touch %s" % singleEnd
  print >>singlePBS, "rm -f %s %s" % (singleFile, singlePBS.name)
  singlePBS.close()
  return singlePBS.name

def gen_output(multiOutput, resultFiles):
  i=0
  for resultFile in resultFiles:
    header, content = get_content(resultFile, hasHeader=True)
    if i==0:
      print >>multiOutput, "\n".join([header]+content)
      i+=1
    else:
      print >>multiOutput, "\n".join(content)
  return

def ssa_pbs(singlePBS):
  try:
    tmp=subprocess.Popen("ssa.py %s" % singlePBS, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    print >>sys.stderr, "submitted", singlePBS
  except ValueError:
    quit()
  return tmp[0]

def main():  

  parser = argparse.ArgumentParser(description="Multiline Input Split and Combine Tool for LSA and LA")

  parser.add_argument("multiInput", metavar="multiInput", type=argparse.FileType('rU'), help="the multiline input file")
  parser.add_argument("multiOutput", metavar="multiOutput", type=argparse.FileType('w'), help="the multiline output file")
  parser.add_argument("singleCmd", metavar="singleCmd", help="single line command line in quotes")
  parser.add_argument("workDir", metavar="workDir", help="set current working directory")
  parser.add_argument("-m", "--maxMem", dest="maxMem", default=12, type=int, help="max memory per process in MB")
  parser.add_argument("-d", "--dryRun", dest="dryRun", default="", help="generate slurm files only")

#  """par_ana ARISA.txt ARISA.la 'la_compute %s ARISA.laq %s -s 114 -r 1 -p 1000'"""
#  """par_ana ARISA.txt ARISA.lsa 'lsa_compute %s %s -s 114 -r -p theo'"""
  arg_namespace=parser.parse_args()
  multiInput=vars(arg_namespace)['multiInput']
  multiOutput=vars(arg_namespace)['multiOutput']
  singleCmd=vars(arg_namespace)['singleCmd']
  workDir=vars(arg_namespace)['workDir']
  dryRun=vars(arg_namespace)['dryRun']
  vmem=vars(arg_namespace)['maxMem']
  
  print >>sys.stderr, "vmem=", str(vmem)+"000mb"
  #ws=os.path.join(os.environ.get("HOME"),'tmp','multi')
  print >>sys.stderr, "workDir=", workDir
  print >>sys.stderr, """Note: if deadlocked with unfinished jobs finally, manually collect the corresponding slurm files in above path and run"""

  singleFiles,resultFiles,endFiles=gen_singles(multiInput,multiOutput,workDir)
  #print >>sys.stderr, singleFiles,resultFiles,endFiles
  for endFile in endFiles: #ensure .end file from previous runs will be removed
    if os.path.exists(endFile):
      os.remove(endFile)

  inProgress=set()
  endJob=set()
  while(len(singleFiles)!=0):
    singleFile=singleFiles.pop()
    endFile=endFiles.pop()
    pbsFile=gen_pbs(singleFile, singleCmd, workDir, endFile, vmem)
    inProgress.add(endFile)
    print >>sys.stderr, pbsFile
    if dryRun=='':
      ssa_pbs(pbsFile)
  if dryRun!='':
    print >>sys.stderr, "finish dryRun"
    quit()

  #print >>sys.stderr, "inProgress=", inProgress
  #print >>sys.stderr, "endJob=", endJob
  #print >>sys.stderr, "am I here?"
  while(len(endJob)!=len(inProgress)):
    for job in inProgress:
      #time.sleep(1)
      if os.path.exists(job):
        header, content = get_content(open(job[:-4]+".tmp",'r'),hasHeader=True)
        if len(endJob)==0:
          print >>multiOutput, "\n".join([header]+content)
        else:
          print >>multiOutput, "\n".join(content)
        os.remove(job)
        endJob.add(job)
        print >>sys.stderr, "ended", job
        print >>sys.stderr, "remaining jobs", inProgress.difference(endJob), "total", len(inProgress.difference(endJob))

  #gen_output(multiOutput, resultFiles)

if __name__ == "__main__":
  main()
