#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

# This script generates the plots from the spectras
# J. Galan - Javier.Galan.Lacarra@cern.ch
# 29 - Nov - 2017

import os,sys, time, commands
import stat
import glob

userName = os.environ['USER']

jobList = os.popen("condor_q | grep " +str( userName)).read()

nJobs = len(jobList.split('\n')) - 1

print "Number of jobs running : " + str(nJobs)

