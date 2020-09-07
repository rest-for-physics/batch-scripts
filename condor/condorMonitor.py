#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

# This script generates the plots from the spectras
# J. Galan - Javier.Galan.Lacarra@cern.ch
# 29 - Nov - 2017

import os,sys, time, commands
import stat
import glob

narg = len(sys.argv)
cfgFile = ""
sectionName = ""
jobName = ""

maxJobs = 100
userName = os.environ['USER']
maxJobsNow = 100
timeBetweenJobs = 1

condorPath = os.environ['CONDOR_PATH'] + "/../"
os.chdir( condorPath )

condorScriptsList = sorted( glob.glob( "condor/*.condor"), key=os.path.getmtime )

jobsToLaunch = len(condorScriptsList)

if( jobsToLaunch <= 0 ):
    print "No jobs to launch found"
    sys.exit(0)



#jobList = os.system( "condor_q | grep " +str( userName) ) #    else:

jobList = os.popen("condor_q | grep " +str( userName)).read()


#print jobList

nJobs = len(jobList.split('\n')) - 1

print "Number of jobs running : " + str(nJobs)

while( nJobs < maxJobs and maxJobsNow > 0 ):
        condorCommand = "condor_submit " + condorScriptsList[0] 
        print commands.getstatusoutput(  condorCommand ) #    else:
        os.system("rm " + condorScriptsList[0])
        print "Removing : " + condorScriptsList[0]

        time.sleep(timeBetweenJobs)
        #Updating the list of jobs running in condor
        jobList = os.popen("condor_q | grep " + str( userName)).read()
        nJobs = len(jobList.split('\n')) - 1

        #Updating the list of jobs to launch (This may produce seconds delay if there are many files)
        #condorScriptsList = sorted( glob.glob( "condor/*.condor"), key=os.path.getmtime )
        # Just removing the condor file submitted should be ok
        condorScriptsList.pop(0)

        jobsToLaunch = len(condorScriptsList)

        maxJobsNow = maxJobsNow - 1

        if( jobsToLaunch <= 0 ):
            print "No jobs to launch found"
            sys.exit(0)


#cont = 0
#
#rpt = repeat
#while ( rpt > 0 ):


#    cont = cont + 1
#    rpt = rpt-1
#
#    g = open( scriptName + "_" + str(cont) + ".condor", "w" )
#    g.write("Universe   = vanilla\n" );
#    g.write("Executable = " + scriptName + ".sh\n" )
#    g.write("Arguments = \n" )
#    g.write("Log = " + scriptName + "_" + str(cont) + ".log\n" )
#    g.write("Output = " + scriptName + "_" + str(cont) + ".out\n" )
#    g.write("Error = " + scriptName + "_" + str(cont) + ".err\n" )
#    g.write("Queue\n" )
#    g.close()
#
#    if onlyScripts == 0:
#        print "---> Launching : " + command
#
#        condorCommand = "condor_submit " + scriptName + "_" + str(cont) + ".condor" 
#        print "Condor command : " + condorCommand
#
#        print "Waiting " + str(sleep) + " seconds to launch next job" 
#        time.sleep(sleep)
#
#        print commands.getstatusoutput( condorCommand ) #    else:
#        print "---> Produced condor script : " + str( scriptName ) + "_" + str(cont) + ".condor"
#        print "---> To launch : " + command
#
#    print ""
#print ""
#
