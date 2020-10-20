#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

# This script generates the plots from the spectras
# J. Galan - Javier.Galan.Lacarra@cern.ch
# 29 - Sep- 2016

import os,sys, time, commands
import stat

calrun = 0
bckrun = 0
sleep = 1
repeat = 1

narg = len(sys.argv)
cfgFile = ""
sectionName = ""
jobName = ""


if narg < 2:
    print ""
    print "----------------------------------------------------------------" 
    print ""
    print " This program launches restG4 job to condor	    "
    print " (condor scripts will be created under condor/ directory)"
    print ""
    print " The usual restG4 command is : restG4 CONFIG_FILE [SECTION_NAME]"
    print ""
    print " Values in brackets [] are optional"
    print ""
    print " Usage : restG4ToCondor.py -c CONFIG_FILE "
    print ""
    print " - Options : " 
    print " ----------- " 
    print ""
    print " -n or --sectionName SECTION_NAME :"
    print " Defines the name of the section to be used from CONFIG_FILE"
    print ""
    print ""
    print " -r or --repeat REPEAT_VALUE :"
    print " This option defines the number of simulations we will launch"
    print ""
    print " -s or --sleep SLEEP_TIME :"
    print " Time delay between launching 2 reapeated jobs (default is 5 seconds)"
    print " Random seed is connected to the time stamp"
    print ""
    print " -j or --jobName JOB_NAME :"
    print " JOB_NAME defines the name of scripts and output files stored under condor/"
    print ""
    print "----------------------------------------------------------------" 
    print ""
    sys.exit(1)

onlyScripts=0

for x in range(narg-1):
    if ( sys.argv[x+1] == "--repeat" or sys.argv[x+1] == "-r" ):
        repeat = int(sys.argv[x+2])

    if ( sys.argv[x+1] == "--cfgFile" or sys.argv[x+1] == "-c" ):
        cfgFile = sys.argv[x+2]

    if ( sys.argv[x+1] == "--sleep" or sys.argv[x+1] == "-s" ):
        sleep = int( sys.argv[x+2] )

    if ( sys.argv[x+1] == "--sectionName" or sys.argv[x+1] == "-n" ):
        sectionName = sys.argv[x+2]

    if ( sys.argv[x+1] == "--jobName" or sys.argv[x+1] == "-j" ):
        jobName = sys.argv[x+2]

    if ( sys.argv[x+1] == "--onlyScripts" or sys.argv[x+1] == "-o" ):
        onlyScripts = 1


if not os.path.exists("condor"):
    os.makedirs("condor")

if jobName == "":
    jobName = cfgFile[cfgFile.rfind("/")+1:cfgFile.rfind(".rml")]

################################################
# Creating job environment and execution command
################################################
scriptName = "condor/" + jobName

f = open( scriptName + ".sh", "w" )
f.write("#!/bin/bash\n")

# We transfer env variables to Condor environment
for key in os.environ.keys(): 
    print( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find( "DATA") == 0:
	f.write( "export " + key + "=" + os.environ[key] +"\n" )
        print( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find("GDML") == 0:
	f.write( "export " + key + "=" + os.environ[key] +"\n" )
	print( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find("GEOMETRY") >= 0:
        f.write( "export " + key + "=" + os.environ[key] +"\n" )
        print( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find("REST") == 0:
        f.write( "export " + key + "=" + os.environ[key] +"\n" )
        print( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find("G4") == 0:
        f.write( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find("PATH") == 0:
        print( "export " + key + "=" + os.environ[key] +"\n" )
        f.write( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find("LD_LIBRARY_PATH") == 0:
        print( "export " + key + "=" + os.environ[key] +"\n" )
        f.write( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find("GARFIELD_") == 0:
        print( "export " + key + "=" + os.environ[key] +"\n" )
        f.write( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find("HEED_") == 0:
        f.write( "export " + key + "=" + os.environ[key] +"\n" )
    if key.find("PWD") == 0:
        f.write( "export " + key + "=" + os.environ[key] +"\n" )

f.write("export USER="+ os.environ['USER']+"\n\n")

command = "restG4 " + os.environ['PWD'] + "/" + cfgFile + " " + sectionName
f.write(  command + "\n" )
f.close()

st = os.stat( scriptName + ".sh" )
os.chmod( scriptName + ".sh", st.st_mode | stat.S_IEXEC)
################################################

cont = 0

rpt = repeat
while ( rpt > 0 ):
    cont = cont + 1
    rpt = rpt-1

    g = open( scriptName + "_" + str(cont) + ".condor", "w" )
    g.write("Executable = " + scriptName + ".sh\n" )
    g.write("Arguments = \n" )
    g.write("Log = " + scriptName + "_" + str(cont) + ".log\n" )
    g.write("Output = " + scriptName + "_" + str(cont) + ".out\n" )
    g.write("Error = " + scriptName + "_" + str(cont) + ".err\n" )
    g.write("queue 1\n" )
    g.close()

    if onlyScripts == 0:
        print "---> Launching : " + command

        condorCommand = "condor_submit " + scriptName + "_" + str(cont) + ".condor" 
        print "Condor command : " + condorCommand

        print "Waiting " + str(sleep) + " seconds to launch next job" 
        time.sleep(sleep)

        print commands.getstatusoutput( condorCommand )
    else:
        print "---> Produced condor script : " + str( scriptName ) + "_" + str(cont) + ".condor"
        print "---> To launch : " + command

    print ""
print ""

