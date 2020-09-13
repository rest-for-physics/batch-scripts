#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

# This script generates the plots from the spectras
# J. Galan - Javier.Galan.Lacarra@cern.ch
# 7 - Sep- 2020

import os,sys, time, commands
import stat

# Change this to modify your log output directory!

# Change this by your mail address!

delay = 30
repeat = 10

narg = len(sys.argv)
cfgFile = ""
sectionName = ""
jobName = ""
email = ""
logPath = ""
idOffset = 0


if narg < 2:
    print ""
    print "----------------------------------------------------------------" 
    print ""
    print " This program launches restG4 jobs to slurm	    "
    print ""
    print " The usual restG4 command is : restG4 RML_FILE [SECTION_NAME]"
    print ""
    print " Values in brackets [] are optional"
    print ""
    print " Usage : restG4ToSlurm.py -c RML_FILE "
    print ""
    print " - Options : " 
    print " ----------- " 
    print ""
    print " -n or --sectionName SECTION_NAME :"
    print " Defines the name of the section to be used from RML_FILE"
    print ""
    print " -e or --email MAIL :"
    print " It allows to specify the e-mail for batch system job notifications"
    print ""
    print " -i or --idOffset VALUE :"
    print " An integer number to introduce an offset to the log id file."
    print ""
    print " -r or --repeat REPEAT_VALUE :"
    print " This option defines the number of simulations we will launch (default is 10)"
    print ""
    print " -d or --delay DELAY_TIME :"
    print " Time delay between launching 2 reapeated jobs (default is 30 seconds)"
    print " Random seed is connected to the time stamp."
    print ""
    print " -j or --jobName JOB_NAME :"
    print " JOB_NAME defines the name of scripts and output files stored under slurmJobs/"
    print ""
    print " -o or --onlyScripts :"
    print " It will just generate the slurm scripts without launching to the queue"
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

    if ( sys.argv[x+1] == "--logPath" or sys.argv[x+1] == "-l" ):
        logPath = sys.argv[x+2]

    if ( sys.argv[x+1] == "--idOffset" or sys.argv[x+1] == "-i" ):
        idOffset = int(sys.argv[x+2])

    if ( sys.argv[x+1] == "--delay" or sys.argv[x+1] == "-d" ):
        delay = int( sys.argv[x+2] )

    if ( sys.argv[x+1] == "--sectionName" or sys.argv[x+1] == "-n" ):
        sectionName = sys.argv[x+2]

    if ( sys.argv[x+1] == "--jobName" or sys.argv[x+1] == "-j" ):
        jobName = sys.argv[x+2]

    if ( sys.argv[x+1] == "--email" or sys.argv[x+1] == "-e" ):
        email = sys.argv[x+2]

    if ( sys.argv[x+1] == "--onlyScripts" or sys.argv[x+1] == "-o" ):
        onlyScripts = 1

if cfgFile == "":
    print  ( "RML file was not provided. Use -c file.rml" )
    sys.exit(1)

if jobName == "":
    jobName = cfgFile[cfgFile.rfind("/")+1:cfgFile.rfind(".rml")]

print ( "Number of jobs to launch : " + str(repeat) )
print ( "Jobs name : " + str(jobName) )
print ( "Delay in seconds between jobs : " + str(delay) )

for x in xrange(repeat):
    ################################################
    # Creating job environment and execution command
    ################################################
    scriptName = "/tmp/" + os.environ["USER"] + ".slurm"

    f = open( scriptName, "w" )
    f.write("#!/bin/bash\n\n")

    f.write("#SBATCH -J " + str(x+idOffset) + "_" + jobName + "\n" )
    f.write("#SBATCH --begin=now+" + str((x+1)*delay) + "\n" )
    f.write("#SBATCH --nodes=1\n" )
    if logPath != "":
        f.write("#SBATCH -o " + logPath + "/" + jobName + "_" + str(x+idOffset) + ".log\n")
        f.write("#SBATCH -e " + logPath + "/" + jobName + "_" + str(x+idOffset) + ".error\n")
    if email != "":
        f.write("#SBATCH --mail-user " + email + "\n")
    f.write("#SBATCH --ntasks=1\n")
    f.write("#SBATCH --mail-type=ALL\n")
    f.write("#SBATCH -p bifi\n")

    f.write("export USER="+ os.environ['USER']+"\n\n")

    # We transfer env variables to SLURM environment
    for key in os.environ.keys(): 
        if key.find( "DATA") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
     #       print( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "REST") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
     #       print( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "PRESSURE") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
     #       print( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "GAS") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
     #       print( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "QUENCHER") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
     #       print( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "GEOMETRY") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
     #       print( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "G4") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "PATH") == 0:
     #       print( "export " + key + "=" + os.environ[key] +"\n" )
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find("LD_LIBRARY_PATH") == 0:
     #       print( "export " + key + "=" + os.environ[key] +"\n" )
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "GARFIELD_") == 0:
     #       print( "export " + key + "=" + os.environ[key] +"\n" )
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "HEED_") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find( "PWD") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n\n" )

    if cfgFile[0] == "/": # It is absolute path
        command = "srun restG4 " + cfgFile + " " + sectionName
    else: # It is relative path
        command = "srun restG4 " + os.environ['PWD'] + "/" + cfgFile + " " + sectionName
    f.write(  command + "\n" )
    f.close()

    print commands.getstatusoutput( "sbatch " + scriptName )

    #st = os.stat( scriptName + ".sh" )
    #os.chmod( scriptName + ".sh", st.st_mode | stat.S_IEXEC)
    #################################################

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
    #        print commands.getstatusoutput( condorCommand )
    #    else:
    #        print "---> Produced condor script : " + str( scriptName ) + "_" + str(cont) + ".condor"
    #        print "---> To launch : " + command
    #
    #    print ""
    #print ""
    ###
