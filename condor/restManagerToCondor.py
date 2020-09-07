#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

# J. Galan - Javier.Galan.Lacarra@cern.ch
# 2 - Oct- 2016

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
fileList = ""

onlyScripts=0


if narg < 2:
    print ""
    print "----------------------------------------------------------------" 
    print ""
    print " This program launches restManager job to condor	    "
    print " (condor scripts will be created under condor/ directory)"
    print ""
    print " The usual restManager command is : restManager --c CONFIG_FILE --f FILENAME [--n SECTION_NAME]"
    print ""
    print " Values in brackets [] are optional"
    print ""
    print " Usage : restManagerToCondor.py -c CONFIG_FILE -f LISTFILE [-n SECTION_NAME] "
    print ""
    print " The file LISTFILE must a contain a REST file to be processed, one file per line and full path"
    print ""
    print " - Options : " 
    print " ----------- " 
    print ""
    print " -j or --jobName JOB_NAME :"
    print " JOB_NAME defines the name of scripts and output files stored under condor/"
    print ""
    print " -s or --sleep SLEEP_TIME :"
    print " Time delay between launching 2 reapeated jobs (default is 5 seconds)"
    print " Random seed is connected to the time stamp"
    print ""
    print "----------------------------------------------------------------" 
    print ""


for x in range(narg-1):
    if ( sys.argv[x+1] == "--cfgFile" or sys.argv[x+1] == "-c" ):
        cfgFile = sys.argv[x+2]

    if ( sys.argv[x+1] == "--sectionName" or sys.argv[x+1] == "-n" ):
        sectionName = sys.argv[x+2]

    if ( sys.argv[x+1] == "--sleep" or sys.argv[x+1] == "-s" ):
        sleep = int( sys.argv[x+2] )

    if ( sys.argv[x+1] == "--jobName" or sys.argv[x+1] == "-j" ):
        jobName = sys.argv[x+2]

    if ( sys.argv[x+1] == "--fileList" or sys.argv[x+1] == "-f" ):
        fileList = sys.argv[x+2]

    if ( sys.argv[x+1] == "--onlyScripts" or sys.argv[x+1] == "-o" ):
        onlyScripts = 1


if not os.path.exists("condor"):
    os.makedirs("condor")

if jobName == "":
    jobName = cfgFile[cfgFile.rfind("/")+1:cfgFile.rfind(".rml")]

if cfgFile == "":
    print "Please specify a RML config file list using -c flag." 
    sys.exit( 1 )

if fileList == "":
    print "Please specify a file list using -f flag." 
    sys.exit( 1 )


f = open(fileList,'r')
lines = []
for line in f.readlines():
    if line[0] != "#":
        lines.append( line )
f.close()

cont = 0
#!/bin/bash
for fileToProcess in lines:

    scriptName = "condor/" + jobName+"_"+str(cont)

    f = open( scriptName + ".sh", "w" )
    f.write("#!/bin/bash\n")
    #f.write("export REST_DATAPATH="+ os.environ['REST_DATAPATH']+"\n")

    for key in os.environ.keys(): 
        if key.find("REST") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        print( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find("PATH") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find("LD_LIBRARY_PATH") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find("GARFIELD_") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find("HEED_") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )
        if key.find("PWD") == 0:
            f.write( "export " + key + "=" + os.environ[key] +"\n" )

    f.write("export BL2_HOME="+ os.environ['BL2_HOME']+"\n")
    f.write("export SCRATCH_HOME="+ os.environ['SCRATCH_HOME']+"\n")
    f.write("export OPT_PATH="+ os.environ['OPT_PATH']+"\n")
    f.write("export USER="+ os.environ['USER']+"\n\n")

    command = "restManager --c " + os.environ['PWD'] + "/" + cfgFile + " --f " + fileToProcess
    if sectionName != "":
        command = command + " --n " + sectionName
    f.write(  command + "\n" )
    f.close()

    st = os.stat( scriptName + ".sh" )
    os.chmod( scriptName + ".sh", st.st_mode | stat.S_IEXEC)

    cont = cont + 1

    g = open( scriptName + ".condor", "w" )
    g.write("Universe   = vanilla\n" );
    g.write("Executable = " + scriptName + ".sh\n" )
    g.write("Arguments = \n" )
    g.write("Log = " + scriptName + ".log\n" )
    g.write("Output = " + scriptName + ".out\n" )
    g.write("Error = " + scriptName + ".err\n" )
    g.write("Queue\n" )
    g.close()

    if onlyScripts == 0:
        print "---> Launching : " + command

        condorCommand = "condor_submit " + scriptName + "_" + str(cont) + ".condor" 
        print "Condor command : " + condorCommand

        print "Waiting " + str(sleep) + " seconds to launch next job" 
        time.sleep(sleep)

        print commands.getstatusoutput( condorCommand )
    else:
        print "---> Produced condor script : " + str( scriptName ) + "_" + str(cont) + "_" + str( jobName ) + ".condor"
        print "---> To launch : " + command

