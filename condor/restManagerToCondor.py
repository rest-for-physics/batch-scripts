#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

# J. Galan - Javier.Galan.Lacarra@cern.ch
# 2 - Oct- 2016

import os,sys, time, commands
import stat, glob

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

requestTime = 0

runStart = 1000

def ProcessFilesInList( file_list ):
	cont = 0
	for fileToProcess in file_list:

		scriptName = "condor/" + jobName+"_"+str(cont)

		f = open( scriptName + ".sh", "w" )
		f.write("#!/bin/bash\n")
		#f.write("export REST_DATAPATH="+ os.environ['REST_DATAPATH']+"\n")

		for key in os.environ.keys(): 
			if key.find("REST") == 0:
				f.write( "export " + key + "=" + os.environ[key] +"\n" )
		 #   print( "export " + key + "=" + os.environ[key] +"\n" )
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

		f.write("export USER="+ os.environ['USER']+"\n\n")

		command = os.environ['REST_PATH'] + "/bin/restManager --c " + os.environ['PWD'] + "/" + cfgFile + " --f " + fileToProcess
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
		if( requestTime > 0 ):
			g.write("+RequestRuntime = " + str(requestTime) + "\n" )
		g.write("Queue\n" )
		g.close()

		if onlyScripts == 0:
			print "---> Launching : " + command

			condorCommand = "condor_submit " + scriptName + ".condor" 
			print "Condor command : " + condorCommand

			print "Waiting " + str(sleep) + " seconds to launch next job" 
			time.sleep(sleep)

			print commands.getstatusoutput( condorCommand )
		else:
			print "---> Produced condor script : " + str( scriptName ) + "_" + str(cont) + "_" + str( jobName ) + ".condor"
			print "---> To launch : " + command

	if (fileList == "" ):
		scriptName = "condor/" + jobName

		f = open( scriptName + ".sh", "w" )
		f.write("#!/bin/bash\n")
		#f.write("export REST_DATAPATH="+ os.environ['REST_DATAPATH']+"\n")

		for key in os.environ.keys(): 
			if key.find( "HOME") == 0:
				f.write( "export " + key + "=" + os.environ[key] +"\n" )
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

		command = "restManager --c " + cfgFile
		if sectionName != "":
			command = command + " --n " + sectionName
		f.write(  command + "\n" )
		f.close()

		st = os.stat( scriptName + ".sh" )
		os.chmod( scriptName + ".sh", st.st_mode | stat.S_IEXEC)

		cont = cont + 1

		g = open( scriptName + ".condor", "w" )
		g.write("Executable = " + scriptName + ".sh\n" )
		g.write("Arguments = \n" )
		g.write("Log = " + scriptName + ".log\n" )
		g.write("Output = " + scriptName + ".out\n" )
		g.write("Error = " + scriptName + ".err\n" )
		g.write("Queue\n" )
		g.close()

		if onlyScripts == 0:
			print "---> Launching : " + command

			condorCommand = "condor_submit " + scriptName + ".condor" 
			print "Condor command : " + condorCommand

			print "Waiting " + str(sleep) + " seconds to launch next job" 
			time.sleep(sleep)

			print commands.getstatusoutput( condorCommand )
		else:
			print "---> Produced condor script : " + str( scriptName ) + "_" + str( jobName ) + ".condor"
			print "---> To launch : " + command

def ProcessWithoutFile( ):
	rpt = repeat
	cont = 0
	while ( rpt > 0 ):
		rpt = rpt-1

		scriptName = "condor/" + jobName+"_"+str(runStart+cont)

		f = open( scriptName + ".sh", "w" )
		f.write("#!/bin/bash\n")
		#f.write("export REST_DATAPATH="+ os.environ['REST_DATAPATH']+"\n")

		for key in os.environ.keys(): 
			if key.find("REST") == 0:
				f.write( "export " + key + "=" + os.environ[key] +"\n" )
		 #   print( "export " + key + "=" + os.environ[key] +"\n" )
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

		f.write("export USER="+ os.environ['USER']+"\n\n")
		f.write( "export CONDOR_RUN=" + str(runStart + cont) +"\n" )

		command = os.environ['REST_PATH'] + "/bin/restManager --c " + os.environ['PWD'] + "/" + cfgFile
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
		if( requestTime > 0 ):
			g.write("+RequestRuntime = " + str(requestTime) + "\n" )
		g.write("Queue\n" )
		g.close()

		if onlyScripts == 0:
			print "---> Launching : " + command
			print ( "---> Run number : " + str(runStart + cont) )

			condorCommand = "condor_submit " + scriptName + ".condor" 
			print "Condor command : " + condorCommand

			print "Waiting " + str(sleep) + " seconds to launch next job" 
			time.sleep(sleep)

			print commands.getstatusoutput( condorCommand )
		else:
			print "---> Produced condor script : " + str( scriptName ) + "_" + str(cont) + "_" + str( jobName ) + ".condor"
			print "---> To launch : " + command


if narg < 2:
	print ""
	print "----------------------------------------------------------------" 
	print ""
	print " This program launches restManager job to condor	    "
	print " (condor scripts will be created under condor/ directory)"
	print ""
	print " The usual restManager command is : restManager --c CONFIG_FILE --f FILENAME"
	print ""
	print " Usage : restManagerToCondor.py -c CONFIG_FILE -f FILELIST/GLOBPATTERN "
	print ""
	print " Two options are allowed as input for the -f option "
	print " - FILELIST. A file containing one file per line"
	print " - GLOBPATTERN. A glob filename pattern using wild cards *?" 
	print ""
	print " In all cases full path file location should be given!"
	print ""
	print " - Other options : " 
	print " ----------------- " 
	print ""
	print " -j or --jobName JOB_NAME :"
	print " JOB_NAME defines the name of scripts and output files stored under condor/"
	print ""
	print " -s or --sleep SLEEP_TIME :"
	print " Time delay between launching 2 reapeated jobs (default is 5 seconds)"
	print " Random seed is connected to the time stamp"
	print ""
	print " -n or --section SECTION_NAME :"
	print " We may select the section in case the RML contains multiple definitions"
	print ""
	print " -r or --repeat N :"
	print " It defines the number of repetitions the restManager command should"
	print " trigger. Only active if not -f FILENAME was given."
	print ""
	print " -m or --runStart N :"
	print " It defines the value for the first run number. It will set variable"
	print " CONDOR_RUN to a different value in each iteration."
	print " The run file should define <parameter name=\"runNumber\" value=\"${CONDOR_RUN}\" />"
	print " Only active if not -f FILENAME was given."
	print ""
	print " -t or --requestTime T[hours] :"
	print " This parameter will allow to extend the requested CPU time (NAF-IAXO default is 3-hours)"
	print " The argument T must be given in hours"
	print "----------------------------------------------------------------" 
	print ""

for x in range(narg-1):
	if ( sys.argv[x+1] == "--cfgFile" or sys.argv[x+1] == "-c" ):
		cfgFile = sys.argv[x+2]

	if ( sys.argv[x+1] == "--sectionName" or sys.argv[x+1] == "-n" ):
		sectionName = sys.argv[x+2]

	if ( sys.argv[x+1] == "--sleep" or sys.argv[x+1] == "-s" ):
		sleep = int( sys.argv[x+2] )

	if ( sys.argv[x+1] == "--repeat" or sys.argv[x+1] == "-r" ):
		repeat = int( sys.argv[x+2] )

	if ( sys.argv[x+1] == "--jobName" or sys.argv[x+1] == "-j" ):
		jobName = sys.argv[x+2]

	if ( sys.argv[x+1] == "--runStart" or sys.argv[x+1] == "-m" ):
		runStart = int(sys.argv[x+2])

	if ( sys.argv[x+1] == "--requestTime" or sys.argv[x+1] == "-t" ):
		requestTime = int(sys.argv[x+2]) * 3600

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

lines = []
if (fileList != ""):
	if( not "*" in fileList ):
		f = open(fileList,'r')
		for line in f.readlines():
			if line[0] != "#":
				lines.append( line )
		f.close()
	else:
		print( "##" + fileList + "##" )
		lines = glob.glob( fileList ) 

	print ( "Files to process:" )
	print ( lines )

	ProcessFilesInList( lines )
else:
	print( " ======> Launching processing without input file <======" )
	ProcessWithoutFile( )

