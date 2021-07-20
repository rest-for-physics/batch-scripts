#!/bin/bash

################################################################
#######
####### A script to do rsync to a remote machine ready to be used with a cronjob
#######
####### If rsync is already running it does nothing, but if it is running for more 
####### than 2-hours it will kill the rsync process
#######
####### Usage: Replace the upper case variables at "rsync" command with the local 
####### and remote systems coordinates.
#######
####### The remote system should know the remote public ssh key, usually located
####### at /home/USER/.ssh/id_rsa.pub. In order for the remote system to know the
####### the public key, this must be copied at /home/REMOTE/.ssh/authorized_keys
#######
####### Then, edit the cronjobs at the local system using "crontab -e" command, 
####### adding the following line in order to execute the syncronization every 
####### 5 minutes.
#######
####### 1,6,11,16,21,26,31,36,41,46,51,56 * * * * /path/to/script/launcRsync.sh
#######
################################################################

# Check if rsync is running
# -x flag only match processes whose name (or command line if -f is
# specified) exactly match the pattern. 

if pgrep -x "rsync" > /dev/null
then
	echo "rsync is running. I do nothing"
	rsyncCmdOut=`ps -A | grep rsync`
	hour=`echo $rsyncCmdOut | cut -d ':' -f 2`
	var='02'
	if [ "$hour" = "$var" ]; then
		echo "rsync was already running for 2 hours!"
		echo "Killing the process!"
		pkill rsync
	fi
else
	echo "Launching copy to SERVER"
	rsync -ai -e "ssh -i /home/USER/.ssh/id_rsa" /PATH/TO/DATA/IN/ORIGIN/ --exclude="lost+found" USER_REMOTE@SERVER_NAME:/PATH/TO/DESTINATION
fi
