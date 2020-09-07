List of the scripts in this directory:

- **condorMonitor.py**: A script to monitor the job activity and keep sending jobs to the cluster without exceeding a number of jobs in the queue. This was prepared in order to bypass the number of maximum jobs to be launched at SJTU cluster. So, I could send 1000 jobs, and as new queue slots where available new jobs would be submitted.
- **countJobs.py**: A simple scripts to count the number of jobs from USER are running in condor queue system.
- **restG4ToCondor.py**: A script to prepare condor scripts and send `restG4` to a condor batch system. Execute `./restG4ToCondor.py` without arguments for reference.
- **restManagerToCondor.py**: A script to prepare condor scripts and send `restManager` to a condor batch system. Execute `./restManagerToCondor.py` without arguments for reference.

