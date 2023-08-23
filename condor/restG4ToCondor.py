# Author: Luis Obis (lobis@unizar.es)

# Usage:
# python3 restG4ToCondor.py --rml simulation.rml --n-jobs 10 --output-dir /path/to/output/dir
# arguments not specified to this script (--rml, --n-jobs, ...) are passed directly to restG4

import random
import os
import subprocess
from pathlib import Path
import argparse
from datetime import datetime
import re

# REST_PATH environment variable must be set
try:
    REST_PATH = os.environ["REST_PATH"]
except KeyError:
    raise Exception("REST_PATH environment variable must be set")

restG4 = f"{REST_PATH}/bin/restG4"
restRoot = f"{REST_PATH}/bin/restRoot"

# assert this binary exists
subprocess.run([restG4, "--help"], check=True)

# --n-jobs is the number of batch jobs to submit (default 1)
# --rml is the rml config file to use
# The positional arguments are the arguments for the restG4 binary

parser = argparse.ArgumentParser(description="Launch restG4 jobs on condor")
parser.add_argument("--n-jobs", type=int, default=1)
parser.add_argument("--rml", type=str, default="simulation.rml")
parser.add_argument("--output-dir", type=str, default="")
parser.add_argument("--time", type=str, default="1h0m0s")
parser.add_argument("--dry-run", action="store_true", help="Set this flag for a dry run")
parser.add_argument("--merge", action="store_true", help="merge files using 'restGeant4_MergeRestG4Files' macro")


def parse_time_string(time_string) -> int:
    components = re.findall(r'(\d+)([hms])', time_string)
    total_seconds = 0

    for value, unit in components:
        value = int(value)
        if unit == 'h':
            total_seconds += value * 3600
        elif unit == 'm':
            total_seconds += value * 60
        elif unit == 's':
            total_seconds += value

    return total_seconds


timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
parser.add_argument("--name", type=str, default=f"restG4_{timestamp_str}")

args, restG4_args = parser.parse_known_args()

dry_run = args.dry_run == True

merge = args.merge == True

time_in_seconds = parse_time_string(args.time)

number_of_jobs = args.n_jobs

name = args.name
# create output directory if it does not exist

user = os.environ["USER"]
if user == "":
    raise Exception("Could not find current user")

condor_dir = Path(f"/nfs/dust/iaxo/user/{user}") / "condor" / name
condor_dir.mkdir(parents=True, exist_ok=True)

output_dir = args.output_dir
if output_dir == "":
    output_dir = condor_dir / "output"

output_dir = Path(output_dir)
output_dir.mkdir(parents=True, exist_ok=True)

tmp_dir = condor_dir / "tmp"
tmp_dir.mkdir(parents=True, exist_ok=True)

print(f"Condor directory: {condor_dir}")

sub_files = []
seeds = set()

# with 30000 seconds as requested, do not run over 8h (in restG4 parameters)

rml = Path(args.rml)
if not rml.exists():
    raise Exception(f"Could not find rml file {args.rml}")

rml = rml.resolve()

time_additional = 3600  # give 1h of margin

for i in range(number_of_jobs):
    def generate_seed():
        return random.randint(0, 2 ** 30)


    seed = generate_seed()
    # make sure the seed is unique between batch jobs
    while seed in seeds:
        seed = generate_seed()

    output_file = f"{output_dir}/output_{i}.root"
    tmp_file = f"{tmp_dir}/output_{i}.root"

    command = f"""
source {REST_PATH}/thisREST.sh
{restG4} {args.rml} --output {tmp_file} --seed {seed} --time {time_in_seconds}s {" ".join(restG4_args)}
mv {tmp_file} {output_file}
"""

    print(command)

    script_content = f"""
{command}
    """
    name_script = f"""{str(condor_dir / "scripts")}/script_{i}.sh"""
    name_job = f"""{str(condor_dir / "jobs")}/job_{i}.sub"""

    stdout_dir = condor_dir / "stdout"
    stderr_dir = condor_dir / "stderr"
    logs_dir = condor_dir / "logs"

    stdout_dir.mkdir(parents=True, exist_ok=True)
    stderr_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    submission_file_content = f"""
executable   = {name_script}
arguments    =
getenv       = True

output       = {str(stdout_dir)}/output_{i}
error        = {str(stderr_dir)}/error_{i}
log          = {str(logs_dir)}/log_{i}

request_cpus   = 1

+RequestRuntime = {time_in_seconds + time_additional}

should_transfer_files = yes

queue
    """

    # write script_content to file, create parents directory if needed

    os.makedirs(os.path.dirname(name_script), exist_ok=True)
    with open(name_script, "w") as f:
        f.write(script_content)

    os.makedirs(os.path.dirname(name_job), exist_ok=True)
    with open(name_job, "w") as f:
        f.write(submission_file_content)

    sub_files.append(name_job)

print(f"Created {len(sub_files)} submission files")

if not merge:
    for sub_file in sub_files:
        print(sub_file)
        if dry_run:
            continue
        subprocess.run(["condor_submit", sub_file], check=True)
else:
    # merge command
    command = f"""
source {REST_PATH}/thisREST.sh
{restRoot} -q "{REST_PATH}/macros/geant4/REST_Geant4_MergeRestG4Files.C(\\\"{condor_dir}/{name}.root\\\", \\\"{output_dir}\\\")" 
"""
    print(command)

    script_content = f"""
    {command}
        """
    name_script = f"""{str(condor_dir / "scripts")}/script_merge.sh"""
    name_job = f"""{str(condor_dir / "jobs")}/job_merge.sub"""

    stdout_dir = condor_dir / "stdout"
    stderr_dir = condor_dir / "stderr"
    logs_dir = condor_dir / "logs"

    stdout_dir.mkdir(parents=True, exist_ok=True)
    stderr_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    submission_file_content = f"""
executable   = {name_script}
arguments    =
getenv       = True

output       = {str(stdout_dir)}/output_merge
error        = {str(stderr_dir)}/error_merge
log          = {str(logs_dir)}/log_merge

request_cpus   = 1
request_memory = 4000 # 4 GB

+RequestRuntime = {max(number_of_jobs * 60, time_in_seconds) + time_additional}

should_transfer_files = yes

queue
    """

    jobs = "\n".join([f"JOB job_{i} {sub_file}" for i, sub_file in enumerate(sub_files)])
    parent_child_relations = "\n".join([f"PARENT job_{i} CHILD job_merge" for i in range(len(sub_files))])
    dag_submission_content = f"""
{jobs}

JOB job_merge {name_job}
{parent_child_relations}
"""

    name_dag_file = f"{condor_dir}/dag"

    with open(name_script, "w") as f:
        f.write(script_content)

    with open(name_job, "w") as f:
        f.write(submission_file_content)

    with open(name_dag_file, "w") as f:
        f.write(dag_submission_content)

    print(name_dag_file)
    if not dry_run:
        subprocess.run(["condor_submit_dag", name_dag_file])

print(f"Output will be stored in {output_dir}")

# create a symbolic link to the latest output directory

latest_link = Path(os.environ["HOME"]) / "condor" / "latest"
latest_link.unlink(missing_ok=True)
latest_link.symlink_to(condor_dir)
