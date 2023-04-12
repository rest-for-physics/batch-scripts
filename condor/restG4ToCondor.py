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

# REST_PATH environment variable must be set
try:
    REST_PATH = os.environ["REST_PATH"]
except KeyError:
    raise Exception("REST_PATH environment variable must be set")

restG4 = f"{REST_PATH}/bin/restG4"
# assert this binary exists
subprocess.run([restG4, "--help"], check=True)

# --n-jobs is the number of batch jobs to submit (default 1)
# --rml is the rml config file to use
# The positional arguments are the arguments for the restG4 binary

parser = argparse.ArgumentParser(description="Launch restG4 jobs on condor")
parser.add_argument("--n-jobs", type=int, default=1)
parser.add_argument("--rml", type=str, default="simulation.rml")
parser.add_argument("--output-dir", type=str, default="")

timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
parser.add_argument("--name", type=str, default=f"restG4_{timestamp_str}")

args, restG4_args = parser.parse_known_args()

# create output directory if it does not exist

user = os.environ["USER"]
if user == "":
    raise Exception("Could not find current user")

condor_dir = Path(f"/nfs/dust/iaxo/user/{user}") / "condor" / args.name
condor_dir.mkdir(parents=True, exist_ok=True)

output_dir = args.output_dir
if output_dir == "":
    output_dir = condor_dir / "output"

output_dir = Path(output_dir)
output_dir.mkdir(parents=True, exist_ok=True)

print(f"Condor directory: {condor_dir}")

sub_files = []
seeds = set()

# with 30000 seconds as requested, do not run over 8h (in restG4 parameters)

for i in range(args.n_jobs):
    def generate_seed():
        return random.randint(0, 2 ** 30)


    seed = generate_seed()
    # make sure the seed is unique between batch jobs
    while seed in seeds:
        seed = generate_seed()

    output_file = f"{output_dir}/output_{i}.root"
    command = f"""{restG4} {args.rml} --output {output_file} --seed {seed} {" ".join(restG4_args)} """
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

    +RequestRuntime = 30000

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
for sub_file in sub_files:
    print(sub_file)
    subprocess.run(["condor_submit", sub_file], check=True)

print(f"Output will be stored in {output_dir}")

# create a symbolic link to the latest output directory

latest_link = Path(os.environ["HOME"]) / "condor" / "latest"
latest_link.unlink(missing_ok=True)
latest_link.symlink_to(condor_dir)
