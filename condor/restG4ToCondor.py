# Author: Luis Obis (lobis@unizar.es)

# Usage:
# python3 restG4ToCondor.py --rml simulation.rml --n-jobs 10 --output-dir /path/to/output/dir
# arguments not specified to this script (not --rml, --n-jobs, ...) are passed directly to restG4

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
restManager = f"{REST_PATH}/bin/restManager"

# assert this binary exists
subprocess.run([restG4, "--help"], check=True)

# The positional arguments are the arguments for the restG4 binary

parser = argparse.ArgumentParser(description="Launch restG4 jobs on condor")
parser.add_argument("--n-jobs", type=int, default=1, help="Number of jobs to submit")
parser.add_argument("--rml", type=str, default="simulation.rml", help="RML config file")
parser.add_argument("--output-dir", type=str, default="", help="Output directory")
parser.add_argument("--time", type=str, default="1h0m0s", help="Time per job (e.g. 1h0m0s)")
parser.add_argument("--memory", type=int, default="0", help="Memory in MB. If 0, use default value")
parser.add_argument("--dry-run", action="store_true", help="Set this flag for a dry run")
parser.add_argument("--merge", action="store_true", help="merge files using 'restGeant4_MergeRestG4Files' macro")
parser.add_argument("--merge-chunk", type=int, default=25, help="Number of files to merge at once")
parser.add_argument("--rml-processing", type=str, default=None,
                    help="RML config file for the processing (restManager). If not specified, no processing is performed")
parser.add_argument("--processing-before-merge", action="store_true",
                    help="Run the processing on individual files before merging them")


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


def partition_number(number, chunk_size):
    # returns a list of lists, each list contains a chunk of numbers
    partitions = []
    for start in range(0, number, chunk_size):
        end = min(start + chunk_size - 1, number - 1)
        partitions.append(list(range(start, end + 1)))
    return partitions


timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
parser.add_argument("--name", type=str, default=f"restG4_{timestamp_str}")

args, restG4_args = parser.parse_known_args()

dry_run = args.dry_run == True

merge = args.merge == True

time_in_seconds = parse_time_string(args.time)
memory_sub_string = f"request_memory = {args.memory}" if args.memory != 0 else ""

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

    run_processing = args.rml_processing is not None and args.processing_before_merge
    processing_command = ""
    if run_processing:
        processing_command = f"""
        {restManager} --c {args.rml_processing} --i {tmp_file} --o {tmp_file}
        """
    command = f"""
source {REST_PATH}/thisREST.sh
{restG4} {args.rml} --output {tmp_file} --seed {seed} --time {time_in_seconds}s {" ".join(restG4_args)}
{processing_command}
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
{memory_sub_string}

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
    chunk_size = args.merge_chunk
    partitions = partition_number(number_of_jobs, chunk_size=chunk_size)

    intermediate_merge_files_directory = condor_dir / f"merge/partition_merge"

    for partition in partitions:
        sub_files_partition = [sub_files[i] for i in partition]
        partition_min, partition_max = partition[0], partition[-1]
        partition_suffix = f"{partition_min}_{partition_max}"
        partition_merge_files_directory = str(condor_dir / f"merge/partition_{partition_suffix}")
        partition_merge_file_name = str(intermediate_merge_files_directory / f"merge_{partition_suffix}.root")
        os.makedirs(partition_merge_files_directory, exist_ok=True)
        os.makedirs(os.path.dirname(partition_merge_file_name), exist_ok=True)
        move_files_command = "\n".join(
            [f"mv --no-clobber {output_dir}/output_{i}.root {partition_merge_files_directory}" for i in partition])
        # merge command
        command = f"""
source {REST_PATH}/thisREST.sh
{move_files_command}
{restRoot} -q "{REST_PATH}/macros/geant4/REST_Geant4_MergeRestG4Files.C(\\\"{partition_merge_file_name}\\\", \\\"{partition_merge_files_directory}\\\")"
rm {partition_merge_files_directory}/*.root
    """
        print(command)

        script_content = f"""
        {command}
            """
        name_script = f"""{str(condor_dir / "scripts")}/script_merge_{partition_suffix}.sh"""
        name_job = f"""{str(condor_dir / "jobs")}/job_merge_{partition_suffix}.sub"""

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

output       = {str(stdout_dir)}/output_merge_{partition_suffix}
error        = {str(stderr_dir)}/error_merge_{partition_suffix}
log          = {str(logs_dir)}/log_merge_{partition_suffix}

request_cpus   = 1
{memory_sub_string}

+RequestRuntime = {max(chunk_size * 60, time_in_seconds) + time_additional}

should_transfer_files = yes

queue
"""

        with open(name_script, "w") as f:
            f.write(script_content)

        with open(name_job, "w") as f:
            f.write(submission_file_content)

    # merge all files
    final_merge_output_name = str(condor_dir / f"{name}.root")
    command = f"""
source {REST_PATH}/thisREST.sh
{restRoot} -q "{REST_PATH}/macros/geant4/REST_Geant4_MergeRestG4Files.C(\\\"{final_merge_output_name}\\\", \\\"{str(intermediate_merge_files_directory)}\\\")"
rm {intermediate_merge_files_directory}/*.root
    """
    print(command)

    script_content = f"""
{command}
            """
    name_script_merge = f"""{str(condor_dir / "scripts")}/script_merge.sh"""
    name_job_merge = f"""{str(condor_dir / "jobs")}/job_merge.sub"""

    stdout_dir = condor_dir / "stdout"
    stderr_dir = condor_dir / "stderr"
    logs_dir = condor_dir / "logs"

    stdout_dir.mkdir(parents=True, exist_ok=True)
    stderr_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    submission_file_content = f"""
executable   = {name_script_merge}
arguments    =
getenv       = True

output       = {str(stdout_dir)}/output_merge
error        = {str(stderr_dir)}/error_merge
log          = {str(logs_dir)}/log_merge

request_cpus   = 1
{memory_sub_string}

+RequestRuntime = {time_in_seconds + time_additional}

should_transfer_files = yes

queue
"""

    with open(name_script_merge, "w") as f:
        f.write(script_content)

    with open(name_job_merge, "w") as f:
        f.write(submission_file_content)

    # analyze job
    processing_merge = args.rml_processing is not None and merge and not args.processing_before_merge
    if processing_merge:
        # replace ending ".root" with ".analysis.root"
        final_merge_output_name_analysis = final_merge_output_name[:-5] + ".analysis.root"
        filename_no_path = os.path.basename(final_merge_output_name_analysis)
        # same file name but in tmp directory
        final_merge_output_name_analysis_tmp = str(tmp_dir / filename_no_path)
        command = f"""
source {REST_PATH}/thisREST.sh
{restManager} --c {args.rml_processing} --i {final_merge_output_name} --o {final_merge_output_name_analysis_tmp}
mv {final_merge_output_name_analysis_tmp} {final_merge_output_name_analysis}
"""
        print(command)

        script_content = f"""
{command}
            """
        name_script_analysis = f"""{str(condor_dir / "scripts")}/script_analysis.sh"""
        name_job_analysis = f"""{str(condor_dir / "jobs")}/job_analysis.sub"""

        stdout_dir = condor_dir / "stdout"
        stderr_dir = condor_dir / "stderr"
        logs_dir = condor_dir / "logs"

        stdout_dir.mkdir(parents=True, exist_ok=True)
        stderr_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        submission_file_content = f"""
executable   = {name_script_analysis}
arguments    =
getenv       = True

output       = {str(stdout_dir)}/output_analysis
error        = {str(stderr_dir)}/error_analysis
log          = {str(logs_dir)}/log_analysis

request_cpus   = 1
{memory_sub_string}

+RequestRuntime = {max(time_in_seconds + time_additional, 3600 * 24)}

should_transfer_files = yes

queue
        """

        with open(name_script_analysis, "w") as f:
            f.write(script_content)

        with open(name_job_analysis, "w") as f:
            f.write(submission_file_content)

    jobs = "\n".join([f"JOB job_{i} {sub_file}" for i, sub_file in enumerate(sub_files)])
    parent_child_relations_all = []
    merge_jobs = []
    for partition in partitions:
        partition_min, partition_max = partition[0], partition[-1]
        partition_suffix = f"{partition_min}_{partition_max}"
        parent_child_relations = "\n".join([f"PARENT job_{i} CHILD job_merge_{partition_suffix}" for i in partition])
        parent_child_relations_all.append(parent_child_relations)
        parent_child_relations_all.append(f"PARENT job_merge_{partition_suffix} CHILD job_merge")

        name_merge_job = f"""{str(condor_dir / "jobs")}/job_merge_{partition_suffix}.sub"""
        merge_jobs.append(f"JOB job_merge_{partition_suffix} {name_merge_job}")

    merge_jobs.append(f"JOB job_merge {name_job_merge}")
    if processing_merge:
        merge_jobs.append(f"JOB job_analysis {name_job_analysis}")
        parent_child_relations_all.append(f"PARENT job_merge CHILD job_analysis")
    parent_child_relations = "\n".join(parent_child_relations_all)
    merge_jobs = "\n".join(merge_jobs)
    dag_submission_content = f"""
{jobs}
{merge_jobs}
{parent_child_relations}
"""

    name_dag_file = f"{condor_dir}/dag"

    with open(name_dag_file, "w") as f:
        f.write(dag_submission_content)

    print(name_dag_file)
    if not dry_run:
        # clear the ~/.rest directory
        clean_home_rest_command = f"""rm -rf {os.environ["HOME"]}/.rest"""
        os.system(clean_home_rest_command)
        subprocess.run(["condor_submit_dag", name_dag_file])

print(f"Output will be stored in {output_dir}")

# create a symbolic link to the latest output directory

latest_link = Path(os.environ["HOME"]) / "condor" / "latest"
latest_link.unlink(missing_ok=True)
latest_link.symlink_to(condor_dir)
