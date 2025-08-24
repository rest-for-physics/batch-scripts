# Author: Luis Obis (lobis@unizar.es)

# Usage:
# python3 restManagerToCondor.py --rml simulation.rml --input-file input.root --name my_analysis
# arguments not specified to this script (not --rml, --n-jobs, ...) are passed directly to restG4

from __future__ import annotations

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

programs_base_dir = "/data/dust/user/porronla/programs"
env_export = f"""
bash -c "source {programs_base_dir}/root/root-6.26.10/root_build/bin/thisroot.sh"
bash -c "source {programs_base_dir}/geant4/geant4-v11.0.3/install/bin/geant4.sh"

export G4NEUTRONHPDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/G4NDL4.6
export G4LEDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/G4EMLOW8.0
export G4LEVELGAMMADATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/PhotonEvaporation5.7
export G4RADIOACTIVEDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/RadioactiveDecay5.6
export G4PARTICLEXSDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/G4PARTICLEXS4.0
export G4PIIDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/G4PII1.3
export G4REALSURFACEDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/RealSurface2.2
export G4SAIDXSDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/G4SAIDDATA2.0
export G4ABLADATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/G4ABLA3.1
export G4INCLDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/G4INCL1.0
export G4ENSDFSTATEDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/G4ENSDFSTATE2.3
export G4TENDLDATA={programs_base_dir}/geant4/geant4-v11.0.3/v11.0.3_data/G4TENDL1.4

export XercesC_INCLUDE_DIR={programs_base_dir}/xerces/xerces-c-3.3.0/install/include
export XercesC_LIBRARY={programs_base_dir}/xerces/xerces-c-3.3.0/install/lib64/libxerces-c.so
export LD_LIBRARY_PATH={programs_base_dir}/xerces/xerces-c-3.3.0/install/lib64:$LD_LIBRARY_PATH

source {programs_base_dir}/garfield/install/share/Garfield/setupGarfield.sh
source {programs_base_dir}/rest/latest/install/thisREST.sh
"""

restRoot = f"{REST_PATH}/bin/restRoot"
restManager = f"{REST_PATH}/bin/restManager"

# The positional arguments are the arguments for the restG4 binary
timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

parser = argparse.ArgumentParser(description="Launch restG4 jobs on condor")
parser.add_argument("--rml", type=str, default="simulation.rml", help="RML config file", required=True)
parser.add_argument("--input-file", type=str, required=True, help="Input file")
parser.add_argument("--output-dir", type=str, default="", help="Output directory")
parser.add_argument("--memory", type=int, default="0", help="Memory in MB. If 0, use default value")
parser.add_argument("--dry-run", action="store_true", help="Set this flag for a dry run")
parser.add_argument("--env", nargs="+", default=[], help="Environment variables to submit to condor")
parser.add_argument("--name", type=str, default=f"restG4_{timestamp_str}")

args, restManager_args = parser.parse_known_args()

dry_run = args.dry_run == True

memory_sub_string = f"request_memory = {args.memory}" if args.memory != 0 else ""

# split and store env variables in dict
env_vars = {}
for env_var in args.env:
    key, value = env_var.split("=")
    env_vars[key] = value

name = args.name
# create output directory if it does not exist

user = os.environ["USER"]
if user == "":
    raise Exception("Could not find current user")

condor_dir = Path(f"/data/dust/user/{user}") / "condor" / name
condor_dir.mkdir(parents=True, exist_ok=True)

output_dir = args.output_dir
if output_dir == "":
    output_dir = condor_dir / "output"

output_dir = Path(output_dir)
output_dir.mkdir(parents=True, exist_ok=True)

tmp_dir = condor_dir / "tmp"
tmp_dir.mkdir(parents=True, exist_ok=True)

print(f"Condor directory: {condor_dir}")

rml = Path(args.rml)
if not rml.exists():
    raise Exception(f"Could not find rml file {args.rml}")

rml = rml.resolve()

time_additional = 3600  # give 1h of margin

# same as input but instead of ending in .root ends in .analysis.root
output_filename = Path(args.input_file).stem + "_analysis.root"
output_file = str(output_dir / output_filename)
tmp_file = f"{tmp_dir}/output.root"

env_var_string = '\n'.join([f"export {key}={value}" for key, value in env_vars.items()])
command = f"""
{env_export}
export REST_PATH=/data/dust/user/porronla/programs/rest/latest/install

export ANALYSIS_IS_SIMULATION="ON"
export REST_GAS="Argon-Isobutane 2Pct 10-10E3Vcm"
export PRESSURE="1.4"
export DRIFT_FIELD="183.33"
export SAMPLING_TPC="40"
export SAMPLING_VETO="200"
export TRIG_DELAY_TPC="15"
export TRIG_DELAY_VETO="75"
export SIMULATION_SIGNAL_BIN="128"
export TRIG_DELAY_TCM="1"

export LD_LIBRARY_PATH={REST_PATH}/lib:\
{programs_base_dir}/geant4/geant4-v11.0.3/install/lib64:\
{programs_base_dir}/xerces/xerces-c-3.3.0/install/lib64:\
{programs_base_dir}/root/root-6.26.10/root_install/lib:\
{programs_base_dir}/rest/latest/install/lib:\
{programs_base_dir}/garfield/install/lib64:\
$LD_LIBRARY_PATH

{env_var_string}
{restManager} --c {args.rml} --i {args.input_file} --o {tmp_file} {" ".join(restManager_args)}
mv {tmp_file} {output_file}
"""
print(command)
script_content = f"""
{command}
"""
name_script = f"""{str(condor_dir / "scripts")}/script.sh"""
name_job = f"""{str(condor_dir / "jobs")}/job.sub"""

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

output       = {str(stdout_dir)}/output
error        = {str(stderr_dir)}/error
log          = {str(logs_dir)}/log
stream_output  = True
stream_error   = True

request_cpus   = 1
{memory_sub_string}

+RequestRuntime = 43200

should_transfer_files = yes

queue
"""

os.makedirs(os.path.dirname(name_script), exist_ok=True)
with open(name_script, "w") as f:
    f.write(script_content)

os.makedirs(os.path.dirname(name_job), exist_ok=True)
with open(name_job, "w") as f:
    f.write(submission_file_content)

if not dry_run:
    subprocess.run(["condor_submit", name_job], check=True)
