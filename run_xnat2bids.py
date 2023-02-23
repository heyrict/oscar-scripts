from toml import load
import datetime
import subprocess, shlex
import os
import glob
from getpass import getpass
import shutil

# Load config file into dictionary
arglist = load('x2b_config.toml')

# Extract arglist from dictionary
for key in arglist:

    # Populate sbatch arguments
    if (key == "sbatch-args"):
        time = arglist[key]['time'] 
        mem = arglist[key]['mem'] 
        nodes = arglist[key]['nodes'] 
        cpus = arglist[key]['cpus'] 
        job =  arglist[key]['job'] 
        output = arglist[key]['output'] 
        email = arglist[key]['email']

    # Populate xnat2bids arguments
    if(key == "xnat2bids-args"):
        version = arglist[key]['version'] 
        data_dir = arglist[key]['data_dir'] 
        bids_root = arglist[key]['bids_root'] 
        session = arglist[key]['session'] 

# Fetch latest version if not provided
if(version == ""):
    list_of_versions = glob.glob('/gpfs/data/bnc/simgs/brownbnc/*') 
    latest_version = max(list_of_versions, key=os.path.getctime)
    version = latest_version.split('-')[-1].replace('.sif', '')

# Fetch user credentials 
user = input('Enter Username: ')
password = getpass('Enter Password: ')

# Define bids root directory
if(bids_root == ""):
    bids_root = f"{data_dir}/shared/bids-export/{user}"

# Define singularity image 
simg=f"/gpfs/data/bnc/simgs/brownbnc/xnat-tools-{version}.sif"

# Run xnat2bids via srun
subprocess.run(shlex.split(
    f"srun -t {time} --mem {mem} -N {nodes} -c {cpus} -J {job} \
        -o {output} --mail-type=ALL --mail-user {email} \
    singularity exec --contain -B {data_dir} {simg} \
    xnat2bids {session} {bids_root} -u {user} -p {password}"
))


