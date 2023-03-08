import argparse
from getpass import getpass
import glob
import logging
import os
import shlex
import shutil
import subprocess
from toml import load


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.NOTSET)

def extractParams(param, value):
    arg = []
    for v in value:
        arg.append(f"--{param} {v}")
    return ' '.join(arg)

def compileArgumentList(arg_dict, user):
    """Create command line argument list from TOML dictionary."""
    x2b_param_list = []
    slurm_param_list = []
    bindings = []

    param_lists = ["includeseq", "skipseq"]

    # Compile list of appended arguments
    for section_name, section_dict in arg_dict.items():
        if section_name == "xnat2bids-args":
            for param, value in section_dict.items():
                if value != "" and value is not None:
                    # Set {session} as first parameter
                    if param == "session":
                        arg = f"{value}"
                        x2b_param_list.insert(0,arg)
                    # Set {bids_root} as second parameter
                    elif param == "bids_root":
                        arg = f"/gpfs/data/bnc/shared/bids-export/{user}"
                        bindings.append(arg)
                        x2b_param_list.insert(1, arg)
                    elif param == "bidsmap-file":
                        arg = f"--{param} {value}"
                        bindings.append(value)
                        x2b_param_list.append(arg)
                    # If verbose is equal to 1, set flag.
                    elif param == "verbose":
                        arg = f"--{param}"
                        if value == 1: x2b_param_list.append(arg)
                    # If overwrite is equal to true, set flag.
                    elif param == "overwrite":
                        arg = f"--{param}"
                        if value == True: x2b_param_list.append(arg) 
                    # Extract parameters from include / skip lists
                    elif param in param_lists:
                        arg = extractParams(param, value)
                        x2b_param_list.append(arg)
                    # Other arguments follow --param value format.
                    else:
                        arg = f"--{param} {value}"
                        x2b_param_list.append(arg)

        elif section_name == "slurm-args":
            for param, value in section_dict.items():
                if value != "" and value is not None:
                    arg = f"--{param} {value}"
                    slurm_param_list.append(arg)

    return x2b_param_list, slurm_param_list, bindings

def main():
    # Instantiate argument parserß
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to user config")
    args = parser.parse_args()

    # Load default config file into dictionary
    default_params = load('/gpfs/data/bnc/shared/scripts/fmcdona4/oscar-scripts/x2b_default_config.toml')

    # Set arglist. If user provides config, merge dictionaries.
    if args.config is None:
        arglist = default_params
    else:
        user_params = load(args.config)
        default_params.update(user_params)
        arglist = user_params

    # Fetch user credentials 
    user = input('Enter Username: ')
    password = getpass('Enter Password: ')

    # Fetch compiled xnat2bids abd slurm parameter lists
    x2b_param_list, slurm_param_list, bindings = compileArgumentList(arglist, user)

    # Insert username and password into x2b_param_list
    x2b_param_list.insert(2, f"--user {user}")
    x2b_param_list.insert(3, f"--pass {password}")

    # Fetch latest version if not provided
    if(arglist['xnat2bids-args']['version'] == ""):
        list_of_versions = glob.glob('/gpfs/data/bnc/simgs/brownbnc/*') 
        latest_version = max(list_of_versions, key=os.path.getctime)
        version = latest_version.split('-')[-1].replace('.sif', '')

    # Define singularity image 
    simg=f"/gpfs/data/bnc/simgs/brownbnc/xnat-tools-{version}.sif"

    output = arglist['slurm-args']['output']
    if not (os.path.exists(os.path.dirname(output))):
        os.mkdir(os.path.dirname(output))

    # Compile bindings into formated string
    bindings_str = ' '.join(f"-B {path}" for path in bindings)

    # Process command string for SRUN
    srun_cmd = shlex.split(f"srun {' '.join(slurm_param_list)} \
        singularity exec --no-home {bindings_str} {simg} \
        xnat2bids {' '.join(x2b_param_list)}")

    logging.debug("Running xnat2bids")
    logging.debug("-------------------------------------")
    logging.debug("Command Input: %s", srun_cmd)
    logging.debug("-------------------------------------")
    
    # Run xnat2bids
    subprocess.run(srun_cmd)

if __name__ == "__main__":
    main()