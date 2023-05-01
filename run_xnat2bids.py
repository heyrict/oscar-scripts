import argparse
import asyncio
import copy
from collections import defaultdict
from getpass import getpass
import glob
import logging
import os
import pathlib
import shlex
import shutil
import subprocess
from toml import load

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.NOTSET)
logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.WARNING)
   
def set_logging_level(x2b_arglist: list):
    if "--verbose"  in x2b_arglist:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

def fetch_latest_version():
    list_of_versions = glob.glob('/gpfs/data/bnc/simgs/brownbnc/*') 
    latest_version = max(list_of_versions, key=os.path.getctime)
    return (latest_version.split('-')[-1].replace('.sif', ''))

def extract_params(param, value):
    arg = []
    for v in value:
        arg.append(f"--{param} {v}")
    return ' '.join(arg)

def merge_config_files(user_cfg, default_cfg):

        user_slurm = user_cfg['slurm-args']
        user_x2b = user_cfg['xnat2bids-args']
        default_slurm = default_cfg['slurm-args']
        default_x2b = default_cfg['xnat2bids-args']

        # Assemble merged dictionary with default values.
        merged_dict = defaultdict(dict)
        merged_dict['xnat2bids-args'].update(default_x2b)
        merged_dict['slurm-args'].update(default_slurm)

        # Update merged dictionary with user provided arguments.
        merged_dict['slurm-args'].update(user_slurm)
        merged_dict['xnat2bids-args'].update(user_x2b)
        
        # Add session specific parameter blocks
        for key in user_cfg.keys():
            if key == 'slurm-args' or key == 'xnat2bids-args':
                continue
            merged_dict[key].update(user_cfg[key])

        return merged_dict

def compile_x2b_arglist(xnat2bids_dict, session, bindings):
        x2b_param_list = []
        param_lists = ["includeseq", "skipseq"]
        
        for param, value in xnat2bids_dict.items():
            if value == "" or value is  None:
                continue
            # Set {session} as first parameter
            elif param == "sessions":
                x2b_param_list.insert(0,session)
            # Set {bids_root} as second parameter
            elif param == "bids_root":
                arg = f"{value}"
                bindings.append(arg)
                x2b_param_list.insert(1, arg)
            elif param == "bidsmap-file":
                arg = f"--{param} {value}"
                bindings.append(value)
                x2b_param_list.append(arg)
            # Set as many verbose flags as specified.
            elif param == "verbose":
                arg = f"--{param}"
                for i in range(value):
                    x2b_param_list.append(arg)
            # If overwrite is equal to true, set flag.
            elif param == "overwrite":
                arg = f"--{param}"
                if value == True: x2b_param_list.append(arg) 
            elif param == "cleanup":
                arg = f"--{param}"
                if value == True: x2b_param_list.append(arg)
            # If version is specified, continue
            elif param == "version":
                continue
            # Extract parameters from include / skip lists
            elif param in param_lists:
                arg = extract_params(param, value)
                x2b_param_list.append(arg)
            # Other arguments follow --param value format.
            else:
                arg = f"--{param} {value}"
                x2b_param_list.append(arg)

        return x2b_param_list

def compile_argument_list(session, arg_dict, user):
    """Create command line argument list from TOML dictionary."""

    # Create copy of dictionary, so as not to update
    # the original object reference while merging configs.
    arg_dict_copy = copy.deepcopy(arg_dict) 

    slurm_param_list = []
    bindings = []
    # Compile list of appended arguments
    x2b_param_dict = {}
    for section_name, section_dict in arg_dict_copy.items():
        # Extract xnat2bids-args from original dictionary
        if section_name == "xnat2bids-args":
            x2b_param_dict = section_dict

        # Extract slurm-args from original dictionary
        elif section_name == "slurm-args":
            for param, value in section_dict.items():
                if value != "" and value is not None:
                    arg = f"--{param} {value}"
                    slurm_param_list.append(arg)

        # If a session key exist for the current session being 
        # processed, update final config with session block. 
        elif section_name == session:
                x2b_param_dict.update(section_dict)
    
    # Transform session config dictionary into argument list.
    x2b_param_list = compile_x2b_arglist(x2b_param_dict, session, bindings)
    return x2b_param_list, slurm_param_list, bindings

async def main():
    # Instantiate argument parserß
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to user config")
    args = parser.parse_args()

    # Load default config file into dictionary
    script_dir = pathlib.Path(__file__).parent.resolve()
    default_params = load(f'{script_dir}/x2b_default_config.toml')

    # Set arg_dict. If user provides config, merge dictionaries.
    if args.config is None:
        arg_dict = default_params
    else:
        # Load user configuration
        user_params = load(args.config)

        # Merge with default configuration
        arg_dict = merge_config_files(user_params, default_params)

    # If sessions does not exist in arg_dict, prompt user for Accession ID(s).
    if 'sessions' not in arg_dict['xnat2bids-args']:
        sessions_input = input("Enter Session(s) (comma-separated): ")
        arg_dict['xnat2bids-args']['sessions'] = [s.strip() for s in sessions_input.split(',')]
        

    # Fetch user credentials 
    user = input('Enter XNAT Username: ')
    password = getpass('Enter Password: ')

    # Assemble parameter lists per session
    argument_lists = []
    for session in arg_dict['xnat2bids-args']['sessions']:

        # Fetch compiled xnat2bids and slurm parameter lists
        x2b_param_list, slurm_param_list, bindings = compile_argument_list(session, arg_dict, user)

        # Insert username and password into x2b_param_list
        x2b_param_list.insert(2, f"--user {user}")
        x2b_param_list.insert(3, f"--pass {password}")

        # Fetch latest version if not provided
        if not ('version' in arg_dict['xnat2bids-args']):
            version = fetch_latest_version()
        else:
            version =  arg_dict['xnat2bids-args']['version']

        # Define singularity image 
        simg=f"/gpfs/data/bnc/simgs/brownbnc/xnat-tools-{version}.sif"

        # Define output for logs
        if not ('output' in arg_dict['slurm-args']):
            output = f"/gpfs/scratch/{user}/logs/%x-{session}-%J.txt"
            arg = f"--output {output}"
            slurm_param_list.append(arg)
        else:
            output = arg_dict['slurm-args']['output']

        if not (os.path.exists(os.path.dirname(output))):
            os.mkdir(os.path.dirname(output))

        # Define bids root directory
        if not ('bids_root' in arg_dict['xnat2bids-args']):
            bids_root = f"/users/{user}/bids-export/"
            x2b_param_list.insert(1, bids_root)
            bindings.append(bids_root)
        else:
            bids_root = x2b_param_list[1]

        if not (os.path.exists(os.path.dirname(bids_root))):
            os.mkdir(os.path.dirname(bids_root))  

        # Store xnat2bids, slurm, and binding paramters as tuple.
        argument_lists.append((x2b_param_list, slurm_param_list, bindings))

        # Set logging level per session verbosity. 
        set_logging_level(x2b_param_list)

        # Remove the password parameter from the x2b_param_list
        x2b_param_list_without_password = [param for param in x2b_param_list if not param.startswith('--pass')]

        logging.debug({
        "message": "Argument List",
        "session": session,
         "slurm_param_list": slurm_param_list,
        "x2b_param_list": x2b_param_list_without_password,

        })

    # Loop over argument lists for provided sessions.
    tasks = []
    for args in argument_lists:
        # Compile bindings into formated string
        bindings = ' '.join(f"-B {path}" for path in args[2])

        # Compilie slurm and xnat2bids args 
        xnat2bids_options = ' '.join(args[0])
        slurm_options = ' '.join(args[1])

        # Build shell script for sbatch
        sbatch_script = f"\"$(cat << EOF #!/bin/sh\n \
            apptainer exec --no-home {bindings} {simg} \
            xnat2bids {xnat2bids_options}\nEOF\n)\""

        # Process command string for SRUN
        sbatch_cmd = shlex.split(f"sbatch -Q {slurm_options} \
            --wrap {sbatch_script}")    

        # Set logging level per session verbosity. 
        set_logging_level(args[0])

        # Remove the password from sbatch command before logging 
        xnat2bids_options_without_password = []
        exclude_next_opt = False

        for opt in xnat2bids_options.split():
            if exclude_next_opt:
                exclude_next_opt = False
            elif opt == "--pass":
                exclude_next_opt = True
                continue
            else:
                xnat2bids_options_without_password.append(opt)

        sbatch_script_without_password = f"apptainer exec --no-home {bindings} {simg} \
                                            xnat2bids {xnat2bids_options_without_password}"

        sbatch_cmd_without_password = shlex.split(f"sbatch -Q {slurm_options} \
                                                    --wrap {sbatch_script_without_password}")   

        logging.debug({
            "message": "Executing xnat2bids",
            "session": args[0][0],
            "command": sbatch_cmd_without_password
        })
        
        # Run xnat2bids asynchronously
        task = asyncio.create_task(asyncio.create_subprocess_exec(*sbatch_cmd))
        tasks.append(task)

    # Wait for all subprocess tasks to complete
    await asyncio.gather(*tasks)

    logging.info("Launched %d %s", len(tasks), "jobs" if len(tasks) > 1 else "job")
    logging.info("Processed Scans Located At: %s", bids_root)

if __name__ == "__main__":
    asyncio.run(main())