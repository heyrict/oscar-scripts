import argparse
import asyncio
import copy
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
logging.getLogger('asyncio').setLevel(logging.WARNING)

# Define a coroutine function to run subprocess command
async def run_subprocess(srun_cmd):
    await asyncio.create_subprocess_exec(*srun_cmd)
   


def extractParams(param, value):
    arg = []
    for v in value:
        arg.append(f"--{param} {v}")
    return ' '.join(arg)

def mergeConfigFiles(user_cfg, default_cfg):

        user_slurm = user_cfg['slurm-args']
        user_x2b = user_cfg['xnat2bids-args']
        default_slurm = default_cfg['slurm-args']
        default_x2b = default_cfg['xnat2bids-args']

        # Update default config with user provided parameters
        default_slurm.update(user_slurm)
        default_x2b.update(user_x2b)

        # Assemble final argument list 
        merged_dict = {}
        merged_dict['slurm-args'] = default_slurm
        merged_dict['xnat2bids-args'] = default_x2b

        # Add session specific parameter blocks
        for key in user_cfg.keys():
            if key == 'slurm-args' or key == 'xnat2bids-args':
                continue
            merged_dict[key] = user_cfg[key]

        return merged_dict

def compileX2BArgList(xnat2bids_dict, session):
        x2b_param_list = []
        param_lists = ["includeseq", "skipseq"]
        
        for param, value in xnat2bids_dict.items():
            if value != "" and value is not None:
                # Set {session} as first parameter
                if param == "sessions":
                    arg = f"{value[session]}"
                    x2b_param_list.insert(0,arg)
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
                # Extract parameters from include / skip lists
                elif param in param_lists:
                    arg = extractParams(param, value)
                    x2b_param_list.append(arg)
                # Other arguments follow --param value format.
                else:
                    arg = f"--{param} {value}"
                    x2b_param_list.append(arg)

        return x2b_param_list

def compileArgumentList(session, arg_dict, user):
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
        else:
            # If a session key exist for the current session being 
            # processed, update final config with session block. 
            if section_name == x2b_param_dict['sessions'][session]:
                x2b_param_dict.update(section_dict)
    
    # Transform session config dictionary into argument list.
    x2b_param_list = compileX2BArgList(x2b_param_dict, session)
    return x2b_param_list, slurm_param_list, bindings

async def main():
    # Instantiate argument parserß
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to user config")
    args = parser.parse_args()

    # Load default config file into dictionary
    script_dir = pathlib.Path(__file__).parent.resolve()
    default_params = load(f'{script_dir}/x2b_default_config.toml')

    # Set arglist. If user provides config, merge dictionaries.
    if args.config is None:
        arglist = default_params
    else:
        # Load user configuration
        user_params = load(args.config)

        # Merge with default configuration
        arglist = mergeConfigFiles(user_params, default_params)

    # Fetch user credentials 
    user = input('Enter Username: ')
    password = getpass('Enter Password: ')

    # Fetch number of sessions to process
    num_sessions = len(arglist['xnat2bids-args']['sessions'])

    # Assemble parameter lists per session
    argument_lists = []
    for session in range(num_sessions):
        # Fetch compiled xnat2bids and slurm parameter lists
        x2b_param_list, slurm_param_list, bindings = compileArgumentList(session, arglist, user)

        # Insert username and password into x2b_param_list
        x2b_param_list.insert(2, f"--user {user}")
        x2b_param_list.insert(3, f"--pass {password}")

        # Fetch latest version if not provided
        if not ('version' in arglist['xnat2bids-args']):
            list_of_versions = glob.glob('/gpfs/data/bnc/simgs/brownbnc/*') 
            latest_version = max(list_of_versions, key=os.path.getctime)
            version = latest_version.split('-')[-1].replace('.sif', '')

        # Define singularity image 
        simg=f"/gpfs/data/bnc/simgs/brownbnc/xnat-tools-{version}.sif"

        # Define output for logs
        if not ('output' in arglist['slurm-args']):
            output = f"/users/{user}/logs/%x-%J.txt"
            arg = f"--output {output}"
            slurm_param_list.append(arg)

        if not (os.path.exists(os.path.dirname(output))):
            os.mkdir(os.path.dirname(output))

        # Define bids root directory
        if not ('bids_root' in arglist['xnat2bids-args']):
            bids_root = f"/users/{user}/bids-export/"
            x2b_param_list.insert(1, bids_root)
            bindings.append(bids_root)

        if not (os.path.exists(os.path.dirname(bids_root))):
            os.mkdir(os.path.dirname(bids_root))  

        # Store xnat2bids, slurm, and binding paramters as tuple.
        argument_lists.append((x2b_param_list, slurm_param_list, bindings))
        
        logging.debug("Argument List for Session: %s", arglist['xnat2bids-args']['sessions'][session])
        logging.debug("-------------------------------------")
        logging.debug("xnat2bids: %s", x2b_param_list)
        logging.debug("slurm: %s", slurm_param_list)
        logging.debug("-------------------------------------")

    # Loop over argument lists for provided sessions.
    tasks = []
    for args in argument_lists:
        # Compile bindings into formated string
        bindings_str = ' '.join(f"-B {path}" for path in args[2])

        # Process command string for SRUN
        srun_cmd = shlex.split(f"srun -Q {' '.join(args[1])} \
            singularity exec --no-home {bindings_str} {simg} \
            xnat2bids {' '.join(args[0])}")

        logging.debug("Running xnat2bids %s", args[0][0])
        logging.debug("-------------------------------------")
        logging.debug("Command Input: %s", srun_cmd)
        logging.debug("-------------------------------------")
        
        # Run xnat2bids asynchronously
        task = asyncio.create_task(run_subprocess(srun_cmd))
        tasks.append(task)

    # Wait for all subprocess tasks to complete
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())