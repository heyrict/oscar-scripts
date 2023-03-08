from toml import load
import os
import sys 
sys.path.append(os.path.abspath("../"))
from run_xnat2bids import compileArgumentList

def test_xnat2bids():
    # Load test parameters
    test_params = load("/gpfs/data/bnc/shared/scripts/fmcdona4/oscar-scripts/tests/x2b_test_config.toml")
    user = "test_user"

    # Initialize validation sets
    x2b_validation_set = [
    'XNAT_E00152', 
    '/gpfs/data/bnc/shared/bids-export/test_user', 
    '--host https://xnat.bnc.brown.edu', 
    '--session-suffix -1', \
    '--includeseq 1 --includeseq 2', 
    '--skipseq 3', 
    '--overwrite']


    slurm_validation_set = [
        '--time 04:00:00', 
        '--mem 16000', 
        '--nodes 1', 
        '--cpus-per-task 2', 
        '--job-name xnat2bids', 
        '--output /gpfs/data/bnc/scratch/logs/%J.txt', 
        '--mail-user ford_mcdonald@brown.edu', 
        '--mail-type ALL']

    # Run compileArgumentList()
    x2b_param_list, slurm_param_list = compileArgumentList(test_params, user)

    assert x2b_param_list == x2b_validation_set
    assert slurm_param_list == slurm_validation_set


if __name__ == "__main__":
    test_xnat2bids()
    