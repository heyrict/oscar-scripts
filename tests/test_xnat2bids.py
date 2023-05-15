from toml import load
import os
import sys 
import unittest
sys.path.append(os.path.abspath("../"))
from run_xnat2bids import compile_xnat2bids_list, compile_slurm_list, extract_params
from run_xnat2bids import parse_x2b_params


class TestRunXnat2BIDS(unittest.TestCase):

    def test_compile_x2b_lists(self):
        # Load test parameters
        test_params = load("/gpfs/data/bnc/shared/scripts/fmcdona4/oscar-scripts/tests/x2b_test_config.toml")
        user = "test_user"
        session = test_params['xnat2bids-args']['sessions'][0]
        
        # Initialize validation sets
        x2b_validation_set = [
        'XNAT_E00152', 
        '/gpfs/data/bnc/shared/bids-export/', 
        '--host https://xnat.bnc.brown.edu', 
        '--bidsmap-file /gpfs/data/bnc/shared/scripts/fmcdona4/bidsmap.json',
        '--includeseq 1 --includeseq 2', 
        '--skipseq 3', 
        '--overwrite']


        slurm_validation_set = [
            '--time 04:00:00', 
            '--mem 16000', 
            '--nodes 1', 
            '--cpus-per-task 2', 
            '--job-name xnat2bids', 
            '--output /gpfs/scratch/%u/logs/xnat2bids_test%J.log', 
            '--mail-user example-user@brown.edu', 
            '--mail-type ALL']

        bindings_validation_set = [
            "/gpfs/data/bnc/shared/bids-export/",
            "/gpfs/data/bnc/shared/scripts/fmcdona4/bidsmap.json"
        ]

        # Run compile_xnat2bids_list()
        x2b_param_list, bindings = compile_xnat2bids_list(session, test_params, user)
    

        self.assertEqual(x2b_param_list, x2b_validation_set)
        self.assertEqual(bindings, bindings_validation_set)

        # Run compile_slurm_list()      
        slurm_param_list = compile_slurm_list(test_params, user)

        self.assertEqual(slurm_param_list, slurm_validation_set)


    def test_parse_x2b_params(self):
        # Define test input data
        xnat2bids_dict = {
            "bids_root": "/path/to/bids",
            "includeseq": ["t1w", "t2w"],
            "export-only": True,
            "verbose": 3,
        }
        session = "test_session"
        bindings = []

        # Call the function being tested
        result = parse_x2b_params(xnat2bids_dict, session, bindings)

        # Define the expected output
        expected_result = [
            "test_session",
            "/path/to/bids",
            "--includeseq t1w --includeseq t2w",
            "--export-only",
            "--verbose",
            "--verbose",
            "--verbose",
        ]
        expected_bindings = ["/path/to/bids"]

        # Check that the result matches the expected output
        self.assertEqual(result, expected_result)
        self.assertEqual(bindings, expected_bindings)

    def test_extract_params_list(self):
        # Test extraction of list input
        param = "includeseq"
        value = [1, 2, 3, 4]
        result = extract_params(param, value)
        expected_result = "--includeseq 1 --includeseq 2 --includeseq 3 --includeseq 4"
        self.assertEqual(result, expected_result)

    def test_extract_params_range(self):
        # Test extraction of range input
        param = "includeseq"
        value = "1-4, 7, 10"
        result = extract_params(param, value)
        expected_result = "--includeseq 1 --includeseq 2 --includeseq 3 --includeseq 4 --includeseq 7 --includeseq 10"
        self.assertEqual(result, expected_result)

    def test_extract_params_skipseq(self):
        # Test extraction of skipseq input
        param = "skipseq"
        value = "3-5, 7"
        result = extract_params(param, value)
        expected_result = "--skipseq 3 --skipseq 4 --skipseq 5 --skipseq 7"
        self.assertEqual(result, expected_result)

    def test_extract_params_empty(self):
        # Test extraction of empty input
        param = "includeseq"
        value = []
        result = extract_params(param, value)
        expected_result = ""
        self.assertEqual(result, expected_result)

if __name__ == "__main__":
    unittest.main()
    