import os
import pandas as pd
import pydicom

def load_config(config_path):
    df = pd.read_csv(config_path)
    config = dict(zip(df['Tag'], df['Value']))
    return config

def anonymize_dicom(file_path, output_path, config):
    dataset = pydicom.dcmread(file_path)
    for tag, value in config.items():
        if hasattr(dataset, tag):
            if value == "CLEAR":
                setattr(dataset, tag, None)
            elif value == "DELETE":
                del dataset[tag]
            else:
                setattr(dataset, tag, value)
        else:
            print(f"Warning: DICOM field {tag} not found. Skipping.")
    dataset.save_as(output_path)
    print(f"Modified and saved: {output_path}")

def anonymize_directory(input_dir, output_dir, config_path):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    config = load_config(config_path)

    for root, _, files in os.walk(input_dir):
        # Create the corresponding directory structure in the destination directory
        rel_path = os.path.relpath(root, input_dir)
        dest_path = os.path.join(output_dir, rel_path)
        os.makedirs(dest_path, exist_ok=True)
        
        skip_extensions = ['json','nii','nii.gz','txt','doc','gif','csv']
        for file in files:
            if os.path.splitext(file)[1] not in skip_extensions:
                input_file_path = os.path.join(root, file)
                output_file_path = os.path.join(dest_path, file)
                try:
                    anonymize_dicom(input_file_path, output_file_path, config)
                except pydicom.errors.InvalidDicomError:
                    print(f"{input_file_path} is not a valid DICOM. Skipping.")
                    pass


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Anonymize a directory of DICOM files.')
    parser.add_argument('-input_dir', type=str, help='Path to the input directory containing DICOM files.')
    parser.add_argument('-output_dir', type=str, help='Path to the output directory for anonymized DICOM files.')
    parser.add_argument('-config_path', type=str, help='Path to the CSV configuration file.')
    args = parser.parse_args()

    anonymize_directory(args.input_dir, args.output_dir, args.config_path)

if __name__ == "__main__":
    main()
