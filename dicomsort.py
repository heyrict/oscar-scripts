import os
import shutil
import argparse
import sys
try:
    import pydicom
except ImportError:
    print("\nMissing required package: pydicom.\n\n\
On Oscar, you can activate a virtual environment \ncontaining pydicom by typing: \n\n\
source /oscar/data/bnc/src/python_venvs/pydicom/bin/activate \n\n\
and then run this script again. \n\nTo get out of the virtual environment \
when you are done, \njust type 'deactivate' on the command line.\n")
    quit()

# Define DICOM tags
snumtag = '00200011'     # series number
anumtag = '00200012'     # acquisition number
inumtag = '00200013'     # instance number
nametag = '00100010'     # patient name
idtag = '00100020'       # patient ID
sdesctag = '0008103e'    # protocol name

def parse_arguments():
    usage = "python dicomsort [-r] [-d destdir] [-s sourcedir] [-i] [-q] [-n]\n\n\
options:\n\
    -r: Rename files. Default is to copy.\n\
    -d: Destination directory. Default is ./renamed.\n\
    -s: Source directory. Default is current directory.\n\
    -i: Create subdirectories by subject ID.\n\
    -q: Create subdirectories by series description.\n\
    -n: Don't recurse into subdirectories.\n"

    description ="Dicomsort is a script to sort and rename dicom files\nin alphabetical order according to \
series and slice number.\n\nThis is the python version of dicomsort, which requires the python package pydicom\n\
This version of dicomsort can handle MR spectroscopy DICOMs.\
\n\nOn Oscar, you can activate a virtual environment \ncontaining pydicom by typing: \n\n\
source /oscar/data/bnc/src/python_venvs/pydicom/bin/activate \n\n\
and then run this script again. \n\nTo get out of the virtual environment \
when you are done, \njust type 'deactivate' on the command line.\n"

    parser = argparse.ArgumentParser(usage=usage, description=description, add_help=False)
    parser.add_argument('-r', action='store_true', help='Rename files instead of copying')
    parser.add_argument('-d', default='renamed', help='Destination directory (default: ./renamed)')
    parser.add_argument('-s', default='./', help='Source directory (default: current directory)')
    parser.add_argument('-i', action='store_true', help='Create subdirectories by subject ID')
    parser.add_argument('-q', action='store_true', help='Create subdirectories by series description')
    parser.add_argument('-n', action='store_false', help="Don't recurse into subdirectories")
    parser.add_argument('-h', '--help', action='store_true', help='Show this help message and exit')

    args = parser.parse_args()
    
    if args.help:
        print(usage)
        print(description)
        parser.exit()

        # Prompt user for confirmation if no arguments were provided
    if not len(sys.argv) > 1:
        currdir = os.getcwd()
        confirm = input(f"No options provided, using default settings.\n\
All DICOMs in {currdir} will be copied.\n\
Do you want to proceed? (y/n): ")
        if confirm.lower() != 'y':
            print("Operation cancelled by user.")
            parser.exit()
        
    return args

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

def process_files(args):
    sourcedir = args.s
    destdir = args.d
    rename = args.r
    usesubdir = args.i
    useseqdir = args.q
    recurse = args.n

    create_directory(destdir)

    print("Processing...")

    for root, _, files in os.walk(sourcedir):
        if not recurse and root != sourcedir:
            continue

        for f in files:
            filepath = os.path.join(root, f)
            if not os.path.isfile(filepath):
                continue

            try:
                print("converting "+filepath)
                dicom = pydicom.dcmread(filepath, stop_before_pixels=True)
            except pydicom.errors.InvalidDicomError:
                print(filepath + " is not a valid DICOM. skipping.")
                continue

            snum = dicom[snumtag].value
            #some files don't have an acquisition number, like the Phoenix Zip Report
            try:  
                anum = dicom[anumtag].value
            except KeyError:
                anum=0
            inum = dicom[inumtag].value

            targdir = destdir
            if usesubdir:
                substring = dicom[nametag].value
                subdir = ''.join(e for e in substring if e.isalnum())
                targdir = os.path.join(destdir, subdir)
                create_directory(targdir)

            if useseqdir:
                seqdir = dicom[sdesctag].value
                serpre = f"{int(snum):02}_"
                targdir = os.path.join(targdir, f"{serpre}{seqdir}")
                create_directory(targdir)

            targfile = os.path.join(targdir, f"dcmS{int(snum):04}A{int(anum):04}I{int(inum):04}")

            try:
                if rename:
                    shutil.move(filepath, targfile)
                    print(f"RENAMING {filepath} to {targfile}")
                else:
                    shutil.copy2(filepath, targfile)
                    print(f"COPYING {filepath} to {targfile}")
            except shutil.SameFileError:
                print(f"{targfile} already exists. skipping.")
                continue
            except IOError as e:
                print("Unable to process file. %s" % e)
                continue
            except:
                print("Unexpected error:", sys.exc_info())

        if not recurse:
            break

    print("dicomsort complete")

if __name__ == "__main__":
    args = parse_arguments()
    process_files(args)
