#!/bin/bash
#This script calls multiple afni commands to generate a one volume NIFTI file, 
#where each voxel contains the median of the 1/nT2* measurement across time
#############################
set -euo pipefail

# Set Variables
output="ironmap"
normalize=0
mask=""
version="Version: 1.0"
usage="Usage: ironmap.sh [-i input NIFTI file] [-m brain mask] [-o output file suffix, default=ironmap] \
[-a take the average/mean of all volumes rather than the median] [-v prints script version] [-h prints help message]"
helptext="Ironmap is a script that receives preprocessed 3D+Time fMRI data and outputs one volume, \
where each voxel is the inverse of the normalized T2* measurement. It does this by:" \
steps="1) Normalizing the voxels of each volume to the mean of that volume. \
2) Taking the median of each voxel across time. \
3) Calculating the inverse."

# Command Line Options
while getopts ":i:m:o:avh" options; do
    case $options in 
        i ) input=$OPTARG;;
        m ) mask=$OPTARG;;
        o ) output=$OPTARG;;
        a ) normalize=1;;
        v ) echo $version;; 
        h ) echo $usage
            echo $helptext
            echo $steps
            echo "Options: "
            echo "-i: REQUIRED. Input one fMRI 3d+time NIFTI file."
            echo "-m: OPTIONAL. Input a brain mask. An MNI brain mask is recommended. If none is provided, "
            echo "one will be created using afni 3dAutomask."
            echo "-o: OPTIONAL. Output file suffix. Will be attached to the end of the input filename. Default is "ironmap"."
            echo "-a: OPTIONAL. Take the average/mean of each voxel across time rather than the median."
            echo "-v: Print script version."
            echo "-h: Print this help text.";;
        \? ) echo $usage;;
        * ) echo $usage
            exit 1;;
    esac
done

if [ $OPTIND -eq 1 ]; then echo "Error: No options were passed. $usage"; fi

for file in $input
do
    filebase="${file%%.*}"

# Step 1: Normalize the voxels of each volume to the mean of the entire volume
## If no mask is given: Create a mask using afni 3dAutomask
    if [ -z "$mask" ]
        then
            echo "No mask given: Creating a brain mask."
            #Create one volume by taking the mean of each voxel over time (Pre Skull Stripping)
            3dTstat -mean -prefix ${filebase}_preSS.nii.gz $input
            #Skull Strip that volume
            3dSkullStrip -input ${filebase}_preSS.nii.gz -prefix ${filebase}_SS.nii.gz
            #Create brain mask
            3dAutomask -prefix ${filebase}_automask.nii.gz ${filebase}_SS.nii.gz 
            #Remove intermediate files 
            rm ${filebase}_preSS.nii.gz ${filebase}_SS.nii.gz
            mask="${filebase}_automask.nii.gz"
            echo "Mask created."
    fi
## Take the mean of all voxels per volume
    echo "Taking the mean of each volume"
    3dmaskave -mask ${mask} -quiet ${input} > ${filebase}_volmeans.1D

## Normalize/scale each voxel (per volume) to that mean
    echo "Normalizing each voxel per volume."
    3dcalc -a ${input} -b ${filebase}_volmeans.1D -expr "(a/b)" -prefix ${filebase}_scaled.nii.gz

# Step 2: Take the median/mean of each voxel across all volumes 
    if [ $normalize -eq 0 ]
        then 
            echo "Taking the median of all volumes."
            3dTstat -median -mask ${mask} -prefix ${filebase}_scaledavg_${normalize}.nii.gz ${filebase}_scaled.nii.gz
        else
            echo "Taking the mean of all volumes."
            3dTstat -mean -mask ${mask} -prefix ${filebase}_scaledavg_${normalize}.nii.gz ${filebase}_scaled.nii.gz
    fi

# Step 3: Take the inverse, 1/nT2*
    echo "Taking the inverse."
    3dcalc -a ${filebase}_scaledavg_${normalize}.nii.gz -expr "(1/a)" -prefix ${filebase}_${output}.nii.gz

# Step 4: Remove intermediate files
    echo "Removing intermediate files." 
    rm ${filebase}_volmeans.1D ${filebase}_scaled.nii.gz ${filebase}_scaledavg_${normalize}.nii.gz 
    echo "Done!"

done 
