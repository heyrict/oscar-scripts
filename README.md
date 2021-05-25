oscar-scripts
--------------

Scripts used by BNC on Oscar

xnat-token 
==========

Generates a new user token for XNAT

**USAGE**

xnat-token [OPTIONS]

**OPTIONS**

  -d    Connect to development XNAT
  
  -h    Displays this help message
  
  -u    Username for XNAT

**EXAMPLES**

  xnat-token -u broarr

**NOTES**

  XNAT tokens live for 48 hours before they're invalidated.
  
singularity-sync
================

Sbatch script to sync [registered singularity images](https://github.com/brown-bnc/bnc-resource-registry/blob/master/singularity-manifest.yml) into Oscar 

dicomsort
=========

Script that calls the afni program `dicom_hdr` to read header information, reads the information 
and then renames and sorts the files alphabetically
