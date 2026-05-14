#! /usr/bin/env python

# implementation: uv run select_rand_atlas.py

# contains code to select a random sample of sources from ATLAS-Refcat2

# background: the ATLAS-Refcat2 data is stored on the server "Aurora" in groups of healpix:
# there are 192 level-1 numbered directories, each with 16 level-2 numbered diros,
# each level-2 diro has a "part-0.feather" file with sources from 4 healpix(NSIDE=32)
# making 3072 feather files, each with 4 hpix(NSIDE=32) worth of sources.

# ATLAS-Refcat2 contains 991M rows - on Aurora, the following columns:
# ['ra', 'dec', 'parallax', 'pmra', 'pmdec', 'g', 'r', 'i', 'h32', 'h64', 'h256']

from argparse import ArgumentParser
import glob
import numpy as np
import lsdb 
import pandas as pd
import random
import sys 
import time


def get_rand_cat_sample(cat="atlas",cat_path="/etc/rico/atlas_refcat2",cat_frac=0.001,nfiles=0,verbose=False):

    '''
    Selects a random sample fraction (frac) of a source catalog.  
    Assumes catalog files are saved in feather format on your local server. 
    This function prints out timing stats if verbose.   

    Inputs:
    -------
    cat(str, Default: "atlas" [ATLAS-Refcat2])
        Name of cat (must be a source cat, saved in feather files)
        Currently supported:
            "atlas" (ATLAS-Refcat2, assumes files are kept in "cat_path/*/*/*.feather" formats)
    cat_path (str, Default: "/etc/rico/atlas_refcat2")
        Location of cat files with sources on your server
    cat_frac (float, Default: 0.001, so 0.1% of the catalog)
        Fraction of the sources to select from each file
        **** Please do not select more than 0.2% of a large catalog, as the data pulled from source files will be fully loaded in memory
    nfiles (int, Default: 0)
        For testing on small numbers of files, call the number of files you want to test on.
        If 0, it will pull data from all available files.
    verbose (bool, Default: False)
        Print out what's going on and some stats

    Returns:
    --------
    cat_samp (pandas DF with at least ra,dec columns)

    '''

    if cat=="atlas":
        cat_name="ATLAS-Refcat2"
        file_extension = "/*/*/*.feather"
    else: 
        print(f"Cat {cat_name} not supported, goodbye")
        sys.exit()

    # Get path/names of all files of cat
    files = glob.glob(cat_path+file_extension,recursive=True) # all data fles, which are in feather format
    tot_files = len(files) # total #files that cat is saved in
 
    # Are we testing on a smaller number of files?
    if nfiles!=0: # if so, choose only that fraction of files (random sampling)
        num_files = nfiles
        file_samp = random.sample(list(range(0,tot_files)),nfiles)
        files = np.array(files)[file_samp] 
    else: 
        num_files = tot_files
    if verbose: print(f"Pulling random sample of {cat_name}:\nSelecting {(cat_frac*100):.4f}% of rows from {num_files}/{tot_files} files")

    # Loop through files, read in data 
    dfs = [] # a list to fill with dataframes
    t1 = time.process_time() #timestamp before reading files & extracting data
    for fil in files:
        # for each file, select a sample of points 
        dat = pd.read_feather(fil) # read in the data
        ndet = len(dat) # total #rows
        ndet_samp = int(ndet*cat_frac) # number of sample sources to take (based on total #rows)
        samp_inds = random.sample(list(range(0,ndet)),ndet_samp) # get indices of random sample
        samp_dat = dat.iloc[samp_inds] # select the sample data for this file
        dfs.append(samp_dat) # add the data to the df list
    t2 = time.process_time() # timestamp after reading files & extracting data
 
    # Concatenate dataframes from files
    sample_dat = pd.concat(dfs) # full sample 
    t3 = time.process_time() # timestamp after concatting dataframes
 
    # Print out helpful stats for the user
    if verbose:
        print(f"    time to read files = {(t2-t1):.4f} sec")
        print(f"    time to concat dataframes = {(t3-t2):.4f} sec")
        print(f"    {len(sample_dat)} rows read from files")
    
    return sample_dat




if __name__=="__main__":
    
    # Imputs
    #--------

    # Define input arguments
    parser = ArgumentParser(description="Select a ''small'' (1-2M rows) random fraction of a catalog\nfor the purpose of importing to a HATS format with lsdb.from_dataframe()\n    *If you're here, you're probably testing!")
    parser.add_argument('--cat',type=str,nargs=1,default=["atlas"],help="Name of cat.  Currently supported:\n    ''atlas'' (ATLAS-Refcat2, Default)")
    parser.add_argument('--cat_path',type=str,nargs=1,default=["/etc/rico/atlas_refcat2/"],help="Full path to cat on your machine (Default: ''/etc/rico/atlas_refcat2'')")
    parser.add_argument('--cat_frac', type=float, nargs=1, default=[0.001],help="Fraction of cat to select (Default: 0.001)\n    WARNING: don't select more than like 0.2% or the catalog will be\n    too big for the lsdb.from_dataframe() function :(") 
    parser.add_argument('--test',action='store_true',help="Call to test for timing cat file-reading & concatenation at 10,100,500,1000 files")
    parser.add_argument('--verbose',action='store_true',help="Print out what's going on and some stats")
    args = parser.parse_args()

    # Are we verbose:
    verbosity = args.verbose
    if verbosity: 
        vrb = True
    else:
        vrb = False 

    # Is it a test?
    test = args.test
    if test: # if so, enforce verbosity
        print("\n -*#*- In testing mode...verbosity enforced! -*#*- \n")
        vrb = True
  
    # Get name of cat
    cat = args.cat[0]
    if cat=="atlas":
        cat_name = "ATLAS-Refcat2"
    else: 
        print(f"Cat {cat} is not supported, goodbye")
        sys.exit()
    if vrb: print(f"Reference catalog: {cat_name}")

    # Get path to cat
    cat_path = args.cat_path[0]

    # Get fraction of cat to select
    cat_frac = float(args.cat_frac[0])
    if cat_frac>0.002:
        print(f"This cat fraction ({cat_frac}) is simply too big, goodbye")
        sys.exit()


    # Get down to business
    # --------------------
    
    # Testing
    if test:
        cat_sample = get_cat_sample(cat,cat_path,cat_frac,10,vrb)    
        cat_sample = get_cat_sample(cat,cat_path,cat_frac,100,vrb)
        cat_sample = get_cat_sample(cat,cat_path,cat_frac,500,vrb)
        cat_sample = get_cat_sample(cat,cat_path,cat_frac,1000,vrb)

# Results of my testing:

# A test of .15% of ATLAS-Refcat2
#selecting samples from 10 files
#time to read files =  0.11876798000000033
#time to concat dataframes =  0.0004841209999995044
#selecting samples from 100 files
#time to read files =  2.3230688799999992
#time to concat dataframes =  0.003506717000000492
#selecting samples from 1000 files
#time to read files =  89.42158921699999
#time to concat dataframes =  0.02769331700000066

#Reading in 0.15% of ATLAS-Refcat2, selecting samples from 10 files
#    time to read files = 0.1126 sec
#    time to concat dataframes = 0.0004 sec
#    840 rows read from file
#Reading in 0.15% of ATLAS-Refcat2, selecting samples from 100 files
#    time to read files = 2.1295 sec
#    time to concat dataframes = 0.0029 sec
#    17777 rows read from file
#Reading in 0.15% of ATLAS-Refcat2, selecting samples from 500 files
#    time to read files = 31.7025 sec
#    time to concat dataframes = 0.0131 sec
#    258449 rows read from file
#Reading in 0.15% of ATLAS-Refcat2, selecting samples from 1000 files
#    time to read files = 78.1922 sec
#    time to concat dataframes = 0.0354 sec
#    627694 rows read from file


#Reading in 0.10% of ATLAS-Refcat2, selecting samples from 10 files
#    time to read files = 0.0982 sec
#    time to concat dataframes = 0.0005 sec
#    557 rows read from file
#Reading in 0.10% of ATLAS-Refcat2, selecting samples from 100 files
#    time to read files = 1.6943 sec
#    time to concat dataframes = 0.0026 sec
#    11829 rows read from file
#Reading in 0.10% of ATLAS-Refcat2, selecting samples from 500 files
#    time to read files = 27.8976 sec
#    time to concat dataframes = 0.0106 sec
#    172206 rows read from file
#Reading in 0.10% of ATLAS-Refcat2, selecting samples from 1000 files
#    time to read files = 68.6014 sec
#    time to concat dataframes = 0.0267 sec
#    418290 rows read from file


#Reference catalog: ATLAS-Refcat2
#Pulling random sample of ATLAS-Refcat2:
#selecting 0.10% of rows from 10/3072 files
#    time to read files = 0.4234 sec
#    time to concat dataframes = 0.0005 sec
#    1623 rows read from file
#Pulling random sample of ATLAS-Refcat2:
#selecting 0.10% of rows from 100/3072 files
#    time to read files = 8.2316 sec
#    time to concat dataframes = 0.0027 sec
#    33287 rows read from file
#Pulling random sample of ATLAS-Refcat2:
#selecting 0.10% of rows from 500/3072 files
#    time to read files = 35.0523 sec
#    time to concat dataframes = 0.0158 sec
#    137074 rows read from file
#Pulling random sample of ATLAS-Refcat2:
#selecting 0.10% of rows from 1000/3072 files
#    time to read files = 80.2771 sec
#    time to concat dataframes = 0.0264 sec
#    302412 rows read from file


#Reference catalog: ATLAS-Refcat2
#Pulling random sample of ATLAS-Refcat2:
#Selecting 0.15% of rows from 10/3072 files
#    time to read files = 0.4658 sec
#    time to concat dataframes = 0.0006 sec
#    3604 rows read from file
#Pulling random sample of ATLAS-Refcat2:
#Selecting 0.15% of rows from 100/3072 files
#    time to read files = 3.6899 sec
#    time to concat dataframes = 0.0027 sec
#    37474 rows read from file
#Pulling random sample of ATLAS-Refcat2:
#Selecting 0.15% of rows from 500/3072 files
#    time to read files = 30.1051 sec
#    time to concat dataframes = 0.0151 sec
#    239827 rows read from file
#Pulling random sample of ATLAS-Refcat2:
#Selecting 0.15% of rows from 1000/3072 files
#    time to read files = 65.3235 sec
#    time to concat dataframes = 0.0242 sec
#    461083 rows read from file