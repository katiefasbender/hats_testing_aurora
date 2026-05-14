from argparse import ArgumentParser
import glob
import numpy as np
import lsdb 
import pandas as pd
import random
import os
import sys 
import time


def feathers_to_csv(cat="atlas",cat_path="/etc/rico/atlas_refcat2"):

    '''
    Selects a 

    Assumes ATLAS files are saved in feather format

    Inputs:
    -------
    cat(str, Default: "atlas" [ATLAS-Refcat2])
        Name of cat (must be a source cat, saved in feather files)
        Currently supported:
            "atlas" (ATLAS-Refcat2, assumes files are kept in "cat_path/*/*/*.feather" formats)
    cat_path (str, Default: "/etc/rico/atlas_refcat2")
        Location of cat on your server
    cat_frac (float, Default: 0.001, so 0.1% of the catalog)
        Fraction of the entire catalog to select
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
 
 
    # Loop through files, read in data 
    dfs = [] # a list to fill with dataframes
    t1 = time.process_time() #timestamp before reading files & extracting data
    for fil in files:
        # for each file, select a sample of points 
        dat = pd.read_feather(fil) # read in the data
        fs = fil.split("/")
        #os.mkdir("/net/scratch/kmfas/atlas_refcat2/atlas_refcat2_csv/"+fs[-3])
        #os.mkdir("/net/scratch/kmfas/atlas_refcat2/atlas_refcat2_csv/"+fs[-3]+"/"+fs[-2])
        outpath = "/net/scratch/kmfas/atlas_refcat2/atlas_refcat2_csv/"+fil.split("/")[-3]+"/part-"+fil.split("/")[-2]+".csv"
        #print(outpath)
        dat.to_csv(outpath+"part-0.csv")
    t2 = time.process_time() # timestamp after reading files & extracting data
 
    # Concatenate dataframes from files
    #sample_dat = pd.concat(dfs) # full sample 
    #t3 = time.process_time() # timestamp after concatting dataframes
 
    # Print out helpful stats for the user
    #if verbose:
    #    print(f"    time to read files = {(t2-t1):.4f} sec")
    #    print(f"    time to concat dataframes = {(t3-t2):.4f} sec")
    #    print(f"    {len(sample_dat)} rows read from files")
    
    #return sample_dat