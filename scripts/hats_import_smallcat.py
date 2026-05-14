'''
Import a small "toy" source catalog to HATS format (for testing out the method) with lsdb.from_dataframe()

'''

from argparse import ArgumentParser

import lsdb
import numpy as np
import pandas as pd
import os
import sys
import time

import select_cat_sample as selcat # python script (in this directory!!!!!) with a function to select a small fraction of your source catalog

if __name__=="__main__":

    # Imputs
    #--------

    # Define input arguments
    parser = ArgumentParser(description="Select a ''small'' (1-2M rows) random fraction of a catalog\nand import to a HATS format with lsdb.from_dataframe()")
    parser.add_argument('--cat',type=str,nargs=1,default=["atlas"],help="Name of cat.  Currently supported:\n    ''atlas'' (ATLAS-Refcat2, Default)")
    parser.add_argument('--cat_path',type=str,nargs=1,default=["/etc/rico/atlas_refcat2/"],help="Full path to cat on your machine (Default: ''/etc/rico/atlas_refcat2'')")
    parser.add_argument('--cat_frac', type=float, nargs=1, default=[0.001],help="Fraction of cat to select (Default: 0.001)\n    WARNING: don't select more than like 0.2%% or the catalog will be\n    #toobig for the lsdb.from_dataframe() function :(") 
    parser.add_argument('--use_cat', type=str,nargs=1,default=['no'],help="If you want to used a cat you have already saved as a single dataframe in .csv format.  Include full path to cat, e.g. ''/home/you/cat.csv''")
    parser.add_argument('--verbose',action='store_true',help="Print out what's going on and some stats")
    args = parser.parse_args()

    # Get inputs
    #------------

    # cat name
    cat = args.cat[0]
    if cat=="atlas":
        cat_name="ATLAS-Refcat2"
        cat_out_dir = "/net/scratch/kmfas/atlas_refcat2"
    else:
        print(f"So sorry, cat ''{cat}'' is not supported.  Goodbye")
        sys.exit()

    # cat path
    cat_in_dir = args.cat_path[0]

    # cat fraction to sample
    cat_frac = args.cat_frac[0]
    samp_name = "sample_"+str(cat_frac)[0]+"_"+str(cat_frac)[2:] # format example: sample_0_01 for 0.01 cat_frac value

    # use a pre-saved cat?
    use_cat = args.use_cat[0]

    # are we verbose?
    verbosity = args.verbose
    if verbosity:
        verb = True
    else: 
        verb = False

    if verb: print(f"Welcome to ''HATS-ing 1-2M rows of a cat of\nyour choice using lsdb.from_dataframe()''!\n---------------------------------------------")


    # Get down to business
    #---------------------

    # Get the sample cat
    if use_cat=="no":
        #if verb: print(f"Selecting {(100*cat_frac):.4f} of {cat_name}....")
        cat_sample = selcat.get_cat_sample(cat_frac=cat_frac,verbose=verb)
        #print(f"length of this 0.15% of ATLAS-Refcat2 = {len(cat_sample)}")
        cat_csv_path = cat_out_dir+"/sample_"+str(cat_frac)[0]+"_"+str(cat_frac)[2:]+".csv"
        if verb: print(f"-\nWriting sample cat to {cat_csv_path}...")
        t1 = time.process_time()
        cat_sample.to_csv(cat_csv_path,index=True) # save catalog's csv form :)
        t2 = time.process_time()
        if verb: print(f"    time to write sample catalog to csv = {(t2-t1):.4f} sec")
    else:
        if verb: print(f"Using cat in {use_cat}")
        cat_sample = pd.read_csv(use_cat)


    # Import the cat! (HATS it)

    # use lsdb.from_dataframe() method:
    #in the from_dataframe() function, you can call the cat as pd.read_csv(cat_csv_path)
    if verb: print(f"-\nImporting {cat_name} to HATS with lsdb.from_dataframe().....")
    t3 = time.process_time()
    cat_hatsed = lsdb.from_dataframe(cat_sample,lowest_order=2,highest_order=10,partition_rows=1000)
    t4 = time.process_time()
    if verb: print(f"    time to import catalog = {(t4-t3):.4f} sec")

    # Write HATSed catalog
    cat_out_path = cat_out_dir+"/hatsed/"+samp_name+"/"
    cat_name = "from_dataframe"
    if verb: print(f"-\nWriting HATSed catalog (name of ''{cat_name}'') to {cat_out_path}")
    t5 = time.process_time()
    cat_hatsed.write_catalog(cat_out_path,catalog_name=cat_name,overwrite=True)
    t6 = time.process_time()
    if verb: print(f"    time to write hatsed catalog = {(t6-t5):.4f} sec")


        


