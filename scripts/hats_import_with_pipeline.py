'''
Import a catalog (or a fraction of a catalog) with the hats-import pipeline and a Dask Client.


'''


from argparse import ArgumentParser
import glob
from hats_import.catalog.arguments import ImportArguments
from hats_import.pipeline import pipeline
import healpy as hp
import numpy as np
import os
import pandas as pd
import sys
import time


from hats.io import file_io
from hats.io.file_io.file_pointer import get_upath

import pandas as pd
import pyarrow as pa

from pathlib import Path
from upath import UPath

from collections.abc import Generator
from hats_import.catalog.file_readers.input_reader import InputReader




class FeatherReader(InputReader):
    """Feather reader for the most common Feather reading arguments.

    This uses `pandas.read_feather`

    Attributes:
        header (int, list of int, None, default 'infer'): rows to
            use as the header with column names
        schema_file (str): path to a parquet schema file. if provided, header names
            and column types will be pulled from the parquet schema metadata.
        column_names (list[str]): the names of columns if no header is available
        type_map (dict): the data types to use for columns
        parquet_kwargs (dict): additional keyword arguments to use when
            reading the parquet schema metadata, passed to pandas.read_parquet.
            See https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html
        kwargs (dict): additional keyword arguments to use when reading
            the feather files with pandas.read_feather.
    """

    def __init__(
        self,
        chunksize=500_000,
        header="infer",
        schema_file=None,
        column_names=None,
        type_map=None,
        parquet_kwargs=None,
        upath_kwargs=None,
        **kwargs,
    ):
        self.chunksize = chunksize
        self.header = header
        self.schema_file = schema_file
        self.column_names = column_names
        self.type_map = type_map
        self.parquet_kwargs = parquet_kwargs
        self.upath_kwargs = upath_kwargs
        self.kwargs = kwargs

        schema_parquet = None
        if self.schema_file:
            if self.parquet_kwargs is None:
                self.parquet_kwargs = {}
            schema_parquet = file_io.read_parquet_file_to_pandas(
                self.schema_file,
                **self.parquet_kwargs,
            )

        if self.column_names:
            self.kwargs["names"] = self.column_names
        elif not self.header and schema_parquet is not None:
            self.kwargs["names"] = list(schema_parquet.columns)

        if self.type_map:
            self.kwargs["dtype"] = self.type_map
        elif schema_parquet is not None:
            self.kwargs["dtype"] = schema_parquet.dtypes.to_dict()

    def read(self, input_file,read_columns=None) -> Generator[pd.DataFrame]:
        f_file = pd.read_feather(input_file)
        for sub_file in [f_file[i:i+self.chunksize] for i in range(0,f_file.shape[0],self.chunksize)]:
            yield sub_file



def import_pipeline(lst,cat_outpath,cat_outname,filereader,lo_hporder=2,hi_hporder=10,pix_thresh=5000,n_dask=4):
    '''
    Import a catalog to HATS format with hats-import pipeline()
    
    Arguments
    ---------
        lst: (str list) list of files with cat data in native non-hatsed format; expects "ra" and "dec" columns
        cat_outpath: (str) Full path to directory where HATSed cat will be written
        cat_outname: (str) Name of HATSed cat
        filereader: (FileReader Object) File Reader to pull in native-format cat data (i.e. FeatherReader)
        lo_hporder: (int, Default = 2) Lowest-order HPix for tiling
        hi_hporder: (int, Default = 10) Highest-order HPix for tiling
        pix_thresh: (int, Default = 5000) Max #sources per tile
        n_dask: (int, Default = 4) # dask workers for Client

    Returns
    --------
        Nothing, HATSed cat is written to cat_outpath/cat_outname
    '''
    args = ImportArguments(
        ra_column="ra",
        dec_column="dec",
        input_file_list=lst,
        file_reader=filereader,
        output_artifact_name=cat_outname,
        output_path=cat_outpath,
        lowest_healpix_order=lo_hporder,
        highest_healpix_order=hi_hporder,
        pixel_threshold=pix_thresh,
        dask_n_workers=n_dask,
        )
    pipeline(args)



if __name__=="__main__":


    # Imputs
    #--------

    # Define input arguments
    parser = ArgumentParser(description="Import a catalog (cat) to HATS format with the hats-import pipeline.")
    parser.add_argument('--cat',type=str,nargs=1,default=["atlas"],help="Name of cat.  Currently supported:\n    ''atlas'' (ATLAS-Refcat2, Default)")
    parser.add_argument("--cat_native_files",type=str,nargs=1,default=["*/*/*.feather"],help="Cat native format file locations/extention, Default = ''*/*/*.feather''")
    parser.add_argument('--cat_outpath',type=str,nargs=1,default=["atlas_refcat2/"],help="Path to directory in /net/scratch/kmfas/ where HATS-ed cat will be written (Default: ''atlas_refcat2/'')")
    parser.add_argument('--cat_outname',type=str,nargs=1,default=["atlas_hatsed"],help="Name of HATS-ed cat; it will be written to /net/scratch/kmfas/<cat_outpath>/<cat_outname>, will not overwrite!!! (Default: ''atlas_hatsed'')")
    parser.add_argument('--nfiles', type=float, nargs=1, default=[0],help="Number of files to import (For testing purposes).  Default: 0 (meaning import all files)")
    args = parser.parse_args()

    # Get inputs

    # cat name
    cat = args.cat[0]
    if cat=="atlas":
        cat_name = "ATLAS-Refcat2"
        cat_path = "/etc/rico/atlas_refcat2/" # where cat is stored in native format 
    else:
        print(f"So sorry, cat {cat} is not supported.  Good bye.")
        sys.exit()

    # cat native files (Native file format in which un-HATS-ed cat is stored)
    cat_native_files = args.cat_native_files[0]

    # cat outpath (where  hatsed cat will go)
    cat_outpath = "/net/scratch/kmfas/" + args.cat_outpath[0]

    # cat outname (name of HATASed cat)
    cat_outname = args.cat_outname[0]

    # number of files (for testing; if 0, import from all files)
    nfiles = int(args.nfiles[0])
    if nfiles<1000 and nfiles>0:
        lo_hp = 2
        hi_hp = 10
        pix_thrsh = 5000
        n_dask = 4
    elif nfiles>=1000:
        lo_hp = 2
        hi_hp = 12
        pix_thrsh = 5000
        n_dask = 6
    elif nfiles==0:
        lo_hp = 2
        hi_hp = 12
        pix_thrsh = 5000
        n_dask = 6


    # Get down to business
    # --------------------

    # Get a list of cat files
    lst = glob.glob(cat_path+cat_native_files,recursive=True)
    ntot = len(lst)
    if nfiles!=0: lst = lst[:nfiles]

    # Import the catalog 
    print(f"Importing catalog {cat_name} to HATS format....")
    if nfiles!=0: print(f"...only from {nfiles}/{ntot} files in {cat_path}")
    else: print(f"...from all {ntot} files in {cat_path}")
    print(f"HATSed catalog will be written to {cat_outpath+cat_outname}")
    print("---------------------------------------")
    import_pipeline(lst,cat_outpath,cat_outname,FeatherReader(),lo_hp,hi_hp,pix_thrsh,n_dask)
    print("---------------------------------------")
    print("Catalog imported?")


# Timing results:

# from feather files

# 1000 files ------------------------------------------------------------


#Importing catalog ATLAS-Refcat2 to HATS format....
#...only from 1000/3072 files in /etc/rico/atlas_refcat2/
#HATSed catalog will be written to /net/scratch/kmfas/atlas_refcat2/atlas_1000files
#---------------------------------------
#Catalog: Planning  : 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 493.91it/s]
#Catalog: Mapping   : 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1000/1000 [05:01<00:00,  3.32it/s]
#Catalog: Binning   : 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [11:44<00:00, 352.20s/it]
# Failed b/c dask client kept getting dropped


# 100 files ------------------------------------------------------------

# 100 files, 4 dask workers:
#Catalog: Planning  : 100%|██████████| 4/4 [00:00<00:00, 748.08it/s]
#Catalog: Mapping   : 100%|██████████| 100/100 [00:04<00:00, 24.08it/s]
#Catalog: Binning   : 100%|██████████| 2/2 [00:39<00:00, 19.75s/it]
#Catalog: Splitting : 100%|██████████| 100/100 [00:26<00:00,  3.78it/s]
#Catalog: Reducing  : 100%|██████████| 4936/4936 [00:49<00:00, 99.42it/s] 
#Catalog: Finishing : 100%|██████████| 6/6 [00:04<00:00,  1.35it/s]

# 100 files, 4 dask workers:
#Catalog: Planning  : 100%|██████████| 4/4 [00:00<00:00, 484.30it/s]
#Catalog: Mapping   : 100%|██████████| 100/100 [00:03<00:00, 25.25it/s]
#Catalog: Binning   : 100%|██████████| 2/2 [00:42<00:00, 21.14s/it]
#Catalog: Splitting : 100%|██████████| 100/100 [00:33<00:00,  3.03it/s]
#Catalog: Reducing  : 100%|██████████| 4936/4936 [00:49<00:00, 99.00it/s] 
#Catalog: Finishing : 100%|██████████| 6/6 [00:07<00:00,  1.17s/it]

#Importing catalog....
#...only from 100 files out of 3072
#---------------------------------------
#Catalog: Planning  : 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 150.90it/s]
#Catalog: Mapping   : 100%|███████████████████████████████████████████████████████████████████████████████████████████████████| 100/100 [00:02<00:00, 35.06it/s]
#Catalog: Binning   : 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:39<00:00, 19.76s/it]
#Catalog: Splitting : 100%|███████████████████████████████████████████████████████████████████████████████████████████████████| 100/100 [00:38<00:00,  2.62it/s]
#Catalog: Reducing  : 100%|█████████████████████████████████████████████████████████████████████████████████████████████████| 4936/4936 [01:08<00:00, 71.91it/s]
#Catalog: Finishing : 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████| 6/6 [00:06<00:00,  1.04s/it]
#---------------------------------------
#Catalog imported?

#Importing catalog....
#...only from 100 files out of 3072 in /etc/rico/atlas_refcat2/
#HATSed catalog will be written to /net/scratch/kmfas/atlas_refcat2/atlas_hatsed
#---------------------------------------
#Catalog: Planning  : 100%|██████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 637.53it/s]
#Catalog: Mapping   : 100%|███████████████████████████████████████████████████████████████████████████| 100/100 [00:03<00:00, 33.20it/s]
#Catalog: Binning   : 100%|███████████████████████████████████████████████████████████████████████████████| 2/2 [00:39<00:00, 19.60s/it]
#Catalog: Splitting : 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 100/100 [00:31<00:00,  3.18it/s]
#Catalog: Reducing  : 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 4936/4936 [01:03<00:00, 77.61it/s]
#Catalog: Finishing : 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 6/6 [00:05<00:00,  1.03it/s]
#---------------------------------------
#Catalog imported?

# from CSV files

# 0.001 fraction of atlas (1 dask worker)
#Catalog: Planning  : 100%|████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 661.01it/s]
#Catalog: Mapping   : 100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:01<00:00,  1.07s/it]
#Catalog: Binning   : 100%|█████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:44<00:00, 22.41s/it]
#Catalog: Splitting : 100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:12<00:00, 12.67s/it]
#Catalog: Reducing  : 100%|███████████████████████████████████████████████████████████████████████████████| 2181/2181 [01:18<00:00, 27.76it/s]
#Catalog: Finishing : 100%|█████████████████████████████████████████████████████████████████████████████████████| 6/6 [00:02<00:00,  2.09it/s]
#51.392803512  seconds to import 0.001

# 0.01 fraction of atlas (1 dask worker)
#Catalog: Planning  : 100%|████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 736.94it/s]
#Catalog: Mapping   : 100%|█████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:08<00:00,  8.19s/it]
#Catalog: Binning   : 100%|█████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:49<00:00, 24.74s/it]
#Catalog: Splitting : 100%|████████████████████████████████████████████████████████████████████████████████████| 1/1 [01:59<00:00, 119.43s/it]
#Catalog: Reducing  : 100%|█████████████████████████████████████████████████████████████████████████████| 21693/21693 [12:14<00:00, 29.52it/s]
#Catalog: Finishing : 100%|█████████████████████████████████████████████████████████████████████████████████████| 6/6 [00:44<00:00,  7.46s/it]
#150.899238338  seconds to import 0.01


# 0.01` with 4 dask workers`
#Catalog: Planning  : 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 595.23it/s]
#Catalog: Mapping   : 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:08<00:00,  8.25s/it]
#Catalog: Binning   : 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:47<00:00, 23.94s/it]
#Catalog: Splitting : 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [02:00<00:00, 120.32s/it]
#Catalog: Reducing  : 100%|███████████████████████████████████████████████████████████████████████████████████████████████| 21693/21693 [03:46<00:00, 95.74it/s]
#Catalog: Finishing : 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████| 6/6 [00:44<00:00,  7.40s/it]
#145.84248328500001  seconds to import 0.01


#Catalog: Planning  : 100%|████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 829.65it/s]
#Catalog: Binning   : 100%|█████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:38<00:00, 19.43s/it]
#Catalog: Splitting : 100%|█████████████████████████████████████████████████████████████████████████████████████████████| 333/333 [04:37<00:00,  1.20it/s]
#Catalog: Reducing  : 100%|█████████████████████████████████████████████████████████████████████████████████████████| 26586/26586 [07:46<00:00, 56.95it/s]
#Catalog: Finishing : 100%|█████████████████████████████████████████████████████████████████████████████████████████████████| 6/6 [00:55<00:00,  9.23s/it]
#177.52023015199998  seconds to import failed csv conversion batch, 4 dask workers
