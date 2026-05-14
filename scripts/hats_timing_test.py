'''
Testing the amount of time to open catalogs with LSDB. 

Purpose: 
    - To answer the question: "should/can we use LSDB and HATS to quickly access a reference catalog of sources for fast calibration in Argus HDPS?"
    - In other words, "Does it take longer to retrieve sources from a HATS-ed source catalog with LSDB than it does to read them in from feather files (the original method)?"

This script times how long it takes to retrieve sources within a cone search of various radii in crowded/sparse fields, and lazily load them.  
'''

import lsdb
import pandas as pd
import time
import timeit

def test_cone(cat_path,sra,sdec,radi):
    cat = lsdb.open_catalog(cat_path,search_filter=lsdb.ConeSearch(ra=sra, dec=sdec, radius_arcsec=(3600*radi)))


if __name__=="__main__":

    print(f"Testing on HATS-ed catalog: 1%% of ATLAS-Refcat2")

    atlas_path = "/net/scratch/kmfas/atlas_refcat2/atlas_0_01/"
    #t1 = time.process_time()
    #cat_3 = lsdb.open_catalog(atlas_path,search_filter=lsdb.ConeSearch(ra=40, dec=20, radius_arcsec=(3600*3)))
    #t2 = time.process_time()
    #cat_1 = lsdb.open_catalog(atlas_path,search_filter=lsdb.ConeSearch(ra=40, dec=20, radius_arcsec=(3600*1)))
    #t3 = time.process_time()
    #cat_05 = lsdb.open_catalog(atlas_path,search_filter=lsdb.ConeSearch(ra=40, dec=20, radius_arcsec=(3600*0.5)))
    #t4 = time.process_time()

    print(f"\nSparse field: Opening catalog with a cone search around RA=40deg, Dec=20deg....")

    loop = 100
    sra = 40
    sdec = 20
   
    radi = 3
    result = timeit.timeit('test_cone(atlas_path,sra,sdec,radi)',globals=globals(),number=loop)
    print(f"For 3deg radius, {result/loop:.4f} sec")

    radi = 1
    result = timeit.timeit('test_cone(atlas_path,sra,sdec,radi)',globals=globals(),number=loop)
    print(f"For 1deg radius, {result/loop:.4f} sec")

    radi = 0.5
    result = timeit.timeit('test_cone(atlas_path,sra,sdec,radi)',globals=globals(),number=loop)
    print(f"For 0.5deg radius, {result/loop:.4f} sec")

    #print(f"{(t2-t1):.4f} sec for 3deg radius")
    #print(f"{(t3-t2):.4f} sec for 1deg radius")
    #print(f"{(t4-t3):.4f} sec for 0.5deg radius")

    #t1 = time.process_time()
    #cat_3 = lsdb.open_catalog(atlas_path,search_filter=lsdb.ConeSearch(ra=75, dec=40, radius_arcsec=(3600*3)))
    #t2 = time.process_time()
    #cat_1 = lsdb.open_catalog(atlas_path,search_filter=lsdb.ConeSearch(ra=75, dec=40, radius_arcsec=(3600*1)))
    #t3 = time.process_time()
    #cat_05 = lsdb.open_catalog(atlas_path,search_filter=lsdb.ConeSearch(ra=75, dec=40, radius_arcsec=(3600*0.5)))
    #t4 = time.process_time()

    print(f"\nCrowded field: Opening catalog with a cone search around RA=75deg, Dec=40deg....")
    #print(f"{(t2-t1):.4f} sec for 3deg radius")
    #print(f"{(t3-t2):.4f} sec for 1deg radius")
    #print(f"{(t4-t3):.4f} sec for 0.5deg radius")

    loop = 100
    sra = 75
    sdec = 40
   
    radi = 3
    result = timeit.timeit('test_cone(atlas_path,sra,sdec,radi)',globals=globals(),number=loop)
    print(f"For 3deg radius, {result/loop:.4f} sec")

    radi = 1
    result = timeit.timeit('test_cone(atlas_path,sra,sdec,radi)',globals=globals(),number=loop)
    print(f"For 1deg radius, {result/loop:.4f} sec")

    radi = 0.5
    result = timeit.timeit('test_cone(atlas_path,sra,sdec,radi)',globals=globals(),number=loop)
    print(f"For 0.5deg radius, {result/loop:.4f} sec")