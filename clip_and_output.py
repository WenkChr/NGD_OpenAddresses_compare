import os, sys, arcpy
import pandas as pd
import numpy as np
from arcgis import GeoAccessor
from pandas.core.reshape.merge import merge

arcpy.env.overwriteOutput = True

#------------------------------------------------------------------------------------------------------

def csvJoiner(NGD_AL, ranges_csv, side, outPath, joinField='NGD_UID'):
    # Take in NGD_AL and output ranges csv from other processes and merge them while keeping only key fields
    # Output the join and then create the 

    #Check side and determine side not in use
    offside = 'R'
    if side == 'R': offside= 'L'    
    
    print(f'Creating output for side: {side}')
    #Load in ranges_csv
    print('Reading in address ranges')
    ranges_df = pd.read_csv(ranges_csv)
    print(f'{len(ranges_df)} address ranges uploaded')
    print('Building where clause for NGD_AL load in.')
    where = f"NGD_UID IN {str(tuple(ranges_df.NGD_UID.tolist()))}" #Create whereclause from the unique values in the NGD_UID field

    #Load in NGD_AL and remove fields for side not in use
    print('Loading in NGD_AL')
    NGD_AL_sdf = pd.DataFrame.spatial.from_featureclass(NGD_AL, where_clause= where) # Where clause limits the load of the NGD_AL
    NGD_AL_cols = NGD_AL_sdf.columns.to_list()
    print('Removing offside columns')
    NGD_AL_cols = [c for c in NGD_AL_cols if not c[-2:] == f'_{offside}'] # Remove offside columns
    NGD_AL_sdf = NGD_AL_sdf.loc[:, ['NGD_UID', 'SHAPE']]

    #Join NGD_AL and the ranges_csv and export
    print('Joining and exporting final df')
    joined_df = NGD_AL_sdf.merge(ranges_df, on=joinField)
    joined_df.spatial.to_featureclass(location= outPath)


#-------------------------------------------------------------------------------------------------------
#Inputs

workingPath = r'H:\NGD_OpenAddresses_compare'
workingGDB = r'H:\NGD_OpenAddresses_compare\Working.gdb'
SBgR_RANGES_BASE = r'H:\NGD_OpenAddresses_compare\vancouver_min_max_sbgr.csv'
NGD_AL = r'H:\NGD_AGOL_Download\Final_Export_2020-09-28_2.gdb\NGD_AL'
outGDB = r'H:\NGD_OpenAddresses_compare\SBgR_range_matching.gdb'
#-------------------------------------------------------------------------------------------------------
#Logic

csvJoiner(NGD_AL, f'H:\\NGD_OpenAddresses_compare\\Merged_OA_SBgR_L.csv', 'L', os.path.join(outGDB, 'NGD_AL_SBgR_L'))
csvJoiner(NGD_AL, f'H:\\NGD_OpenAddresses_compare\\Merged_OA_SBgR_R.csv', 'R', os.path.join(outGDB, 'NGD_AL_SBgR_R'))
print('DONE!')
