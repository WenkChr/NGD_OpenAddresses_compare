import os, sys, arcpy
import pandas as pd
import numpy as np
from arcgis import GeoAccessor
arcpy.env.overwriteOutput = True

#----------------------------------------------------------------------------------------------------------
#Functions
def RangeCompareMatcher(AF_VAL, AT_VAL, OA_MAX, OA_MIN, side): 
    # Compares the OA range and the NGD range in matches and return a set class value for easy comparison    
    vals =[AF_VAL, AT_VAL, OA_MIN, OA_MAX]
    if AF_VAL > AT_VAL:
        # Then AF pairs with min and AT pairs with max
        vals = [AT_VAL, AF_VAL, OA_MIN, OA_MAX]
    NGD_Range = int(vals[0]) - int(vals[1])
    OA_Range = int(vals[2]) - int(vals[3])
    if vals[0] == OA_MIN and vals[1] == OA_MAX:
        return 'EQUAL'
    if vals[0] > OA_MIN and vals[1] < OA_MAX:
        return 'NGD RANGE INSIDE SBgR RANGE'
    if OA_MIN > vals[0] and OA_MAX < vals[1]:
        return 'SBgR RANGE INSIDE NGD RANGE'
    if (OA_MIN == vals[0] and OA_MAX != vals[1]) or (OA_MIN != vals[0] and OA_MAX == vals[1]):
        return 'ONE VALUE MATCH'
    else: return 'RANGES OFFSET'

#----------------------------------------------------------------------------------------------------------
#Inputs
workingGDB = r'H:\NGD_OpenAddresses_compare\Working.gdb'
SBgR_ranges = r'H:\NGD_OpenAddresses_compare\vancouver_min_max_sbgr.csv'
NGD_AL = os.path.join(workingGDB, 'NGD_AL_clipped')

#-----------------------------------------------------------------------------------------------------------
#Logic

#Load in SBgR Ranges from excel
SBgR_df = pd.read_csv(SBgR_ranges)

#Create complete street name field (CSN)
SBgR_df['CSN'] = SBgR_df.apply(lambda x: '%s %s %s' % (x.ST_NAME, x.ST_TYPE, x.ST_DIRECTION), axis=1)
SBgR_df['CSN'] = SBgR_df['CSN'].str.replace('nan', '')
SBgR_df['CSN'] = SBgR_df['CSN'].str.upper()
SBgR_df['CSN'] = SBgR_df['CSN'].str.lstrip()
SBgR_df['ST_NAME'] = SBgR_df['ST_NAME'].str.upper()
#Loop over sides to get ranges
for side in ['L', 'R']:
    # Side outputs
    out_df_columns = ['NGD_UID','BF_UID', 'MIN', 'MAX', 'SBgR_Street_Name', f'NGD_STR_ID_{side}', f'AF{side}_VAL', f'AF{side}_SRC', f'AT{side}_VAL', f'AT{side}_SRC', f'STR_LABEL_NME_{side}', 'Match_Type']
    out_df_rows = []
    rejects_rows = []
    # NGD_AL DF setup and selection vars
    al_fields = ['NGD_UID', f'BF_UID_{side}', f'AF{side}_VAL', f'AT{side}_VAL', f'AF{side}_SRC', f'AT{side}_SRC', f'NGD_STR_UID_{side}', 'STR_LABEL_NME', 'STR_NME', 'STR_TYP']
    where_clause = f"AF{side}_SRC = 'GISI' Or AF{side}_SRC = 'DRA' Or AT{side}_SRC = 'GISI' Or AT{side}_SRC = 'DRA'"
    NGD_AL_df = pd.DataFrame.spatial.from_featureclass(NGD_AL, fields= al_fields, where_clause= where_clause)
    NGD_AL_df['STR_LABEL_NME'] = NGD_AL_df['STR_LABEL_NME'].str.upper()
    NGD_AL_df['STR_NME'] = NGD_AL_df['STR_NME'].str.upper()
    print(f'Length of {side}: {len(NGD_AL_df)}')
    # Loop over the NGD_AL and find matches with BB ranges
    print(f'Comparing ranges on {side} side to the NGD_AL')
    for Nrow in NGD_AL_df.itertuples():
        # compare df's on the street name. If this returns a match add to the out df
        bfuid = NGD_AL_df.iloc[Nrow.Index][f'BF_UID_{side}']
        bfuid = int(bfuid)
        # Partial Street Match check
        test = SBgR_df.loc[(SBgR_df['BF_SDI_BF_UID'] == bfuid)]
        UID_ranges_df = SBgR_df.loc[(SBgR_df['BF_SDI_BF_UID'] == bfuid) & (SBgR_df['ST_NAME'] == Nrow.STR_NME)]
        if len(UID_ranges_df) > 0:
            row = [Nrow.NGD_UID, #NGD_UID
                        bfuid, #BF_UID
                        UID_ranges_df.iloc[0]['MIN'], # MIN        
                        UID_ranges_df.iloc[0]['MAX'], # MAX
                        UID_ranges_df.iloc[0]['ST_NAME'], # OA_Street_Name
                        NGD_AL_df.iloc[Nrow.Index][f'NGD_STR_UID_{side}'], # NGD_STR_UID
                        NGD_AL_df.iloc[Nrow.Index][f'AF{side}_VAL'], # AF VAL
                        NGD_AL_df.iloc[Nrow.Index][f'AF{side}_SRC'], # AF SRC
                        NGD_AL_df.iloc[Nrow.Index][f'AT{side}_VAL'], # AT Val
                        NGD_AL_df.iloc[Nrow.Index][f'AT{side}_SRC'], # AT SRC
                        Nrow.STR_LABEL_NME, # STR_LABEL_NME 
                        'MATCH'] # Street name match type 
            out_df_rows.append(row)
            continue
        # if you get to this point then these records don't have a match via the current methods
        UID_ranges_df = SBgR_df.loc[(SBgR_df['BF_SDI_BF_UID'] == bfuid)]
        if len(UID_ranges_df) > 0:
            for r in UID_ranges_df.itertuples(): 
                row = [NGD_AL_df.iloc[Nrow.Index][f'BF_UID_{side}'], #NGD_UID
                            bfuid, #BF_UID        
                            r.MIN, # MIN
                            r.MAX, # MAX
                            r.ST_NAME, # OA_Street_Name
                            NGD_AL_df.iloc[Nrow.Index][f'NGD_STR_UID_{side}'], # NGD_STR_UID
                            NGD_AL_df.iloc[Nrow.Index][f'AF{side}_VAL'], # AF VAL
                            NGD_AL_df.iloc[Nrow.Index][f'AF{side}_SRC'], # AF SRC
                            NGD_AL_df.iloc[Nrow.Index][f'AT{side}_VAL'], # AT Val
                            NGD_AL_df.iloc[Nrow.Index][f'AT{side}_SRC'], # AT SRC
                            Nrow.STR_LABEL_NME, # STR_LABEL_NME 
                            ] 
            rejects_rows.append(row)
    print(f"Final output count for {side}: Matches- {len(out_df_rows)} Rejects- {len(rejects_rows)}")
    out_df = pd.DataFrame(out_df_rows, columns= out_df_columns)
    reject_df = pd.DataFrame(rejects_rows,  columns= out_df_columns[:len(out_df_columns)-1])
    print(f"Number of rows to manually recheck: {len(reject_df['NGD_UID'].unique().tolist())}")
    out_df[f'AF{side}_VAL'] = out_df[f'AF{side}_VAL'].fillna(0)
    out_df[f'AT{side}_VAL'] = out_df[f'AT{side}_VAL'].fillna(0)
    # Calculate match type var
    out_df['Range_Match_Type'] =  out_df.apply(lambda row: RangeCompareMatcher(row[f'AF{side}_VAL'], row[f'AT{side}_VAL'], row['MAX'], row['MIN'], side), axis=1)
    out_df.to_csv(os.path.join(r'H:\NGD_OpenAddresses_compare', f'testSBgR_{side}.csv'), index= False)
    reject_df.to_csv(os.path.join(r'H:\NGD_OpenAddresses_compare', f'testSBgR_{side}_rejects.csv'), index= False)

print('DONE!')
