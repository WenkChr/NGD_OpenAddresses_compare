import os, sys, arcpy
import pandas as pd
import numpy as np
from arcgis import GeoAccessor

arcpy.env.overwriteOutput = True
#---------------------------------------------------------------------------
# Inputs

workingGDB = r'H:\NGD_A_Complete_Ranges\Working.gdb'
csd_fc = os.path.join(workingGDB, 'testAreaCSD')
NGD_AL = os.path.join(workingGDB, 'NGD_AL_clipped')
bc_points = os.path.join(workingGDB, 'test_area_points')

#---------------------------------------------------------------------------
#Logic

# for side in ['LEFT', 'RIGHT']: #Join NGD_AL to points based on side
#     print(f'Adding fields to points on {side} side')
#     buffer = arcpy.Buffer_analysis(NGD_AL, os.path.join(workingGDB, f'NGD_AL_Buffer{side[0]}'), '75', line_side= side, line_end_type= 'FLAT')
#     arcpy.SpatialJoin_analysis(bc_points, buffer, os.path.join(workingGDB, f'test_area_points_NGD_UID_{side[0]}'))

# create ranges for the NGD_AL points that have streets near them
for side in ['L', 'R']:
    points_df = pd.DataFrame.spatial.from_featureclass(os.path.join(workingGDB, f'test_area_points_NGD_UID_{side}'), 
                                                    where_clause= "NGD_UID IS NOT NULL",
                                                    fields= ['NUMBER', 'STREET', 'NGD_UID', 'STR_LABEL_NME'] )
    points_df['STR_LABEL_NME'] = points_df['STR_LABEL_NME'].str.upper()
    print('fixing Leading W or E')
    for row in points_df.itertuples(): # Move leading W or E to end of string in line with the NGD format
        if points_df.STREET is not None:
            street = str(row.STREET)
            if street.startswith('W ') or street.startswith('E '):
                index0 = street[0] + ' '
                street = street.replace(index0, '')
                out_street = street + ' ' + index0.strip(' ')
                points_df.at[row.Index, 'STREET'] = out_street
    print('Looking for ranges')
    out_ranges_df = pd.DataFrame(columns= ['NGD_UID', 'Max_Address', 'Min_Address', 'Street_Name'])
    for UID in points_df['NGD_UID'].unique().tolist():
        
        UID_records_df = points_df.loc[(points_df['NGD_UID'] == UID) & (points_df['STREET'] == points_df['STR_LABEL_NME'])]
        if len(UID_records_df) == 0:
            print(f'NGD_UID {UID} has 0 matches. Going to next UID')
            continue
        print(f'Range found for {UID}. Max: {UID_records_df.NUMBER.max()}, Min: {UID_records_df.NUMBER.min()}, {UID_records_df.STREET.iloc[0]}')
        out_ranges_df.loc[len(out_ranges_df)] = [UID, UID_records_df.NUMBER.max(), UID_records_df.NUMBER.min(), UID_records_df.STREET.iloc[0]]

    print(out_ranges_df.head)
    out_ranges_df.to_csv(os.path.join(r'H:\NGD_A_Complete_Ranges', f'test{side}.csv'), index= False)

print('Done!')
