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
NGD_A = os.path.join(workingGDB, 'NGD_A')
bc_points = os.path.join(workingGDB, 'test_area_points')

#-----------------------------------------------------------------------------
#Logic
#Join on BB method
SJ_NAME = os.path.join(workingGDB, 'points_NGD_A_join')
print('Joining NGD_A to points')
arcpy.SpatialJoin_analysis(bc_points, NGD_A, SJ_NAME)

points_df = pd.DataFrame.spatial.from_featureclass(SJ_NAME, 
                                                    where_clause= "BB_UID IS NOT NULL",
                                                    fields= ['NUMBER', 'STREET', 'BB_UID'] )
points_df['STREET'] = points_df['STREET'].str.upper()
points_df['NUMBER'] = points_df['NUMBER'].str.extract('(\d+)', expand= False) # remove non numeric values via regex
points_df['NUMBER'] = pd.to_numeric(points_df.NUMBER, errors='coerce') # converts to floats and null values become NAN
points_df = points_df.dropna(subset= ['NUMBER']) # drop ew NAN values
points_df['NUMBER'] = points_df['NUMBER'].astype(int) # Finally convert to int
print('fixing Leading directional indicatior')
for row in points_df.itertuples(): # Move leading W or E, etc to end of string in line with the NGD format
    if points_df.STREET is not None:
        street = str(row.STREET)
        # Correct address inconsistencies
        if ' AV ' in street:
            street = street.replace(' AV ', ' AVE ')
        if ' CRESCENT' in street:
            street = street.replace('CRESCENT', 'CRES')
        
        # Fix directional inconsistencies
        if street.startswith('W ') or street.startswith('E ') or street.startswith('SE ') or street.startswith('SW ')or street.startswith('NE ') or street.startswith('NW '):
            index0 = street[0] + ' '
            street = street.replace(index0, '')
            out_street = street + ' ' + index0.strip(' ')
            points_df.at[row.Index, 'STREET'] = out_street

print('Creating ranges')
out_ranges_df = pd.DataFrame(columns= ['BB_UID', 'Max_Address', 'Min_Address', 'Street_Name'])
for UID in points_df['BB_UID'].unique().tolist():
    UID_records_df = points_df.loc[(points_df['BB_UID'] == UID)]
    if len(UID_records_df) == 0:
        print(f'NGD_UID {UID} has 0 matches. Going to next UID')
        continue
    for street in UID_records_df['STREET'].unique().tolist():
        street_df = UID_records_df.loc[UID_records_df['STREET'] == street]
        out_ranges_df.loc[len(out_ranges_df)] = [UID, 
                                                street_df.NUMBER.max(), 
                                                street_df.NUMBER.min(), 
                                                street]

print(out_ranges_df.head)
out_ranges_df.to_csv(os.path.join(r'H:\NGD_A_Complete_Ranges', f'testBB.csv'), index= False)

print('DONE!')
