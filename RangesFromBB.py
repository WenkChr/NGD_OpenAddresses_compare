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
street_types = {'PLACE' : 'PL', 
            'AV': 'AVE', 
            'AVENUE': 'AVE', 
            'CRESCENT': 'CRES', 
            'ROAD': 'RD', 
            'DRIVE': 'DR', 
            'STREET' : 'ST'}


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
        for stype in street_types:
            if ' ' + stype in street:
                street = street.replace(stype, street_types[stype])
        # Fix directional inconsistencies
        if street.startswith('W ') or street.startswith('E ') or street.startswith('SE ') or street.startswith('SW ')or street.startswith('NE ') or street.startswith('NW '):
            index0 = street[0] + ' '
            street = street.replace(index0, '')
            street = street + ' ' + index0.strip(' ')
        points_df.at[row.Index, 'STREET'] = street

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
# Compare BB ranges to the NGD A ranges
out_df = pd.DataFrame(columns= ['NGD_UID','BB_UID', 'Max_Address', 'Min_Address', 'OA_Street_Name', 'NGD_STR_ID' 'AF_VAL', 'AF_SRC', 'AT_VAL', 'AT_SRC', 'STR_LABEL_NME'])
for side in ['L', 'R']:
    out_df = pd.DataFrame(columns= ['NGD_UID','BB_UID', 'Max_Address', 'Min_Address', 'OA_Street_Name', f'NGD_STR_ID_{side}' f'AF{side}_VAL', 'AF_SRC', f'AT{side}_VAL', f'AT{side}_SRC', f'STR_LABEL_NME_{side}'])
    al_fields = [ 'NGD_UID', f'BB_UID_{side}', f'AF{side}_VAL', f'AT{side}_VAL', f'AF{side}_SRC', f'AT{side}_SRC', f'NGD_STR_UID_{side}', 'STR_LABEL_NME']
    where_clause = f"AF{side}_SRC = 'GISI' Or AF{side}_SRC = 'DRA' Or AT{side}_SRC = 'GISI' Or AT{side}_SRC = 'DRA'"
    NGD_AL_df = pd.DataFrame.spatial.from_featureclass(NGD_AL, fields= al_fields, where_clause= where_clause)
    NGD_AL_df['STR_LABEL_NME'] = NGD_AL_df['STR_LABEL_NME'].str.upper()
    print(f'Comparing ranges on {side} side to the NGD_AL')
    for row in NGD_AL_df.itertuples():
        # compare df's on the street name. If this returns a match add to the out df
        bbuid = NGD_AL_df.iloc[row.Index][f'BB_UID_{side}']
        UID_ranges_df = out_ranges_df.loc[(out_ranges_df['BB_UID'] == bbuid) & (out_ranges_df['Street_Name'] == row.STR_LABEL_NME)]
        if len(UID_ranges_df) > 0: # If there is a matching record add this to the out df
            out_df.loc[len(out_ranges_df)] = pd.Series([row.NGD_UID, #NGD_UID
                                            UID, #BB_UID        
                                            UID_ranges_df.iloc[0]['Max_Address'], # Max_Address
                                            UID_ranges_df.iloc[0]['Min_Address'], # Min_Address
                                            UID_ranges_df.iloc[0]['Street_Name'], # OA_Street_Name
                                            NGD_AL_df.iloc[row.Index][f'NGD_STR_UID_{side}'], # NGD_STR_UID
                                            NGD_AL_df.iloc[row.Index][f'AF{side}_VAL'], # AF VAL
                                            NGD_AL_df.iloc[row.Index][f'AF{side}_SRC'], # AF SRC
                                            NGD_AL_df.iloc[row.Index][f'AT{side}_VAL'], # AT Val
                                            NGD_AL_df.iloc[row.Index][f'AT{side}_SRC'], # AT SRC
                                            row.STR_LABEL_NME]) # STR_LABEL_NME
            
        out_df.to_csv(os.path.join(r'H:\NGD_A_Complete_Ranges', f'testBB_{side}.csv'))
    
sys.exit()
print(out_ranges_df.head)
out_ranges_df.to_csv(os.path.join(r'H:\NGD_A_Complete_Ranges', f'testBB.csv'), index= False)

print('DONE!')
