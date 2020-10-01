import geopandas as gpd
import numpy as np
import os
import pandas as pd
import shapely
import sys
import fiona
import pathlib
#-------------------------------------------------------------------------------------------------------------------------
#Functions
def records(filename, usecols, **kwargs):
    with fiona.open(filename, **kwargs) as source:
        for feature in source:
            f = {k: feature[k] for k in ['id', 'geometry']}
            f['properties'] = {k: feature['properties'][k] for k in usecols}
            yield f

#-------------------------------------------------------------------------------------------------------------------------
# Inputs

workingGDB = r'H:\NGD_A_Complete_Ranges\Working.gdb'
csd_fc = os.path.join(workingGDB, 'CSD_A')
NGD_AL = os.path.join(workingGDB, 'NGD_AL_NGD_ALIAS1_2')
NGD_A = os.path.join(workingGDB, 'NGD_A')
bc_points = os.path.join(workingGDB, 'bc_all')
areaCSD_UID = '5915022'

#---------------------------------------------------------------------------------------------------------------------------
# Logic
#Import and take only the correct csd
csd_area = gpd.read_file(os.path.split(csd_fc)[0], layer= os.path.split(csd_fc)[1], driver= 'OpenFileGDB')
csd_area = csd_area.loc[csd_area['CSD_UID'] == areaCSD_UID]

# Import and clip to csd all remaining inputs
print('Uploading NGD_AL')
NGD_AL_gdf = gpd.read_file(os.path.split(NGD_AL)[0], layer= os.path.split(NGD_AL)[1], driver= 'OpenFileGDB', mask= csd_area)
NGD_AL_gdf = NGD_AL_gdf.loc[(NGD_AL_gdf.AFL_SRC == 'GISI') | (NGD_AL_gdf.AFL_SRC == 'DRA') | 
                            (NGD_AL_gdf.ATL_SRC == 'GISI') | (NGD_AL_gdf.ATL_SRC == 'DRA') |
                            (NGD_AL_gdf.AFR_SRC == 'GISI') | (NGD_AL_gdf.AFR_SRC == 'DRA') | 
                            (NGD_AL_gdf.ATR_SRC == 'GISI') | (NGD_AL_gdf.ATR_SRC == 'DRA') ]

points_gdf = gpd.read_file(os.path.split(bc_points)[0], layer= os.path.split(bc_points)[1], driver= 'OpenFileGDB', mask= csd_area)

street_types = {'PLACE' : 'PL', 
            'AV': 'AVE', 
            'AVENUE': 'AVE', 
            'CRESCENT': 'CRES', 
            'ROAD': 'RD', 
            'DRIVE': 'DR', 
            'STREET' : 'ST', 
            'BOULEVARD' : 'BLVD',
            } # Street type inconsistencies found in OA data  

points_gdf['STREET'] = points_gdf['STREET'].str.upper()
points_gdf['NUMBER'] = points_gdf['NUMBER'].str.extract('(\d+)', expand= False) # remove non numeric values via regex
points_gdf['NUMBER'] = pd.to_numeric(points_gdf.NUMBER, errors='coerce') # converts to floats and null values become NAN
points_gdf = points_gdf.dropna(subset= ['NUMBER']) # drop ew NAN values
points_gdf['NUMBER'] = points_gdf['NUMBER'].astype(int) # Finally convert to int
print('Fixing Leading directional indicator')
for row in points_gdf.itertuples(): # Move leading W or E, etc to end of string in line with the NGD format
    if points_gdf.STREET is not None:
        street = str(row.STREET)
            # Correct address inconsistencies
        for stype in street_types:
            if ' ' + stype in street:
                street = street.replace(stype, street_types[stype])
        # Fix directional inconsistencies
        for d in ['NORTH', 'SOUTH', 'EAST', 'WEST']:
            street = street.replace(d, d[0])
        if street.startswith(('W ', 'E ', 'SE ', 'SW ', 'NE ', 'NW ')) and not street.endswith((' W', ' E', ' SE', ' SW', ' NE', ' NW')):
            index0 = street.split(' ')[0] + ' '
            street = street.replace(index0, '')
            street = street + ' ' + index0.strip(' ')
        
        points_gdf.at[row.Index, 'STREET'] = street

NGD_A_gdf = gpd.read_file(os.path.split(NGD_A)[0], layer= os.path.split(NGD_A)[1], driver= 'OpenFileGDB', mask= csd_area, columns=['BB_UID'])
#points_gdf['geometry'] = points_gdf['geometry'].to_crs(epsg= 9001)
NGD_A_cols = NGD_A_gdf.columns.to_list()
NGD_A_cols.remove('geometry')
NGD_A_gdf = NGD_A_gdf.drop(columns= NGD_A_cols[1:], axis= 1)

if NGD_A_gdf.crs != points_gdf.crs:
    print('CRS does not match check reprojection script')
    sys.exit()
# Perform Spatial Join between the NGD_A and the points to bring over the bb_uid field
points_sjoin =gpd.sjoin(points_gdf, NGD_A_gdf, how='left', op='intersects')
points_sjoin.drop(columns='index_right', axis=1, inplace= True)

#Create the address ranges from the address points
out_ranges_df = pd.DataFrame(columns= ['BB_UID', 'Max_Address', 'Min_Address', 'Street_Name'])
for UID in points_sjoin['BB_UID'].unique().tolist():
    UID_records_df = points_sjoin.loc[(points_sjoin['BB_UID'] == UID)]
    if len(UID_records_df) == 0:
        print(f'NGD_UID {UID} has 0 matches. Going to next UID')
        continue
    for street in UID_records_df['STREET'].unique().tolist():
        street_df = UID_records_df.loc[UID_records_df['STREET'] == street]
        out_ranges_df.loc[len(out_ranges_df)] = [UID, 
                                                street_df.NUMBER.max(), 
                                                street_df.NUMBER.min(), 
                                                street]
out_ranges_df['S_NAME_ONLY'] = out_ranges_df.Street_Name.str.split().str.get(0)

print('DONE!')
                