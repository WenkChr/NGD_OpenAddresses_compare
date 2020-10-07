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
        return 'NGD RANGE INSIDE OA RANGE'
    if OA_MIN > vals[0] and OA_MAX < vals[1]:
        return 'OA RANGE INSIDE NGD RANGE'
    if (OA_MIN == vals[0] and OA_MAX != vals[1]) or (OA_MIN != vals[0] and OA_MAX == vals[1]):
        return 'ONE VALUE MATCH'
    else: return 'RANGES OFFSET'
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
# Compare BB ranges to the NGD A ranges
for side in ['L', 'R']:
    # Side outputs
    out_df_columns = ['NGD_UID','BB_UID', 'Max_Address', 'Min_Address', 'OA_Street_Name', f'NGD_STR_ID_{side}', f'AF{side}_VAL', f'AF{side}_SRC', f'AT{side}_VAL', f'AT{side}_SRC', f'STR_LABEL_NME_{side}', 'Match_Type']
    out_df_rows = []
    rejects_rows = []
    output_counts = {'FULL': 0, 'PARTIAL' : 0, 'RATIO' : 0}
    print(f'Length of {side}: {len(NGD_AL_gdf)}')
    # Loop over the NGD_AL and find matches with BB ranges
    print(f'Comparing ranges on {side} side to the NGD_AL')
    for Nrow in NGD_AL_gdf.itertuples():
        # compare df's on the street name. If this returns a match add to the out df
        bbuid = NGD_AL_gdf.iloc[Nrow.Index]
        bbuid = bbuid[f'BB_UID_{side}']
        # Perfect street name match check
        UID_ranges_df = out_ranges_df.loc[(out_ranges_df['BB_UID'] == bbuid) & (out_ranges_df['Street_Name'] == Nrow.STR_LABEL_NME)]
        if len(UID_ranges_df) > 0: # If there is a matching record add this to the out df
            row = [Nrow.NGD_UID, #NGD_UID
                        bbuid, #BB_UID        
                        UID_ranges_df.iloc[0]['Max_Address'], # Max_Address
                        UID_ranges_df.iloc[0]['Min_Address'], # Min_Address
                        UID_ranges_df.iloc[0]['Street_Name'], # OA_Street_Name
                        NGD_AL_gdf.iloc[Nrow.Index][f'NGD_STR_UID_{side}'], # NGD_STR_UID
                        NGD_AL_gdf.iloc[Nrow.Index][f'AF{side}_VAL'], # AF VAL
                        NGD_AL_gdf.iloc[Nrow.Index][f'AF{side}_SRC'], # AF SRC
                        NGD_AL_gdf.iloc[Nrow.Index][f'AT{side}_VAL'], # AT Val
                        NGD_AL_gdf.iloc[Nrow.Index][f'AT{side}_SRC'], # AT SRC
                        Nrow.STR_LABEL_NME, # STR_LABEL_NME 
                        'FULL'] # Street Name Match Type                    
            out_df_rows.append(row)
            output_counts['FULL'] += 1
            continue
        # Partial Street Match check
        UID_ranges_df = out_ranges_df.loc[(out_ranges_df['BB_UID'] == bbuid) & (out_ranges_df['S_NAME_ONLY'] == Nrow.STR_LABEL_NME.split(' ')[0])]
        if len(UID_ranges_df) > 0:
            row = [Nrow.NGD_UID, #NGD_UID
                        bbuid, #BB_UID        
                        UID_ranges_df.iloc[0]['Max_Address'], # Max_Address
                        UID_ranges_df.iloc[0]['Min_Address'], # Min_Address
                        UID_ranges_df.iloc[0]['Street_Name'], # OA_Street_Name
                        NGD_AL_gdf.iloc[Nrow.Index][f'NGD_STR_UID_{side}'], # NGD_STR_UID
                        NGD_AL_gdf.iloc[Nrow.Index][f'AF{side}_VAL'], # AF VAL
                        NGD_AL_gdf.iloc[Nrow.Index][f'AF{side}_SRC'], # AF SRC
                        NGD_AL_gdf.iloc[Nrow.Index][f'AT{side}_VAL'], # AT Val
                        NGD_AL_gdf.iloc[Nrow.Index][f'AT{side}_SRC'], # AT SRC
                        Nrow.STR_LABEL_NME, # STR_LABEL_NME 
                        'PARTIAL'] # Street name match type 
            out_df_rows.append(row)
            output_counts['PARTIAL'] += 1
            continue
        # if you get to this point then these records don't have a match via the current methods
        UID_ranges_df = out_ranges_df.loc[(out_ranges_df['BB_UID'] == bbuid)]
        if len(UID_ranges_df) > 0:
            for r in UID_ranges_df.itertuples(): 
                row = [Nrow.NGD_UID, #NGD_UID
                            bbuid, #BB_UID        
                            r.Max_Address, # Max_Address
                            r.Min_Address, # Min_Address
                            r.Street_Name, # OA_Street_Name
                            NGD_AL_gdf.iloc[Nrow.Index][f'NGD_STR_UID_{side}'], # NGD_STR_UID
                            NGD_AL_gdf.iloc[Nrow.Index][f'AF{side}_VAL'], # AF VAL
                            NGD_AL_gdf.iloc[Nrow.Index][f'AF{side}_SRC'], # AF SRC
                            NGD_AL_gdf.iloc[Nrow.Index][f'AT{side}_VAL'], # AT Val
                            NGD_AL_gdf.iloc[Nrow.Index][f'AT{side}_SRC'], # AT SRC
                            Nrow.STR_LABEL_NME, # STR_LABEL_NME 
                            ] 
            rejects_rows.append(row)
    print(f"Final output counts for {side}. FULL: {output_counts['FULL']} PARTIAL: {output_counts['PARTIAL']} RATIO: {output_counts['RATIO']}")
    out_df = pd.DataFrame(out_df_rows, columns= out_df_columns)
    reject_df = pd.DataFrame(rejects_rows,  columns= out_df_columns[:len(out_df_columns)-1])
    print(f"Number of rows to manually recheck: {len(reject_df['NGD_UID'].unique().tolist())}")
    out_df[f'AF{side}_VAL'] = out_df[f'AF{side}_VAL'].fillna(0)
    out_df[f'AT{side}_VAL'] = out_df[f'AT{side}_VAL'].fillna(0)
    # Calculate match type var
    out_df['Range_Match_Type'] =  out_df.apply(lambda row: RangeCompareMatcher(row[f'AF{side}_VAL'], row[f'AT{side}_VAL'], row.Max_Address, row.Min_Address, side), axis=1)
    out_df.to_csv(os.path.join(r'H:\NGD_A_Complete_Ranges', f'testBB_{side}.csv'), index= False)
    reject_df.to_csv(os.path.join(r'H:\NGD_A_Complete_Ranges', f'testBB_{side}_rejects.csv'), index= False)
print('DONE!')   
                