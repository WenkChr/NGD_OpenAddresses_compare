import os, sys, arcpy
import pandas as pd
import numpy as np
from arcgis import GeoAccessor
arcpy.env.overwriteOutput = True

#---------------------------------------------------------------------------
def levenshtein_ratio_and_distance(s, t, ratio_calc = False):
    """ levenshtein_ratio_and_distance:
        Calculates levenshtein distance between two strings.
        If ratio_calc = True, the function computes the
        levenshtein distance ratio of similarity between two strings
        For all i and j, distance[i,j] will contain the Levenshtein
        distance between the first i characters of s and the
        first j characters of t
    """
    '''Function Source: https://www.datacamp.com/community/tutorials/fuzzy-string-python '''
    # Initialize matrix of zeros
    rows = len(s)+1
    cols = len(t)+1
    distance = np.zeros((rows,cols),dtype = int)

    # Populate matrix of zeros with the indeces of each character of both strings
    for i in range(1, rows):
        for k in range(1,cols):
            distance[i][0] = i
            distance[0][k] = k

    # Iterate over the matrix to compute the cost of deletions,insertions and/or substitutions    
    for col in range(1, cols):
        for row in range(1, rows):
            if s[row-1] == t[col-1]:
                cost = 0 # If the characters are the same in the two strings in a given position [i,j] then the cost is 0
            else:
                # In order to align the results with those of the Python Levenshtein package, if we choose to calculate the ratio
                # the cost of a substitution is 2. If we calculate just distance, then the cost of a substitution is 1.
                if ratio_calc == True:
                    cost = 2
                else:
                    cost = 1
            distance[row][col] = min(distance[row-1][col] + 1,      # Cost of deletions
                                 distance[row][col-1] + 1,          # Cost of insertions
                                 distance[row-1][col-1] + cost)     # Cost of substitutions
    if ratio_calc == True:
        # Computation of the Levenshtein Distance Ratio
        Ratio = ((len(s)+len(t)) - distance[row][col]) / (len(s)+len(t))
        return Ratio
    else:
        # print(distance) # Uncomment if you want to see the matrix showing how the algorithm computes the cost of deletions,
        # insertions and/or substitutions
        # This is the minimum number of edits needed to convert string a to string b
        return "The strings are {} edits away".format(distance[row][col])

def RangeCompareMatcher(AF_VAL, AT_VAL, OA_MAX, OA_MIN, side): 
    # Compares the OA range and the NGD range in matches and return a set class value for easy comparison    
    vals =[AF_VAL, AT_VAL, OA_MIN, OA_MAX]
    if AF_VAL > AT_VAL:
        # Then AF pairs with min and AT pairs with max
        vals = [At_VAL, AF_VAL, OA_MIN, OA_MAX]

    NGD_Range = int(vals[0]) - int(vals[1])
    OA_Range = int(vals[2]) - int(vals[3])
    if NGD_Range == OA_Range:
        return 'EQUAL'
    if NGD_Range < OA_Range:
        return 'NGD SMALLER'
    if NGD_Range > OA_Range:
        return 'NGD BIGGER'
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
out_ranges_df['S_NAME_ONLY'] = out_ranges_df.Street_Name.str.split().str.get(0)
# Compare BB ranges to the NGD A ranges
for side in ['L', 'R']:
    # Side outputs
    out_df_columns = ['NGD_UID','BB_UID', 'Max_Address', 'Min_Address', 'OA_Street_Name', f'NGD_STR_ID_{side}', f'AF{side}_VAL', f'AF{side}_SRC', f'AT{side}_VAL', f'AT{side}_SRC', f'STR_LABEL_NME_{side}', 'Match_Type']
    out_df_rows = []
    output_counts = {'FULL': 0, 'PARTIAL' : 0, 'RATIO' : 0}
    # NGD_AL DF setup and selection vars
    al_fields = ['NGD_UID', f'BB_UID_{side}', f'AF{side}_VAL', f'AT{side}_VAL', f'AF{side}_SRC', f'AT{side}_SRC', f'NGD_STR_UID_{side}', 'STR_LABEL_NME']
    where_clause = f"AF{side}_SRC = 'GISI' Or AF{side}_SRC = 'DRA' Or AT{side}_SRC = 'GISI' Or AT{side}_SRC = 'DRA'"
    NGD_AL_df = pd.DataFrame.spatial.from_featureclass(NGD_AL, fields= al_fields, where_clause= where_clause)
    NGD_AL_df['STR_LABEL_NME'] = NGD_AL_df['STR_LABEL_NME'].str.upper()
    print(f'Length of {side}: {len(NGD_AL_df)}')
    # Loop over the NGD_AL and find matches with BB ranges
    print(f'Comparing ranges on {side} side to the NGD_AL')
    for row in NGD_AL_df.itertuples():
        # compare df's on the street name. If this returns a match add to the out df
        bbuid = NGD_AL_df.iloc[row.Index][f'BB_UID_{side}']
        # Perfect street name match check
        UID_ranges_df = out_ranges_df.loc[(out_ranges_df['BB_UID'] == bbuid) & (out_ranges_df['Street_Name'] == row.STR_LABEL_NME)]
        if len(UID_ranges_df) > 0: # If there is a matching record add this to the out df
            row = [row.NGD_UID, #NGD_UID
                        bbuid, #BB_UID        
                        UID_ranges_df.iloc[0]['Max_Address'], # Max_Address
                        UID_ranges_df.iloc[0]['Min_Address'], # Min_Address
                        UID_ranges_df.iloc[0]['Street_Name'], # OA_Street_Name
                        NGD_AL_df.iloc[row.Index][f'NGD_STR_UID_{side}'], # NGD_STR_UID
                        NGD_AL_df.iloc[row.Index][f'AF{side}_VAL'], # AF VAL
                        NGD_AL_df.iloc[row.Index][f'AF{side}_SRC'], # AF SRC
                        NGD_AL_df.iloc[row.Index][f'AT{side}_VAL'], # AT Val
                        NGD_AL_df.iloc[row.Index][f'AT{side}_SRC'], # AT SRC
                        row.STR_LABEL_NME, # STR_LABEL_NME 
                        'FULL'] # Street Name Match Type                    
            out_df_rows.append(row)
            output_counts['FULL'] += 1
            continue
        # Partial Street Match check
        UID_ranges_df = out_ranges_df.loc[(out_ranges_df['BB_UID'] == bbuid) & (out_ranges_df['S_NAME_ONLY'] == row.STR_LABEL_NME.split(' ')[0])]
        if len(UID_ranges_df) > 0:
            row = [row.NGD_UID, #NGD_UID
                        bbuid, #BB_UID        
                        UID_ranges_df.iloc[0]['Max_Address'], # Max_Address
                        UID_ranges_df.iloc[0]['Min_Address'], # Min_Address
                        UID_ranges_df.iloc[0]['Street_Name'], # OA_Street_Name
                        NGD_AL_df.iloc[row.Index][f'NGD_STR_UID_{side}'], # NGD_STR_UID
                        NGD_AL_df.iloc[row.Index][f'AF{side}_VAL'], # AF VAL
                        NGD_AL_df.iloc[row.Index][f'AF{side}_SRC'], # AF SRC
                        NGD_AL_df.iloc[row.Index][f'AT{side}_VAL'], # AT Val
                        NGD_AL_df.iloc[row.Index][f'AT{side}_SRC'], # AT SRC
                        row.STR_LABEL_NME, # STR_LABEL_NME 
                        'PARTIAL'] # Street name match type 
            out_df_rows.append(row)
            output_counts['PARTIAL'] += 1
            continue
        # Ratio match method
        # UID_ranges_df = out_ranges_df.loc[(out_ranges_df['BB_UID'] == bbuid) & (levenshtein_ratio_and_distance(out_ranges_df['S_NAME_ONLY'], row.STR_LABEL_NME.split(' ')[0],  ratio_calc= True)> 0.5)]
        # if len(UID_ranges_df) > 0:
        #     row = [row.NGD_UID, #NGD_UID
        #                 bbuid, #BB_UID        
        #                 UID_ranges_df.iloc[0]['Max_Address'], # Max_Address
        #                 UID_ranges_df.iloc[0]['Min_Address'], # Min_Address
        #                 UID_ranges_df.iloc[0]['Street_Name'], # OA_Street_Name
        #                 NGD_AL_df.iloc[row.Index][f'NGD_STR_UID_{side}'], # NGD_STR_UID
        #                 NGD_AL_df.iloc[row.Index][f'AF{side}_VAL'], # AF VAL
        #                 NGD_AL_df.iloc[row.Index][f'AF{side}_SRC'], # AF SRC
        #                 NGD_AL_df.iloc[row.Index][f'AT{side}_VAL'], # AT Val
        #                 NGD_AL_df.iloc[row.Index][f'AT{side}_SRC'], # AT SRC
        #                 row.STR_LABEL_NME, # STR_LABEL_NME 
        #                 'RATIO'] # Street name match type 
        #     out_df_rows.append(row)
        #     output_counts['RATIO'] += 1
        #     continue
        
    print(f"Final output counts for {side}. FULL: {output_counts['FULL']} PARTIAL: {output_counts['PARTIAL']} RATIO: {output_counts['RATIO']}")
    out_df = pd.DataFrame(out_df_rows, columns= out_df_columns)
    out_df[f'AF{side}_VAL'] = out_df[f'AF{side}_VAL'].fillna(0)
    out_df[f'AT{side}_VAL'] = out_df[f'AT{side}_VAL'].fillna(0)
    # Calculate match type var
    out_df['Range_Match_Type'] =  out_df.apply(lambda row: RangeCompareMatcher(row[f'AF{side}_VAL'], row[f'AT{side}_VAL'], row.Max_Address, row.Min_Address, side), axis=1)
    out_df.to_csv(os.path.join(r'H:\NGD_A_Complete_Ranges', f'testBB_{side}.csv'), index= False)

print('DONE!')
