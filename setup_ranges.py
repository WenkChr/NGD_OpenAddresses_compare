import arcpy, os

arcpy.env.overwriteOutput = True

#-----------------------------------------------------------------------------------------------------
# inputs
workingGDB = r'H:\NGD_A_Complete_Ranges\Working.gdb'
csd_fc = os.path.join(workingGDB, 'CSD_A')
NGD_AL = os.path.join(workingGDB, 'NGD_AL_NGD_ALIAS1_2')
bc_points = os.path.join(workingGDB, 'bc_all')
areaCSD_UID = '5915022'

#-----------------------------------------------------------------------------------------------------
#Logic
#Clip all data to test area
print('Clipping CSD data to test area')
areaCSD = arcpy.FeatureClassToFeatureClass_conversion(csd_fc, workingGDB, 'tetstAreaCSD', where_clause= f"CSD_UID = '{areaCSD_UID}'")
print('Clipping NGD_AL')
SQL_query = f"(AFL_SRC = 'GISI' OR AFL_SRC = 'DRA' OR ATL_SRC = 'GISI' OR ATL_SRC = 'DRA' OR AFR_SRC = 'GISI' OR AFR_SRC = 'DRA' OR ATR_SRC = 'GISI' OR ATR_SRC = 'DRA') AND CSD_UID= '{areaCSD_UID}'"
van_al = arcpy.FeatureClassToFeatureClass_conversion(NGD_AL, workingGDB, 'NGD_AL_clipped', where_clause= SQL_query)

print('Clipping BC ADD Points')
fl = arcpy.MakeFeatureLayer_management(bc_points)
arcpy.SelectLayerByLocation_management(fl, 'INTERSECT', areaCSD)
arcpy.FeatureClassToFeatureClass_conversion(fl, workingGDB, 'test_area_points')

print('DONE!')
