This project performs analysis on the Open Addresses data and compares them to the NGD_AL address range data for the purpose of updating the NGD address ranges source fields from proprietary to open source. This project is designed to run on a single CSD at a time in order to limit the required processing power to perform the analysis.

The process runs in a couple of steps:

1.)  Clip all data to the Area of Analysis (AOA) using the most recently available CSD_A file from the NGD

2.) Create a spatial join between the NGD_A and the Open Adress points to add the NGD_A field sto the addres points

3.) Clean the address point data in the following ways:
                - Remove all non numeric and or null values from the 'NUMBER' field
                - Capitalize the 'STREET' field and shorten the street type (Street to ST, etc) to bring it inline with the NGD_AL STR_LABEL_NME field if necessary reorder the values so that they match the format of the NGD_AL addresses

4.) Create address ranges from the address points by taking the BB_UID field added during the spatial and grouping the points by street name. Then by street name take the min and max address values and use that to create a new dataframe for ranges 

5.) For each NGD_UID in the NGD_AL perform a couple of checks to compare the OA ranges by BB data
                
                a.) Compare the the 'STREET' field in the OA BB ranges against the STR_LABEL_NME field in the NGD_AL feature. If there is a match then add the range as a match the out csv and assign the Match_Type field as 'FULL'

                b.) Compare the the 'S_NAME_ONLY' field in the OA BB ranges against the STR_LABEL_NME field in the NGD_AL feature. If there is a match then add the range as a match the out csv and assign the Match_Type field as 'PARTIAL'

                c.) If the NGD_UID fails the above 2 checks but has matches on the BB_UID then the row is sent to the reject row csv for manual check with all its potential matches

6.) The matched and reject rows are then compiled into seperate dataframes and null values are replaced with 0's for querying.

7.) The dataframes of matches and rejects are then exported.

There is also a makeOpenAddressRanges.py file in the repo this script makes the ranges using a single sided buffer to select the points that fall within the range. This process is not quite as presise as the BB based process as it sometimes includes points that are not in the BB or might miss points in the BB outside of the buffer. But the flip side is that the segment might show a more accurate view of the address range covered by the line segment. The choice of methodology used is at the users discretion.