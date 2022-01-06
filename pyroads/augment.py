import numpy as np
import pandas as pd 

##Determine whether the section is metro or rural
def rurality(data, RA, road_no, SLK = "SLK") :
    import numpy as np
    regionType = np.where((data[RA] == 7 & data[road_no].isin(['M045','H006','M026','M010']))| 
                    ((data[SLK].astype(int) >= 48.5) & data[road_no] == 'H005')|
    ((data[SLK].astype(int) >= 48.5) & data[road_no] == 'H005')|
    ((data[SLK].astype(int) >= 12.22) & data[road_no] == 'H052')|
    ((data[SLK].astype(int) >= 35.5) & data[road_no] == 'H001')|
    ((data[SLK].astype(int) >= 18.54) & data[road_no] == 'H009'), 'Rural', 'Metro')
    return regionType

##Assign a natural key based on key variables
def natural_key(data, SLK = None, ID_name = "NATURAL_KEY", true_SLK = None, key_vars = []):

    #Create a natural key based on SLK and ID variables
    new_data = data.copy()
    new_data.insert(0, ID_name, "")
    for var in key_vars +[SLK, true_SLK]:
        if var is not None:
            new_data[ID_name] += new_data[var].astype(str) + '-'

    return new_data

##Function that allows one to add columns to a dataset from one with a shared natural key.  
def concat_cols(left, right, key = "NATURAL_KEY", new_cols = []):
    new_cols.append(key)
    concat_data = left.copy().merge(right[new_cols], how = 'left', on = key, suffixes = ('','_right'))
    return concat_data

def surf_type(data, old_col, method = "short"):
    surf_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    full = ["Asphalt Dense Graded", "Asphalt Intersection Mix", "Asphalt Open Graded", "Concrete", "Paving", "Primer Seal", "Rubberised Seal", "Single Seal", "Slurry Seal", "Two Coat Seal", "Asphalt Sotne Mastic", "Asphalt Open Graded on Dense Graded", "Asphalt Gap Graded Rubberised (GGAR)"]
    short = ["DGA", "IMA", "OGA", "Concrete", "Paving", "Primer Seal", "Rubberised Seal", "Single Seal", "Slurry Seal", "Two Coat Seal", "SMA", "OGA on DGA", "GGAR"]
    asphalt = ["DGA", "IMA", "OGA", "Other", "Other", "Seal", "Seal", "Seal", "Seal", "Seal", "SMA", "OGA on DGA", "GGAR"]
    grouped = ["Asphalt", "Asphalt", "Asphalt", "Other", "Other", "Seal", "Seal", "Seal", "Seal", "Seal", "Asphalt", "Asphalt", "Asphalt", 'Asphalt']
    group_id = [1,1,1,2,2,3,3,3,3,3,1,1,1,1]
    
    if method == "full":
        surf_dict = dict(zip(surf_id, full))
    elif method == "short":
        surf_dict = dict(zip(surf_id, short))
    elif method == "asphalt":
        surf_dict = dict(zip(surf_id, asphalt))
    elif method == "grouped":
        surf_dict = dict(zip(surf_id, grouped))
    elif method == 'group_id':
         surf_dict = dict(zip(surf_id, group_id))   
    else:
        return print("ERROR surf_type(method): Please choose a method from ['full', 'short', 'asphalt', grouped'].")
    
    return data[old_col].map(surf_dict)
 
def surf_id(data, old_col, method = "short"):
    surf_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    full = ["Asphalt Dense Graded", "Asphalt Intersection Mix", "Asphalt Open Graded", "Concrete", "Paving", "Primer Seal", "Rubberised Seal", "Single Seal", "Slurry Seal", "Two Coat Seal", "Asphalt Stone Mastic", "Asphalt Open Graded on Dense Graded", 'Asphalt Gap Graded Rubberised (GGAR)']
    short = ["DGA", "IMA", "OGA", "Concrete", "Paving", "Primer Seal", "Rubberised Seal", "Single Seal", "Slurry Seal", "Two Coat Seal", "SMA", "OGA on DGA"]
    asphalt = ["DGA", "IMA", "OGA", "Other", "Other", "Seal", "Seal", "Seal", "Seal", "Seal", "SMA", "OGA on DGA", 'GGAR']
    grouped = ["Asphalt", "Asphalt", "Asphalt", "Other", "Other", "Seal", "Seal", "Seal", "Seal", "Seal", "Asphalt", "Asphalt", "Asphalt", 'Asphalt']
    group_id = [1,1,1,2,2,3,3,3,3,3,1,1,1,1]
    
    if method == "full":
        surf_dict = dict(zip(full, surf_id))
    elif method == "short":
        surf_dict = dict(zip(short, surf_id))
    elif method == "asphalt":
        surf_dict = dict(zip(asphalt, surf_id))
    elif method == "grouped":
        surf_dict = dict(zip(grouped, surf_id))
    elif method == 'group_id':
         surf_dict = dict(zip(group_id, surf_id))   
    else:
        return print("ERROR surf_type(method): Please choose a method from ['full', 'short', 'asphalt', grouped'].")
    
    return data[old_col].map(surf_dict)
 
def ra_transform(x, method = "name"):

    import pandas as pd

    number = [1, 2, 5, 6, 7, 8, 11, 14]
    name = ["Great Southern", "South West", "Goldfields-Esperance", "Kimberley", "Metropolitan", "Wheatbelt", "Pilbara", "Mid West-Gascoyne"]
    
    if method == "name":
        ra_dict = dict(zip(name, number))
    if method == "number":
        ra_dict = dict(zip(number, name))

    ra = a.map(ra_dict).astype(pd.Int64Dtype())
    
    return ra

def route_change(x, method = "new"):
    old_routes = [1.1, 2.1, 3.1, 2.2, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0]
    new_routes = [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]

    if method == "new":
        route_dict = dict(zip(old_routes, new_routes))
    if method == "old": 
        route_dict = dict(zip(new_routes, old_routes))
    
    route_no = a.map(route_dict) 
    return route_no
    
def route_description(x, method = "new"):
    
    old_routes = [1.1, 2.1, 3.1, 2.2, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0]
    new_routes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
    route_description = ['Perth to Adelaide', ' Perth to Darwin', ' Perth to Bunbury', 'Perth to Port Hedland', ' Perth to Albany', ' Bunbury to Albany', 'Geraldton to Esperance', ' Perth to Esperance','Bunbury to Augusta', 'Albany to Esperance', 'Bindoon (H006) to Dongara (H004) ', 'Manjimup to Albany', 'Nanutarra to GNH', 'Paraburdoo to GNH', 'Derby to GNH', 'Newman to Port Hedland', 'Albany to Lake Grace', 'Perth to Merredin ', 'Bunbury to Lake King', 'Minilya (NWCH) to Exmouth', 'Byford to Coalfields Hwy', 'Busselton to SWH via Pembeton', 'Donnybrook (SWH) to Kojonup (Aly Hwy)', 'Roe Hwy to Toodyay']

    if method == "new":
        route_dict = dict(zip(new_routes, route_description))
    if method == "old": 
        route_dict = dict(zip(old_routes, route_description))
    
    route = a.map(route_dict)

    return route

def mabcd(x, new = 'mabcd'):
    
    link_category = ['MI', 'MFF', 'AW', 'AW+', 'BW', 'BW+', 'CW', 'DW']
    mabcd = ['M', 'M', 'A', 'A', 'B', 'B', 'C', 'D']

    mabcd_dict = dict(zip(link_category, mabcd))
    link_cat_dict = dict(zip(link_category, mabcd))

    if 'mabcd' in new.lower():
        return [mabcd_dict[k] for k in a]

    if 'link' in new.lower(): 
        return [link_cat_dict[k] for k in a]
    
    else:
        return "`new` must be either 'link' or 'mabcd'"

pave_type_dict = {'Asphalt': 10, 'Clay': 5, 'Concrete': 6, 'Crushed Rock': 3, 'Ferricrete': 11, 'Gravel': 2, 'Limestone': 7, 'HCT': 9, 'Recycled Material': 8, 'Sand': 4, 'Sand Clay': 1, 'Unknown': 0}

def get_lanes(x, current = 'xsp', new = 'dtims'):
    import pandas as pd
    import numpy as np

    #Check that the user is using possible 'current' and 'new' parameters
    if not np.prod([x in ['xsp', 'lane', 'description', 'dtims'] for x in [current, new]]):
        return "`current` and `new` must be from ['xsp', 'dirn', 'description, 'dtims']"

    xsp = pd.Series(['L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'R1', 'R2', 'R3', 'R4', 'R5', 'LL1', 'LL2', 'LL3', 'LR1', 'LR2', 'LR3', 'RL1', 'RL2', 'RL3', 'RR1', 'RR2', 'RR3', 'LO', 'RO', 'L', 'R'])
    lane = np.where(xsp.str.contains("^L", regex = True), "L", np.where(xsp.str.contains("^R", regex = True), "R", 'Unknown'))
    description = ["Left lane 2", "Left lane 3", "Left lane 4", "Left lane 5", "Left lane 6", "Right lane 1", "Right lane 2", "Right lane 3", "Right lane 4", "Right lane 5", "Left carriageway, left turn pocket 1", "Left carriageway, left turn pocket 2", "Left carriageway, left turn pocket 3", "Left carriageway, right turn pocket 1", "Left carriageway, right turn pocket 2", "Left carriageway, right turn pocket 3", "Right carriageway, left turn pocket 1", "Right carriageway, left turn pocket 2", "Right carriageway, left turn pocket 3", "Right carriageway, right turn pocket 1", "Right carriageway, right turn pocket 2", "Right carriageway, right turn pocket 3", "Left overtaking lane", "Right overtaking lane", "Left shoulder", "Right shoulder"]
    dtims = [1, 3, 5, 7, 9, 11, 2, 4, 6, 8, 10, 21, 23, 25, 31, 33, 35, 22, 24, 26, 32, 34, 36, 41, 42, 51, 52] 
   
    if current == 'xsp':
        old_data = xsp
    if current == 'description':
        old_data = description
    if current == 'lane':
        old_data = lane
    if current == 'dtims':
        old_data = dtims
    if new == 'xsp':
        new_data = xsp
    if new == 'description':
        new_data = description
    if new == 'lane':
        new_data = lane
    if new == 'dtims':
        new_data = dtims

    lane_dict = dict(zip(old_data, new_data))

    return a.map(lane_dict) 

def get_roads(x, current = 'number', new = 'name'):
    import pands as pd
    import numpy as np

    #Check that the user is using possible 'current' and 'new' parameters
    if not np.prod([x in ['id', 'number', 'name'] for x in [current, new]]):
        return "`current` and `new` must be from ['id', 'number', 'name']"

    