##Deal with an SLK column as metres in integers to avoid the issue of calculating on floating numbers
def asmetres(var):

    m = round(var*1000).astype(int)

    return m

def gcd_list(items):
    items = list(items)

    def gcd(a, b):
        while b > 0:
            a, b = b, a%b
        return a

    result = items[0]

    for i in items[1:]:
        result = gcd(result, i)
    
    return result

##Turn each observation into sections of specified lengths
def stretch(data, starts, ends, prefixes = ['', 'true_'], segment_size = 'GCD', keep_ranges = False, sort = None, keep_ends = False, km = True):

    import numpy as np
 

    SLKs = [SLK for SLK in starts + ends]
    new_data = data.copy().reset_index(drop = True) #Copy of the dataset
    new_data = new_data.dropna(thresh = 2)  #drop any row that does not contain at least two non-missing values.
    
    if type(sort) == list:
        new_data.sort_values(sort, inplace = True)

    #Change SLK variables to 32 bit integers of metres to avoid the issue with calculations on floating numbers
    new_data[SLKs] = new_data[SLKs].apply(asmetres)
    
    if segment_size == 'GCD':
        lengths = new_data[ends[0]] - new_data[starts[0]]
        segment_size = gcd_list(lengths)
        
    #Reshape the data into size specified in 'obs_length'
    new_data = new_data.reindex(new_data.index.repeat(np.ceil((new_data[ends[0]] - new_data[starts[0]])/segment_size))) #reindex by the number of intervals of specified length between the start and the end.
    for start, prefix in zip(starts, prefixes):
        new_data[prefix + 'start'] = (new_data[start] +  new_data.groupby(level = 0).cumcount()*segment_size)
    for start, end, prefix in zip(starts, ends, prefixes):
    #End SLKs are equal to the lead Start SLKS except where the segment ends
        new_data[prefix + 'end'] = np.where((new_data[prefix + 'start'].shift(-1) - new_data[prefix + 'start']) == segment_size, new_data[prefix+'start'].shift(-1), new_data[end])
        if km:
            new_data[prefix + 'start'] = new_data[prefix + 'start']/1000
            if keep_ends:
                new_data[prefix + 'end'] = new_data[prefix + 'end']/1000

        
    
    new_data = new_data.reset_index(drop = True)

    #Drop the variables no longer required
    if not keep_ranges:
        new_data.drop([SLK for SLK in SLKs], axis = 1, inplace = True)
        new_data = new_data.reset_index(drop = True)
    else: 
        for SLK in SLKs:
            new_data[SLK] = new_data[SLK]/1000

    if not keep_ends:
        new_data = new_data.loc[:, ~new_data.columns.str.endswith('end')]
    return new_data

def compact(data, true_SLK = None, lanes = [], SLK = None, obs_length = 10, idvars = [], grouping = [], new_start_col = "start", new_end_col = "end"):
    
    SLKs = [SLK for SLK in [true_SLK, SLK] if SLK is not None]
    
    import numpy as np
    #Sort by all columns in grouping, then by true SLK, then by SLK
    new_data = data.copy().sort_values(idvars + SLKs + lanes).reset_index(drop = True)

    #Create a column that is a concatenation of all the columns in the grouping
    new_data.insert(0, "groupkey", "")
    for var in grouping+idvars:
        new_data["groupkey"] += new_data[var].astype(str) + '-'

    #Change SLK variables to 32 bit integers of metres to avoid the issue with calculations on floating numbers
    for SLK in SLKs:
        new_data[SLK] = asmetres(new_data[SLK])

    #Create lag and lead columns for SLK, true SLK, and the grouping key to check whether a new group has started
    for var in SLKs:
        new_data['lead_' + var] = new_data[var].shift(-1, fill_value = 1000)
        new_data['lag_' + var] = new_data[var].shift(1, fill_value = 1000)
    new_data['lead_groupkey'] = new_data['groupkey'].shift(-1, fill_value = 'End')
    new_data['lag_groupkey'] = new_data['groupkey'].shift(1, fill_value = 'Start')

    #Create columns based on whether they represent the start or end of a section.
    starts, ends = {}, {}
    
    for var in SLKs:
        starts[var] = np.where((new_data['lag_' + var] == (new_data[var]-obs_length)) & (new_data['lag_groupkey'] == new_data['groupkey']), False,True) #if the lagged SLKs are not one observation length less than the actual or the lagged group-key is different.
        ends[var] = np.where((new_data['lead_' + var] == (new_data[var] + obs_length)) & (new_data['lead_groupkey'] == new_data['groupkey']), False,True)  #if the lead SLKs are not one observation length more than the actual or the lead group-key is different.
    start = np.prod([i for i in starts.values()], axis = 0, dtype = bool)
    end = np.prod([i for i in ends.values()], axis = 0, dtype = bool)

    #Create the compact dataset

    compact_data = new_data.copy()[start][idvars + grouping].reset_index(drop = True) #Data for the id and grouping variables for every start instance.
    new_SLKs = []
    for var in SLKs:
        compact_data.insert(len(idvars),  var + new_start_col, (new_data[start].reset_index(drop = True)[var])/1000)
        compact_data.insert(len(idvars)+1,  var + new_end_col, (new_data[end].reset_index(drop = True)[var] + obs_length)/1000)
        new_SLKs += [var + new_start_col, var + new_end_col]   

    compact_data = compact_data.sort_values(idvars + new_SLKs + lanes).reset_index(drop = True) #Sort data by the location varibles
    
    return compact_data
    
def make_segments(data, SLK_type = "both", start = None, end = None, true_start = None, true_end = None, max_segment = 100):
    
    import numpy as np
    
    original = [start, end]
    true = [true_start, true_end]
    starts = [var for var in [start, true_start] if var is not None]
    ends = [var for var in [end, true_end] if var is not None]
    
    new_data = data.copy() #Copy of the dataset
    
    #Describe the SLK_type methods
    if SLK_type not in ['both', 'true', 'original']:
        return print("`SLK_type` must be one of 'both', 'true', or 'original'.")
    
    SLKs = [var for var in original + true if var is not None]
    
    if SLK_type == 'both' and None in SLKs:
        return print("All SLK variables must be declared when using `SLK_type` 'both'.")
    
    if SLK_type == 'original':
        if any(var in true for var in SLKs):
            print("'True' SLK variables will be ignored when using `SLK_type` 'original'.")
            new_data.drop([true_start, true_end], axis = 1, errors = 'ignore')

    if SLK_type == 'true': 
        if any(var in original for var in SLKs):
            print("'Original' SLK variables will be ignored when using `SLK_type` 'true'.")
            new_data.drop([start, end], axis = 1, errors = 'ignore')     
    
    new_data = new_data.dropna(thresh = 2)  #drop any row that does not contain at least two non-missing values.

    #Change SLK variables to 32 bit integers of metres to avoid the issue with calculations on floating numbers
    for var in SLKs:
        new_data[var] = asmetres(new_data[var])
    
    #Reshape the data into size specified in 'obs_length'
    new_data = new_data.reindex(new_data.index.repeat(np.ceil((new_data[ends[0]] - new_data[starts[0]])/max_segment))) #reindex by the number of intervals of specified length between the start and the end.
    for var in starts:
    #Increment the rows by the segment size
        new_data[var] = (new_data[var] + new_data.groupby(level=0).cumcount()*max_segment) 
    for start_,end_ in zip(starts, ends):
    #End SLKs are equal to the lead Start SLKS except where the segment ends
        new_data[end_] = np.where((new_data[start_].shift(-1) - new_data[start_]) == max_segment, new_data[start_].shift(-1), new_data[end_])
    
    #Convert SLK variables back to km
    for var in SLKs:
        new_data[var] = new_data[var]/1000
        
    new_data = new_data.reset_index(drop = True)

    return new_data

def merge(left, rights = [], by = ['road_no', 'cway'], start_names = [], end_names = [], columns = [], slk_true = True ):
    
    import numpy as np
    import pandas as pd

    #convert all column names to all lowercase so there is no case sensitivity related issues 
    dfs_original = [left] + rights
    left_copy = left.copy()
    rights_copy = [x.copy() for x in rights]
    dfs = [left_copy] + [x for x in rights_copy]
    
    for i in range(len(dfs)):
        dfs[i].rename(columns=str.lower, inplace = True)
    
    #convert all parameter column names to lowercase
    cols = [by, start_names, end_names]
    
    for i in range(len(cols)):
        cols[i] = [x.lower() for x in cols[i]]

    start_names = start_names + ['start', 'start slk', 'slk start', 'from slk', 'slk from', 'start_slk', 'slk_start', 'from_slk', 'slk_from', 'from', 'start']
    
    end_names = end_names + ['end', 'end slk', 'slk end', 'slk to', 'to slk', 'end_slk', 'slk_end', 'to_slk', 'slk_to', 'to']

    if slk_true:
        start_names = start_names + ['start true', 'start_true','true start', 'true_start', 'true start slk', 'true slk start', 'true from slk', 'true slk from', 'true_start_slk', 'true_slk_start', 'true_from_slk', 'true_slk_from', 'true_from', 'true_start', 'trueslk_from', 'trues']
        end_names = end_names + ['end true', 'end_true','true end', 'true_end','true end slk', 'true slk end', 'true slk to', 'true to slk', 'ture_end_slk', 'true_slk_end', 'true_to_slk', 'true_slk_to', 'true to', 'true_to', 'trueslk_to', 'truee']                             
    
    road_no_names = ['road_no', 'road_number', 'road no', 'road number', 'road'] 
    
    cway_names = ['cway', 'carriageway', 'carriage way', 'cways']
    
    side_names = ['side', 'direction']
    
    lane_names = ['xsp', 'lane', 'lane no', 'lane number']
    
    #Give all dataframes the same start and end slk column names.
    import numpy as np
    for i in range(len(dfs)):
        dfs[i].rename(lambda x: 'start_slk' if x in start_names else ('end_slk' if x in end_names else ('road_no' if x in road_no_names else ('cway' if x in cway_names else ('side' if x in side_names else ('xsp' if x in lane_names else x))))), axis = 1, inplace = True)

    #find the greatest common divisor SLKs for all of the datasets
    start_and_end = dfs[0].loc[:,['start_slk', 'end_slk']]
    
    for df in rights_copy:
        rights_start_end = df[['start_slk', 'end_slk']]
        start_and_end = pd.concat([start_and_end, rights_start_end])
        
    gcd = gcd_list(asmetres(start_and_end.iloc[:,1]) - asmetres(start_and_end.iloc[:,0]))
    
    #stretch by the greatest common divisor    
    left_copy = stretch(left_copy, starts = ['start_slk'], ends = ['end_slk'], segment_size = gcd, keep_ranges = True)
    for i in range(len(rights)):
        rights_copy[i] = stretch(rights_copy[i], starts = ['start_slk'], ends = ['end_slk'], segment_size = gcd)
     
    #set index names for joining
    index_names = ['road_no' if x in road_no_names else 'cway' if x in cway_names else 'side' if x in side_names else 'xsp' if x in lane_names else "" for x in by]
    index_names = [x for x in index_names if x!= ""] + ['start_slk', 'end_slk']
    
    for i in range(len(dfs)):
        dfs[i].set_index(index_names, inplace = True)
    
    merged_data = left_copy.copy()
    for df in rights_copy:
        merged_data = merged_data.join(df[[x for x in df.columns if x not in merged_data.columns]])
    
    return merged_data 
    
def cway_to_side(data, name = None):

    import numpy as np
    import pandas as pd 

    if name == None:
        names = [col for col in data.columns if "cway" in col.lower()]
        if len(names) != 1:
            if len(names) == 0:
                return 'ERROR cway_to_side(name): Please specify the name of the carriageway column.'
            else:
                return print('ERROR cway_to_side(name): Please specify the name of the carriageway column.',  'Found:', names)
        else:
            name = names[0]
    data_right = data[data[name].isin(['s', 'r', 'S', 'R'])].copy()
    data_right.loc[:,'side'] = 'R'
    data_left = data[data[name].isin(['s', 'l', 'S', 'R'])].copy()
    data_left.loc[:,'side'] = 'L'
    new_data = pd.concat([data_right, data_left])
    return new_data.reset_index(drop = True)

def hsd_to_side(data, side_name = 'side'):
    
    import numpy as np

    new_data = cway_to_side(data)
    new_data.loc[:,'Rutt'] = np.where(new_data[side_name] == 'L', new_data.filter(regex = "^L_RUT").mean(axis = 1), new_data.filter(regex = "^R_RUT").mean(axis = 1))
    new_data.loc[:,'Rough'] = np.where(new_data[side_name] == 'L', new_data.filter(regex = "^L_LANE_IRI").mean(axis = 1), new_data.filter(regex = "^R_LANE_IRI").mean(axis = 1))
    new_data = new_data.loc[:,[col for col in new_data.columns if col not in new_data.filter(regex = "^(L|R)_").columns]]
    
    return new_data.reset_index(drop = True)

def lane_to_side(data, name = None, side_name = 'side'):

    import numpy as np

    new_data = data.copy()
    if name == None:
        names = [col for col in data.columns if "xsp" in col.lower() or "lane" in col.lower()]
        if len(names) != 1:
            if len(names) == 0:
                return 'ERROR lane_to_side(name = None): Please specify the name of the `lane` column.'
            else:
                return print('ERROR lane_to_side(name = None): Please specify the name of the `lane` column.',  'Found:', names)
        else:
            name = names[0]            
    new_data.loc[:, side_name] = np.where(data[name].str.contains("^L"), 'L', 'R')
    return new_data.reset_index(drop = True)

def widen_by_lane(data, start, end, ids, grouping, xsp = 'xsp', side = 'side', reverse = True, keep_config = True, max_segment = 100):
    
    #0: Prep
    prep_df = data.loc[:, ids + [start, end] + grouping + [xsp, side]].copy() #0.1 Select only the columns of interest
    
    #1: Create Direction based dataframe
    prep_df.loc[:,xsp] = prep_df.loc[:,xsp].str[1:] #1.1 Remove the direction prefix from the xsp column 
    prep_df_l, prep_df_r = prep_df[prep_df.loc[:,side] == "L"], prep_df[prep_df.loc[:,side] == "R"] #1.2 Split into two frames, one for each direction.
    side_df = pd.concat([prep_df_l, prep_df_r]) #1.3 Concatenate the split frames together
    
    #2: Stretch dataframe into equal segment lengths
    stretch_size = gcd_list(asmetres(side_df[end]) - asmetres(side_df[start])) #Size by which to stretch into equal segments
    stretched_df = stretch(side_df, starts = [start], ends = [end], max_segment = stretch_size, sort = ids + [side, start])
    
    #3: Pivot frame on ID variables by xsp for selected grouping columns
    if keep_config:
        pivoted_df = stretched_df.pivot(index = ids + ['start', 'end', side], columns = xsp, values = grouping + [xsp]) #Keep dummy variables of the lanes if keep_config true
    else:
        pivoted_df = stretched_df.pivot(index = ids + ['start', 'end', side], columns = xsp, values = grouping)
        
    if reverse:
        pivoted_df = pivoted_df[sorted(pivoted_df.columns, reverse = True)]
        
    pivoted_df.columns = ['_'.join(col) for col in pivoted_df.columns]
    pivoted_df = pivoted_df.reset_index()
    
    compact_by = []
    for i in grouping:
        for col in pivoted_df.columns:
            if i in col:
                compact_by.append(col)
                
    #4: Compact by IDs + Direction
    lanes = [lane for lane in pivoted_df.columns if xsp in lane]
    if keep_config:
        compact_df = compact(pivoted_df, SLK = 'start', lanes = lanes, obs_length = stretch_size, idvars = ids + [side],  grouping = compact_by + [col for col in pivoted_df.columns if xsp in col])
    else:
        compact_df = compact(pivoted_df, SLK = 'start', lanes = lanes, obs_length = stretch_size, idvars = ids + [side],  grouping = compact_by)
    
    #5: Make into segment lengths of choice
    final_df = make_segments(compact_df, SLK_type = "true", true_start = "startstart", true_end = "startend", max_segment = max_segment)
    
    return final_df