import numpy as np
import pandas as pd 

##Deal with an SLK column as metres in integers to avoid the issue of calculating on floating numbers
def as_metres(var):

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
def stretch(data, start = None, end = None, start_true = None, end_true = None, segment_size = 'GCD', use_ranges = False, sort = None, as_km = True):

    starts = [i for i in [start, start_true] if i is not None]
    ends = [i for i in [end, end_true] if i is not None]
    names = [v for k,v in zip([start, start_true], ['SLK', 'true_SLK']) if k is not None]
    SLKs = starts + ends

    new_data = data.copy().reset_index(drop = True) #Copy of the dataset
    new_data = new_data.dropna(thresh = 2)  #drop any row that does not contain at least two non-missing values.
    
    if type(sort) == list:
        new_data =  new_data.sort_values(sort)

    #Change SLK variables to 32 bit integers of metres to avoid the issue with calculations on floating numbers
    new_data[SLKs] = new_data[SLKs].apply(as_metres)
    
    if segment_size == 'GCD':
        lengths = new_data[ends[0]] - new_data[starts[0]]
        segment_size = gcd_list(lengths)
        
    #Reshape the data into size specified in 'obs_length'
    new_data = new_data.reindex(new_data.index.repeat(np.ceil((new_data[ends[0]] - new_data[starts[0]])/segment_size))) #reindex by the number of intervals of specified length between the start and the end.
    
    #increment the start points by observation length
    for start_slk, end_slk, name in zip(starts, ends, names):
        new_data[name] = new_data[start_slk] +  new_data.groupby(level = 0).cumcount()*segment_size
        if as_km:
                new_data[name] = new_data[name]/1000               
        
    for start_slk, end_slk in zip(starts, ends):
    #End SLKs are equal to the lead Start SLKS except where the segment ends
        new_data[end_slk] = np.where((new_data[start_slk].shift(-1) - new_data[start_slk]) == segment_size, new_data[start_slk].shift(-1), new_data[end_slk])
        
    new_data = new_data.reset_index(drop = True)

    #Drop the variables no longer required
    if keep_ranges:
        if as_km:
            for SLK in SLKs:
                new_data[SLK] = new_data[SLK]/1000 
            new_data.rename(columns = {"SLK" : "SLK_m", "true_SLK": "true_SLK_m"})           
    else:
        new_data = new_data.drop([SLK for SLK in SLKs], axis = 1)
        new_data = new_data.reset_index(drop = True)

    return new_data

def get_segments(data, idvars, SLK = None, true_SLK = None, start = None, end = None, start_true = None, end_true = None, lane = None, grouping = True, summarise = True, as_km = True):

    new_data = data.copy()

    #Stretch the dataframe into equal length segments if is not already
    if not bool(SLK or true_SLK):
        new_data = stretch(data, start = start, end = end, start_true = start_true, end_true = end_true, as_km = False)
        if bool(start):
            SLK = 'SLK'
        if bool(start_true):
            true_SLK = 'true_SLK'
    
    #Detect whether operating on SLK, True SLK, or both.
    SLKs = [slk for slk in [true_SLK, SLK] if slk is not None]

    #Calculate the observation length 
    obs_length = new_data.loc[1, SLKs[0]] - new_data.loc[0, SLKs[0]]
    
    #Treat `lane` as empty list if not declared
    lane = []
    #By default, group by all columns other than the `idvars` (ID variables) 
    if grouping == True:
        grouping = [col for col in new_data.columns() if col not in idvars and col not in SLKs]
        if isinstance(summarise, dict):
            grouping = [col for col in new_data.columns() if col not in idvars and col not in SLKs and col not in list(summarise.keys())]
    #If grouping is False, the variable is an empty list
    if grouping == False:
        grouping = []

    #Sort by all columns in grouping, then by true SLK, then by SLK, then lane if declared
    new_data = new_data.sort_values(idvars + SLKs + lane).reset_index(drop = True)

    #Treat all NAs the same
    new_data.loc[:, grouping] = new_data.loc[:, grouping].fillna(-1)
    
    #If both SLK and true SLK are declared, create a column equal to the diference between the two, in order to check for Points of Equation by changes in the variable
    if bool(true_SLK and SLK):
        new_data['slk_diff'] = new_data[true_SLK] - new_data[SLK]
        new_data.insert(0, "groupkey", new_data.groupby(grouping + idvars + lane + ['slk_diff']).ngroup())
    else:
        new_data.insert(0, "groupkey", new_data.groupby(grouping + idvars + lane).ngroup())

    #Create lag and lead columns for SLK, true SLK, and the grouping key to check whether a new group has started
    for var in SLKs:
        new_data['lead_' + var] = new_data[var].shift(-1, fill_value = 0)
        new_data['lag_' + var] = new_data[var].shift(1, fill_value = 0)
    new_data['lead_groupkey'] = new_data['groupkey'].shift(-1, fill_value = 0)
    new_data['lag_groupkey'] = new_data['groupkey'].shift(1, fill_value = max(new_data['groupkey']))

    #Create columns based on whether they represent the start or end of a section.
    starts, ends = {}, {}
    
    for var in SLKs:
        starts[var] = np.where((new_data['lag_' + var] == (new_data[var]-obs_length)) & (new_data['lag_groupkey'] == new_data['groupkey']), False,True) #if the lagged SLKs are not one observation length less than the actual or the lagged group-key is different.
        ends[var] = np.where((new_data['lead_' + var] == (new_data[var] + obs_length)) & (new_data['lead_groupkey'] == new_data['groupkey']), False,True)  #if the lead SLKs are not one observation length more than the actual or the lead group-key is different.
    new_data.loc[:, 'start_bool'] = np.prod([i for i in starts.values()], axis = 0, dtype = bool)
    new_data.loc[:, 'end_bool'] = np.prod([i for i in ends.values()], axis = 0, dtype = bool)
    
    new_data['segment_id'] = new_data.groupby('groupkey')['start_bool'].cumsum()
    new_data['segment_id'] =  new_data.groupby(['groupkey', 'segment_id']).ngroup()
    
    new_data = new_data.drop(['start_bool', 'end_bool', 'groupkey'] + [col for col in new_data.columns if 'lag' in col or 'lead' in col], axis = 1)
    
    #Summarise the data by `segment_id`
    
    #By default, summarise the SLK into min and max columns, representing Start and End SLK respectively
    if bool(summarise):
        agg_dict = {SLK: [min, max] for SLK in SLKs }
    
    #If an aggregation dictionary is provided to `summarise`, add the methods to the SLK method detailed in the previous step
    if isinstance(summarise, dict):
        agg_dict.update(summarise)
    
    new_data = new_data.groupby(['segment_id'] + idvars + lane + grouping).agg(agg_dict)
    
    new_data.columns = ["_".join(x) for x in new_data.columns]
    new_data = new_data.rename(columns = {'SLK_min': 'START_SLK', 'SLK_max': 'END_SLK', 'true_SLK_min': 'START_TRUE', 'true_SLK_max': 'END_TRUE'})
    start_cols = [col for col in ['START_SLK', 'START_TRUE'] if col in new_data.columns] 
    
    end_cols = [col for col in ['END_SLK', 'END_TRUE'] if col in new_data.columns] 
    slk_cols = start_cols + end_cols
    
    #Increment slk_ends by the observation length
    for col in end_cols:
        new_data.loc[:,col] = new_data.loc[:,col] + obs_length

    if as_km:
        #Turn into km by default
        for col in slk_cols:
            new_data.loc[:,col] = new_data.loc[:,col]/1000
        
    #Add the groupbys back to the columns
    new_data = new_data.reset_index()
    
    #After the aggregations are done, the missing data can go back to being NaN
    new_data.loc[:, grouping] = new_data.loc[:, grouping].replace(-1, np.nan)
    new_data = new_data.sort_values([i for i in idvars + ['START_SLK', 'START_TRUE'] if i in new_data.columns]).reset_index(drop = True)
    new_data['segment_id'] = [i for i in range(len(new_data))]

    return new_data

def interval_merge(left_df, right_df, idvars = None, start = None, end = None, start_true = None, end_true = None, idvars_left = None, idvars_right = None, start_left = None,  end_left = None, start_right = None, end_right = None, start_true_left = None, end_true_left = None, start_true_right = None, end_true_right = None, grouping = True, summarise = True, km = True, use_ranges = True):
    
    if idvars is not None:
        idvars_left, idvars_right = idvars, idvars
    if start is not None:
        start_left, start_right = start, start
    if end is not None:
        end_left, end_right = end, end
    if start_true is not None:
        start_true_left, start_true_right = start_true, start_true
    if end_true is not None:
        end_true_left, end_true_right = end_true, end_true
    
    
    #Create copies as to not change the original data
    left_copy = left_df.copy()
    right_copy = right_df.copy()

    #Define the interval columns for the datasets 
    starts_left = [start for start in [start_left, start_true_left] if start != None]
    ends_left = [end for end in [end_left, end_true_left] if end != None]
    starts_right = [start for start in [start_right, start_true_right] if start != None]
    ends_right = [end for end in [end_right, end_true_right] if end != None]
    
    if km:
        #Convert SLKs to metres for easier operations
        left_metres = left_copy.loc[:,starts_left + ends_left].apply(as_metres)
        right_metres = right_copy.loc[:, starts_right + ends_right].apply(as_metres)
    
    #Find the greatest common divisor (GCD) of both of the dataframes in order to stretch into equal length segments
    gcds = []

    #Find the gcd for all start-end pairs
    #left
    for start, end in zip(starts_left, ends_left):
        gcds.append(gcd_list(left_metres[end] - left_metres[start]))
    #right    
    for start, end in zip(starts_right, ends_right):
        gcds.append(gcd_list(right_metres[end] - right_metres[start]))
    #Find the minimum
    gcd = min(gcds)
    
    
    #rename SLKs congruently
    slk_dict = {start_right: 'START_SLK', start_true_right: 'START_TRUE', end_right: 'END_SLK', end_true_right: 'END_TRUE', start_left: 'START_SLK', start_true_left: 'START_TRUE', end_left: 'END_SLK', end_true_left: 'END_TRUE'}
    

    left_copy = left_copy.rename(columns = slk_dict)
    right_copy = right_copy.rename(columns = slk_dict)
    
    #Don't include the missing parameters
    for key in list(slk_dict.keys()):
        if key == None:
            slk_dict[key] = None
            
    #Stretch both dataframes by the GCD   
    left_stretched = stretch(left_copy, start = slk_dict[start_left], end = slk_dict[end_left], start_true = slk_dict[start_true_left], end_true = slk_dict[end_true_left], as_km = False, keep_ranges = use_ranges)
    
    right_stretched = stretch(right_copy, start = slk_dict[start_right], end = slk_dict[end_right], start_true = slk_dict[start_true_right], end_true = slk_dict[end_true_right], as_km = False)
    
    #index by mutual ID variables and stretched SLKs
    
    id_len = min(len(idvars_left),len(idvars_right))#max number of mutual IDs
    #Rename the right ID Vars to be congruent with the left 
    id_dict = dict(zip(idvars_right[0:id_len], idvars_left[0:id_len]))
    right_stretched = right_stretched.rename(columns = id_dict)
    idvars = idvars_left[0:id_len] #Now that they have the same names, the mutual ID variables are the same as the parameter idvars_left to the maximum mutual length of the idvars 
    left_stretched = left_stretched.set_index(idvars + [col for col in ['SLK', 'true_SLK'] if col in left_stretched.columns])
    right_stretched = right_stretched.set_index(idvars + [col for col in ['SLK', 'true_SLK'] if col in right_stretched.columns])
    
    #join by index
    joined = left_stretched.join(right_stretched, how = 'left')
    joined = joined[~joined.index.duplicated(keep = 'first')].reset_index()

    if use_ranges:
        #change the name of the original SLKs before creating segments to avoid confusion
        slks = [i for i in list(slk_dict.values()) if i in joined.columns]
        joined.columns = ['org_' + col if col in slks else col for col in joined.columns]
    else:
        slks = []

    for col in [col for col in joined.columns if 'org' in col]:
        joined[col] /= 1000

    if bool(grouping) == True:
        grouping = [col for col in joined.columns if col not in ['true_SLK', 'SLK'] + idvars]
        if isinstance(summarise, dict):
            grouping = [col for col in joined.columns if col not in ['true_SLK', 'SLK'] + idvars + list(summarise.keys())]

    else:
        grouping = []

    #compact
    segments = get_segments(joined, true_SLK = 'true_SLK' if 'true_SLK' in joined.columns else None, SLK = 'SLK' if 'SLK' in joined.columns else None, idvars = idvars, grouping = grouping, as_km = True, summarise = summarise)
    
    #Drop the duplicates of the SLK columns caused by `get_segments` if the original ranges are being used for the segments
    if use_ranges:
        segments = segments.drop(slks, axis = 1)
        segments.columns = [col[4:] if col[:4]== "org_" else col for col in segments.columns]

    return segments
    
def make_segments(data, start = None, end = None, start_true = None, end_true = None, max_segment = 100, split_ends = True, as_km = True):
    
    starts = [var for var in [start, start_true] if bool(var)]
    ends = [var for var in [end, end_true] if bool(var)]
    
    new_data = data.copy() #Copy of the dataset

    SLKs = [slk for slk in starts + ends if slk is not None]
    
    new_data = new_data.dropna(thresh = 2)  #drop any row that does not contain at least two non-missing values.
    
    #Change SLK variables to 32 bit integers of metres to avoid the issue with calculations on floating numbers
    for var in SLKs:
        new_data[var] = as_metres(new_data[var])

    new_data['Length'] = new_data[ends[0]] - new_data[starts[0]]

    if bool(split_ends):
        new_data['start_end'] = np.where(new_data['Length'] <= max_segment, True, False)
    
    #Reshape the data into size specified in 'max_segment'
    new_data = new_data.reindex(new_data.index.repeat(np.ceil((new_data[ends[0]] - new_data[starts[0]])/max_segment))) #reindex by the number of intervals of specified length between the start and the end.
    
    for start_,end_ in zip(starts, ends):
    #Increment the start rows by the segment size
        new_data[start_] = (new_data[start_] + new_data.groupby(level=0).cumcount()*max_segment) 
    #End SLKs are equal to the lead Start SLKS except where the segment ends
        new_data[end_] = np.where((new_data[start_].shift(-1) - new_data[start_]) == max_segment, new_data[start_].shift(-1), new_data[end_])

    #Check for minimum segment lengths
    if bool(split_ends):
        for start_,end_ in zip(starts, ends):
            #where the difference between the `end` and `start` is less than the minimum segment size and isn't a start_end, subtract the difference from the `start` and set the same value as the previous `end`
            new_data['too_short'] = np.where(((new_data[end_] - new_data[start_]) < max_segment) & (new_data['start_end'] == False), True, False)
            new_data[end_] = np.where(new_data['too_short'].shift(-1) == True, (new_data[end_].shift(-1) + new_data[start_])/2, new_data[end_])
            new_data[start_] = np.where(new_data['too_short'] == True, new_data[end_].shift(1),  new_data[start_])
            #Drop the boolean columns
            new_data = new_data.drop(['start_end', 'too_short'], axis = 1) 
   
    if as_km:
        #Convert SLK variables back to km
        for var in SLKs:
            new_data[var] = new_data[var]/1000

    #recalculate length   
    new_data['Length'] = new_data[ends[0]] - new_data[starts[0]]
    new_data = new_data.reset_index(drop = True)

    new_data['segment_id'] = [i for i in range(len(new_data))]

    return new_data
    
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

def hsd_to_side(data, cway_name = None, side_name = 'side'):
    
    import numpy as np

    new_data = cway_to_side(data, name = cway_name)
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
    stretch_size = gcd_list(as_metres(side_df[end]) - as_metres(side_df[start])) #Size by which to stretch into equal segments
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
    final_df = make_segments(compact_df, SLK_type = "true", start_true = "startstart", end_true = "startend", max_segment = max_segment)
    
    return final_df

    