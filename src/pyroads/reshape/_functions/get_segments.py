from typing import Optional, Union, Literal
import numpy as np
import pandas as pd

from .stretch import stretch

def get_segments(
	data: pd.DataFrame, 
	id_vars: Union[str, list],
	SLK: Optional[str] = None, 
	true_SLK: Optional[str] = None, 
	start: Optional[str] = None, 
	end: Optional[str] = None, 
	start_true: Optional[str] = None, 
	end_true: Optional[str] = None, 
	lane: Optional[str] = None, 
	split_at: Union[str, list, bool] = False,
	km: bool = True, 
	summarise: Union[dict, bool] = True, 
	as_km: bool = True,  
	id: bool = False
	) -> 'pd.DataFrame':

	"""
	Aggregates observations into segments grouped by categorical variables.
	Optionally, the output can simply return IDs for future aggregation.

	One of the following combinations must be provided:

	* `SLK`
	* `true_SLK`
	* `SLK` and `true_SLK`
	* `start` and `end`
	* `start_true` and `end_true`
	* `start`, `end`, `start_true` and `end_true`

	Parameters:
	-----------
	data (DataFrame):  Dataframe containing the data to be reshaped
	id_vars:		   Column(s) to use as identifier variables for the linear reference points.
	SLK:               Column name of the linear reference point.
	true_SLK:          Column name of the linear reference point, in true distance.
	start (label):       Column name of the start of the segment.
	end (label):         Column name of the end of the segment.
	start_true (label):  Column name of the start of the segment, in true distance.
	end_true (label):    Column name of the end of the segment, in true distance.
	lane (label):        Column name containing the lane information.
	split_at (label, list of labels, or bool): Variable(s) other than the `id_vars` by which to split segments where there is discontinuity. If true, all non-reference point labels will be used. If false, splits are only done at discontinuities in `id_vars` and gaps/overlaps in the linear references.	
	km (bool):         Whether of not the linear reference columns are measured in kilometres.

	Returns:
		(DataFrame) new dataframe with containing all input columns segmented by and summarised on.
	
	Examples
	--------

	>>> df = pd.DataFrame({'ROAD': ['H001', 'H001', 'H001', 'H001', 'H001', 'H001'], 'CWAY': ['S', 'S', 'S', 'S', 'L', 'L'],
	'START_SLK': [12.2, 12.25, 12.3, 12.6, 0, 0.4], 
	'END_SLK': [12.25, 12.3, 12.6, 13, 0.4, 0.86],
	'ROUGHNESS': [0.2, 0.6, 0.7, 0.5, 2, 3],
	'CAT': ['a', 'b', 'b', 'c', 'f', 'f'],
	'SURF_TYPE': ['Asphalt', 'Asphalt', 'Seal', 'Seal', 'Seal', 'Seal']})
	>>> df
	    ROAD  CWAY START_SLK  END_SLK  SURF_TYPE
	0   H001    S   12.2      12.25    Asphalt
	1   H001    S   12.25     12.3     Asphalt
	2   H001    S   12.3      12.6     Seal
	3   H001    S   12.6      13       Seal
	4   H001    L   0         0.4      Seal
	5   H001    L   0.4       0.86     Seal
			
	Get segments for `id_vars` `ROAD` and `CWAY` only.
	>>> get_segments(df, id_vars = ['ROAD', 'CWAY'], start = 'START_SLK', end = 'END_SLK')
	    ROAD  CWAY  START_SLK  END_SLK
	0   H001   L    0.0        0.86
	1   H001   S    12.2       13

	Get segments for `id_vars` `ROAD` and `CWAY` split at changes in `SURF_TYPE`.
	>>> get_segments(df, id_vars = ['ROAD', 'CWAY'], start = 'START_SLK', end = 'END_SLK')
	    ROAD  CWAY  START_SLK  END_SLK SURF_TYPE
	0   H001   L      0.0       0.86    Seal
	1   H001   S      12.2      12.3    Asphalt
	2   H001   S      12.3      13      Seal

	Split at changes in `SURF_TYPE`, and summarise `ROUGHNESS` and `CAT` by mean, and most common values respectively.
	>>> from pyroads.calc import most_common
	>>> reshape.get_segments(df, id_vars = ['ROAD', 'CWAY'], start = 'START_SLK', end = 'END_SLK', summarise= {'ROUGHNESS': 'mean', 'CAT': most_common}, id = True)	    
	    ROAD  CWAY  START_SLK  END_SLK  SURF_TYPE  ROUGHNESS_mean  CAT_most_common
	0   H001   L     0.0	   0.86      Seal       2.534884         f
	1   H001   S     12.2	   12.3      Seal       0.400000         a
	2   H001   S     12.3	   13        Seal       0.585714         c
	"""
	new_data = data.copy()

	#If using point parameters, make sure they are in metres
	if km and bool(SLK or true_SLK):
		data.loc[:, [slk for slk in [true_SLK, SLK] if slk is not None]]*1000
	
	# Stretch the dataframe into equal length segments if is not already
	if not bool(SLK or true_SLK):
		new_data = stretch(data, start=start, end=end, start_true=start_true, end_true=end_true, as_km=False)
		if bool(start):
			SLK = 'SLK'
		if bool(start_true):
			true_SLK = 'true_SLK'
	
	# Detect whether operating on SLK, True SLK, or both.
	SLKs = [slk for slk in [true_SLK, SLK] if slk is not None]
	
	# Calculate the observation length
	obs_length = new_data.loc[1, SLKs[0]] - new_data.loc[0, SLKs[0]]
	
	if bool(lane):
		lane = [lane]
	# Treat `lane` as empty list if not declared
	else:
		lane = []
	
	# split_at - the variables for which to ensure are not broken between segments
	if bool(split_at):
		# If split_at is on, group by all columns other than the `id_vars` (ID variables)
		if split_at == True:
			split_at = [col for col in new_data.columns if col not in id_vars + SLKs + lane]
			if isinstance(summarise, dict):
				split_at = [col for col in new_data.columns if col not in id_vars + SLKs + lane + list(summarise.keys())]
			
			# Treat split_at as list if a single label is given
		if isinstance(split_at, str) and len(split_at.split()) == 1:
			split_at = [split_at]
		
		# Treat all NAs the same
		new_data.loc[:, split_at] = new_data.loc[:, split_at].fillna(-1)

	# If split_at is False, the variable is an empty list
	else:
		split_at = []
	
	# Sort by all columns in split_at, then by true SLK, then by SLK, then lane if declared
	new_data = new_data.sort_values(id_vars + lane + SLKs).reset_index(drop=True)
	
	#Check for Points of Equation
	#If both SLK and true SLK are declared, create a column equal to the diference between the two abd check for changes in the variable
	if bool(true_SLK and SLK):
		new_data['slk_diff'] = new_data[true_SLK] - new_data[SLK]
		new_data.insert(0, "groupkey", new_data.groupby(split_at + id_vars + lane + ['slk_diff']).ngroup())
	else:
		new_data.insert(0, "groupkey", new_data.groupby(split_at + id_vars + lane).ngroup())
	
	# Create lag and lead columns for SLK, true SLK, and the split_at key to check whether a new group has started
	for var in SLKs:
		new_data['lead_' + var] = new_data[var].shift(-1, fill_value=0)
		new_data['lag_' + var] = new_data[var].shift(1, fill_value=0)
	new_data['lead_groupkey'] = new_data['groupkey'].shift(-1, fill_value=0)
	new_data['lag_groupkey'] = new_data['groupkey'].shift(1, fill_value=max(new_data['groupkey']))
	# Create columns based on whether they represent the start or end of a section.
	starts, ends = {}, {}
	
	for var in SLKs:
		starts[var] = np.where((new_data['lag_' + var] == (new_data[var] - obs_length)) & (new_data['lag_groupkey'] == new_data['groupkey']), False, True)  # if the lagged SLKs are not one observation length less than the actual or the lagged group-key is different.
		ends[var] = np.where((new_data['lead_' + var] == (new_data[var] + obs_length)) & (new_data['lead_groupkey'] == new_data['groupkey']), False, True)  # if the lead SLKs are not one observation length more than the actual or the lead group-key is different.
	new_data.loc[:, 'start_bool'] = np.prod([i for i in starts.values()], axis=0, dtype=bool)
	new_data.loc[:, 'end_bool'] = np.prod([i for i in ends.values()], axis=0, dtype=bool)
	
	new_data['segment_id'] = new_data.groupby('groupkey')['start_bool'].cumsum()
	new_data['segment_id'] = new_data.groupby(['groupkey', 'segment_id']).ngroup()
	
	new_data = new_data.drop(['start_bool', 'end_bool', 'groupkey'] + [col for col in new_data.columns if 'lag' in col or 'lead' in col], axis=1)
	
	# Summarise the data by `segment_id`

	# By default, summarise the SLK into min and max columns, representing Start and End SLK respectively
	if bool(summarise):
		agg_dict = {SLK: [min, max] for SLK in SLKs}
	
	# If an aggregation dictionary is provided to `summarise`, add the methods to the SLK method detailed in the previous step
	if isinstance(summarise, dict):
		agg_dict.update(summarise)
		
	if bool(summarise):
		new_data = new_data.groupby(['segment_id'] + id_vars + lane + split_at).agg(agg_dict)
		new_data.columns = ["_".join(x) for x in new_data.columns]
		new_data = new_data.rename(columns={'SLK_min': 'START_SLK', 'SLK_max': 'END_SLK', 'true_SLK_min': 'START_TRUE', 'true_SLK_max': 'END_TRUE'})
	start_cols = [col for col in ['START_SLK', 'START_TRUE'] if col in new_data.columns]
	end_cols = [col for col in ['END_SLK', 'END_TRUE'] if col in new_data.columns]
	
	slks = start_cols + end_cols
	
	# Increment slk_ends by the observation length
	for col in end_cols:
		new_data[col] = new_data[col] + obs_length
	
	if as_km:
		# Turn into km by default
		for col in slks:
			new_data[col] = new_data[col] / 1000
	
	if bool(summarise):
		# Add the groupbys back to the columns
		new_data = new_data.reset_index('segment_id', drop=True)
		new_data = new_data.reset_index()
	
	# After the aggregations are done, the missing data can go back to being NaN
	new_data.loc[:, split_at] = new_data.loc[:, split_at].replace(-1, np.nan)
	new_data = new_data.sort_values(id_vars + lane + start_cols)

	#Order the columns to be the id_vars followed by the SLKs
	for slk in slks:
		x = 1
		new_data.insert(len(id_vars) + x, slk, new_data.pop(slk))
		x = x + 1

	if id:
		new_data['segment_id'] = [i for i in range(len(new_data))]

	#sort columns
	new_data = new_data[id_vars + slks + lane + split_at + [col for col in new_data.columns if col not in id_vars + slks + lane + split_at]]

	new_data = new_data.reset_index(drop=True)
	
	return new_data