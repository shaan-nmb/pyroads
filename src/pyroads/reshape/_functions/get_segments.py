import numpy as np

from .stretch import stretch


def get_segments(data, idvars, SLK=None, true_SLK=None, start=None, end=None, start_true=None, end_true=None, lane=None, grouping=False, summarise=True, as_km=True):
	"""Aggregates observations into sections of specified lengths. Optionally the output can simply return ids for future aggregation"""
	new_data = data.copy()

	#If using point parameters, make sure they are in metres
	if km and bool(SLK or true_SLK):
		data.loc[:, SLKs]*1000
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
	
	# grouping - the variables for which to ensure are not broken between segments
	if bool(grouping):
		# If grouping is on, group by all columns other than the `idvars` (ID variables)
		if grouping == True:
			grouping = [col for col in new_data.columns if col not in idvars and col not in SLKs]
			if isinstance(summarise, dict):
				grouping = [col for col in new_data.columns if col not in idvars and col not in SLKs and col not in list(summarise.keys())]
			
			# Treat grouping as list if a single label is given
		if isinstance(grouping, str) and len(grouping.split()) == 1:
			grouping = [grouping]
		
		# Treat all NAs the same
		new_data.loc[:, grouping] = new_data.loc[:, grouping].fillna(-1)
	
	# If grouping is False, the variable is an empty list
	else:
		grouping = []
	
	# Sort by all columns in grouping, then by true SLK, then by SLK, then lane if declared
	new_data = new_data.sort_values(idvars + lane + SLKs).reset_index(drop=True)
	
	# If both SLK and true SLK are declared, create a column equal to the diference between the two, in order to check for Points of Equation by changes in the variable
	if bool(true_SLK and SLK):
		new_data['slk_diff'] = new_data[true_SLK] - new_data[SLK]
		new_data.insert(0, "groupkey", new_data.groupby(grouping + idvars + lane + ['slk_diff']).ngroup())
	else:
		new_data.insert(0, "groupkey", new_data.groupby(grouping + idvars + lane).ngroup())
	
	# Create lag and lead columns for SLK, true SLK, and the grouping key to check whether a new group has started
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
		new_data = new_data.groupby(['segment_id'] + idvars + lane + grouping).agg(agg_dict)
	
	new_data.columns = ["_".join(x) for x in new_data.columns]
	new_data = new_data.rename(columns={'SLK_min': 'START_SLK', 'SLK_max': 'END_SLK', 'true_SLK_min': 'START_TRUE', 'true_SLK_max': 'END_TRUE'})
	
	start_cols = [col for col in ['START_SLK', 'START_TRUE'] if col in new_data.columns]
	end_cols = [col for col in ['END_SLK', 'END_TRUE'] if col in new_data.columns]
	
	slk_cols = start_cols + end_cols
	
	# Increment slk_ends by the observation length
	for col in end_cols:
		new_data[col] = new_data[col] + obs_length
	
	if as_km:
		# Turn into km by default
		for col in slk_cols:
			new_data[col] = new_data[col] / 1000
	
	if bool(summarise):
		# Add the groupbys back to the columns
		new_data = new_data.reset_index('segment_id', drop=True)
		new_data = new_data.reset_index()
	
	# After the aggregations are done, the missing data can go back to being NaN
	new_data.loc[:, grouping] = new_data.loc[:, grouping].replace(-1, np.nan)
	new_data = new_data.sort_values(idvars + lane + start_cols)
	new_data['segment_id'] = [i for i in range(len(new_data))]
	new_data = new_data.reset_index(drop=True)
	
	return new_data
