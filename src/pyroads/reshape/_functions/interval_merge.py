from .get_segments import get_segments
from .stretch import stretch
from ._as_metres import as_metres
import numpy as np


def interval_merge(left_df, right_df, idvars=None, start=None, end=None, start_true=None, end_true=None, idvars_left=None, idvars_right=None, start_left=None, end_left=None, start_right=None, end_right=None, start_true_left=None, end_true_left=None, start_true_right=None, end_true_right=None, grouping=True, summarise=True, km=True, use_ranges=True, id = True):
	if idvars is not None:
		if idvars_left == None:
			idvars_left = idvars
		else:
			if isinstance(idvars_left, str) and len(idvars_left.split()) == 1:
				idvars_left = [idvars_left]
			idvars_left = idvars + idvars_left
		if idvars_right == None:
			idvars_right = idvars
		else:
			if isinstance(idvars_right, str) and len(idvars_right.split()) == 1:
				idvars_right = [idvars_right]
			idvars_right = idvars + idvars_right
	if start is not None:
		start_left, start_right = start, start
	if end is not None:
		end_left, end_right = end, end
	if start_true is not None:
		start_true_left, start_true_right = start_true, start_true
	if end_true is not None:
		end_true_left, end_true_right = end_true, end_true
	
	# drop segment id columns if in datasets
	if 'segment_id' in left_df.columns:
		left_df = left_df.drop('segment_id', axis=1)
	if 'segment_id' in right_df.columns:
		right_df = right_df.drop('segment_id', axis=1)
	
	# Create copies as to not change the original data
	left_copy = left_df.copy()
	right_copy = right_df.copy()
	
	# Define the interval columns for the datasets
	starts_left = [start for start in [start_left, start_true_left] if start != None]
	ends_left = [end for end in [end_left, end_true_left] if end != None]
	starts_right = [start for start in [start_right, start_true_right] if start != None]
	ends_right = [end for end in [end_right, end_true_right] if end != None]
	
	if km:
		# Convert SLKs to metres for easier operations
		left_metres = left_copy.loc[:, starts_left + ends_left].apply(as_metres)
		right_metres = right_copy.loc[:, starts_right + ends_right].apply(as_metres)
	
	# Find the greatest common divisor (GCD) of both of the dataframes in order to stretch into equal length segments
	gcds = []
	
	# Find the gcd for all start-end pairs
	# left
	for start, end in zip(starts_left, ends_left):
		gcds.append(np.gcd.reduce(left_metres[end] - left_metres[start]))
	# right
	for start, end in zip(starts_right, ends_right):
		gcds.append(np.gcd.reduce(right_metres[end] - right_metres[start]))
	# Find the minimum
	gcd = min(gcds)
	
	# rename SLKs congruently
	slk_dict = {
		start_right: 'START_SLK' if bool(start_right) else None, 
		start_true_right: 'START_TRUE' if bool(start_true_right) else None, 
		end_right: 'END_SLK' if bool(end_right) else None, 
		end_true_right: 'END_TRUE' if bool(end_true_right) else None, 
		start_left: 'START_SLK' if bool(start_left) else None, 
		start_true_left: 'START_TRUE' if bool(start_true_left) else None, 
		end_left: 'END_SLK' if bool(end_left) else None, 
		end_true_left: 'END_TRUE' if bool(end_true_left) else None}
	
	left_copy = left_copy.rename(columns=slk_dict)
	right_copy = right_copy.rename(columns=slk_dict)
	
	# Don't include the missing parameters
	for key in list(slk_dict.keys()):
		if key == None:
			slk_dict[key] = None
	
	# Stretch both dataframes by the GCD
	left_stretched = stretch(left_copy, start=slk_dict[start_left], end=slk_dict[end_left], start_true=slk_dict[start_true_left], end_true=slk_dict[end_true_left], as_km=False, keep_ranges=use_ranges)
	
	right_stretched = stretch(right_copy, start=slk_dict[start_right], end=slk_dict[end_right], start_true=slk_dict[start_true_right], end_true=slk_dict[end_true_right], as_km=False)
	
	# index by mutual ID variables and stretched SLKs
	id_len = min(len(idvars_left), len(idvars_right))  # max number of mutual IDs
	# Rename the right ID Vars to be congruent with the left
	id_dict = dict(zip(idvars_right[0:id_len], idvars_left[0:id_len]))
	right_stretched = right_stretched.rename(columns=id_dict)
	idvars = idvars_left[0:id_len]  # Now that they have the same names, the mutual ID variables are the same as the parameter idvars_left to the maximum mutual length of the idvars
	left_stretched = left_stretched.set_index(idvars + [col for col in ['SLK', 'true_SLK'] if col in left_stretched.columns])
	right_stretched = right_stretched.set_index(idvars + [col for col in ['SLK', 'true_SLK'] if col in right_stretched.columns])
	
	# join by index
	joined = left_stretched.join(right_stretched, how='left')
	
	if use_ranges:
		# change the name of the original SLKs before creating segments to avoid confusion
		slks = list(set([i for i in list(slk_dict.values()) if i in joined.columns]))
		joined.columns = ['org_' + col if col in slks else col for col in joined.columns]
		org_slks = ['org_' + i for i in slks]
		joined = joined.set_index(org_slks, append=True).reset_index([i for i in ['SLK', 'true_SLK'] if i in joined.index.names])
	else:
		slks = []
	
	if isinstance(grouping, list):
		grouping = grouping
		if use_ranges:
			grouping = grouping + org_slks
	elif grouping == True:
		if isinstance(summarise, dict):
			grouping = [col for col in joined.columns if col not in ['true_SLK', 'SLK'] + idvars + list(summarise.keys())]
		else: 
			grouping = [col for col in joined.columns if col not in ['true_SLK', 'SLK'] + idvars]
		if use_ranges:
			grouping = grouping + org_slks
	else:
		grouping = []
		if use_ranges:
			grouping = grouping + org_slks
	
	if use_ranges:
		joined = joined.reset_index()
	else:
		joined = joined[~joined.index.duplicated(keep='first')].reset_index()

	segments = get_segments(joined, true_SLK='true_SLK' if 'true_SLK' in joined.columns else None, SLK='SLK' if 'SLK' in joined.columns else None, idvars=idvars, grouping=grouping, as_km=True, summarise=summarise, id = id)
	
	# Drop the duplicates of the SLK columns caused by `get_segments` if the original ranges are being used for the segments
	if use_ranges:
		segments = segments.drop(slks, axis=1)
		segments.columns = [col[4:] if col[:4] == "org_" else col for col in segments.columns]
		segments[slks] = segments[slks] / 1000
	
	segments = segments.reset_index(drop=True)
	
	return segments
