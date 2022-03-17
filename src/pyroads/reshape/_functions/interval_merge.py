from .get_segments import get_segments
from .stretch import stretch
from ._as_metres import as_metres
import numpy as np


def interval_merge(left_df, right_df, idvars=None, start=None, end=None, start_true=None, end_true=None, idvars_left=None, idvars_right=None, start_left=None, end_left=None, start_right=None, end_right=None, start_true_left=None, end_true_left=None, start_true_right=None, end_true_right=None, grouping='left', summarise=True, km=True, use_ranges=True, id = False):
	
	#Set all of the right and left variables depending on what parameters the user chose
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

	# Define the interval columns for the datasets
	starts_left = [start for start in [start_left, start_true_left] if start is not None]
	ends_left = [end for end in [end_left, end_true_left] if end != None]
	starts_right = [start for start in [start_right, start_true_right] if start is not None]
	ends_right = [end for end in [end_right, end_true_right] if end is not None]
	slk_interval_cols = starts_left + starts_right + ends_left + ends_right
	
	# Create copies as to not change the original data
	left_copy = left_df.copy()
	right_copy = right_df.copy()
	#drop segment id columns if in datasets
	if 'segment_id' in left_df.columns:
		left_copy = left_copy.drop('segment_id', axis=1)
	if 'segment_id' in right_copy.columns:
		right_copy = right_copy.drop('segment_id', axis=1)
	

	# Get a list of all the columns that will be used, and drop all of the rest

	## List of columns that will be summarised on
	if isinstance(summarise, dict):
		summarise_cols = list(summarise.keys())
	else:
		summarise_cols = []		
	## List of ID variables
	### Find the number of ID variables to join on
	id_len = min(len(idvars_left), len(idvars_right))  # max number of mutual IDs
	### Rename the right ID Vars to be congruent with the left
	id_dict = dict(zip(idvars_right[0:id_len], idvars_left[0:id_len]))
	right_copy = right_copy.rename(columns=id_dict)
	idvars = idvars_left[0:id_len]
	## List of columns that will be grouped on
	if isinstance(grouping, list):
		grouping = grouping
	elif grouping == True:
		grouping = [col for col in [col for col in left_copy.columns]  + [col for col in right_copy.columns] if col not in slk_interval_cols + idvars + summarise_cols] + [var for var in idvars_left + idvars_right if var not in idvars]
	elif grouping == 'left':
		grouping = [col for col in left_copy.columns if col not in starts_left + ends_left + idvars_left + summarise_cols] + [var for var in idvars_left if var not in idvars]
	elif grouping == 'right':
		grouping = [col for col in right_copy.columns if col not in starts_right + ends_right + idvars + summarise_cols] + [var for var in idvars_left if var not in idvars]
	else:
		grouping = []
	
	## Add the lists of idvars, summarise_cols, slk_intervals, and grouping together as the columns to keep
	keep_cols = idvars+ slk_interval_cols + grouping + summarise_cols

	## Drop everything outside of the keep cols
	right_copy = right_copy.loc[:, [col for col in right_copy.columns if col in keep_cols]]
	left_copy = left_copy.loc[:, [col for col in left_copy.columns if col in keep_cols]]
	
	# Stretch both dataframes by a shared Greatest Common Divisor (GCD) of lengths

	##If data is provided in km convert SLKs to integer metres for easier operations
	if km:
		left_metres = left_copy.loc[:, starts_left + ends_left].apply(as_metres)
		right_metres = right_copy.loc[:, starts_right + ends_right].apply(as_metres)
	
	## Find the GCD
	### Create a list of all the lengths	
	# left
	for start, end in zip(starts_left, ends_left):
		lengths = tuple(left_metres[end] - left_metres[start])
	# right
	for start, end in zip(starts_right, ends_right):
		lengths = lengths + tuple(right_metres[end] - right_metres[start])
	# Find the minimum
	gcd = np.gcd.reduce(lengths)
	
	# rename SLKs congruently
	#Dictionary with which to rename the SLK columns
	slk_dict = {
		start_right: 'START_SLK' if bool(start_right) else None, 
		start_true_right: 'START_TRUE' if bool(start_true_right) else None, 
		end_right: 'END_SLK' if bool(end_right) else None, 
		end_true_right: 'END_TRUE' if bool(end_true_right) else None, 
		start_left: 'START_SLK' if bool(start_left) else None, 
		start_true_left: 'START_TRUE' if bool(start_true_left) else None, 
		end_left: 'END_SLK' if bool(end_left) else None, 
		end_true_left: 'END_TRUE' if bool(end_true_left) else None}
	##Don't include the missing parameters
	for key in list(slk_dict.keys()):
		if key == None:
			slk_dict[key] = None
	##rename
	left_copy = left_copy.rename(columns=slk_dict)
	right_copy = right_copy.rename(columns=slk_dict)
	
	#Stretch
	left_stretched = stretch(left_copy, start=slk_dict[start_left], end=slk_dict[end_left], start_true=slk_dict[start_true_left], end_true=slk_dict[end_true_left], segment_size = gcd, as_km=False, keep_ranges=use_ranges)	
	right_stretched = stretch(right_copy, start=slk_dict[start_right], end=slk_dict[end_right], start_true=slk_dict[start_true_right], end_true=slk_dict[end_true_right], segment_size = gcd, as_km=False)
	
	# Now that they have the same names, the mutual ID variables are the same as the parameter idvars_left to the maximum mutual length of the idvars
	left_stretched = left_stretched.set_index(idvars + [col for col in ['SLK', 'true_SLK'] if col in left_stretched.columns])
	right_stretched = right_stretched.set_index(idvars + [col for col in ['SLK', 'true_SLK'] if col in right_stretched.columns])
	
	# join by index
	joined = left_stretched.join(right_stretched, how='left')

	if use_ranges:
		# change the name of the original SLKs before creating segments to avoid confusion
		slks = list(set([i for i in list(slk_dict.values()) if i in joined.columns]))
		joined.columns = ['org_' + col if col in slks else col for col in joined.columns]
		org_slks = ['org_' + i for i in slks]
		idvars = idvars + org_slks
		joined = joined.reset_index()
	else:
		slks = []
		joined = joined[~joined.index.duplicated(keep='first')].reset_index()

	summarised_df = get_segments(joined, true_SLK='true_SLK' if 'true_SLK' in joined.columns else None, SLK='SLK' if 'SLK' in joined.columns else None, idvars=idvars, grouping=grouping, as_km=True, summarise=summarise, id = id)
	
	# Drop the duplicates of the SLK columns caused by `get_segments` if the original ranges are being used for the segments
	if use_ranges:
		summarised_df = summarised_df.drop(slks, axis=1)
		summarised_df.columns = [col[4:] if col[:4] == "org_" else col for col in summarised_df.columns]
		summarised_df[slks] = summarised_df[slks] / 1000
	
	#Order the columns to be the idvars followed by the SLKs
	for slk in slks:
		x = 0
		summarised_df.insert(len(idvars) + x, slk, summarised_df.pop(slk))
		x = x + 1
	
	summarised_df = summarised_df.drop_duplicates()
	summarised_df = summarised_df.reset_index(drop=True)
	
	return summarised_df
