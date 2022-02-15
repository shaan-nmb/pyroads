import numpy as np

from pyroads.reshape._functions._as_metres import as_metres

def make_segments(data, start=None, end=None, start_true=None, end_true=None, max_segment=100, split_ends=True, as_km=True, id = False):
	"""
	Takes larger segments of variable length and splits them into a smaller regular length.
	The attributes of each original observation are repeated for each split output segment.
	Some segments at the end may be shorter than the specified length

	Args:
		data (DataFrame):  Dataframe containing the data to be reshaped
		start (str):       Column name of the start of the segment
		end (str):         Column name of the end of the segment
		start_true (str):  Column name of the start of the segment, in true distance
		end_true (str):    Column name of the end of the segment, in true distance
		max_segment (int): Maximum segment length of output. Segments in the input that are already shorter may not be split.
		split_ends (bool): Prevent the last segment being very short by combining it with the second last segment then splitting that segment in half
		as_km (bool):      If True, the start and end columns are assumed to be in km, and are converted to metres before being used.

	Returns:
		(DataFrame) new dataframe with the same columns as the input, but with additional repeated rows such that the the segmentation length now no longer than `max_segment`.
	"""

	starts = [var for var in [start, start_true] if bool(var)]
	ends = [var for var in [end, end_true] if bool(var)]
	
	new_data = data.copy()  # Copy of the dataset
	
	SLKs = [slk for slk in starts + ends if slk is not None]
	
	new_data = new_data.dropna(thresh=2)  # drop any row that does not contain at least two non-missing values.
	
	# Change SLK variables to 32 bit integers of metres to avoid the issue with calculations on floating numbers
	for var in SLKs:
		new_data[var] = as_metres(new_data[var])
	
	new_data.insert(len(new_data.columns) - 1, 'Length', new_data[ends[0]] - new_data[starts[0]])
	
	# Reshape the data into size specified in 'max_segment'
	new_data = new_data.reindex(new_data.index.repeat(np.ceil((new_data[ends[0]] - new_data[starts[0]]) / max_segment)))  # reindex by the number of intervals of specified length between the start and the end.
	
	if bool(start) and bool(start_true):
		for start_, end_ in zip(starts, ends):
			# Increment the start rows by the segment size
			new_data[start_] = (new_data[start_] + new_data.groupby(level=0).cumcount() * max_segment)
			new_data[end_] = np.where((new_data[start_].shift(-1) - new_data[start_]) == max_segment, new_data[start_].shift(-1), new_data[end_])
	else:
		for start_, end_ in zip(starts, ends):
			# Increment the start rows by the segment size
			new_data[start_] = (new_data[start_] + new_data.groupby(level=0).cumcount() * max_segment)
			new_data[end_] = np.where((new_data[start_].shift(-1) - new_data[start_]) == max_segment, new_data[start_].shift(-1), new_data[end_])

	# Check for minimum segment lengths
	if split_ends:
		for start_, end_ in zip(starts, ends):
			#A start_end is any observation that is too small to be split, hence it is both the start and the end of a segment.	
			new_data['start_end'] = np.where(new_data['Length'] <= max_segment, True, False)
			# where the difference between the `end` and `start` is less than the minimum segment size and isn't a start_end, subtract the difference from the `start` and set the same value as the previous `end`
			new_data['too_short'] = np.where(((new_data[end_] - new_data[start_]) < max_segment) & (new_data['start_end'] == False), True, False)		
			new_data[end_] = np.where(new_data['too_short'].shift(-1) == True, (new_data[end_].shift(-1) + new_data[start_]) / 2, new_data[end_])
			new_data[start_] = np.where(new_data['too_short'] == True, new_data[end_].shift(1), new_data[start_])	
			#Find the 2 decimal place ceiling and floor for start and end values respectively.
			new_data[start_] = np.round(new_data[start_]/10) * 10
			new_data[end_] = np.round(new_data[end_]/10) * 10
			# Drop the boolean columns
			new_data = new_data.drop(['start_end', 'too_short'], axis=1)
	if as_km:
		# Convert SLK variables back to km
		for var in SLKs:
			new_data[var] = new_data[var] / 1000
	
	# recalculate length
	new_data = new_data.drop('Length', axis = 1) 
	new_data['Length'] = new_data[ends[0]] - new_data[starts[0]]
	
	if id:
		new_data['original_id'] = new_data.index + 1
		new_data['segment_id'] = [i+1 for i in range(len(new_data))]		
	new_data = new_data.reset_index(drop = True)
	
	return new_data