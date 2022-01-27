from typing import Optional, Union, Literal

import numpy as np
import pandas as pd

from ._gcd_list import gcd_list
from ._as_meters import as_metres


def stretch(
		data: pd.DataFrame,
		start: Optional[str] = None,
		end: Optional[str] = None,
		start_true: Optional[str] = None,
		end_true: Optional[str] = None,
		segment_size: Union[int, Literal["GCD"]] = 'GCD',
		sort: Optional[list[str]] = None,
		as_km: bool = True,
		keep_ranges: bool = False
	):
	"""
	Replicates rows such that all observations have the same slk length

	One of the following must be provided

		- `start` and `end`
		- `start_true` and `end_true`
		- `start`, `end`, `start_true` and `end_true`

	Args:
		data: DataFrame
		start (str): Column name of the `start SLK` of the segment in kilometres
		end (str): Column name of the `end SLK` of the segment in kilometres
		start_true (str): Column name of the `start true distance` of the segment in kilometres
		end_true (str): Column name of the `end true distance` of the segment in kilometres
		segment_size (int, 'GCD'): Number of meters or Greatest Common Divisor
		sort (list[str]): Columns to sort the dataframe by
		as_km (bool): If True, the output will be in kilometres
		keep_ranges (bool): Retains the slk_from and slk_to columns, but the values are repeated. Useful for regrouping into original intervals later.
	"""
	new_data = data.copy().reset_index(drop=True)  # Copy of the dataset
	new_data = new_data.dropna(thresh=2)  # drop any row that does not contain at least two non-missing values.
	
	# rename columns for consistency of output
	new_data = new_data.rename(columns={start: 'START_SLK', start_true: 'START_TRUE', end: 'END_SLK', end_true: 'END_TRUE'})
	
	starts = [col for col in ['START_SLK', 'START_TRUE'] if col in new_data.columns]
	ends = [col for col in ['END_SLK', 'END_TRUE'] if col in new_data.columns]
	names = [v for k, v in zip([start, start_true], ['SLK', 'true_SLK']) if k is not None]
	SLKs = starts + ends
	
	if type(sort) == list:
		new_data = new_data.sort_values(sort)
	
	# Change SLK variables to 32 bit integers of metres to avoid the issue with calculations on floating numbers
	new_data[SLKs] = new_data[SLKs].apply(as_metres)
	
	lengths = new_data[ends[0]] - new_data[starts[0]]
	# gcd = gcd_list(lengths)
	gcd = np.gcd.reduce(lengths)
	
	if segment_size == 'GCD':
		segment_size = gcd
	
	if segment_size > gcd:
		segment_size = gcd
		print(f'`segment_size` is too large. Defaulting to the GCD, of {gcd}m.')
	
	# Reshape the data into size specified in 'obs_length'
	new_data = new_data.reindex(new_data.index.repeat(np.ceil((new_data[ends[0]] - new_data[starts[0]]) / segment_size)))  # reindex by the number of intervals of specified length between the start and the end.
	
	# increment the start points by observation length
	for start_slk, end_slk, name in zip(starts, ends, names):
		new_data[name] = new_data[start_slk] + new_data.groupby(level=0).cumcount() * segment_size
		if as_km:
			new_data[name] = new_data[name] / 1000
	
	for start_slk, end_slk in zip(starts, ends):
		# End SLKs are equal to the lead Start SLKS except where the segment ends
		new_data[end_slk] = np.where((new_data[start_slk].shift(-1) - new_data[start_slk]) == segment_size, new_data[start_slk].shift(-1), new_data[end_slk])
	
	new_data = new_data.reset_index(drop=True)
	
	# Drop the variables no longer required
	if keep_ranges:
		if as_km:
			for SLK in SLKs:
				new_data[SLK] = new_data[SLK] / 1000
		else:
			new_data.rename(columns={"SLK": "SLK_m", "true_SLK": "true_SLK_m"})
	else:
		new_data = new_data.drop([SLK for SLK in SLKs], axis=1)
		new_data = new_data.reset_index(drop=True)
	
	return new_data
