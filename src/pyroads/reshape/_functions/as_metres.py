from typing import Union 
import numpy as np
import pandas as pd

def as_metres(
	var: Union[pd.Series, np.ndarray] 
	):

	"""
	Transforms kilometre values of type `float` into metres of type `int`.  
	
	Parameters:
	-----------
	var (pd.Series or np.ndarray):  Column containing the data kept as kilometres.
	"""

	m = np.round(var*1000).astype(int)

	return m