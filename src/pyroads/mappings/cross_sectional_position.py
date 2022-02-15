from typing import Literal, Union, Optional
from xmlrpc.client import Boolean

import numpy as np
import pandas as pd


def get_lanes(x, current: Literal['xsp', 'description', 'lane', 'dtims'] = 'xsp', new: Literal['xsp', 'description', 'lane', 'dtims'] = 'dtims'):
	xsp = pd.Series(['L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'R1', 'R2', 'R3', 'R4', 'R5', 'LL1', 'LL2', 'LL3', 'LR1', 'LR2', 'LR3', 'RL1', 'RL2', 'RL3', 'RR1', 'RR2', 'RR3', 'LO', 'RO', 'L', 'R'])
	lane = np.where(xsp.str.contains("^L", regex=True), "L", np.where(xsp.str.contains("^R", regex=True), "R", 'Unknown'))
	description = ["Left lane 2", "Left lane 3", "Left lane 4", "Left lane 5", "Left lane 6", "Right lane 1", "Right lane 2", "Right lane 3", "Right lane 4", "Right lane 5", "Left carriageway, left turn pocket 1", "Left carriageway, left turn pocket 2", "Left carriageway, left turn pocket 3", "Left carriageway, right turn pocket 1", "Left carriageway, right turn pocket 2", "Left carriageway, right turn pocket 3", "Right carriageway, left turn pocket 1", "Right carriageway, left turn pocket 2", "Right carriageway, left turn pocket 3", "Right carriageway, right turn pocket 1", "Right carriageway, right turn pocket 2", "Right carriageway, right turn pocket 3", "Left overtaking lane", "Right overtaking lane", "Left shoulder", "Right shoulder"]
	dtims = [1, 3, 5, 7, 9, 11, 2, 4, 6, 8, 10, 21, 23, 25, 31, 33, 35, 22, 24, 26, 32, 34, 36, 41, 42, 51, 52]
	
	if current == 'xsp':
		old_data = xsp
	elif current == 'description':
		old_data = description
	elif current == 'lane':
		old_data = lane
	elif current == 'dtims':
		old_data = dtims
	else:
		raise ValueError("current must be one of 'xsp' 'description' 'lane' or 'dtims'")
	
	if new == 'xsp':
		new_data = xsp
	elif new == 'description':
		new_data = description
	elif new == 'lane':
		new_data = lane
	elif new == 'dtims':
		new_data = dtims
	else:
		raise ValueError("new must be one of 'xsp' 'description' 'lane' or 'dtims'")
	
	lane_dict = dict(zip(old_data, new_data))
	
	return x.map(lane_dict)


def cway_to_side(data, name = None):

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

def lane_to_col(
	data: pd.DataFrame,
	idvars: Optional[list[str]] = None,
	xsp: Optional[str] = None,
	values: Optional[Union[str, list[str]]] = None,
	):
	
	new_data = data.copy()
    
    #Select only regular lanes
	new_data = new_data.loc[new_data[xsp].isin(['L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'R1', 'R2', 'R3', 'R4', 'R5'])]
	#Split into two columns. One for direction, and one for lane number.
	new_data['DIRN'] = new_data[xsp].str[0]
	new_data['LANE_NO'] = new_data[xsp].str[1]
	#Drop the full XSP column
	new_data = new_data.drop(xsp, axis = 1)
	idvars.append('DIRN')
    
	#pivot
	new_data = new_data.pivot(index = idvars, columns = 'LANE_NO', values = values).reset_index()
	new_data.columns = ["".join(x) for x in new_data.columns]

	return new_data

def lane_to_row(
	data: pd.DataFrame,
	dirn: str,
	idvars: Union[str, list[str]],
	start: Optional[str] = None,
	end: Optional[str] = None,
	true_start: Optional[str] = None,
	true_end: Optional[str] = None,
	prefixes: Optional[Union[str, list[str]]] = None,
	):

	new_data = data.copy()
	slks = [col for col in [start, end, true_start, true_end] if bool(col)]

	if prefixes is None:
			lane_df_base = new_data[idvars + [dirn] + ['6','5','4','3','2','1','TP']].melt(id_vars = idvars + [dirn], value_vars = ['6','5','4','3','2','1','TP'], var_name = 'LANE_NO', ignore_index = False)	
			lane_df_base = lane_df_base.drop('value', axis = 1)
	if isinstance(prefixes, str):
		cols = [col for col in new_data.columns if prefixes in col]
		lane_df_base = new_data[cols + idvars + [dirn] + slks].melt(id_vars = idvars + [dirn], var_name = 'LANE_NO', value_name = prefixes[0], ignore_index = False)
	
	if isinstance(prefixes, list):
		cols = [col for col in new_data.columns if prefixes[0] in col]
		lane_df_base = new_data[cols + idvars + [dirn] + slks].melt(id_vars = idvars + [dirn] + slks, var_name = 'LANE_NO', value_name = prefixes[0], ignore_index = False)
		lane_df_base = lane_df_base.drop([col for col in lane_df_base if col not in idvars + slks + ['LANE_NO', dirn, prefixes[0]]], axis = 1)
		for prefix in prefixes[1:]:
			cols = [col for col in new_data.columns if prefix in col]
			lane_df_temp = new_data[cols + idvars].melt(id_vars = idvars, var_name = 'LANE_NO', value_name = prefix, ignore_index = False)
			lane_df_temp = lane_df_temp[prefix]
			lane_df_base = lane_df_base.join(lane_df_temp)
	lane_df_base['LANE_NO'] = lane_df_base.loc[:, 'LANE_NO'].str[-1] 
	lane_df_base.insert(len(lane_df_base.columns) -1, 'XSP', lane_df_base[dirn].astype(str) + lane_df_base.LANE_NO.astype(str))
	lane_df_base['XSP'] = np.where(lane_df_base['XSP'].str.contains('P'), 'TP', lane_df_base['XSP'])
	lane_df = lane_df_base.drop(['LANE_NO', dirn], axis = 1) 

	return lane_df