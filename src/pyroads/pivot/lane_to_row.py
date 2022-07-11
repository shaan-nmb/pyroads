import numpy as np

def lane_to_row(
	data,
	id_vars,
	dirn = None,
	SLK = None,
	true_SLK = None,
	start =  None,
	end = None,
	start_true =  None,
	end_true = None,
	prefixes = None,
	dir_split = False,
	all_lanes = False
):

	new_data = data.copy()
	slks = [col for col in [start, end, start_true, end_true, SLK, true_SLK] if bool(col)]
	
	if bool(dirn):
		dirn = [dirn]
	else:
		dirn = []

	if dir_split:
		lane_var = 'LANE_NO'
	else:
		lane_var = 'XSP'

	if all_lanes:
		lanes =  ['L1', 'L2', 'L3', 'L4', 'LL1', 'R1', 'R2', 'LR1', 'L', 'RR1', 'R', 'R3', 'LR2', 'LR3', 'RL1', 'RO', 'LO', 'RR2', 'RL2', 'ER', 'R4', 'R5', 'L5', 'L6', 'LL2', 'RR3', 'LL3']
	else:
		lanes = ['L1', 'L2', 'L3', 'L4', 'R1', 'R2', 'R3','R4', 'R5', 'L5', 'L6']
	
	if prefixes is None:
			lane_df_base = new_data[id_vars + dirn + lanes + slks].melt(id_vars = id_vars + dirn + slks, value_vars = lanes, var_name = lane_var, ignore_index = True)	

	if isinstance(prefixes, str):
		cols = [col for col in new_data.columns if prefixes in col]
		lane_df_base = new_data[cols + id_vars + dirn + slks].melt(id_vars = id_vars + dirn, var_name = lane_var, value_name = prefixes, ignore_index = True)

	if isinstance(prefixes, list):
		cols = [col for col in new_data.columns if prefixes[0] in col]
		lane_df_base = new_data[cols + id_vars + [dirn] + slks].melt(id_vars = id_vars + [dirn] + slks, var_name = lane_var, value_name = prefixes[0], ignore_index = True)
		lane_df_base = lane_df_base.drop([col for col in lane_df_base if col not in id_vars + slks + dirn + [lane_var + prefixes[0]]], axis = 1)
		for prefix in prefixes[1:]:
			cols = [col for col in new_data.columns if prefix in col]
			lane_df_temp = new_data[cols + id_vars].melt(id_vars = id_vars, var_name = lane_var, value_name = prefix, ignore_index = True)
			lane_df_temp = lane_df_temp[prefix]
			lane_df_base = lane_df_base.join(lane_df_temp)

	if dir_split:
		lane_df_base['LANE_NO'] = lane_df_base.loc[:, 'LANE_NO'].str[-1] 
		lane_df_base.insert(len(lane_df_base.columns) -1, 'XSP', lane_df_base[dirn].astype(str) + lane_df_base.LANE_NO.astype(str))
		lane_df_base['XSP'] = np.where(lane_df_base['XSP'].str.contains('P'), 'TP', lane_df_base['XSP'])
		lane_df = lane_df_base.drop(['LANE_NO', dirn], axis = 1) 
		lane_df = lane_df.drop_duplicates().reset_index(drop = True)
	else:
		lane_df = lane_df_base

	#Order the columns to be the id_vars followed by the SLKs
	for slk in slks:
		x = 1
		lane_df.insert(len(id_vars) + x, slk, lane_df.pop(slk))
		x = x + 1

	return lane_df