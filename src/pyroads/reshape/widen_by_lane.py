from deprecated import deprecated

from ._util.gcd_list import gcd_list
from .stretch import stretch


@deprecated(version="0.2.0", reason="this code does not appear to be functional. Needs to be re-written", action="error")
def widen_by_lane(data, start, end, ids, grouping, xsp='xsp', side='side', reverse=True, keep_config=True, max_segment=100):
	raise NotImplemented("TODO: this function needs to be reviewed and re-written")
	
	# 0: Prep
	prep_df = data.loc[:, ids + [start, end] + grouping + [xsp, side]].copy()  # 0.1 Select only the columns of interest
	
	# 1: Create Direction based dataframe
	prep_df.loc[:, xsp] = prep_df.loc[:, xsp].str[1:]  # 1.1 Remove the direction prefix from the xsp column
	prep_df_l, prep_df_r = prep_df[prep_df.loc[:, side] == "L"], prep_df[prep_df.loc[:, side] == "R"]  # 1.2 Split into two frames, one for each direction.
	side_df = pd.concat([prep_df_l, prep_df_r])  # 1.3 Concatenate the split frames together
	
	# 2: Stretch dataframe into equal segment lengths
	stretch_size = gcd_list(as_metres(side_df[end]) - as_metres(side_df[start]))  # Size by which to stretch into equal segments
	stretched_df = stretch(side_df, starts=[start], ends=[end], max_segment=stretch_size, sort=ids + [side, start])
	
	# 3: Pivot frame on ID variables by xsp for selected grouping columns
	if keep_config:
		pivoted_df = stretched_df.pivot(index=ids + ['start', 'end', side], columns=xsp, values=grouping + [xsp])  # Keep dummy variables of the lanes if keep_config true
	else:
		pivoted_df = stretched_df.pivot(index=ids + ['start', 'end', side], columns=xsp, values=grouping)
	
	if reverse:
		pivoted_df = pivoted_df[sorted(pivoted_df.columns, reverse=True)]
	
	pivoted_df.columns = ['_'.join(col) for col in pivoted_df.columns]
	pivoted_df = pivoted_df.reset_index()
	
	compact_by = []
	for i in grouping:
		for col in pivoted_df.columns:
			if i in col:
				compact_by.append(col)
	
	# 4: Compact by IDs + Direction
	lanes = [lane for lane in pivoted_df.columns if xsp in lane]
	if keep_config:
		compact_df = compact(pivoted_df, SLK='start', lanes=lanes, obs_length=stretch_size, idvars=ids + [side], grouping=compact_by + [col for col in pivoted_df.columns if xsp in col])
	else:
		compact_df = compact(pivoted_df, SLK='start', lanes=lanes, obs_length=stretch_size, idvars=ids + [side], grouping=compact_by)
	
	# 5: Make into segment lengths of choice
	final_df = make_segments(compact_df, SLK_type="true", start_true="startstart", end_true="startend", max_segment=max_segment)
	
	return final_df
