
import numpy as np

def lane_to_side(data, name = None, side_name = 'side'):

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