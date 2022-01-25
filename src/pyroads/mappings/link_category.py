from deprecated import deprecated
from typing import Literal

@deprecated(version="0.2.0", reason="Easy mapping to perform without this function. Keeping for now as it contains a complete list of link category strings. to be replaced with simpler function or a dict/list for reference purporses in the future")
def map_link_category_to_mabcd(x, new: Literal["mabcd", "link"] = 'mabcd'):
	link_category = ['MI', 'MFF', 'AW', 'AW+', 'BW', 'BW+', 'CW', 'DW']
	mabcd = ['M', 'M', 'A', 'A', 'B', 'B', 'C', 'D']
	
	if 'mabcd' in new.lower():
		mabcd_dict = dict(zip(link_category, mabcd))
		return [mabcd_dict[k] for k in x]
	elif 'link' in new.lower():
		link_cat_dict = dict(zip(link_category, mabcd))
		return [link_cat_dict[k] for k in x]
	else:
		raise ValueError("`new` must be either 'link' or 'mabcd'")
