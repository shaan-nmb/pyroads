from typing import Literal

import pandas as pd


def surf_type(data: pd.DataFrame, old_col: str, method: Literal["full", "short", "asphalt", "grouped", "group_id"] = "short") -> pd.DataFrame:
	surf_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
	full = ["Asphalt Dense Graded", "Asphalt Intersection Mix", "Asphalt Open Graded", "Concrete", "Paving", "Primer Seal", "Rubberised Seal", "Single Seal", "Slurry Seal", "Two Coat Seal", "Asphalt Sotne Mastic", "Asphalt Open Graded on Dense Graded", "Asphalt Gap Graded Rubberised (GGAR)"]
	short = ["DGA", "IMA", "OGA", "Concrete", "Paving", "Primer Seal", "Rubberised Seal", "Single Seal", "Slurry Seal", "Two Coat Seal", "SMA", "OGA on DGA", "GGAR"]
	asphalt = ["DGA", "IMA", "OGA", "Other", "Other", "Seal", "Seal", "Seal", "Seal", "Seal", "SMA", "OGA on DGA", "GGAR"]
	grouped = ["Asphalt", "Asphalt", "Asphalt", "Other", "Other", "Seal", "Seal", "Seal", "Seal", "Seal", "Asphalt", "Asphalt", "Asphalt", 'Asphalt']
	group_id = [1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 1, 1, 1, 1]
	
	if method == "full":
		surf_dict = dict(zip(surf_id, full))
	elif method == "short":
		surf_dict = dict(zip(surf_id, short))
	elif method == "asphalt":
		surf_dict = dict(zip(surf_id, asphalt))
	elif method == "grouped":
		surf_dict = dict(zip(surf_id, grouped))
	elif method == 'group_id':
		surf_dict = dict(zip(surf_id, group_id))
	else:
		raise ValueError("ERROR surf_type(method): Please choose a method from ['full', 'short', 'asphalt', grouped'].")
	
	return data[old_col].map(surf_dict)


def surf_id(data: pd.DataFrame, old_col: str, method="short") -> pd.DataFrame:
	"""
	convert surface type id to text
	"""
	
	surf_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
	full = ["Asphalt Dense Graded", "Asphalt Intersection Mix", "Asphalt Open Graded", "Concrete", "Paving", "Primer Seal", "Rubberised Seal", "Single Seal", "Slurry Seal", "Two Coat Seal", "Asphalt Stone Mastic", "Asphalt Open Graded on Dense Graded", 'Asphalt Gap Graded Rubberised (GGAR)']
	short = ["DGA", "IMA", "OGA", "Concrete", "Paving", "Primer Seal", "Rubberised Seal", "Single Seal", "Slurry Seal", "Two Coat Seal", "SMA", "OGA on DGA"]
	asphalt = ["DGA", "IMA", "OGA", "Other", "Other", "Seal", "Seal", "Seal", "Seal", "Seal", "SMA", "OGA on DGA", 'GGAR']
	grouped = ["Asphalt", "Asphalt", "Asphalt", "Other", "Other", "Seal", "Seal", "Seal", "Seal", "Seal", "Asphalt", "Asphalt", "Asphalt", 'Asphalt']
	group_id = [1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 1, 1, 1, 1]
	
	if method == "full":
		surf_dict = dict(zip(full, surf_id))
	elif method == "short":
		surf_dict = dict(zip(short, surf_id))
	elif method == "asphalt":
		surf_dict = dict(zip(asphalt, surf_id))
	elif method == "grouped":
		surf_dict = dict(zip(grouped, surf_id))
	elif method == 'group_id':
		surf_dict = dict(zip(group_id, surf_id))
	else:
		raise ValueError("ERROR surf_type(method): Please choose a method from ['full', 'short', 'asphalt', grouped'].")
	
	return data[old_col].map(surf_dict)


