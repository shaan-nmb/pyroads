
def test_make_segments_slk_only():
	import pandas as pd
	import numpy as np
	from pyroads.reshape import make_segments
	
	data_to_segment = pd.DataFrame(
		columns=["road_no", "carriageway", "slk_from", "slk_to", "value"],
		data=[
			["H001", "L", 0.010, 0.050, "a"],
			["H001", "L", 0.050, 0.080, "b"],
			["H001", "L", 0.080, 0.100, "c"],
			["H001", "L", 0.100, 0.120, "d"],
		]
	)
	
	actual_result = make_segments(
		data_to_segment,
		start="slk_from",
		end="slk_to",
		max_segment=10,
		as_km=True
	)
	
	expected_result = pd.DataFrame(
		columns=["road_no", "carriageway", "slk_from", "slk_to", "Length", "value" , "segment_id"],
		data=[
			["H001", "L", 0.010, 0.020, 0.010, "a",  0],
			["H001", "L", 0.020, 0.030, 0.010, "a",  1],
			["H001", "L", 0.030, 0.040, 0.010, "a",  2],
			["H001", "L", 0.040, 0.050, 0.010, "a",  3],
			["H001", "L", 0.050, 0.060, 0.010, "b",  4],
			["H001", "L", 0.060, 0.070, 0.010, "b",  5],
			["H001", "L", 0.070, 0.080, 0.010, "b",  6],
			["H001", "L", 0.080, 0.090, 0.010, "c",  7],
			["H001", "L", 0.090, 0.100, 0.010, "c",  8],
			["H001", "L", 0.100, 0.110, 0.010, "d",  9],
			["H001", "L", 0.110, 0.120, 0.010, "d", 10],
		]
	)
	# length column might be slightly fuzzy
	actual_result["Length"] = actual_result["Length"].round(3)
	
	try:

		assert actual_result.equals(expected_result)

	except AssertionError:

		raise Exception("\n\nEquality Test Failed - Assertion Error\n" + str(actual_result.compare(expected_result)) + "\n\n")