



def test_stretch_1():
	import pandas as pd
	from pyroads.reshape import stretch

	segmentation = pd.DataFrame(
		columns=["road_no", "carriageway", "slk_from", "slk_to"],
		data=[
			["H001", "L", 0.010, 0.050],
			["H001", "L", 0.050, 0.100],
			["H001", "L", 0.100, 0.150],
		]
	)

	result = stretch(
		data=segmentation,
		start="slk_from",
		end="slk_to",
		segment_size=10,
	)

	correct = pd.DataFrame(
		columns=["road_no", "carriageway", "SLK"],
		data=[
			["H001", "L", 0.010],
			["H001", "L", 0.020],
			["H001", "L", 0.030],
			["H001", "L", 0.040],
			
			["H001", "L", 0.050],
			["H001", "L", 0.060],
			["H001", "L", 0.070],
			["H001", "L", 0.080],
			["H001", "L", 0.090],

			["H001", "L", 0.100],
			["H001", "L", 0.110],
			["H001", "L", 0.120],
			["H001", "L", 0.130],
			["H001", "L", 0.140],
		]
	)
	assert result.equals(correct)


def test_stretch_2():
	import pandas as pd
	from pyroads.reshape import stretch
	
	df = pd.read_csv("./test/test_data/tsd_albany_hwy_poe_1.4.csv")

	result = stretch(
		data=df,
		start="START_SLK",
		end="END_SLK",
		start_true="START_TRUE",
		end_true="END_TRUE",
		segment_size=10,
		keep_ranges=True,
	)

	# The TSD data sample is already in 10 metre segments. There should be no change except for the
	#  addition of two additional columns
	assert result.iloc[:,:-2].equals(df)
