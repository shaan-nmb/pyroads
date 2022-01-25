

def test_imports():
	"""
	this test may produce unexpected behaviour from pytest; imports should not be done inside functions.
	we are just trying to ensure the public import interface to this library is stable-ish here.
	"""
	from pyroads.reshape import stretch, interval_merge, make_segments, get_segments
	
	from pyroads.mappings.cross_sectional_position import cway_to_side, hsd_to_side, lane_to_side, get_lanes
	from pyroads.mappings.link_category import map_link_category_to_mabcd
	from pyroads.mappings.pavement import map_pavement_type_name_to_id, map_pavement_type_id_to_name
	from pyroads.mappings.region import map_region_name_to_number, map_region_number_to_region_name
	from pyroads.mappings.route import route_change, route_description
	from pyroads.mappings.rurality import rurality
	from pyroads.mappings.surface import surf_id, surf_type

	from pyroads.misc.custom_aggregations import first, last,most,q75,q90,q95
	from pyroads.misc.deteriorate import deteriorate