def route_change(x, method="new"):
	old_routes = [1.1, 2.1, 3.1, 2.2, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0]
	new_routes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
	
	if method == "new":
		route_dict = dict(zip(old_routes, new_routes))
	if method == "old":
		route_dict = dict(zip(new_routes, old_routes))
	else:
		raise ValueError("method must be 'new' or 'old'")
	
	route_no = x.map(route_dict)
	return route_no


def route_description(x, format="new"):
	old_routes = [1.1, 2.1, 3.1, 2.2, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0]
	new_routes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
	route_description = ['Perth to Adelaide', ' Perth to Darwin', ' Perth to Bunbury', 'Perth to Port Hedland', ' Perth to Albany', ' Bunbury to Albany', 'Geraldton to Esperance', ' Perth to Esperance', 'Bunbury to Augusta', 'Albany to Esperance', 'Bindoon (H006) to Dongara (H004) ', 'Manjimup to Albany', 'Nanutarra to GNH', 'Paraburdoo to GNH', 'Derby to GNH', 'Newman to Port Hedland', 'Albany to Lake Grace', 'Perth to Merredin ', 'Bunbury to Lake King', 'Minilya (NWCH) to Exmouth', 'Byford to Coalfields Hwy', 'Busselton to SWH via Pembeton', 'Donnybrook (SWH) to Kojonup (Aly Hwy)', 'Roe Hwy to Toodyay']
	
	if format == "new":
		route_dict = dict(zip(new_routes, route_description))
	elif format == "old":
		route_dict = dict(zip(old_routes, route_description))
	else:
		raise ValueError("method must be 'new' or 'old'")
	
	route = x.map(route_dict)
	
	return route
