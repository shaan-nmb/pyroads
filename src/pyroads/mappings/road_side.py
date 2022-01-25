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