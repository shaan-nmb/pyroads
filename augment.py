##Determine whether the section is metro or rural
def rurality(data, RA, road_no, SLK = "SLK") :
    import numpy as np
    regionType = np.where((data[RA] == 7 & data[road_no].isin(['M045','H006','M026','M010']))| 
                    ((data[SLK].astype(int) >= 48.5) & data[road_no] == 'H005')|
    ((data[SLK].astype(int) >= 48.5) & data[road_no] == 'H005')|
    ((data[SLK].astype(int) >= 12.22) & data[road_no] == 'H052')|
    ((data[SLK].astype(int) >= 35.5) & data[road_no] == 'H001')|
    ((data[SLK].astype(int) >= 18.54) & data[road_no] == 'H009'), 'Rural', 'Metro')
    return regionType

##Assign a natural key based on key variables
def naturalKey(data, SLK = 'SLK', trueSLK = 'TRUE_SLK', keyVars = []):

    #Create a natural key based on SLK and ID variables
    newData = data.copy()
    newData.insert(0, "NATURAL_KEY", "")
    for var in keyVars:
        newData["NATURAL_KEY"] += newData[var].astype(str) + '-'
    newData["NATURAL_KEY"] = newData["NATURAL_KEY"] + newData[SLK].astype(str) + '-' + newData[trueSLK].astype(str)   

    return newData

##Function that allows one to add columns to a dataset from one with a shared natural key.  
def concatCols(left, right, NaturalKey = "NATURAL_KEY", newCols = []):
    newCols.append(NaturalKey)
    concatData = left.merge(right[newCols], how = 'left', on = NaturalKey, suffixes = ('','_right'))
    return concatData

###Find points of equation and add the information to the dataset
# def pointOfEquation(data, SLK, locationVars)
# #Find the Points of Equations by searching for duplicate SLKs for the same location variables
