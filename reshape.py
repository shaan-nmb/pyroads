##Deal with a SLK column as metres in integers to avoid the issue of calculating on floating numbers
def asMetres(var):

    m = round(var*1000).astype(int) 

    return m

##Turn each observation into sections of specified lengths
def stretch(data, start, end, trueStart, trueEnd, obsLength = 10, dropVars = []):

    newData = data.copy() #Copy of the dataset
    newData = newData.dropna(thresh = 2)  #drop any row that does not contain at least two non-missing values.

    #Change start, end, and trueStart variables to 32 bit integers of metres to avoid the issue with calculations on floating numbers
    newData[start] = asMetres(newData[start])
    newData[end] = asMetres(newData[end])
    newData[trueStart] = asMetres(newData[trueStart])

    #Reshape the data into size specified in 'obsLength'
    newData = newData.reindex(newData.index.repeat(newData[end]/obsLength - newData[start]/obsLength)) #reindex by the number of intervals of specified length between the start and the end.
    newData['SLK'] = newData[start] + newData.groupby(level=0).cumcount()*obsLength #Create SLK point column based on start plus the cumulative count of how many intervals away it is multiplied by the specified observation length. 
    newData['TRUE_SLK'] = newData[trueStart] + newData.groupby(level=0).cumcount()*obsLength #Create SLK point column based on start plus the cumulative count of how many intervals away it is multiplied by the specified observation length.
    
    #Convert metre references back to kilometres
    newData['SLK'] = newData['SLK']/1000
    newData['TRUE_SLK'] = newData['TRUE_SLK']/1000
    
    newData = newData.reset_index(drop = True) 
    
    #Drop variables no longer required
    for var in dropVars + [start, end, trueStart, trueEnd]:
        newData.drop([var], axis = 1, inplace = True)
    newData = newData.reset_index(drop = True)
    
    return newData

##Function that allows for the merging of two datasets with shared observation lengths on all mutual columns
def fuse(datasets = [], ignore = None, join = 'inner'):
    
    if join not in ['outer', 'inner'] and len(datasets)>2:
        print("Only outer or inner joins applicable for more than two datasets.")
        return
        
    #Determine which columns to merge over
    allColumns = [] #initialise empty list
    for dataset in datasets:
        allColumns = allColumns + list(dataset.columns.values) #Add the column names iteratively from each dataset
    sharedColumns = [x for x in allColumns if allColumns.count(x) >= len(datasets)]  #Identify which columns appear in all the datasets
    
    if ignore != None:
        for i in ignore:
            if i in sharedColumns:
                sharedColumns.remove(i)
    
    #loop the specified join based on the shared columns
    for i in range(len(datasets)):
        while i <= (len(datasets) - 1):
            newData = datasets[i-1].merge(datasets[i], how = join, on= sharedColumns)
            i+=1 
    return newData

##Function that Compact 10m data so that it has 'from' and 'to' SLK columns 
def compact(data, SLK = "SLK", trueSLK = "TRUE_SLK", obsLength = 10, idVars = [], groupBy = []):

    import numpy as np

    #Sort by all columns in groupBy, then by true SLK, then by SLK
    newData = data.copy().sort_values(idVars + [SLK]).reset_index(drop = True)

    #Create a column that is a concatenation of all the columns in the groupBy
    newData.insert(0, "groupKey", "")
    for var in groupBy + idVars:
        newData["groupKey"] += newData[var].astype(str) + '-'

    #Convert SLK and trueSLK columns into integers of metres to avoid the problems caused by calculations on floating numbers
    newData[SLK] = asMetres(newData[SLK])
    newData[trueSLK] = asMetres(newData[trueSLK])

    #Create lag and lead columns for SLK, true SLK, and the grouping key to check whether a new group has started
    newData['leadSLK'] = newData[SLK].shift(-1, fill_value = 1000)
    newData['lagSLK'] = newData[SLK].shift(1, fill_value = 1000)
    newData['leadTrueSLK'] = newData[trueSLK].shift(-1, fill_value = 1000)
    newData['lagTrueSLK'] = newData[trueSLK].shift(1, fill_value = 1000)
    newData['leadGroupKey'] = newData['groupKey'].shift(-1, fill_value = 'End')
    newData['lagGroupKey'] = newData['groupKey'].shift(1, fill_value = 'Start')

    #Create columns based on whether they represent the start or end of a section.

    #if the lagged SLKs are not one observation length less than the actual or the lagged group-key is different.
    start = np.where((newData['lagSLK'] == newData[SLK] - obsLength) & (newData['lagTrueSLK'] == newData[trueSLK] - obsLength) & (newData['lagGroupKey'] == newData['groupKey']), False,True)
    #if the lead SLKs are not one observation length more than the actual or the lead group-key is different.
    end   = np.where((newData['leadSLK'] == newData[SLK] + obsLength) & (newData['leadTrueSLK'] == newData[trueSLK] + obsLength) & (newData['leadGroupKey'] == newData['groupKey']), False, True)
    newData['start'] = start
    newData['end'] = end

    #Create the compact dataset

    compactData = newData.copy()[start][idVars + groupBy].reset_index(drop = True) #Data for the id and grouping variables for every start instance. 
    compactData.insert(2, "START_SLK", newData[start].reset_index(drop = True)[SLK]) 
    compactData.insert(4, "START_TRUE", newData[start].reset_index(drop = True)[trueSLK])
    #As each start instance has a corresponding end instance with shared grouping and id variables, it can be joined horizontally.
    compactData.insert(3, "END_SLK", newData[end].reset_index(drop = True)[SLK] + obsLength) 
    compactData.insert(5, "END_TRUE", newData[end].reset_index(drop = True)[trueSLK] + obsLength)
    
    compactData = compactData.sort_values(idVars).reset_index(drop = True) #Sort data by the location varibles
    return compactData
    