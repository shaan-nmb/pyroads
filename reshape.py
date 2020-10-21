##Deal with an SLK column as metres in integers to avoid the issue of calculating on floating numbers
def asmetres(var):

    m = round(var*1000).astype(int) 

    return m

##Turn each observation into sections of specified lengths
def stretch(data, start, end, true_start, true_end, obs_length = 10, dropvars = []):

    new_data = data.copy() #Copy of the dataset
    new_data = new_data.dropna(thresh = 2)  #drop any row that does not contain at least two non-missing values.

    #Change start, end, and true_start variables to 32 bit integers of metres to avoid the issue with calculations on floating numbers
    new_data[start] = asmetres(new_data[start])
    new_data[end] = asmetres(new_data[end])
    new_data[true_start] = asmetres(new_data[true_start])

    #Reshape the data into size specified in 'obs_length'
    new_data = new_data.reindex(new_data.index.repeat(new_data[end]/obs_length - new_data[start]/obs_length)) #reindex by the number of intervals of specified length between the start and the end.
    new_data['SLK'] = new_data[start] + new_data.groupby(level=0).cumcount()*obs_length #Create SLK point column based on start plus the cumulative count of how many intervals away it is multiplied by the specified observation length. 
    new_data['TRUE_SLK'] = new_data[true_start] + new_data.groupby(level=0).cumcount()*obs_length #Create SLK point column based on start plus the cumulative count of how many intervals away it is multiplied by the specified observation length.
    
    #Convert metre references back to kilometres
    new_data['SLK'] = new_data['SLK']/1000
    new_data['TRUE_SLK'] = new_data['TRUE_SLK']/1000
    
    new_data = new_data.reset_index(drop = True) 
    
    #Drop the variables no longer required
    for var in (dropvars + [start, end, true_start, true_end]):
        new_data.drop([var], axis = 1, inplace = True)
    new_data = new_data.reset_index(drop = True)
    
    return new_data

##Function that allows for the merging of two datasets with shared observation lengths on all mutual columns
def fuse(datasets = [], ignore = None, join = 'inner'):
    
    if join not in ['outer', 'inner'] and len(datasets)>2:
        print("Only outer or inner joins applicable for more than two datasets.")
        return
        
    #Determine which columns to merge over
    all_columns = [] #initialise empty list
    for dataset in datasets:
        all_columns = all_columns + list(dataset.columns.values) #Add the column names iteratively from each dataset
    shared_columns = [x for x in all_columns if all_columns.count(x) >= len(datasets)]  #Identify which columns appear in all the datasets
    
    if ignore != None:
        for i in ignore:
            if i in shared_columns:
                shared_columns.remove(i)
    
    #loop the specified join based on the shared columns
    for i in range(len(datasets)):
        while i <= (len(datasets) - 1):
            new_data = datasets[i-1].merge(datasets[i], how = join, on= shared_columns)
            i+=1 
    return new_data

##Function that Compact 10m data so that it has 'from' and 'to' SLK columns 
def compact(data, SLK = "SLK", true_SLK = "TRUE_SLK", obs_length = 10, idvars = [], grouping = []):

    import numpy as np

    #Sort by all columns in grouping, then by true SLK, then by SLK
    new_data = data.copy().sort_values(idvars + [SLK]).reset_index(drop = True)

    #Create a column that is a concatenation of all the columns in the grouping
    new_data.insert(0, "groupkey", "")
    for var in grouping+idvars:
        new_data["groupkey"] += new_data[var].astype(str) + '-'

    #Convert SLK and true_SLK columns into integers of metres to avoid the problems caused by calculations on floating numbers
    new_data[SLK] = asmetres(new_data[SLK])
    new_data[true_SLK] = asmetres(new_data[true_SLK])

    #Create lag and lead columns for SLK, true SLK, and the grouping key to check whether a new group has started
    new_data['leadSLK'] = new_data[SLK].shift(-1, fill_value = 1000)
    new_data['lagSLK'] = new_data[SLK].shift(1, fill_value = 1000)
    new_data['leadtrue_SLK'] = new_data[true_SLK].shift(-1, fill_value = 1000)
    new_data['lagtrue_SLK'] = new_data[true_SLK].shift(1, fill_value = 1000)
    new_data['leadgroupkey'] = new_data['groupkey'].shift(-1, fill_value = 'End')
    new_data['laggroupkey'] = new_data['groupkey'].shift(1, fill_value = 'Start')

    #Create columns based on whether they represent the start or end of a section.

    #if the lagged SLKs are not one observation length less than the actual or the lagged group-key is different.
    start = np.where((new_data['lagSLK'] == new_data[SLK]-obs_length) & (new_data['lagtrue_SLK'] == new_data[true_SLK]-obs_length) & (new_data['laggroupkey'] == new_data['groupkey']), False,True)
    #if the lead SLKs are not one observation length more than the actual or the lead group-key is different.
    end   = np.where((new_data['leadSLK'] == new_data[SLK]+obs_length) & (new_data['leadtrue_SLK'] == new_data[true_SLK]+obs_length) & (new_data['leadgroupkey'] == new_data['groupkey']), False, True)
    new_data['start'] = start
    new_data['end'] = end

    #Create the compact dataset

    compact_data = new_data.copy()[start][idvars + grouping].reset_index(drop = True) #Data for the id and grouping variables for every start instance. 
    compact_data.insert(2, "START_SLK", new_data[start].reset_index(drop = True)[SLK]) 
    compact_data.insert(4, "START_TRUE", new_data[start].reset_index(drop = True)[true_SLK])
    #As each start instance has a corresponding end instance with shared grouping and id variables, it can be joined horizontally.
    compact_data.insert(3, "END_SLK", new_data[end].reset_index(drop = True)[SLK] + obs_length) 
    compact_data.insert(5, "END_TRUE", new_data[end].reset_index(drop = True)[true_SLK] + obs_length)
    
    compact_data = compact_data.sort_values(idvars).reset_index(drop = True) #Sort data by the location varibles
    return compact_data
    