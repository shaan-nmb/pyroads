# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 03:07:34 2020

@author: Analyst_02
"""

"""
Created on Tue Dec 15 05:08:16 2020

@author: Analyst_02
"""
import pandas as pd
import pyodbc as odbc
from sqlalchemy import create_engine, event
from sqlalchemy.pool import StaticPool
import sys
import time
import gc


Driver = "SQL Server Native Client 17.0"
Server_Name= "mrw-aue-nmbas-sql-dev.database.windows.net"
Database_Name = "DW"
Authentication = 'ActiveDirectoryInteractive'



#Dictronaries
surfDict = {1:30,2:40,3:25,4:200,5:75,6:6,7:14,8:14,9:9,10:14,12:60,13:40}

regionDict = {1:'Great Southern',2:'South West',5:'Goldfields-Esperance',6:'Kimberley',7:'Metropolitan',8:'Wheatbelt',11:'Pilbara',14:'Mid West-Gascoyne'}

SULA_Agg_Dict ={1:4,2:6,3:10,4:14,5:16,6:14,7:16,8:10,9:14,10:14,11:10,12:20}


POARdict = {1:"Sandy Clay", 2:"Gravel",3:"Crushed Rock",4:"Sand",5:"Clay",6:"Concrete",7:"Limestone",8:"Recycled Material",9: "Hydrated Cement Treated Crushed Rock Base",10:"Asphalt",11:"Ferricrete",99:"Unknown"}

surfDict = {1:"Asphalt Dense Graded",2:"Asphalt Intersection Mix",3:"Asphalt Open Graded",4:"Concrete",5:"Paving",6:"Primer Seal",7:"Rubberised Seal",8:"Single Seal",9:"Slurry Seal",10:"Two Coat Seal",11:"Asphalt Stone Mastic",12:"Asphalt Open Graded on Dense Graded"}

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
    new_data['True_Dist'] = new_data[true_start] + new_data.groupby(level=0).cumcount()*obs_length #Create SLK point column based on start plus the cumulative count of how many intervals away it is multiplied by the specified observation length.
    
    #Convert metre references back to kilometres
    new_data['SLK'] = new_data['SLK']/1000
    new_data['True_Dist'] = new_data['True_Dist']/1000
    
    new_data = new_data.reset_index(drop = True) 
    
    #Drop the variables no longer required
    for var in (dropvars + [start, end, true_start, true_end]):
        new_data.drop([var], axis = 1, inplace = True)
    new_data = new_data.reset_index(drop = True)
    
    return new_data




if __name__ =="__main__":
    
    gc.collect()
      
    Username = input('Enter MRDWA user login: ')  
       
    # Connect to SQL server using MFA
    try:
       conn = odbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                                   server=Server_Name,
                                   database=Database_Name,
                                   Uid = Username,
                                   Authentication= Authentication)
       
       cursor = conn.cursor()
       print("")
       print('Connection to database '+Database_Name+' on server '+Server_Name+" successful")
       
       
    except Exception as e:
        print(str(e))
        print('Task has terminated')
        sys.exit()
            
    DATATABLE = input("Enter table to expand:  ")
    
    
    if cursor.tables(table = "Processed."+DATATABLE, tableType='TABLE').fetchone():
        
      tableRemove = input('Processed.'+DATATABLE+ " exists in the database. Do want to replace this table. y/n?  ")
          
      if tableRemove == 'y':
          print('Table Processed.' +DATATABLE+' will now be replaced')
          
          
      elif tableRemove == 'n':
          print("Table "+DATATABLE+" will NOT be removed")
          print('Task has terminated')
          cursor.close()
          conn.close()
          sys.exit()
    
                                    
    else:
        print("")
        print(f"Table {DATATABLE} is now being retrieved from the {Database_Name} database.")
        print("")
        
        try:
            # Read in corporate extract data into dataframe. Chunksize creates a generator data type
            df = pd.read_sql('SELECT * FROM  Raw.'+DATATABLE,conn,index_col=None)
            
            
            # Map region name to region number
            df['RA_Name'] = df['RA'].map(regionDict)
            
            #set First condition - Agg Size
            df.loc[df['Asphalt_Depth'] > 0, 'Agg_Size'] = df['Asphalt_Depth']
            
            #set Second condition - Agg Size
            df.loc[df['Asphalt_Depth'].isnull(), 'Agg_Size'] = df['SULA_Agg_Size'].map(SULA_Agg_Dict)
            
            
            #set Third condition
            df.loc[(df['Agg_Size'].isnull()) & (df['Unsealed'].isnull()) , 'Agg_Size'] = df['Surf_Type'].map(surfDict)
            
            # Base Material Map descriptions
            df['Base_Material'] = df['Orig_Base_Mat'].map(POARdict)
            
            # Surface type Map descriptions
            df['Surface_Type'] = df['Surf_Type'].map(surfDict)
           
            
            # Expand the Data
            data = stretch(df,'SLK_From','SLK_To','True_From','True_To')
            print("")
            print("Data Expanded........")
            print(data.head())
            print("")
            print("Data is being written to SQL Database...please be patient.")
            
            
            print(df.head())
            print("")
            print("Data is being Exapnded...please be patient.")
                           
                 
        except Exception as e:
            print(str(e))
        
      
    
        #Write data to SQL database
        t = time.process_time()
        engine = create_engine("mssql+pyodbc://", poolclass=StaticPool, creator=lambda: conn)
        
        @event.listens_for(engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True
                
        
        
        data.to_sql(DATATABLE,
                  con=engine,
                  schema ='Processed',
                  chunksize= 10000,
                  index=False,
                  if_exists='replace'
                  )
            
  
        print("")
        print('Table '+DATATABLE+ ' has been add to '+Database_Name+' database')
        print("")
        print("Connection to "+Database_Name+" database on "+Server_Name+" has been closed")
        cursor.close()
        conn.close()
        gc.collect()
        elapsed_time = time.process_time() - t
        print("The process took "+str(elapsed_time)+" seconds")