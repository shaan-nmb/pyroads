# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 03:07:34 2020


This script expand data ton 10 metre SLK intvarals reading data from the 
a SQl database.

The following steps are preformed.

Connect to sql database with MFA
Ask user to input file to be processed.
Ask user for tale name to be created in sql database.
Write porcessed data to sql database

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
            
            # filter out series
            # corpEx_sub = df.filter(["Road", "Cway", "SLK_From", "SLK_To", "True_From", "True_To", "Unsealed", "RA","Link_Cat"])
            print(df.head())
            print("")
            print("Data is being Exapnded...please be patient.")
            
            data = stretch(df,'SLK_From','SLK_To','True_From','True_To')
            print("")
            print("Data Expanded........")
            print(data.head())
            print("")
            print("Data is being written to SQL Database...please be patient.")

                 
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