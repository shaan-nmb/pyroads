# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 05:08:16 2020

This script extracted data from a SQL database RAW schema on DW to a CSV file.

@author: Analyst_02
"""
import pandas as pd
import pyodbc as odbc
from azure.storage.blob import BlockBlobService
import config
import sys
import time


Driver = "SQL Server Native Client 17.0"
Server_Name= "mrw-aue-nmbas-sql-dev.database.windows.net"
Database_Name = "DW"
Authentication = 'ActiveDirectoryInteractive'

STORAGEACCOUNTNAME= config.AccountName
STORAGEACCOUNTKEY= config.AccountKey
CONTAINERNAME= "data"
    

    
block_blob_service = BlockBlobService(account_name=STORAGEACCOUNTNAME, account_key=STORAGEACCOUNTKEY)

if __name__ =="__main__":
    
    t = time.time()
      
    Username = input('Enter MRDWA user login (email address): ')  
       
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
          
    schemaDict={'1':'Raw','2':'Processed','3':'Production'}
    tablesList = []
   
    print("")
    print(f"This script exports csv files from the {Database_Name} SQL database")
    
    
    #List of tables and schema's in Database
    print("")
    print(f"List of Schema's and Tables in {Database_Name} databse")
    print("")
    for row in cursor.tables(tableType ='TABLE').fetchall():
        
        print(row[1]+"    "+row[2])
        tablesList.append(row[2])
        
    print("")
    
    
    

    
    print("Database schema are as follows:")        
    print("1. Raw \n2. Processed\n3. Production") 
    schemaName = input("Please select a Database schema to use from the above list:  ")
    
    
    print("")
    TABLE = input("Please enter the name of SQL table to export: ")
    
    LOCALFILENAME= input ('Please the name of the csv file to be created:  ')
    
    print(f"Table {TABLE} is being extracted from {Database_Name} SQL database..... Please be patient")
    
    
    # Recieves the value from the schema dictionary
    option = schemaDict[schemaName]
    
    if schemaName in schemaDict:
        pass
    else:
        print(f"Schema Not found in {Database_Name} Database.....exiting")
        cursor.close()
        conn.close()
        sys.exit()
    
       
    
 
    # SQL query to run within pandas dataframe function
    sql_query = pd.read_sql_query(f'''
                                  select * from {option}.{TABLE}
                                  '''
                                  ,conn)
    try:
        #Execute sql query
        dataset = pd.DataFrame(sql_query)
        
        print(dataset.head())

        print("Data is being writen to a CSV file..... please be patient")
        
        # Export to csv file to Azure BLob
        
        output = dataset.to_csv(index = False)
        
        block_blob_service.create_blob_from_text('data','Output/'+LOCALFILENAME+".csv", output)
        print("")
        print("CSV file has how been created in data/Output/ on Azure Blob")
  
        cursor.close()
        conn.close()
    
    except Exception as e:
        print(str(e))
        cursor.close()
        conn.close()
    
    
    
    
    elapsed_time = time.time() - t
    print("The process took "+str(elapsed_time)+" seconds")