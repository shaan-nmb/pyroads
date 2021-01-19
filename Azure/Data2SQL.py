# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 05:08:16 2020

This script is the write extracted cracking data from the 
Azure cloud to an SQl database.

The following steps are preformed.
Connect to Azure Blob
list all csv file by date and name
concatenate dataframes from df list
Rename the field columns
Ask user for table name to be created in database
Connect to sql database with MFA
Write data to sql database

@author: Analyst_02
"""
import pandas as pd
import pyodbc as odbc
from sqlalchemy import create_engine, event
from sqlalchemy.pool import StaticPool
from azure.storage.blob import BlockBlobService
import config
import sys
import time


STORAGEACCOUNTNAME= config.AccountName
STORAGEACCOUNTKEY= config.AccountKey
CONTAINERNAME= "data"


Driver = "SQL Server Native Client 17.0"
Server_Name= "mrw-aue-nmbas-sql-dev.database.windows.net"
Database_Name = "DW"
Authentication = 'ActiveDirectoryInteractive'


optionDict = {'1': "Pavement", '2':"Surface", '3':"Cracking",'4':'Line Marking','5':'Structure','6':'Corp','7':'Condition'}



def blobData():
    
    #download from blob as rename columns
    try:
        global df
        names=[]
        processList=[]
        
        block_blob_service = BlockBlobService(account_name=STORAGEACCOUNTNAME, account_key=STORAGEACCOUNTKEY)
        blob_list = block_blob_service.list_blobs(container_name="data")

        processYear = input(f"Enter the {option} data Year to process:  ")
        
        # List Files avalible on in the data container
        print("List of files found in Azure Blob........")
        
        #Create a list of files within the blob for filtering
        for blobs in blob_list:
            print (blobs.name) 
            names.append(blobs.name)
            
        # create a List of Pavement files to process based on year and name    
        for name in names:
            if (name.startswith(option) & name.__contains__(processYear))==True:  
                processList.append(name)
        print("")
        if len(processList)>0:
            print("Files found to be processed are...........")
            for i in processList:
                     print(i)   
        else:
            print("No files found with the above entered parameters")
            print('Task has terminated')
            cursor.close()
            conn.close()
            sys.exit()
                 
        
        action =input("Proceed in processing these files? (y/n):  ")
        if action == 'y':
          print('Processing the above files')
          
          
        elif action == 'n':
            print('Task has terminated')
            cursor.close()
            conn.close()
            sys.exit()
        else:
            print("Invaild response! \nExiting process....")
            cursor.close()
            conn.close()
            sys.exit()
            
            
        #List of DataFrames to Process        
        dataset =[]
       
        for filename in processList:
            blob_service=BlockBlobService(account_name=STORAGEACCOUNTNAME,account_key=STORAGEACCOUNTKEY)
            blob_service.get_blob_to_path(CONTAINERNAME,filename,LOCALFILENAME)
            
        
            df= pd.read_csv(LOCALFILENAME,low_memory=False, na_values="null")
            dataset.append(df)
            
            df= pd.concat(dataset)
                    

        
        print("Renaming columns")
        df2=pd.read_csv(f"C:\\NMB\Data\\{option}ExSchema.csv")
        
        df.columns=df2.keys()
        print("Columns Renamed")
        
        print(df.head())
        print("")
        print("Data is being written to Database...please be patient.")
                

    except Exception as e:
        print(str(e))


def DropTable(table):
    try:
        cursor.execute('Drop table {}'.format(table))
        cursor.commit()
        conn.commit()
    except Exception as e:
        print(str(e))
        

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
    print("")
    print("This script writes csv files from Azure to DW SQL database raw schema")
    print("")
    print("Options are as follows:")        
    print("1. Pavement \n2. Surface\n3. Cracking\n4. Line Markings\n5. Structure\n6. Corporate\n7. Condition")   
    print("")
    process = input("Please select one of the above options to process:  ")
    
    option = optionDict[process]
    
    LOCALFILENAME= input ('Enter table name to be created in SQL database:  ')
    
 
    
    if cursor.tables(table = LOCALFILENAME, tableType='TABLE').fetchone():
        
      tableRemove = input(LOCALFILENAME+" exist in the database. Do want to replace this table. y/n?  ")
          
      if tableRemove == 'y':
          print('Table ' +LOCALFILENAME+' will now be replaced')
          DropTable(LOCALFILENAME)
          blobData()
          
      elif tableRemove == 'n':
          print("Table "+LOCALFILENAME+" will NOT be removed")
          print('Task has terminated')
          cursor.close()
          conn.close()
          sys.exit()
    
    
                                
    else:
        print("")
        print('Table '+LOCALFILENAME+' is not found '+Database_Name+' database and with be inserted')
        
        # execute function
        blobData()

        engine = create_engine("mssql+pyodbc://", poolclass=StaticPool, creator=lambda: conn)
        
        @event.listens_for(engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True

        
        df.to_sql(LOCALFILENAME,
                  con=engine,
                  schema ='Raw',
                  chunksize= 10000,
                  index=False
                  )
        
     
  
        print("")
        print('Table '+LOCALFILENAME+ ' has been add to '+Database_Name+' database')
        print("")
        print("Connection to "+Database_Name+" database on "+Server_Name+" has been closed")
        cursor.close()
        conn.close()
        
        elapsed_time = time.time() - t
        print("The process took "+str(elapsed_time)+" seconds")