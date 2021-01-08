# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 05:08:16 2020

This script is the write extracted condition data from the 
Azure cloud to an SQl database.

The following steps are preformed.
Connect to Azure Blob
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



def blobData():
    
    #download from blob as rename columns
    try:
        global df
        print("Retrieving data from Azure Blob")
        blob_service=BlockBlobService(account_name=STORAGEACCOUNTNAME,account_key=STORAGEACCOUNTKEY)
        blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
        
        print("Renaming columns")
        df= pd.read_csv(LOCALFILENAME,low_memory=False, na_values="null",)
        df2=pd.read_csv("C:\\NMB\Data\\ConditionExSchema.csv")
          
        df.columns=df2.keys()
        print("Columns renamed")
                
        print(df.dtypes)
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
    
    t = time.process_time()
      
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
            
    
    #User input for table name
    BLOBNAME= input('Enter name of the data file to process: ')
    BLOBNAME = "Condition Extract/"+BLOBNAME
    
    LOCALFILENAME= input ('Enter table name to be created:  ')
    
 
    
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
        
        elapsed_time = time.process_time() - t
        print("The process took "+str(elapsed_time)+" seconds")