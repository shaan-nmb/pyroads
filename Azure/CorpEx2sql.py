# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 05:08:16 2020

This script is the write extracted Corporate data from the 
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
#BLOBNAME= "Corporate Extract/2020_corporate_extract.txt"


Driver = "SQL Server Native Client 17.0"
Server_Name= "mrw-aue-nmbas-sql-dev.database.windows.net"
Database_Name = "DW"
Authentication = 'ActiveDirectoryInteractive'


datatypes ={'SORT_KEY': object,'ROAD_NO,CWAY': object,'SLK_FROM':float,'SLK_TO':float,'NE_ID':int,'NM_BEGIN_MP':int,'NM_END_MP':int,
            'NUMBER_OF_LANES':int,'RESPONSIBLITY_AREA':int,'REGION_BDY':int,'LG_BDY':int,'LKST_LINK_NO':int,
            'FUNCTIONAL_CLASS':object,'COMMONWEALTH_CLASS':object,'TOTAL_PAVE_WIDTH':float,'PAOR_FORM_YEAR':int,
            'PAOR_PAVE_YEAR':int,'PAOR_WIDTH':float,'PAOR_BASE_MATERIAL':object,'PAOR_TOTAL_BASE_DEPTH':int,
            'PAWI_L_PAVE_YEAR':int,'PAWI_L_WIDTH':float,'PAWI_L_BASE_MATERIAL':int,'PAWI_L_TOTAL_BASE_DEPTH':int,
            'PAWI_R_PAVE_YEAR':int,'PAWI_R_WIDTH':float,'PAWI_R_BASE_MATERIAL':int,'PAWI_R_TOTAL_BASE_DEPTH':int,
            'PASH_L_PAVE_YEAR':int,'PASH_L_WIDTH':float,'PASH_L_BASE_MATERIAL':int,'PASH_L_TOTAL_BASE_DEPTH':int,
            'PASH_R_PAVE_YEAR':int,'PASH_R_WIDTH':float,'PASH_R_BASE_MATERIAL':int,'PASH_R_TOTAL_BASE_DEPTH':int,
            'TOTAL_SEAL_WIDTH':float,'XSP_OF_MOST_RECENT_SURFACE_YR':object,'SHARING_XSP_RECENT_SURFACE_YR':object,
            'SULA_YEAR':int,'SULA_TYPE':int,'SULA_DEPTH':int,'SULA_AGG_SIZE':int,'SULA_ENRICHMENT_YEAR':int,'UNSEALED_SHOULDER_L':float,
            'UNSEALED_SHOULDER_R':float,'SUSH_L_WIDTH':float,'SUSH_R_WIDTH':float,'HOAL_SAFE_SPEED':int,'HOAL_CURVE_RADIUS':int,'HOAL_XSP':int,
            'VEAL_K_VALUE':int,'VEAL_ALIGN_TYPE':int,'VEAL_ALIGN_GRADE':float,'SPLI_SPEED_LIMIT':int,'TSDA_YEAR':int,'TSDA_AADT':int,'TSDA_AAWT':int,
            'TSDA_VEH_HEAVY':float,'TSDA_VEH_CLASS1':float,'TSDA_VEH_CLASS2':float,'TSDA_VEH_CLASS3':float,'TSDA_VEH_CLASS4':float,'TSDA_VEH_CLASS5':float,
            'TSDA_VEH_CLASS6':float,'TSDA_VEH_CLASS7':float,'TSDA_VEH_CLASS8':float,'TSDA_VEH_CLASS9':float,'TSDA_VEH_CLASS10':float,'TSDA_VEH_CLASS11':float,
            'TSDA_VEH_CLASS12':float,'OVERTAKING_LANE_L':object,'OVERTAKING_LANE_R':object,'CONTROL_OF_ACCESS':object,'KERB_L,KERB_R':object,'GETE_TYPE':object,
            'ADMA_MATERIAL':int,'ELFE_ELECT_FEDERAL_L':int,'ELFE_ELECT_FEDERAL_R':int,'ELDI_ELECT_DISTRICT_L':int,'ELDI_ELECT_DISTRICT_R':int,
            'ELRE_ELECT_REGION_L':object,'ELRE_ELECT_REGION_R':object,'CDLP_L_NAASRA_LANE':int,'CDLP_L_SURVEY_DATE':int,'CDLP_R_NAASRA_LANE':int,
            'CDLP_R_SURVEY_DATE':int,'RAIN_RAINFALL':int,'RAIN_AVG_AVG_TEMP':float,'RAIN_AVG_MIN_TEMP':float,'RAIN_ENVM':float,'START_TRUE':float,'END_TRUE':float,
            'ROCL_DAYS_CLOSED':int,'LKST_LINK_SUBCATEGORY':object,'LKNL_NLT_LINK':object,'CDLP_L_OUTER_IRI_QC':int,'CDLP_L_INNER_IRI_QC':int,
            'CDLP_L_LANE_IRI_HC':float,'CDLP_R_OUTER_IRI_QC':float,'CDLP_R_INNER_IRI_QC':float,'CDLP_R_LANE_IRI_HC':float,'DGEO_HORIZONTAL':int,
            'DGEO_VERTICAL':int,'UNSEALED':object,'LANEDEF':object,'TURNDEF':object,'TSDA_COUNT_TYPE':object,'ROAD_HIERARCHY':object,'SPECIAL_USE_CATEGORY':object}	





def blobData():
    
    #download from blob as rename columns
    try:
        global df
        print("Retrieving data from Azure Blob")
        blob_service=BlockBlobService(account_name=STORAGEACCOUNTNAME,account_key=STORAGEACCOUNTKEY)
        blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
        
        print("Renaming columns")
        df= pd.read_csv(LOCALFILENAME,low_memory=False, na_values="null",)
        df2=pd.read_csv("C:\\NMB\Data\\CorpExSchema.csv")
        
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
    BLOBNAME= input('Enter name of the data file to process ')
    BLOBNAME = "Corporate Extract/"+BLOBNAME
    
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