import arcpy
import os
from arcpy import env

arcpy.env.overwriteOutput = True

''"For Testing purposes""
#direct = r"//mrwa.wa.gov.au/dfsroot/MyDocs-DAC/C8247/Documents/Development/Python GIS/CSV Files"
#shapeFile = r"//mrwa.wa.gov.au/dfsroot/MyDocs-DAC/C8247/Documents/Development/Python GIS/Shpfiles/Scrh_IRIS_StateRoad_Network_20200921.shp"
#out_gdb = r"//mrwa.wa.gov.au/dfsroot/MyDocs-DAC/C8247/Documents/ArcGIS/NMB.gdb"
#env.workspace = r"//mrwa.wa.gov.au/dfsroot/MyDocs-DAC/C8247/Documents/ArcGIS/NMB.gdb"

direct = input("Enter the Directory the cvs files relocated:  ")
shapeFile = input("Enter the shapefile to be used:  ")
out_gdb = input ("Enter the geodatabase to store results:  ")

fromSLK= input ("Enter the name of the SLK  start field  ")
toSLK= input ("Enter the name of the SLK  end field:  ")
suffix = input("Enter the suffix for the output feature class:  ")

# set workspace
env.workspace = out_gdb

#Get list of  csv files to process
listFiles = [f for f in os.listdir(direct) if f.endswith('.csv')] 

# Conversion of csv files to tables with geodatabase
for i in listFiles:
    L = os.path.splitext(i)[0]  # strips file extension from name
    print ("Creating Table "+L+ " in geodatabase")
    arcpy.TableToTable_conversion(i, out_gdb, L)

# Create Make Route Event
for i in listFiles:
    tbl = os.path.splitext(i)[0] # strips file extension from name
    props = "Road_Cway  LINE {} {}".format(fromSLK, toSLK) # properties for MakeRouteEventLayer function
    lyr =tbl+suffix # Name of the output file
    print("Creating Event  Layer")
    print(lyr)
    rid ='Road_CWY' # fileld name in Shape file 

    arcpy.MakeRouteEventLayer_lr(shapeFile, rid, tbl, props,lyr)
    print("Create feature class in geodatabase.....")

    # Save Event Layer to feature class in geodatabase
    arcpy.CopyFeatures_management(lyr,lyr)
    # Remove unwanted table
    arcpy.Delete_management(tbl)






