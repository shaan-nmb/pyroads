import arcpy
import os
from arcpy import env

arcpy.env.overwriteOutput = True

# get Geodatabase from user
geodatabase = arcpy.GetParameterAsText(0)

#get directory of csv files as stored
direct = arcpy.GetParameterAsText(1)

# get State Roads shapefile to us in processing
shapeFile = arcpy.GetParameterAsText(2)

# get SLK start field
fromSLK = arcpy.GetParameterAsText(3)

# get SLK end field
toSLK = arcpy.GetParameterAsText(4)


#get suffex optional
suffix = arcpy.GetParameterAsText(5)

env.workspace = geodatabase


#Get list of  csv files to process
listFiles = [f for f in os.listdir(direct) if f.endswith('.csv')]


# Conversion of csv files to tables with geodatabase
for i in listFiles:
    L = os.path.splitext(i)[0]  # strips file extension from name
    arcpy.AddMessage("Creating Table "+L+ " in geodatabase")
    arcpy.TableToTable_conversion(i, geodatabase, L)

# Create Make Route Event
for i in listFiles:
    tbl = os.path.splitext(i)[0] # strips file extension from name
    props = "Road_Cway  LINE {} {} ". format(fromSLK, toSLK)  # properties for MakeRouteEventLayer function
    lyr =tbl+suffix 
    arcpy.AddMessage("Creating Event Layer")
    arcpy.AddMessage(lyr)
    rid ='Road_CWY' # fileld name in Shape file


    arcpy.MakeRouteEventLayer_lr(shapeFile, rid, tbl, props,lyr)
    
    arcpy.AddMessage("Create feature class in geodatabase.....")

    # Save Event Layer to feature class in geodatabase
    arcpy.CopyFeatures_management(lyr,lyr)
    # Remove unwanted tables
    arcpy.Delete_management(tbl)







