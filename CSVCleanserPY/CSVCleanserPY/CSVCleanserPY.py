

#class DataLakeHelper(object):
#    """description of class"""
    # py -m pip install azure-storage-file-datalake
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings


def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)


def download_metadata_file(container_Name_Meta,directory_Name_Meta,file_Name_Meta):
    try:

        file_system_client_meta = service_client.get_file_system_client(file_system=container_Name_Meta)
        directory_client_meta = file_system_client_meta.get_directory_client(directory_Name_Meta)
        file_client_meta = directory_client_meta.get_file_client(file_Name_Meta)

        download_meta = file_client_meta.download_file()

        return download_meta

    except Exception as e:
     print(e)


def check_directory_exists(container_Name_Source,directory_Name):
    try:

        global file_system_client
        global directory_client
        
        file_system_client = service_client.get_file_system_client(file_system=container_Name_Source)
        directory_client = file_system_client.get_directory_client(directory_Name)

        return directory_client.exists()

    except Exception as e:
     print(e)

    
def create_directory(container_Name,directory_Name):
    try:

        global file_system_client
        global directory_client
        
        file_system_client = service_client.get_file_system_client(file_system=container_Name_Source)
        directory_client = file_system_client.get_directory_client(directory_Name)

        return directory_client.create_directory()

    except Exception as e:
     print(e)

    
def list_directory_contents(directory_Name):
    try:

        paths = file_system_client.get_paths(path=directory_Name)

        return paths

    except Exception as e:
     print(e)


def download_file_from_directory(file_Name):
    try:

        file_client = directory_client.get_file_client(file_Name)

        download = file_client.download_file()

        return download

    except Exception as e:
     print(e)


def upload_file_sink(container_Name_Sink,directory_Name_Sink,file_Name_Sink,data,metadataDict={}):
    try:

        global file_client_Sink 

        file_system_client_Sink = service_client.get_file_system_client(file_system=container_Name_Sink)
        directory_client_Sink = file_system_client_Sink.get_directory_client(directory_Name_Sink)
        file_client_Sink = directory_client_Sink.get_file_client(file_Name_Sink)

        content_settings = ContentSettings(content_type = "text/csv", content_encoding="UTF-8")

        file_client_Sink.upload_data(data, overwrite=True, metadata=metadataDict, content_settings=content_settings)

        return "Fileupload " + file_Name_Sink +  " successfull"

    except Exception as e:
     print(e)


#class Helper(object):
#    """description of class"""


def get_file_name(file_path):
    try:

        path = file_path
        individual_data = path.split('/')
        length = len(individual_data)-1

        file_name = individual_data[length]

        return file_name

    except Exception as e:
     print(e)


def get_MetadataFileList(header_source,df_file_source,columnDelimiter):
    try:

        fileListMeta = []
        fileListMeta.append("columnName,dataType")

        columns = df_file_source.ncols

        for icolumns in range(columns): 
            row = header_source[icolumns]+columnDelimiter+str(df_file_source.stypes[icolumns]).replace('stype.','').replace('void','str32')
            fileListMeta.append(row)

        return fileListMeta

    except Exception as e:
     print(e)


def get_MetadataFileCSV(filelistMetaCSV):
    try:

        rowDelimiterSink="\r\n"
        file_MetaCSV = rowDelimiterSink.join(filelistMetaCSV)

        return file_MetaCSV

    except Exception as e:
     print(e)


def get_MetadataFileSQL(filelistMetaCSV,schemaName,tableName):
    try:

        file_MetaCSV = "   "
        rowDelimiterSink="\r\n"

        # Dictionary für Datetypen Python --> SQL aufbauen
        dictPythonSQL = {}
        dictPythonSQL['int32'] = '[int]'
        dictPythonSQL['str32'] = '[nvarchar](4000)'
        dictPythonSQL['time64'] = '[datetime]'
        dictPythonSQL['bool8'] = '[bit]'
        dictPythonSQL['float64'] = '[float]'


        file_MetaCSV = "CREATE TABLE [" + schemaName + "].[" + tableName + "](" + rowDelimiterSink

        for irow in range(len(filelistMetaCSV)):
            if irow > 0: #skip Header
                row = filelistMetaCSV[irow]
                individual_data = row.split(',')
                if dictPythonSQL.get(individual_data[1]) is not None:
                    dataTypeSQL = dictPythonSQL.get(individual_data[1])
                else:
                    dataTypeSQL = '[nvarchar](4000)'
                file_MetaCSV = file_MetaCSV + "[" + individual_data[0] + "] " + dataTypeSQL + " NULL," + rowDelimiterSink

        file_MetaCSV = file_MetaCSV[0:len(file_MetaCSV)-3] + rowDelimiterSink
        file_MetaCSV = file_MetaCSV + ") ON [PRIMARY] "

        return file_MetaCSV

    except Exception as e:
     print(e)


def get_SinkFileCSV(df_file_source,header_Sink,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink):
    try:

        rows = df_file_source.nrows
        columns = df_file_source.ncols

        # filelist[] erzeugen und header hinzufügen
        filelist = []
        filelist.append(header_Sink)

        # über alle Zeilen iterieren und Daten filelist[] hinzufügen 

        for irows in range(rows):
            rowlist=[]
            for icolumns in range(columns):
                if df_file_source[irows,icolumns] is not None:
                    if str(df_file_source[irows,icolumns]).find(columnDelimiterSink) > 0:
                        if str(df_file_source[irows,icolumns]).startswith(quoteDelimiterSink) is True: 
                            rowlist.append(str(df_file_source[irows,icolumns]))
                        else:
                            rowlist.append(quoteDelimiterSink+str(df_file_source[irows,icolumns]).replace(quoteDelimiterSink,escapeDelimiterSink.rstrip()+quoteDelimiterSink)+quoteDelimiterSink)
                    elif str(df_file_source[irows,icolumns]).find(quoteDelimiterSink) > -1:
                        rowlist.append(quoteDelimiterSink+str(df_file_source[irows,icolumns]).replace(quoteDelimiterSink,escapeDelimiterSink.rstrip()+quoteDelimiterSink)+quoteDelimiterSink)
                    else:
                        rowlist.append(str(df_file_source[irows,icolumns]))
                else:
                    rowlist.append('')
                row_sink = columnDelimiterSink.join(rowlist)
            filelist.append(row_sink)

        rowDelimiterSink="\r\n"
        file_sink = rowDelimiterSink.join(filelist)

        return file_sink

    except Exception as e:
     print(e)

# Imports
from datetime import datetime
import csv
import os, uuid, sys

# py -m pip install datatable
import datatable as dt

#from Helper import get_file_name
#from Helper import get_MetadataFileList
#from Helper import get_MetadataFileCSV
#from Helper import get_MetadataFileSQL
#from Helper import get_SinkFileCSV

#from DataLakeHelper import initialize_storage_account
#from DataLakeHelper import download_metadata_file
#from DataLakeHelper import check_directory_exists
#from DataLakeHelper import list_directory_contents
#from DataLakeHelper import download_file_from_directory
#rom DataLakeHelper import upload_file_sink


# Übergabeparameter
#sourceName = getArgument("source")
#storageAccountName = getArgument("storageAccountName")
#storageAccountKey = getArgument("storageAccountKey")

sourceName = "LogicApps"
storageAccountName = "lakestoragedn3azqfsk6qoq"
storageAccountKey = "1heDSZ0TvXKyOZZqqSxFZgUrZG9KGdG+hcu8K7sUd2sTHL3yksX2sVsL1UlvaK06AzXQJ8NnNO9TvgiMeAFs9w=="

#sourceName = "external-sql"
#storageAccountName = "dpstoragetstmkr"
#storageAccountKey = "9fHjRnxj7oU9/cNYN8u/zK1kCDhiI3N/SVGJM4P5vqJah9LA+D9yrSS2CNYQKIrXX4g2pH9IKESOCeYLi1hc/w=="

# Allgemeines
pathDate = str(datetime.today().year) + "/" + str(datetime.today().month).rjust(2,"0") + "/" + str(datetime.today().day).rjust(2,"0") + "/"
# str(datetime.today().day).rjust(2,"0")
globalFileExists = True

# DataLake
serviceURI = "https://" + storageAccountName + ".dfs.core.windows.net/"

# Source
containerNameSource = "01-raw"
directoryNameSource = sourceName + "/" + pathDate
fileNameSource = ""

# Sink
containerNameSink = "02-cleansed"
directoryNameSink = sourceName
fileNameSink = ""

# MetaDataGlobal
containerNameMeta = "99-metadata"
directoryNameMetaGlobal = "99-globalcsv"
fileNameMetaGlobal = "CSVGlobalMetadata.csv"

# MetaDataSink
directoryNameMetaSink = sourceName
fileNameMetaCSV = ""
fileNameMetaSQL = ""

# CSV Standards
currentFileNameMeta = ""
metadataFound = "false"
columnDelimiter = ","
rowDelimiter = "\n\r"
escapeDelimiter = "\ "
quoteDelimiter = '"'
addHeader = "0"
changeHeader = "0"
header = ""

# Connect to DataLake
initialize_storage_account(storageAccountName,storageAccountKey)


if globalFileExists==True:
    # Get Metadata
    df_meta = dt.fread(download_metadata_file(containerNameMeta,directoryNameMetaGlobal,fileNameMetaGlobal).readall())
    rows_meta = df_meta.nrows


if check_directory_exists(containerNameSource,directoryNameSource)==True:

    # Get Files in Directory
    file_paths = list_directory_contents(directoryNameSource)

    for file_path in file_paths:

            fileNameSource = get_file_name(file_path.name)
            fileNameSink = fileNameSource.replace('.csv', '') + '_cleansed.csv'
            fileNameMetaCSV = fileNameSource.replace('.csv', '') + '_Metadata.csv'
            fileNameMetaSQL = fileNameSource.replace('.csv', '') + '_SQL.txt'

            # CSV Standards Sink setzen
            columnDelimiterSink = ","
            rowDelimiterSink = "\n\r"
            escapeDelimiterSink = "\ "
            quoteDelimiterSink = '"'

            # CSV Standards setzen
            currentFileNameMeta = ""
            metadataFound = "false"
            columnDelimiter = ","
            rowDelimiter = "\n\r"
            escapeDelimiter = "\ "
            quoteDelimiter = '"'
            addHeader = "0"
            changeHeader = "0"
            header = ""

            if globalFileExists==True:
            # wenn Metadaten für aktuelles File vorhanden, CSV Standards überschreiben
                for irows_meta in range(rows_meta):
                    currentFileNameMeta = df_meta[irows_meta,0]
                    if currentFileNameMeta == fileNameSource:
                        metadataFound = "true"
                        if df_meta[irows_meta,1] is not None:
                            columnDelimiter = df_meta[irows_meta,1]
                        if df_meta[irows_meta,2] is not None:
                            rowDelimiter = df_meta[irows_meta,2]
                        if df_meta[irows_meta,3] is not None:
                            escapeDelimiter = df_meta[irows_meta,3]
                        if df_meta[irows_meta,4] is not None:
                            quoteDelimiter = df_meta[irows_meta,4]
                        if df_meta[irows_meta,5] is not None:
                            addHeader = df_meta[irows_meta,5]
                        if df_meta[irows_meta,6] is not None:
                            changeHeader = df_meta[irows_meta,6]
                        if df_meta[irows_meta,7] is not None:
                            header = df_meta[irows_meta,7]
                        break;


            # File einlesen

            file_source = download_file_from_directory(fileNameSource).readall()
            df_file_source = dt.fread(file_source, fill=True) 

            rows = df_file_source.nrows
            columns = df_file_source.ncols

            # Header setzen     
            if changeHeader is True:
                header_source = header.split(columnDelimiter)
            elif addHeader is True:
                header_source = header.split(columnDelimiter)
            else:
                header_source = df_file_source.names

            header_Sink = columnDelimiterSink.join(header_source)

            # Metadata Dictionary erzeugen
            metadataDict = {}

            for icolumns in range(columns): 
                metadataDict[header_source[icolumns].replace(' ','_').replace('.','')] = str(df_file_source.stypes[icolumns]).replace('stype.','').replace('void','str32').replace('.','')

            # MetadataCSV erzeugen

            file_MetaCSV = get_MetadataFileCSV(get_MetadataFileList(header_source,df_file_source,columnDelimiter))

            # MetadataSQL erzeugen

            file_MetaSQL = get_MetadataFileSQL(get_MetadataFileList(header_source,df_file_source,columnDelimiter),"stage",fileNameSource.replace('.csv', ''))

            # SinkCSV erzeugen

            file_sink = get_SinkFileCSV(df_file_source,header_Sink,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)

            # SinkFiles schreiben
            # Prüfung ob directory bereits vorhanden, wenn nicht dann erzeugen

            # if check_directory_exists(containerNameSink,directoryNameSink)==False:
              #  create_directory(containerNameSink,directoryNameSink)
                        
            # SinkCSV schreiben

            returnstatus = upload_file_sink(containerNameSink,directoryNameSink,fileNameSink,file_sink,metadataDict)
            print(returnstatus);

            # Metadata schreiben

            returnstatus = upload_file_sink(containerNameMeta,directoryNameMetaSink,fileNameMetaCSV,file_MetaCSV)
            print(returnstatus);

            returnstatus = upload_file_sink(containerNameMeta,directoryNameMetaSink,fileNameMetaSQL,file_MetaSQL)
            print(returnstatus);
