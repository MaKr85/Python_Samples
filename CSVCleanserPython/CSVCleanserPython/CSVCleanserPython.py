# Imports
from datetime import datetime
import csv
import os, uuid, sys

# py -m pip install pydatatable
import datatable as dt

from Helper import get_file_name
from Helper import get_MetadataFileList
from Helper import get_MetadataFileCSV
from Helper import get_MetadataFileSQL
from Helper import get_SinkFileCSV

from DataLakeHelper import initialize_storage_account
from DataLakeHelper import download_metadata_file
from DataLakeHelper import check_directory_exists
from DataLakeHelper import list_directory_contents
from DataLakeHelper import download_file_from_directory
from DataLakeHelper import upload_file_sink


# Übergabeparameter
sourceName = "external-sql"

# Allgemeines
pathDate = str(datetime.today().year) + "/" + str(datetime.today().month).rjust(2,"0") + "/" + "12" + "/"
# str(datetime.today().day).rjust(2,"0")

# DataLake
storageAccountName = "dpstoragetstmkr"
storageAccountKey = "9fHjRnxj7oU9/cNYN8u/zK1kCDhiI3N/SVGJM4P5vqJah9LA+D9yrSS2CNYQKIrXX4g2pH9IKESOCeYLi1hc/w=="
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

            # wenn Metadaten für aktuelles File vorhanden, CSV Standards überschreiben
            for irows_meta in range(rows_meta):
                currentFileNameMeta = df_meta[irows_meta,0]
                if currentFileNameMeta == fileNameSource or fileNameSource.startswith(currentFileNameMeta):
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
            df_file_source = dt.fread(file_source) 

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
                metadataDict[header_source[icolumns].replace(' ','_')] = str(df_file_source.stypes[icolumns]).replace('stype.','').replace('void','str32')

            # MetadataCSV erzeugen

            file_MetaCSV = get_MetadataFileCSV(get_MetadataFileList(header_source,df_file_source,columnDelimiter))

            # MetadataSQL erzeugen

            file_MetaSQL = get_MetadataFileSQL(get_MetadataFileList(header_source,df_file_source,columnDelimiter),"stage",fileNameSource.replace('.csv', ''))

            # SinkCSV erzeugen

            file_sink = get_SinkFileCSV(df_file_source,header_Sink,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)

            # SinkFiles schreiben
            # Prüfung ob directory bereits vorhanden, wenn nicht dann erzeugen

            if check_directory_exists(containerNameSink,directoryNameSink)==False:
                create_directory(containerNameSink,directoryNameSink)

            # SinkCSV schreiben

            returnstatus = upload_file_sink(containerNameSink,directoryNameSink,fileNameSink,file_sink,metadataDict)
            print(returnstatus);

            # Metadata schreiben

            returnstatus = upload_file_sink(containerNameMeta,directoryNameMetaSink,fileNameMetaCSV,file_MetaCSV)
            print(returnstatus);

            returnstatus = upload_file_sink(containerNameMeta,directoryNameMetaSink,fileNameMetaSQL,file_MetaSQL)
            print(returnstatus);
