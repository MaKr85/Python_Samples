class Helper(object):
    """description of class"""


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


        file_MetaCSV = "DROP TABLE IF EXISTS [" + schemaName + "].[" + tableName + "]" + rowDelimiterSink + rowDelimiterSink
        file_MetaCSV = file_MetaCSV + "CREATE TABLE [" + schemaName + "].[" + tableName + "](" + rowDelimiterSink

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