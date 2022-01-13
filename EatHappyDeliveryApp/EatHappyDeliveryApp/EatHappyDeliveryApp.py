import pandas as pd
import requests as rq
from datetime import datetime, timedelta
import datatable as dt
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake._models import ContentSettings


def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)


def upload_file_sink(container_Name_Sink,directory_Name_Sink,file_Name_Sink,data):
    try:

        file_system_client_Sink = service_client.get_file_system_client(file_system=container_Name_Sink)
        directory_client_Sink = file_system_client_Sink.get_directory_client(directory_Name_Sink)
        file_client_Sink = directory_client_Sink.get_file_client(file_Name_Sink)

        content_settings = ContentSettings(content_type = "text/csv", content_encoding="UTF-8")

        file_client_Sink.upload_data(data, overwrite=True, content_settings=content_settings)

        return "Fileupload " + file_Name_Sink +  " successfull"

    except Exception as e:
     print(e)

def get_SinkFileCSV(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink):
    try:

        rows = df_file_source.nrows
        columns = df_file_source.ncols

        # filelist[] erzeugen und Header hinzu
        filelist = []
        filelist.append(columnDelimiterSink.join(df_file_source.names))

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


def transform2_df(res):
    df = pd.DataFrame([x.split(';') for x in res.split('\n')])
    df = df.astype(str)
    df = df.applymap(lambda x: x.replace('"', ''))
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df

def df1(df):
    ls1= []
    for i in range(1,len(df.index)):
        dict_ = {}
        if df.loc[i,'tour.documentdiscard1.row.discard'] == '' or df.loc[i,'tour.documentdiscard1.row.discard'] == 'nan': 
            continue
        else:
            list_ = (df.loc[i,'tour.documentdiscard1.row.discard']).split(',')
            ids1 = df.loc[i,'id']
            time1 = df.loc[i,'tour.documentdiscard1.submit.timestamp']

            if len(list_) % 2:
                continue
            else:
                for i in range(0,len(list_),2):
                        dict1 =  {'tour.documentdiscard1.row.discard_Amount': list_[i],
                         'tour.documentdiscard1.row.discard_EAN': list_[i+1],
                         'id': ids1,
                         'tour.documentdiscard1.submit.timestamp':time1
                        }

                        ls1.append(dict1)
    df_1 = pd.DataFrame(ls1)
    return df_1


def df2(df):
    ls2 = []
    for i in range(1,len(df.index)):
        dict_ = {}

        if df.loc[i,"tour.scandiscard.row.discard"] == '' or df.loc[i,"tour.scandiscard.row.discard"] == 'nan':
                continue
        else:
            list2 = (df.loc[i,'tour.scandiscard.row.discard']).split(',')
            ids2 = df.loc[i,'id']
            time1 = df.loc[i,'tour.documentdiscard1.submit.timestamp']

            if len(list2) % 2:
                continue
            else:
                for i in range(0,len(list2),2):
                        dict2 =  {'tour.scandiscard.row.discard_Amount': list2[i],
                            'tour.scandiscard.row.discard_EAN': list2[i+1],
                             'id': ids2,
                             'tour.documentdiscard1.submit.timestamp':time1
                        }

                        ls2.append(dict2)
    df_2 = pd.DataFrame(ls2)
    return df_2



############################ START ###################################

storageAccountName = "lakestoragedn3azqfsk6qoq"
storageAccountKey = "1heDSZ0TvXKyOZZqqSxFZgUrZG9KGdG+hcu8K7sUd2sTHL3yksX2sVsL1UlvaK06AzXQJ8NnNO9TvgiMeAFs9w=="
Directory = "deliveryApp"
X_User = "report"
X_Password = "qbqcEkAgRFrcSazW3J9QcSTv"


# Allgemeines
Date = str(datetime.today().year) + "_" + str(datetime.today().month).rjust(2,"0") + "_" + str(datetime.today().day).rjust(2,"0")

# Sink
containerNameSink = "98-temporary"
directoryNameSink = Directory

# CSV Standards Sink setzen
columnDelimiterSink = ","
rowDelimiterSink = "\n\r"
escapeDelimiterSink = "\ "
quoteDelimiterSink = '"'

# getResponse
from_ = datetime.today() - timedelta(days=3)
to =  datetime.today()
url = 'https://eathappy.structr.com/api/v1/getReportData.csv?from=' + str(from_) + '&to=' + str(to) 

res = rq.post(url=url,headers={'X-User': X_User, 'X-Password': X_Password})

if res.text:
    print('Response received')
    df_og = transform2_df(res.text)

    # Connect to DataLake
    initialize_storage_account(storageAccountName,storageAccountKey)

    # verwurf_gestern
    fileNameSink = "verwurf_gestern_"+Date+".csv"

    output_csv_as_string_verwurf_gestern = df1(df_og).to_csv(sep=';', index=False)
    df_file_source = dt.fread(output_csv_as_string_verwurf_gestern, fill=True)
    file_sink = get_SinkFileCSV(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)
    print(file_sink)
    
    # SinkCSV schreiben
    #returnstatus = upload_file_sink(containerNameSink,directoryNameSink,fileNameSink,file_sink)
    #print(returnstatus)


    # verwurf_tour
    fileNameSink = "verwurf_tour_"+Date+".csv"

    output_csv_as_string_verwurf_tour = df2(df_og).to_csv(sep=';', index=False)
    df_file_source = dt.fread(output_csv_as_string_verwurf_tour, fill=True)
    file_sink = get_SinkFileCSV(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)
    print(file_sink)

    # SinkCSV schreiben verwurf_tour
    #returnstatus = upload_file_sink(containerNameSink,directoryNameSink,fileNameSink,file_sink)
    #print(returnstatus)


else:
    print('No response - No CSV created')

