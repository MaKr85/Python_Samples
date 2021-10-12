import requests
import datetime
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

############################ START ###################################

storageAccountName = "lakestoragedn3azqfsk6qoq"
storageAccountKey = "1heDSZ0TvXKyOZZqqSxFZgUrZG9KGdG+hcu8K7sUd2sTHL3yksX2sVsL1UlvaK06AzXQJ8NnNO9TvgiMeAFs9w=="

# Allgemeines
pathDate = str(datetime.datetime.today().year) + "/" + str(datetime.datetime.today().month).rjust(2,"0") + "/" + str(datetime.datetime.today().day).rjust(2,"0") + "/"
Date = str(datetime.datetime.today().year) + "_" + str(datetime.datetime.today().month).rjust(2,"0") + "_" + str(datetime.datetime.today().day).rjust(2,"0")

# DataLake
serviceURI = "https://" + storageAccountName + ".dfs.core.windows.net/"

# Sink
containerNameSink = "98-temporary"
directoryNameSink = "nectaRosbach"
fileNameSink = "ausgangslieferschein_"+Date+".csv"

# Get Token
get_sessiontoken = requests.post('https://main.necta.at/session?action=login&client=1482&user=Areto5&pwd=Produkt5&lang=2')
sessiontoken = get_sessiontoken.text.replace('@','')

# Save CSV Res
deliveryDateFrom =  str(datetime.datetime.now().date() - datetime.timedelta(days = 3))
deliveryDateTo = str(datetime.datetime.now().date())

url = 'https://main.necta.at/netapi/interfaces/areto/outgoing-delivery-notes?deliveryDateFrom=' + deliveryDateFrom + '&deliveryDateTo=' + deliveryDateTo
res = requests.get(url, headers={'Authorization':'NectaSession ' + sessiontoken + ':Areto5'})

# CSV Standards Sink setzen
columnDelimiterSink = ","
rowDelimiterSink = "\n\r"
escapeDelimiterSink = "\ "
quoteDelimiterSink = '"'

# CSV erstellen
csv = res.text
df_file_source = dt.fread(csv, fill=True)
file_sink = get_SinkFileCSV(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)
#print(file_sink)

# Connect to DataLake
initialize_storage_account(storageAccountName,storageAccountKey)


# SinkCSV schreiben
returnstatus = upload_file_sink(containerNameSink,directoryNameSink,fileNameSink,file_sink)
print(returnstatus);

