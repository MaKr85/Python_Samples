
#pip install azure-storage-file-share
#pip install datatable
#pip install pydifact


from pydifact.segmentcollection import Interchange
import pandas as pd
import datetime
import datatable as dt
import io

from azure.storage.fileshare import ShareServiceClient
from azure.storage.fileshare import ShareDirectoryClient
from azure.storage.fileshare import ShareFileClient


def doTransfer(file_client,file_client_sink,file_client_temporary,file_sink):
    try:

        # CSV in temporary speichern
        file_client_temporary.upload_file(file_sink)

        # Originaldatei ins Archiv verschieben und aus new löschen
        file_client_sink.upload_file(file_sink)
        file_client.delete_file()

        return "successfull"

    except Exception as e:
        print(e)
        return "failed"

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



################################### START #################################


fileShareAccountName = "archivedn3azqfsk6qoq"
fileShareAccountKey = "3M9upkvW4TLtkyRCW7TgDTFKCi5AfsFidUdRyo9lf9XszBe9B+7jg2rW6p+SibjzLq9v3xurcPzzkq8qSdzNig=="
shareName = "logicapps"
directory = "X400_Edifact"

directory_path=directory+"/new"
directory_path_sink=directory+"/archive"
directory_path_temporary=directory+"/temporary"
connection_string = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(fileShareAccountName,fileShareAccountKey)
parent_dir = ShareDirectoryClient.from_connection_string(conn_str=connection_string, share_name=shareName, directory_path=directory_path)
my_files = list(parent_dir.list_directories_and_files())

for file in my_files:
    fileName = file.name

    file_client = ShareFileClient.from_connection_string(conn_str=connection_string, share_name=shareName, file_path=directory_path+"/"+fileName)
    file_client_sink = ShareFileClient.from_connection_string(conn_str=connection_string, share_name=shareName, file_path=directory_path_sink+"/"+fileName.replace(".xlsx",".csv").replace(".xls",".csv").replace(".xlsm",".csv"))
    file_client_temporary = ShareFileClient.from_connection_string(conn_str=connection_string, share_name=shareName, file_path=directory_path_temporary+"/"+fileName.replace(".xlsx",".csv").replace(".xls",".csv").replace(".xlsm",".csv"))


    # CSV Standards Sink setzen
    columnDelimiterSink = ","
    rowDelimiterSink = "\n\r"
    escapeDelimiterSink = "\ "
    quoteDelimiterSink = '"'

    # SinkCSV erzeugen

    if directory == "X400_Edifact":

        stream = file_client.download_file()
        edi = stream.readall().decode('utf-8')

        interchange = Interchange.from_str(edi)

        master_frame = pd.DataFrame(columns = ['Buyer', 'CorporateOffice','Supplier', 'LocationOfSale', 'SalesDate', 'Currency','EAN','LineAmount','Quantity'])

        buyer = ""
        corporateOffice = ""
        supplier = ""
        currency = ""
        locationOfSale = ""
        salesDate = ""
        ean = ""
        lineAmount = ""
        quantity = ""

        for message in interchange.get_messages():
            for segment in message.segments:
                #print('Segment tag: {}, content: {}'.format(segment.tag, segment.elements))

                passedQTY = False

                #Daten aus EDI HEader ermitteln
                if segment.tag == 'NAD':
                    if segment.elements[0] == 'BY':
                        buyer = str(segment.elements[1][0])
                    if segment.elements[0] == 'CO':
                        corporateOffice = str(segment.elements[1][0])
                    if segment.elements[0] == 'SU':
                        supplier = str(segment.elements[1][0])
                if segment.tag == 'CUX':
                    currency = str(segment.elements[0][1])
                if segment.tag == 'LOC':
                    locationOfSale = str(segment.elements[1][0])
                if segment.tag == 'DTM':
                    if segment.elements[0] == '356':
                        salesDate = str(segment.elements[0][1])

                #Daten aus Positionen ermitteln
                if segment.tag == 'LIN':
                    ean =  str(segment.elements[2][0])
                if segment.tag == 'MOA':
                    if segment.elements[0][0] == '203':
                        lineAmount = str(segment.elements[0][1])
                if segment.tag == 'QTY':
                    if segment.elements[0][0] == '153':
                        quantity = str(segment.elements[0][1])
                        passedQTY = True

                #DataFrame füllen
                row = {}
                row['Buyer'] = buyer
                row['CorporateOffice'] = corporateOffice
                row['Supplier'] = supplier
                row['LocationOfSale'] = locationOfSale
                row['SalesDate'] = salesDate
                row['Currency'] = currency
                row['EAN'] = ean
                row['LineAmount'] = lineAmount
                row['Quantity'] = quantity

                if passedQTY == True:
                    master_frame = master_frame.append(row,ignore_index = True)

        wb = master_frame.to_csv(index=False)
        df_file_source = dt.fread(wb, fill=True) 

        # CSV erstellen
        file_sink = get_SinkFileCSV(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)
        print(file_sink)

        # Transfer durchführen
        transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        print(directory+": "+fileName+" "+transfer_result)
