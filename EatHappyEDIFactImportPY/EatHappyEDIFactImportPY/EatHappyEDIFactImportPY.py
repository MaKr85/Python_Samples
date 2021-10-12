
#pip install azure-storage-file-share
#pip install datatable
#pip install pydifact


from pydifact.message import Message
import pandas as pd
import datetime
import datatable as dt
import io
from decimal import *

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

        master_frame = pd.DataFrame(columns = ['Sender','Recipient','DateOfPreperation','IcReference','MessageReference','DocumentDate','ReportStartDate','ReportEndDate','Buyer', 'CorporateOffice','Supplier', 'LocationOfSale', 'Currency', 'Location','SalesDate','EAN','CodeList', 'CodeListAgency','LineItemNo','MonetaryAmountType','LineAmount','QuantityQualifier','Quantity'])
        message = Message.from_str(edi)
        row_buffer = [''] * 23
        
      
        for segment in message.segments:
            passedQTY = False
            if segment.tag == 'UNB':
                row_buffer[0] = str(segment.elements[1][0]) # INTERCHANGE SENDER
                row_buffer[1] = str(segment.elements[2][0]) # INTERCHANGE RECIPIENT
                row_buffer[2] = '20' + str(segment.elements[3][0])
                row_buffer[3] = str(segment.elements[4]) # Interchange control reference
            if segment.tag == 'UNH':
                row_buffer[4:] = [None] * (len(row_buffer) - 4)
                row_buffer[4] = int(segment.elements[0]) # Message reference number
            if segment.tag == 'DTM':
                dtmTypeMapper = {
                137:  5,
                 90:  6,
                 91:  7,
                356: 13
                }
                columnIndex = dtmTypeMapper.get(int(segment.elements[0][0]))
                if type(columnIndex).__name__ != 'int':
                    raise NotImplementedError('Date qualifier ' + str(segment.elements[0][0]) + ' not implemented')
                row_buffer[columnIndex] = str(segment.elements[0][1])[0:8] #edidate_to_str(str(segment.elements[0][1]), int(segment.elements[0][2]))
            if segment.tag == 'NAD':
                nadTypeMapper = {
                'BY':  8,
                'CO':  9,
                'SU': 10
                }
                columnIndex = nadTypeMapper.get(str(segment.elements[0]))
                if type(columnIndex).__name__ != 'int':
                    raise NotImplementedError('Name qualifier ' + str(segment.elements[0]) + ' not implemented') 
                row_buffer[columnIndex] = str(segment.elements[1][0])
            if segment.tag == 'CUX':
                if int(segment.elements[0][0]) == 2:
                    row_buffer[11] = str(segment.elements[0][1])
            if segment.tag == 'LOC':
                if int(segment.elements[0]) == 162:
                    if row_buffer[12] is not None:
                        #ctx.emit(*row_buffer)
                        row_buffer[12:] = [None] * (len(row_buffer) - 12)
                    row_buffer[12] = str(segment.elements[1][0])
            if segment.tag == 'LIN':
                if row_buffer[14] is not None:
                    #ctx.emit(*row_buffer)
                    row_buffer[14:] = [None] * (len(row_buffer) - 14)
            
                row_buffer[14] = int(segment.elements[0])
                if len(segment.elements[2]) > 3:
                    row_buffer[15] = str(segment.elements[2][0])
                    row_buffer[16] = str(segment.elements[2][1])
                    row_buffer[17] = str(segment.elements[2][2])
                    row_buffer[18] = str(segment.elements[2][3])
                else:
                    row_buffer[15] = ''
                    row_buffer[16] = str(segment.elements[2][0])
                    row_buffer[17] = str(segment.elements[2][1])
                    row_buffer[18] = str(segment.elements[2][2])

            if segment.tag == 'MOA':
                row_buffer[19] = str(segment.elements[0][0])
                row_buffer[20] = str(segment.elements[0][1])
            if segment.tag == 'QTY':
                if row_buffer[21] is not None:
                    #ctx.emit(*row_buffer)
                    row_buffer[21] = None
                    row_buffer[22] = None
                row_buffer[21] = str(segment.elements[0][0])
                row_buffer[22] = str(segment.elements[0][1])
                passedQTY = True
            
            if passedQTY == True:
                row = {}
                row['Sender'] = row_buffer[0]
                row['Recipient'] = row_buffer[1]
                row['DateOfPreperation'] = row_buffer[2]
                row['IcReference'] = row_buffer[3]
                row['MessageReference'] = row_buffer[4]
                row['DocumentDate'] = row_buffer[5]
                row['ReportStartDate'] = row_buffer[6]
                row['ReportEndDate'] = row_buffer[7]
                row['Buyer'] = row_buffer[8]
                row['CorporateOffice'] = row_buffer[9]
                row['Supplier'] = row_buffer[10]
                row['LocationOfSale'] = row_buffer[11]
                row['Currency'] = row_buffer[12]
                row['Location'] = row_buffer[13]
                row['SalesDate'] = row_buffer[14]
                row['EAN'] = row_buffer[15]
                row['CodeList'] = row_buffer[16]
                row['CodeListAgency'] = row_buffer[17]
                row['LineItemNo'] = row_buffer[18]
                row['MonetaryAmountType'] = row_buffer[19]
                row['LineAmount'] = row_buffer[20]
                row['QuantityQualifier'] = row_buffer[21]
                row['Quantity'] = row_buffer[22]
                
                master_frame = master_frame.append(row,ignore_index = True)
                #print(master_frame)         
                
        wb = master_frame.to_csv(index=False)
        df_file_source = dt.fread(wb, fill=True)




        #interchange = Interchange.from_str(edi)

        #master_frame = pd.DataFrame(columns = ['Buyer', 'CorporateOffice','Supplier', 'LocationOfSale', 'SalesDate', 'Currency','EAN','LineAmount','Quantity'])

        #buyer = ""
        #corporateOffice = ""
        #supplier = ""
        #currency = ""
        #locationOfSale = ""
        #salesDate = ""
        #ean = ""
        #lineAmount = ""
        #quantity = ""

        #for message in interchange.get_messages():
        #    for segment in message.segments:
        #        #print('Segment tag: {}, content: {}'.format(segment.tag, segment.elements))

        #        passedQTY = False

        #        #Daten aus EDI HEader ermitteln
        #        if segment.tag == 'NAD':
        #            if segment.elements[0] == 'BY':
        #                buyer = str(segment.elements[1][0])
        #            if segment.elements[0] == 'CO':
        #                corporateOffice = str(segment.elements[1][0])
        #            if segment.elements[0] == 'SU':
        #                supplier = str(segment.elements[1][0])
        #        if segment.tag == 'CUX':
        #            currency = str(segment.elements[0][1])
        #        if segment.tag == 'LOC':
        #            locationOfSale = str(segment.elements[1][0])
        #        if segment.tag == 'DTM':
        #            if segment.elements[0] == '356':
        #                salesDate = str(segment.elements[0][1])

        #        #Daten aus Positionen ermitteln
        #        if segment.tag == 'LIN':
        #            ean =  str(segment.elements[2][0])
        #        if segment.tag == 'MOA':
        #            if segment.elements[0][0] == '203':
        #                lineAmount = str(segment.elements[0][1])
        #        if segment.tag == 'QTY':
        #            if segment.elements[0][0] == '153':
        #                quantity = str(segment.elements[0][1])
        #                passedQTY = True

        #        #DataFrame füllen
        #        row = {}
        #        row['Buyer'] = buyer
        #        row['CorporateOffice'] = corporateOffice
        #        row['Supplier'] = supplier
        #        row['LocationOfSale'] = locationOfSale
        #        row['SalesDate'] = salesDate
        #        row['Currency'] = currency
        #        row['EAN'] = ean
        #        row['LineAmount'] = lineAmount
        #        row['Quantity'] = quantity

        #        if passedQTY == True:
        #            master_frame = master_frame.append(row,ignore_index = True)

        #wb = master_frame.to_csv(index=False)
        #df_file_source = dt.fread(wb, fill=True) 

        # CSV erstellen
        file_sink = get_SinkFileCSV(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)
        print(file_sink)

        # Transfer durchführen
        #transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        #print(directory+": "+fileName+" "+transfer_result)
