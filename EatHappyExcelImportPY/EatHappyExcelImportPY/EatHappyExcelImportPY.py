
#pip install azure-storage-file-share
#pip install datatable
#pip install pandas
#pip install openpyxl
#pip install xlrd

# Imports
from datetime import datetime
from datetime import timedelta
import datatable as dt
import csv
import sys, glob, xlrd, io
import pandas as pd
import openpyxl


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


def doTransferNoRows(file_client,file_client_sink,file_client_temporary,file_sink,rows):
    try:

        if rows > 0:
            # CSV in temporary speichern
            file_client_temporary.upload_file(file_sink)

        # Originaldatei ins Archiv verschieben und aus new löschen
        file_client_sink.upload_file(file_sink)
        file_client.delete_file()

        return "successfull"

    except Exception as e:
        print(e)
        return "failed"



def get_SinkFileCSVBilla(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink):
    try:

        rows = df_file_source.nrows
        columns = df_file_source.ncols

        # filelist[] erzeugen
        filelist = []

        # Datum aus erster Zeile ziehen
        date_value=df_file_source[0,0].replace('Umsatzauswertung für Eat Happy Sushi - Div. Filiale per: ','')

        # über alle Zeilen iterieren und Daten filelist[] hinzufügen 

        for irows in range(rows):
            rowlist=[]
            #erste beide Zeilen und die Summenzeile am Ende skippen
            if irows > 1 and irows < (rows-1): 
                if irows == 2:
                    rowlist.append("Datum")
                else:
                     rowlist.append(date_value)
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


def get_SinkFileCSVMerkur(df_file_source,df_file_source_gestern,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink):
    try:

        rows = df_file_source.nrows
        columns = df_file_source.ncols

        # filelist[] erzeugen
        filelist = []

        # Datum aus erster Zeile ziehen, wenn nicht vorhanden, dann in den "gestern"Reiter gehen und dort das Datum nehmen und einen Tag abziehen
        if len(df_file_source[0,0]) > 10:
            date_value=df_file_source_gestern.names[0].replace('Umsatzauswertung für Eat Happy Sushi  - Div. Filiale per: ','')
            date_value_dt=datetime.strptime(date_value,"%d.%m.%Y") - timedelta(days=1)
            date_value=date_value_dt.strftime("%d.%m.%Y")
        else:
            date_value=df_file_source.names[0].replace('Umsatzauswertung für Eat Happy Sushi  - Div. Filiale per: ','')

        # über alle Zeilen iterieren und Daten filelist[] hinzufügen 

        for irows in range(rows):
            rowlist=[]
            #erste beide Zeilen und die Summenzeile am Ende skippen
            if irows > 1 and irows < (rows-1): 
                if irows == 2:
                    rowlist.append("Datum")
                else:
                     rowlist.append(date_value)
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


def get_SinkFileCSVSutterluety(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink):
    try:

        rows = df_file_source.nrows
        columns = df_file_source.ncols

        # filelist[] erzeugen
        filelist = []

        # über alle Zeilen iterieren und Daten filelist[] hinzufügen 

        for irows in range(rows):
            rowlist=[]
            #Summenzeile am Ende skippen
            if irows < (rows-1): 
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


def get_SinkFileCSVEdekaFrauen(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink,isMochi):
    try:

        rows = df_file_source.nrows
        columns = df_file_source.ncols

        # filelist[] erzeugen
        filelist = []

        markt_name = df_file_source.names[0].replace("Umsatzaufstellung ","")

        if isMochi == True:
            umsatzart = "Pick&Mix"
        else:
            umsatzart = "Shopumsatz"

        my_list=[2,9,10,17,18,25,26,33,34,41,42,49,50,57,58]

        # über alle Zeilen iterieren und Daten filelist[] hinzufügen 

        for irows in range(rows):
            rowlist=[]
            #erste Zeile und die Summenzeile am Ende skippen
            if irows > 0 and irows < (rows-1) and irows not in my_list: 
                if irows == 1:
                    rowlist.append("MARKT")
                    rowlist.append("UMSATZART")
                else:
                    rowlist.append(markt_name)
                    rowlist.append(umsatzart)
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


    
def readExcelEdekaNordbayern(file_client):

    with io.BytesIO() as f:
        downloader = file_client.download_file()
        b = downloader.readinto(f)
        
        c_list = ['MARKT_ID', 'MARKT_NAME', 'KONZESSIONAER', 'ARTIKEL_NR', 'UMSATZART', 'GTIN', 'DATUM', 'UMSATZ', 'ZULETZT_ABGEFRAGT', 'KONZESSIONAER_NR', 'LIEFERANTEN_NR', 'MARKT_ID_2', 'KONZESSIONAER_PFAD']
        master_frame = pd.DataFrame([], columns=c_list)
        df = pd.read_excel(f, sheet_name='Tabelle1', index_col=None, header=None)
        
        if type(df.iloc[49,0]) == str:
            raise Exception("Data has no sales info!")
        else:    
            markt_id_2 = df.iloc[7,8]
            konzessionaer_nr = df.iloc[6,8]
            lieferant_nr = df.iloc[14,2]
            last_updated = df.iloc[4,2]
            konzessionaer_pfad = df.iloc[2,2]
        
            with io.BytesIO() as e:
              downloader = file_client.download_file()
              b = downloader.readinto(e)

              df = pd.read_excel(e, sheet_name='Tabelle1', index_col=None, skiprows=[i for i in range(0,49)])
              df = df[(df['Unnamed: 3'] != 'Ergebnis')] #Delete all Rows marked as Ergebnis
              df_attr = df.iloc[1:,:6]
              df_sales = df[[c for c in df.columns
                             if not c.endswith('.1')
                             and len(c) == 10
                             and not c.startswith('Unnamed')
                             and not c.startswith('Gesamt')]] #Deletes all Columns with last years sales and non sales info

              df_sales_t = df_sales.T #Transforms dates as columns to rows
              df_sales_t = df_sales_t[(df_sales_t[0] != 'Konz. Umsatz Vj')].drop(0, 1) #Drops Vorjahresumsätze
              df_sales_t = df_sales_t.reset_index()

              df_attr.columns = c_list[:-7]
              container_frame = pd.DataFrame([], columns=c_list[:-5])
              i = 0
            
              for ind in df_attr.index:
                  for ind_s in df_sales_t.index:
                      container_frame.loc[i] = [
                              df_attr['MARKT_ID'][ind],
                              df_attr['MARKT_NAME'][ind], 
                              df_attr['KONZESSIONAER'][ind], 
                              df_attr['ARTIKEL_NR'][ind],
                              df_attr['UMSATZART'][ind],
                              df_attr['GTIN'][ind],
                              df_sales_t['index'][ind_s],
                              df_sales_t[ind][ind_s]]
                      i += 1

              container_frame['ZULETZT_ABGEFRAGT'] = last_updated
              container_frame['KONZESSIONAER_NR'] = konzessionaer_nr
              container_frame['LIEFERANTEN_NR'] = lieferant_nr
              container_frame['MARKT_ID_2'] = markt_id_2
              container_frame['KONZESSIONAER_PFAD'] = konzessionaer_pfad

              master_frame = master_frame.append(container_frame, ignore_index = True)

              master_frame = master_frame.astype(str)
              master_frame = master_frame.replace(to_replace = 'nan', value = '')
              #for i in range(len(master_frame)):
              #    ctx.emit(*master_frame.iloc[i].values);

              wb = master_frame.to_csv(index=False)
              df_file_source = dt.fread(wb, fill=True)

              return df_file_source   
  
          
def readExcelEdekaSuedbayern(file_client):

    with io.BytesIO() as f:
        downloader = file_client.download_file()
        b = downloader.readinto(f)
        
        temp_frame = pd.DataFrame(columns = ['Markt', 'Datum', 'Umsatz', 'Anzahl_Bons', 'Artikel'])
        master_frame = pd.DataFrame(columns = ['Markt', 'Datum', 'Umsatz', 'Anzahl_Bons', 'Artikel'])

        s_name = [s for s in pd.read_excel(f, None).keys()][1]
        lineOffset = 24

    with io.BytesIO() as f:
        downloader = file_client.download_file()
        b = downloader.readinto(f)

        entry = pd.read_excel(f, sheet_name = 1, skiprows = lineOffset).fillna(0)

    if len(entry.columns) not in (4,6):
        #raise Exception('Excel format has changed. 4 or 6 columns were expected. Received ' + str(len(entry.columns)))
        print('Excel format has changed. 4 or 6 columns were expected. Received ' + str(len(entry.columns)))
    else:
        with io.BytesIO() as f:
            downloader = file_client.download_file()
            b = downloader.readinto(f)

            read_art = pd.read_excel(f, sheet_name = 1, skiprows = lineOffset-2).fillna(0)
            #if len(entry.columns) not in (4,6):
            #    #raise Exception('Excel format has changed. 4 or 6 columns were expected. Received ' + str(len(entry.columns)))
            #    print('Excel format has changed. 4 or 6 columns were expected. Received ' + str(len(entry.columns)))
            entry = entry[(entry['Markt'] != 'Gesamtergebnis') & (entry['Kalendertag'] != 'Ergebnis')]
            temp_entry = entry

            #Shop Umsätze
            if len(entry.columns) == 6:
                temp_entry = entry.drop(columns=['Umsatz.1', 'Anzahl Kunden.1'])
            temp_entry['Artikel'] = read_art.columns.values.tolist()[2]
            temp_entry.columns = temp_frame.columns
            temp_frame = temp_frame.append(temp_entry, ignore_index = True)

            #Mochi Umsätze
            if len(entry.columns) == 6:
                mochi_entry = entry
                mochi_entry = entry.drop(columns=['Umsatz', 'Anzahl Kunden'])
                mochi_entry['Artikel'] = read_art.columns.values.tolist()[4]
                mochi_entry.columns = temp_frame.columns
                temp_frame = temp_frame.append(mochi_entry, ignore_index = True)
            master_frame = master_frame.append(temp_frame, ignore_index = True)
            master_frame = master_frame.astype(str)
            master_frame = master_frame.replace(to_replace = 'nan', value = '')


    wb = master_frame.to_csv(index=False)
    df_file_source = dt.fread(wb, fill=True)
    
    return df_file_source 


def get_sheet(file_client, sname, master_frame):

    with io.BytesIO() as f:
        downloader = file_client.download_file()
        b = downloader.readinto(f)
        entry = pd.read_excel(f,skiprows = 2, sheet_name = sname, dtype = str)
        hours = [time for time in entry.columns.tolist() if ('Unnamed' not in time) & ('Gesamt' not in time)]
        for j in range(1,entry[entry['Unnamed: 0'] == 'Gesamtergebnis'].index.values[0]):
             for i,h in enumerate(hours):
                  row = {}
                  row['Artikelnummer'] = entry.iloc[j,0]
                  row['Bezeichnung'] = entry.iloc[j,1]
                  if pd.notna(entry.iloc[j,2]):
                       row['Datum'] = datetime.strptime(entry.iloc[j,2], '%Y%m%d') + timedelta(hours = int(h))
                  else:
                       datum = datetime.strptime(entry.iloc[1,2], '%Y%m%d')
                       row['Datum'] = datum + timedelta(hours = int(h))
                  row['Markt'] = entry.iloc[j,3]
                  row['Boxen'] = entry.iloc[j,(2*i+4)]
                  row['Netto Umsatz'] = entry.iloc[j,(2*i+5)]
                  master_frame = master_frame.append(row, ignore_index = True)
        return master_frame;
    
      
def readExcelVMarkt(file_client):

    with io.BytesIO() as f:
        downloader = file_client.download_file()
        b = downloader.readinto(f)

        master_frame = pd.DataFrame(columns = ['Artikelnummer', 'Bezeichnung','Datum', 'Markt', 'Boxen', 'Netto Umsatz'])
        s_name = [s for s in pd.read_excel(f, None).keys()]
        if len(s_name) ==1:
             master_frame = get_sheet(file_client, s_name[0], master_frame)            
        else:
             master_frame = get_sheet(file_client, [s for s in s_name if '5%' in s][0], master_frame)   
        master_frame = master_frame.astype(str)
        master_frame = master_frame.replace(to_replace = 'nan', value = '')
    
        wb = master_frame.to_csv(index=False)
        df_file_source = dt.fread(wb, fill=True)
    
        return df_file_source 


def readExcelEchternach(file_client):

    with io.BytesIO() as f:
        downloader = file_client.download_file()
        b = downloader.readinto(f)

        master_frame = pd.DataFrame(columns = ['Tag', 'Umsatz Brutto', 'Monat', 'Jahr', 'Datum'])

        entry = pd.read_excel(f).fillna(-1)
        jahr = entry.iloc[33,0]
        for i in range(1,13):
            month = entry.iloc[:31,[0,i]].copy()
            month = month[month.iloc[:,1] >= 0]
            month['Monat'] = i
            month['Jahr'] = jahr
            df = pd.concat([month['Jahr'],month['Monat'],month['Tag']], axis=1, keys=['Year', 'Month', 'Day'])
            month['Datum'] = pd.to_datetime(df)
            month.columns = master_frame.columns
            master_frame = master_frame.append(month, ignore_index = True)

            wb = master_frame.to_csv(index=False)
            df_file_source = dt.fread(wb, fill=True)
    
        return df_file_source

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
directory = "Echternach"

directory_path=directory+"/new"
directory_path_sink=directory+"/archive"
directory_path_temporary=directory+"/temporary"
connection_string = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(fileShareAccountName,fileShareAccountKey)
parent_dir = ShareDirectoryClient.from_connection_string(conn_str=connection_string, share_name=shareName, directory_path=directory_path)
my_files = list(parent_dir.list_directories_and_files())

#parent_dir = ShareDirectoryClient.from_connection_string(conn_str=connection_string, share_name=shareName, directory_path="")
#my_files = list(parent_dir.list_directories_and_files())

#my_list = []

#for directory in my_files:
#    my_list.append(directory.name)

#print(my_list)

for file in my_files:
    fileName = file.name

    file_client = ShareFileClient.from_connection_string(conn_str=connection_string, share_name=shareName, file_path=directory_path+"/"+fileName)
    file_client_sink = ShareFileClient.from_connection_string(conn_str=connection_string, share_name=shareName, file_path=directory_path_sink+"/"+fileName.replace(".xlsx",".csv").replace(".xls",".csv").replace(".xlsm",".csv").replace(".XLSM",".csv"))
    file_client_temporary = ShareFileClient.from_connection_string(conn_str=connection_string, share_name=shareName, file_path=directory_path_temporary+"/"+fileName.replace(".xlsx",".csv").replace(".xls",".csv").replace(".xlsm",".csv").replace(".XLSM",".csv"))


    # CSV Standards Sink setzen
    columnDelimiterSink = ","
    rowDelimiterSink = "\n\r"
    escapeDelimiterSink = "\ "
    quoteDelimiterSink = '"'

    # SinkCSV erzeugen

    if directory == "AT_Billa":

        with io.BytesIO() as f:
         downloader = file_client.download_file()
         b = downloader.readinto(f)
         df = pd.read_excel(f,sheet_name=5)
         wb = df.to_csv(index=False)
         df_file_source = dt.fread(wb, fill=True) 

        # CSV erstellen
        file_sink = get_SinkFileCSVBilla(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)

        # Transfer durchführen
        transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        print(directory+": "+fileName+" "+transfer_result)

    if directory == "AT_Merkur":

        with io.BytesIO() as f:
         downloader = file_client.download_file()
         b = downloader.readinto(f)
         df = pd.read_excel(f,sheet_name=5)
         wb = df.to_csv(index=False)
         df_file_source = dt.fread(wb, fill=True) 

        with io.BytesIO() as f:
          downloader = file_client.download_file()
          b = downloader.readinto(f)
          df = pd.read_excel(f,sheet_name=1)
          wb = df.to_csv(index=False)
          df_file_source_gestern = dt.fread(wb, fill=True) 
        
        # CSV erstellen
        file_sink = get_SinkFileCSVMerkur(df_file_source,df_file_source_gestern,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)

        # Transfer durchführen
        transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        print(directory+": "+fileName+" "+transfer_result)

    if directory == "AT_SUTTERLUETY":

        with io.BytesIO() as f:
         downloader = file_client.download_file()
         b = downloader.readinto(f)
         df = pd.read_excel(f,sheet_name=2)
         wb = df.to_csv(index=False)
         df_file_source = dt.fread(wb, fill=True) 

        # CSV erstellen
        file_sink = get_SinkFileCSVSutterluety(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)

        # Transfer durchführen
        transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        print(directory+": "+fileName+" "+transfer_result)


    if directory == "Edeka_Frauen":

        with io.BytesIO() as f:
         downloader = file_client.download_file()
         b = downloader.readinto(f)
         df = pd.read_excel(f,sheet_name=0)
         wb = df.to_csv(index=False)
         df_file_source = dt.fread(wb, fill=True)

        isMochi = False

        # CSV erstellen
        file_sink = get_SinkFileCSVEdekaFrauen(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink,isMochi)

        # Transfer durchführen
        transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        print(directory+": "+fileName+" "+transfer_result)

  
    if directory == "Edeka_Frauen_Mochi":

        with io.BytesIO() as f:
         downloader = file_client.download_file()
         b = downloader.readinto(f)
         df = pd.read_excel(f,sheet_name=0)
         wb = df.to_csv(index=False)
         df_file_source = dt.fread(wb, fill=True)

        isMochi = True

        # CSV erstellen
        file_sink = get_SinkFileCSVEdekaFrauen(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink,isMochi)

        # Transfer durchführen
        transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        print(directory+": "+fileName+" "+transfer_result)


    if directory == "EDK_NB":

        df_file_source = readExcelEdekaNordbayern(file_client)

        # CSV erstellen
        file_sink = get_SinkFileCSVEdekaNordbayern(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)
        print(file_sink)

        # Transfer durchführen
        # transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        # print(directory+": "+fileName+" "+transfer_result)


    if directory == "Edeka_Suedbayern":

        df_file_source = readExcelEdekaSuedbayern(file_client)
        rows = df_file_source.nrows

        # CSV erstellen
        file_sink = get_SinkFileCSV(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)
        print(file_sink)

        # Transfer durchführen
        transfer_result = doTransferNoRows(file_client,file_client_sink,file_client_temporary,file_sink,rows)
        print(directory+": "+fileName+" "+transfer_result)


    if directory == "VMarkt":

        df_file_source = readExcelVMarkt(file_client)

        # CSV erstellen
        file_sink = get_SinkFileCSV(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)
        print(file_sink)

        # Transfer durchführen
        transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        print(directory+": "+fileName+" "+transfer_result)


        
    if directory == "Echternach":

        df_file_source = readExcelEchternach(file_client)

        # CSV erstellen
        file_sink = get_SinkFileCSV(df_file_source,columnDelimiterSink,quoteDelimiterSink,escapeDelimiterSink)
        print(file_sink)

        # Transfer durchführen
        transfer_result = doTransfer(file_client,file_client_sink,file_client_temporary,file_sink)
        print(directory+": "+fileName+" "+transfer_result)  
