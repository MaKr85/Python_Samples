from datetime import datetime, timedelta
import requests as rq
import pandas as pd


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



######################### START #########################

from_ = datetime.today() - timedelta(days=3)
to =  datetime.today()
url = 'https://eathappy.structr.com/api/v1/getReportData.csv?from=' + str(from_) + '&to=' + str(to) 

res = rq.post(url=url,headers={'X-User': 'report', 'X-Password': 'qbqcEkAgRFrcSazW3J9QcSTv'})

if res.text:
    print('Response received')
    df_og = transform2_df(res.text)

    output_csv_as_string_verwurf_gestern = df1(df_og).to_csv(sep=';', index=False)
    output_csv_as_string_verwurf_tour = df2(df_og).to_csv(sep=';', index=False)

    print('Save Response as CSV')

    print('Save Response as CSV - Succeeded')
else:
    print('No response - No CSV created')

