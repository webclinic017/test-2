import os 
from datetime import timedelta
import pandas as pd
import requests
import datetime 
import numpy as np

##test
from bs4 import BeautifulSoup
import datetime 
import pandas as pd
import requests
import re
import json
import time
from datetime import timedelta
import cred
from pandas import DataFrame
from sqlalchemy import create_engine

import pandas as pd                        
from pytrends.request import TrendReq
import yfinance as yf
pytrends = TrendReq(hl='en-US', tz=360)

import requests 
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
import mapi


##testing
import logging
logging.basicConfig(filename='logs\Gtrend.log', filemode='w', level=logging.DEBUG)
logging.info('Job started')
if datetime.datetime.today().weekday() in [0,1,2,3,4,5,6]: 
    print("proceed")
    logging.info('Since week day is 2 or 6 proceeding to the computation ')
      



    def get_connection():
        connect = create_engine(
            'postgresql://' + cred.POSTGRESQL_USER + ':' + cred.POSTGRESQL_PASSWORD + '@' + cred.POSTGRESQL_HOST_IP + ':' + cred.POSTGRESQL_PORT + '/' + cred.POSTGRESQL_DATABASE,
            echo=False)
        print("Connecting to database\n	->%s" % connect)
        # connect = sql.connect(conn_string)
        print("Connected!\n")
        return connect
    conn = get_connection()
    raw = conn.raw_connection()
    cursor = raw.cursor()
    engine = create_engine('postgresql://' + cred.POSTGRESQL_USER + ':' + cred.POSTGRESQL_PASSWORD + '@' + cred.POSTGRESQL_HOST_IP + ':' + cred.POSTGRESQL_PORT + '/' + cred.POSTGRESQL_DATABASE,echo=False)


    tod=datetime.datetime.today()
    row_list = [['Gtrend',	'Gtrend',	tod, 'Job Started']]
    dflog = DataFrame (row_list,columns=['script_id','job_name','last_run_date_time','run_status'])
    dflog.to_sql('job_run_hist',schema ='ams',con=engine,chunksize=100,method='multi',index=False,if_exists='append')



    def getgoogletrendsdata(region,kword,n):

        try:

            pytrends = TrendReq(hl='en-US', tz=360)
            pytrends.build_payload(kw_list=[kword],cat=0,timeframe='today 5-y',geo=region)
            data = pytrends.interest_over_time()
            data['Region'] = region
            data ['kword']=kword

            data['Mavg'] = data[kword].rolling(n).mean()
            
            
            data=data[data['Mavg']>0]
            data.reset_index(inplace=True)
            data =data[data['date']>(datetime.datetime.today()-timedelta(395)).strftime('%Y-%m-%d')]
            return data

        except :#KeyError:

            pass
            data=pd.DataFrame()


    import finmodelapi
    def gettickerprice(ticker):

        try:
            ticker=ticker
            url = 'https://financialmodelingprep.com/api/v3/historical-price-full/' +ticker + finmodelapi.api  ###'?apikey=5dd2dbedc129adddb781aaae49d9b610'
            bs=requests.get(url)
            bs=bs.json()
            df = pd.DataFrame.from_dict(bs['historical'])
            df['date']=pd.to_datetime(df['date'])
            df=df[df['date']>(datetime.datetime.today()-timedelta(3650)).strftime('%Y-%m-%d')]

            df=df.sort_values(by=['date'],ascending=True)
            df=df[['date','close']]
            df['year'] = df['date'].dt.week
            df['week']  = df['date'].dt.year
            df=df.groupby(['year','week']).agg({'close':'mean','date':'first'}).reset_index()[['date','close']].sort_values(by=['date'])
            df['Mavg'] = df['close'].rolling(13).mean()
            df=df[df['Mavg']>0]
            df['Region']='stock_price'
            cols=['date','Mavg','Region']
            df=df[cols]
            return df
        except:
            
            pass
            df=pd.DataFrame()
         




    #Beginning
    tod=datetime.datetime.today()
    row_list = [['Gtrend',	'Funkotheme',	tod, 'Job Started']]
    dflog = DataFrame (row_list,columns=['script_id','job_name','last_run_date_time','run_status'])
    dflog.to_sql('job_run_hist',schema ='ams',con=engine,chunksize=100,method='multi',index=False,if_exists='append')

    df=pd.read_excel('Consumer_Keywords.xlsx')
    df['Do Not Alert Me Until']=df['Do Not Alert Me Until'].fillna(datetime.datetime.today()-timedelta(1))
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    df=df[df['Do Not Alert Me Until']<today]

    

    logging.info('Consumer excel read without issues')

    # df=df[df['Ticker']=='4385 JP']
    # df=df.head(3)
    #df=df[df['Ticker']=='GOOS CN']


    alltickers=[]
    allregions=[]
    allsearches=[]
    alldf=[]

    for index, row in df.iterrows():

        for i in row['Google Search Key Words'].split(","):
            #print(i)
            allsearches.append(i)

            for j in row['Region'].split(","):
                #print(i.strip())
                allregions.append(j)
                alltickers.append(row['Ticker'])
                print(row['Ticker'],i,j)
                time.sleep(4)
                df1=getgoogletrendsdata(j,i,n=13)

             

             
                
                df2=gettickerprice(row['Ticker'])
                

                

                if df1 is not None and df2 is not None:


                    year_bk_date = (df1['date'].max()-timedelta(365)).strftime('%Y-%m-%d')

                    try:


                        gtrend_growth = df1['Mavg'].tail(1).values[0]/df1[df1['date']<year_bk_date]['Mavg'].tail(1).values[0]
                    except:
                        IndexError
                        gtrend_growth = df1['Mavg'].tail(1).values[0]/df1['Mavg'].head(1).values[0]




                    try:
                        stock_growth = df2['Mavg'].tail(1).values[0]/df2[df2['date']<year_bk_date]['Mavg'].tail(1).values[0]
                    except:
                        IndexError
                        try:

                            stock_growth=df2['Mavg'].tail(1).values[0]/df2['Mavg'].head(1).values[0]
                        except:
                            pass
                            stock_growth=None
                            


                    


                

                    if gtrend_growth and stock_growth is not None:

                        if (gtrend_growth >1.2 and (gtrend_growth/stock_growth)) >1: ##currently kept as a subjective number, later we can replace it with objective numbers as 

                            remark='Undervalued'

                        elif (gtrend_growth <0.8 and (gtrend_growth/stock_growth)<=1)    :
                            remark='Overvalued'
                        else:
                            remark='unknown'
                        # elif:
                        #     remark='No info or overvalued'
                    else:
                        remark='No Potential Bullish-Bearish Signal'


                    df1.columns=['date','gtrend','isPartial','Region','kword','Mavg']
                    df1['gtrend_growth_yoy_latest']=gtrend_growth
                    df1['stock_growth_yoy_latest']=stock_growth


                    df1['Ticker']=row['Ticker']
                    df1['Remark']=remark
                    df1=df1.tail(1)

                    alldf.append(df1)

    if len(alldf)>0:

        final_df=pd.concat(alldf)
        final_df['Extract_date']=datetime.datetime.now().strftime("%Y-%m-%d")
        #breakpoint()
        #final_df = final_df[final_df['Remark']!='unknown']
        if len(final_df)>0:


            engine = create_engine('postgresql://' + cred.POSTGRESQL_USER + ':' + cred.POSTGRESQL_PASSWORD + '@' + cred.POSTGRESQL_HOST_IP + ':' + cred.POSTGRESQL_PORT + '/' + cred.POSTGRESQL_DATABASE,echo=False)
                
            final_df.to_sql('Google_Trend_inf',schema ='ams',con=engine,chunksize=100,method='multi',index=False,if_exists='append')
            filename=str(datetime.datetime.today().strftime('%Y-%m-%d'))+'_Gtrend.csv'
            final_df.to_csv('FileOutputs\\'+filename)


        #     table_html = (final_df).to_html()
        #     table_html1 =""

        #     html = """\
        #     <html>
        #         <head>
        #         <style>
        #             table, th, td {
        #                 border: 1px solid black;
        #                 border-collapse: collapse;
        #             }
        #             th, td {
        #                 padding: 5px;
        #                 text-align: left;    
        #             }    
        #         </style>
        #         </head>
        #     <body>
        #     <p>Direct Link to the Google sheet!<br>
        #         These are the Cash Allocated Trades--<br>
        #         Link to VIC. <a href="https://www.valueinvestorsclub.com/">link</a> you wanted.<br>
        #         %s
        #     </p>

        #         </body>
        #         </html>
        #         """ % (table_html)






        #     def send_email(email_recipient,
        #                 email_subject,
        #                 email_message,
        #                 attachment_location = ''):

        #         email_sender = 'naveen@huntercaplp.com'

        #         msg = MIMEMultipart()
        #         msg['From'] = email_sender
        #         msg['To'] = email_recipient
        #         msg['Subject'] = email_subject

        #         msg.attach(MIMEText(email_message, 'html'))

        #         if attachment_location != '':
        #             filename = os.path.basename(attachment_location)
        #             attachment = open(attachment_location, "rb")
        #             part = MIMEBase('application', 'octet-stream')
        #             part.set_payload(attachment.read())
        #             encoders.encode_base64(part)
        #             part.add_header('Content-Disposition',
        #                             "attachment; filename= %s" % filename)
        #             msg.attach(part)

        #         #try:
        #         server = smtplib.SMTP('smtp.office365.com', 587)
        #         server.ehlo()
        #         server.starttls()
        #         server.login('naveen@huntercaplp.com', mapi.outl)

        #         text = msg.as_string()

        #         server.sendmail(email_sender, email_recipient, text)
                

            

        #         print('email sent')
        #         server.quit()

        #     send_email('team@huntercaplp.com','Gtrend_Alerts',table_html, 'C:\\Users\\NaveenJoseph\\Hunter Capital Limited Partnership\\Firm - Documents\\QUANTITATIVE\\Scripts\\testemail.txt')


        # tod=datetime.datetime.today()
        # row_list = [['Gtrend',	'Gtrend',	tod, 'Job Finished Sucessfully']]
        # dflog = DataFrame (row_list,columns=['script_id','job_name','last_run_date_time','run_status'])
        # dflog.to_sql('job_run_hist',schema ='ams',con=engine,chunksize=100,method='multi',index=False,if_exists='append')

        # logging.info('Script reached end with no issues')

else:
    logging.info('Its not the day to run the job')


            #breakpoint()




