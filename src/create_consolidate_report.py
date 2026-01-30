#!/usr/bin/python3
from influxdb import InfluxDBClient
import pandas as pd
import psycopg2
from config import (
    CENTRAL_INFLUX_USERNAME,
    CENTRAL_INFLUX_PASSWORD,
    GOOGLE_TENANTS_FILE_PATH,
    GOOGLE_ERROR_FILE_PATH,
    GOOGLE_Warehouse_Manager_FILE_PATH,
    GOOGLE_Warehouse_Manager_site_ops_houe_FILE_PATH,
    CONFFILES_DIR
)

# !/usr/bin/python3
# from ConnectionAndQuery import connection_to_influx
# from ConnectionAndQuery import fetch_data
# from GOMail import send_mail
from datetime import datetime, timedelta
#from GOTimeStamps import get_time_interval
import time
# import pandas as pd
import numpy as np
from functools import reduce

#!/usr/bin/python3
import psycopg2
# from ConnectionAndQuery import connection_to_influx, connection_to_central_influx, connection_to_tower, \
#     connection_to_consolidatedb
# from GOMail import send_mail
import psycopg2.extras as extras
from datetime import datetime, timedelta
# from GOTimeStamps import get_time_interval
import time
# import pandas as pd
# import consolidated_logic
# from influxdb import InfluxDBClient, DataFrameClient
# import numpy as np
# from ConnectionAndQuery import fetch_data
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")

#!/usr/bin/python3
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.CommonFunction import InfluxData, MultiTenantDAG,CommonFunction, postgres_connection
# import pandas as pd
import os

receiver_list = ['ankush.j@greyorange.com']
sender = 'analytics@greyorange.com'

con_error = {'Error': ['DB Connection Error'], 'Messaage': ["Could'nt establish connection"]}

# fetch all sites
d = {'port': 'object', 'tower_port': 'object', 'hour': 'object', 'min': 'object'}
# sites = pd.read_csv(
#     '/home/gor/Documents/analytics_dashboard_scripts/Misc_Scripts/Consolidated/Site_Excel/all_site_excel.csv', dtype=d)

# ideal dataset
ideal_dataset = pd.DataFrame(
    columns=['time', 'kpi', 'value', 'report_level', 'error_category', 'barcode_group', 'value_s'])

class consolidatareport():
# make connection function - host port username password db and site required
    def send_mail(self, html_error_table, subject, sender, receiver_list):
        return #do not send error mail
        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = sender
        receivers = receiver_list

        body_content = html_error_table
        message.attach(MIMEText(body_content, "html"))
        msg_body = message.as_string()

        server = SMTP("email-smtp.us-east-1.amazonaws.com", 25)
        server.starttls()
        server.login('AKIA3FSTBJC6ASH5Q4HF', 'BDS+nRI8cj+TYrhE1JGfCBp8Cky65olG3tubiglIbDwL')
        server.sendmail(message['From'], receivers, msg_body)
        server.quit()
        print('Error Mail Sent!')

        # !/usr/bin/python3

    def consolidate_kpi(self,start_date, end_date):
        self.CommonFunction = CommonFunction()
        self.orignal_start_date= pd.to_datetime(start_date,utc=True)
        self.old_data = self.orignal_start_date - timedelta(days=366)
        self.end_date =pd.to_datetime(end_date,utc=True)
        self.start_date=self.orignal_start_date.strftime("%Y-%m-%d %H:%M:%S")
        self.end_date=self.end_date.strftime("%Y-%m-%d %H:%M:%S")

        # daterange = self.get_datetime_interval3(end_date)
        # for i in daterange.index:
        #     self.start_date = daterange['start_date'][i]
        #     self.end_date = daterange['end_date'][i]
        #     print(f"oneview:startdate:{self.start_date} and enddate:{self.end_date}")
        self.consolidate_kpi1()
        return
    def connection_to_influx(self,host, port, username, password, db,site):
        client = InfluxDBClient(host=host, port=port, username=username, password=password)
        try:
            InfluxDBClient.ping(client)
            client.switch_database(db)
            print("\nConnection to " + db + " InfluxDB successful (" + self.site.get('name') + ")\n")
            return client
        except:
            print("\nFailed to establish connection (" + self.site.get('name') + ")\n")
            return 0


    # fetch data - requires client_tuple of site and client
    def fetch_data(self,client, query):
        try:
            datadict = client.query(query)
        except:
            return [{'error': 'Please check the query', 'query': query}]

        data = list(datadict.get_points())
        if len(data) <= 0:
            return [{'error': 'No data fetched', 'query': query}]

        # returns a list of dictionaries where 'key' is column name and 'value' is data
        return data


    def connection_to_central_influx(self,host, port, username, password, db):
        client = InfluxDBClient(host=host, port=port, username=CENTRAL_INFLUX_USERNAME, password=CENTRAL_INFLUX_PASSWORD, ssl=True, verify_ssl=False)
        try:
            InfluxDBClient.ping(client)
            client.switch_database(db)
            print("\nConnection to central InfluxDB successful (" + self.site.get('name')  + ")\n")
            return client
        except:
            print("\nFailed to establish central influx connection (" + self.site.get('name')  + ")\n")
            return 0


    # def connection_to_tower(self,towerip, towerport, site, db, tower_password=None, sslroot=None, sslcert=None, sslkey=None):
    #     if site == 'Dillards' or site == 'Coupang_Daegu':
    #         try:
    #             client_tower = psycopg2.connect(dbname='tower',
    #                                             user='postgres',
    #                                             sslrootcert=str(sslroot),
    #                                             sslcert=str(sslcert),
    #                                             sslkey=str(sslkey),
    #                                             host=str(towerip),
    #                                             port=str(towerport),
    #                                             password=str(tower_password))
    #             print("\nConnection to " + db + " successful (" + site + ")\n")
    #             return (client_tower, site)
    #         except:
    #             print("\nFailed to establish tower connection (" + site + ")\n")
    #             return (0, 0)
    #     else:
    #         try:
    #             client_tower = psycopg2.connect(user="remote",
    #                                             password="apj0702",
    #                                             host=str(towerip),
    #                                             port=str(towerport),
    #                                             database="tower")
    #             print("\nConnection to " + db + " successful (" + site + ")\n")
    #             return (client_tower, site)
    #         except:
    #             print("\nFailed to establish tower connection (" + site + ")\n")
    #             return (0, 0)


    def connection_to_consolidatedb(self):
        try:
            consoldatedb = psycopg2.connect(user="postgres",
                                            password="postgres",
                                            host="10.11.9.14",
                                            port=5432,
                                            database="consolidatedb")
            return consoldatedb
        except:
            print("\nFailed to establish tableau postgres connection\n")
            return 0


    # def get_time_interval(self,hour, minutes, site_flag, today, yesterday, daybeforeyesterday):
    #     h = hour
    #     m = minutes
    #     timepart = 'T' + h + ':' + m + ':' + '00Z'
    #
    #     if site_flag == 'dby':
    #         # for japanese sites
    #         startdate = daybeforeyesterday + timepart
    #         enddate = yesterday + timepart
    #         return [startdate, enddate]
    #
    #     startdate = yesterday + timepart
    #     enddate = today + timepart
    #     return [startdate, enddate]

    # set mailers list
    receiver_list = ['shivam.j@greyorange.com', 'bhanu.p@greyorange.com', 'sunaal.d@greyorange.com']
    sender = 'sunaal.d@greyorange.com'


    def correct_dates(self,df, hr, minn):
        df['time_old'] = df['time']

        df['time_old'] = df['time_old'].apply(lambda x: x.replace('T', ' ').replace('Z', '')[:x.find(".")])
        df['time_old'] = df['time_old'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))

        df['time'] = df['time'].apply(lambda x: x.split('T')[0] + ' 00:00:00')
        df['time'] = df['time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))

        df['datestart'] = df['time'].apply(lambda x: x + timedelta(hours=hr) + timedelta(minutes=minn))
        df['dateend'] = df['datestart'].apply(lambda x: x + timedelta(days=1))
        df['dateminus'] = df['datestart'].apply(lambda x: x - timedelta(days=1))
        df['time'] = df.apply(
            lambda x: x['datestart'] if x['time_old'] >= x['datestart'] and x['time_old'] <= x['dateend'] else
            (x['dateminus'] if x['time_old'] <= x['datestart'] and x['time_old'] >= x['dateminus'] else np.nan), axis=1)
        df.drop(['datestart', 'dateend', 'dateminus'], axis=1, inplace=True)

        return df


    def remove_duplicate_barcodes(self,dff):
        dff = dff.sort_values(by=['time', 'butler_id', 'time_old'], ascending=True)
        dff['rank'] = 0

        # grouping and ranking
        for i in range(len(dff)):
            if (i == 0):
                dff['rank'][i] = 1
            else:
                if (dff['time'][i] == dff['time'][i - 1] and dff['barcode'][i] == dff['barcode'][i - 1] and
                        dff['butler_id'][i] == dff['butler_id'][i - 1] and dff['L2'][i] == dff['L2'][i - 1] and
                        dff['error_name'][i] == dff['error_name'][i - 1]):
                    dff['rank'][i] = dff['rank'][i - 1]
                else:
                    dff['rank'][i] = dff['rank'][i - 1] + 1

        # calculating time_diff in consecutive erros
        dff['time_diff'] = 0
        for i in range(len(dff)):
            if (i == 0):
                dff['time_diff'][i] = 0
            else:
                if (dff['rank'][i] == dff['rank'][i - 1]):
                    dff['time_diff'][i] = dff['time_old'][i] - dff['time_old'][i - 1]
                else:
                    dff['time_diff'][i] = 0

        dff['time_diff'] = dff['time_diff'].apply(lambda x: x.total_seconds() if x != 0 else 0)

        # creating duplicate flags
        dff['flag'] = 0
        for i in range(len(dff)):
            if (i == 0):
                dff['flag'][i] = 1
            else:
                if (dff['rank'][i] == dff['rank'][i - 1] and dff['time_diff'][i] < 600):
                    dff['flag'][i] = dff['flag'][i - 1]
                else:
                    dff['flag'][i] = dff['flag'][i - 1] + 1

        # removing duplicates
        dff.drop_duplicates(subset="flag", keep='first', inplace=True)
        dff.drop(['rank', 'time_diff', 'flag'], axis=1, inplace=True)
        return dff


    # ------------xxxxxx----------------
    def fetch_butler_nav_incidents(self,client, start_date, end_date, hr, minn):
        nulldataset = pd.DataFrame(columns=['hh'])
        sd = start_date
        ed = end_date

        if self.site.get('name') in ['Sodimac_C&C', 'Sodimac', 'Lixil', 'Nishikawa', 'Sodimac_CnC', 'Sodimac_CnC_florida',
                         'sodimac_cnc5', 'sodimac_cnc8', 'sodimac_cnc3', 'sodimac_cnc7']:
            qry = f"select barcode_group,L1,L2,error_name,zone,butler_id,barcode,time_difference from butler_nav_incidents where time>='{sd}' and time<'{ed}' and Is_auto_resolved='No' and L1!= 'Manual Intervention' and time_difference>10"
        elif self.site.get('name') in ['GXO_NIKE', 'AnF','XPO_HnM','Apotek']:
            qry = f"select barcode_group,L1,L2,error_name,zone,butler_id,barcode,time_difference from butler_nav_incidents where time>='{sd}' and time<'{ed}' and Is_auto_resolved='No' and L1!= 'Manual Intervention' and time_difference>60 and zone!='defzone'"
        elif self.site.get('name') in ['Dillards']:
            qry = f"select barcode_group,L1,L2,error_name,zone,butler_id,barcode,time_difference from butler_nav_incidents where time>='{sd}' and time<'{ed}' and Is_auto_resolved='No' and L1!= 'Manual Intervention' and time_difference>120 and zone!='defzone'"
        else:
            qry = f"select barcode_group,L1,L2,error_name,zone,butler_id,barcode,time_difference from butler_nav_incidents where time>='{sd}' and time<'{ed}' and Is_auto_resolved='No' and L1!= 'Manual Intervention' and time_difference>10 and zone!='defzone'"

        data_dict = self.fetch_data(client, qry)

        error_data = {'Error Message': [], 'Query': []}
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_dat a (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            return (nulldataset, 0, error_data)
        else:
            hr = int(hr)
            minn = int(minn)
            d = self.correct_dates(d, hr, minn)
            d = self.remove_duplicate_barcodes(d)
            return (d, 1)


    def fetch_manual_intervention(self, client, client_alteryx, start_date, end_date, hr, minn):
        ideal_dataset = pd.DataFrame(columns=['time', 'report_level', 'error_category', 'kpi', 'value', 'value_s'])
        nulldataset = pd.DataFrame(columns=['hh'])
        sd = start_date
        ed = end_date

        qry = "select  barcode_group,L1,butler_id,direction,expected_barcode from manual_interventions where  time>='" + sd + "' and time<'" + ed + "' and zone!='defzone'"
        data_dict = self.fetch_data(client_alteryx, qry)

        if 'error' in data_dict[0].keys():
            qry = "select  barcode_group,L1,butler_id,direction,expected_barcode from butler_nav_incidents where  time>='" + sd + "' and time<'" + ed + "' and zone!='defzone'"
            data_dict = self.fetch_data(client, qry)
        
        error_data = {'Error Message': [], 'Query': []}
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name')  + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            return ideal_dataset
        else:
            hr = int(hr)
            minn = int(minn)
            d['time2'] = pd.to_datetime(d['time'])
            d['hour'] = d['time2'].apply(lambda x: x.hour)
            d = self.correct_dates(d, hr, minn)
            d = d.drop_duplicates(subset=['butler_id', 'direction', 'expected_barcode', 'hour'], keep='first')
            dd = d.groupby(by=['time', 'L1', 'barcode_group']).count().iloc[:, 0]
            dd.rename("value", inplace=True)
            dd = pd.DataFrame(dd).reset_index()
            dic = {'L1': 'error_category', 'barcode_group': 'kpi'}
            dd.rename(columns=dic, inplace=True)
            dd['report_level'] = 'manual_intervention barcode group'
            return dd


    def fetch_butler_nav_error(self, client, client_alteryx, start_date, end_date, hr, minn):
        nulldataset = pd.DataFrame(columns=['hh'])
        sd = start_date
        ed = end_date
        qry = "select  butler_id,direction,value,expected_barcode from manual_interventions where time>='" + sd + "' and time<'" + ed + "' and zone!='defzone'"
        data_dict = self.fetch_data(client_alteryx, qry)
        
        if 'error' in data_dict[0].keys():
            qry = "select  butler_id,direction,value,expected_barcode from butler_nav_incidents where  time>='" + sd + "' and time<'" + ed + "' and zone!='defzone'"
            data_dict = self.fetch_data(client, qry)
        # df_gridbarcode = self.get_def_zone(client_alteryx)
        error_data = {'Error Message': [], 'Query': []}
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name')  + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            return (nulldataset, 0, error_data)
        else:
            hr = int(hr)
            minn = int(minn)
            d['time2'] = pd.to_datetime(d['time'])
            d['hour'] = d['time2'].apply(lambda x: x.hour)
            d = self.correct_dates(d, hr, minn)
            d = d.drop_duplicates(subset=['butler_id', 'direction', 'expected_barcode', 'hour'], keep='first')
            d.drop(['direction', 'hour', 'expected_barcode', 'time2'], axis=1, inplace=True)
            return (d, 1)

    def get_def_zone(self, client):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'charger_id', 'value_s'])
        qrylist = f"select * from grid_info where zone !='defzone'"
        error_data = {'Error Message': [], 'Query': []}
        data_dict = self.fetch_data(client, qrylist)
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name')  + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            print(error_data)
            return ideal_dataset
        else:
            return d



    def error_per_category(self,df):
        ideal_dataset = pd.DataFrame(columns=['time', 'report_level', 'error_category', 'kpi', 'value', 'value_s'])
        if df[1] == 0:
            print('please check query')
            print(df[2])
            return ideal_dataset
        else:
            dd = df[0].groupby(by=['time', 'L1', 'barcode_group']).count().iloc[:, 0]
            dd.rename("value", inplace=True)
            dd = pd.DataFrame(dd).reset_index()
            dic = {'L1': 'error_category', 'barcode_group': 'kpi'}
            dd.rename(columns=dic, inplace=True)
            dd['report_level'] = 'barcode group'
            return dd


    def topNbutlers(self,df, n):
        ideal_dataset = pd.DataFrame(columns=['time', 'report_level', 'error_category', 'kpi', 'value', 'value_s'])
        if df[1] == 0:
            print('please check query')
            print(df[2])
            return ideal_dataset
        else:
            dd = df[0].groupby(by=['time', 'L1', 'butler_id']).count().iloc[:, 0]
            dd.rename("value", inplace=True)
            dd = pd.DataFrame(dd).reset_index()
            error_per_bots = dd.groupby(by=['butler_id']).sum().sort_values(by='value', ascending=False).head(n)
            botids = list(error_per_bots.index)
            dd = dd.loc[dd['butler_id'].isin(botids)]
            dic = {'L1': 'error_category', 'butler_id': 'kpi'}
            dd.rename(columns=dic, inplace=True)
            dd['report_level'] = 'Top N butlers'
            return dd

    def topN_Mi_nbutlers(self,df, n):
        ideal_dataset = pd.DataFrame(columns=['time', 'report_level', 'error_category', 'kpi', 'value', 'value_s'])
        if df[1] == 0:
            print('please check query')
            print(df[2])
            return ideal_dataset
        else:
            mi_df =df[0]
            dd = mi_df.groupby(by=['time','butler_id']).count().iloc[:, 0]
            dd.rename("value", inplace=True)
            dd = pd.DataFrame(dd).reset_index()
            error_per_bots = dd.groupby(by=['butler_id']).sum().sort_values(by='value', ascending=False).head(n)
            botids = list(error_per_bots.index)
            dd = dd.loc[dd['butler_id'].isin(botids)]
            dic = {'L1': 'error_category', 'butler_id': 'kpi'}
            dd.rename(columns=dic, inplace=True)
            dd['report_level'] = 'Top N MI butlers'
            return dd

    def topNbarcodes(self,df, n):
        ideal_dataset = pd.DataFrame(columns=['time', 'report_level', 'error_category', 'kpi', 'value', 'value_s'])
        if df[1] == 0:
            print('please check query')
            print(df[2])
            return ideal_dataset
        else:
            dd = df[0].groupby(by=['time', 'L1', 'barcode']).count().iloc[:, 0]
            dd.rename("value", inplace=True)
            dd = pd.DataFrame(dd).reset_index()
            error_per_barcodes = dd.groupby(by=['barcode']).sum().sort_values(by='value', ascending=False).head(n)
            barcodes = list(error_per_barcodes.index)
            dd = dd.loc[dd['barcode'].isin(barcodes)]
            dic = {'L1': 'error_category', 'barcode': 'kpi'}
            dd.rename(columns=dic, inplace=True)
            dd['report_level'] = 'Top N barcodes'
            return dd


    def top5perbots(self,df):
        ideal_dataset = pd.DataFrame(columns=['time', 'report_level', 'kpi', 'value', 'value_s'])
        if df[1] == 0:
            print('please check query')
            print(df[2])
            return ideal_dataset
        else:
            dff = df[0].groupby(by=['time', 'butler_id']).count().iloc[:, 0]
            dff.rename("count_of_errors", inplace=True)
            dff = pd.DataFrame(dff).reset_index()
            dff = dff.sort_values(by=['time', 'count_of_errors'], ascending=False)
            no_of_bots = dff.groupby(['time']).count()[['count_of_errors']].reset_index()

            dfs = []
            for i in no_of_bots.index:
                top5perbots = int(round(0.05 * no_of_bots['count_of_errors'][i], 0))
                d = dff[dff['time'] == no_of_bots['time'][i]].head(top5perbots)
                removebots = list(d['butler_id'])
                k = dff[dff['time'] == no_of_bots['time'][i]]
                k = k.loc[~k['butler_id'].isin(removebots)]
                bot_stops_excl_5per = k.groupby('time').size()
                bot_stops_excl_5per.rename("value", inplace=True)
                bot_stops_excl_5per = pd.DataFrame(bot_stops_excl_5per).reset_index()
                bot_stops_excl_5per['kpi'] = 'bot_stops_excluding_5_per'
                bot_stops_excl_5per['report_level'] = 'bot stops excluding 5%'
                dfs.append(bot_stops_excl_5per)

            if len(dfs) > 0:
                final = pd.concat(dfs)
                return final
            else:
                ideal_dataset


    def error_and_res_time(self,df):
        ideal_dataset = pd.DataFrame(columns=['time', 'barcode_group', 'value', 'report_level', 'value_s'])
        if df[1] == 0:
            print('please check query')
            print(df[2])
            return ideal_dataset
        else:
            # removing time diff greater than 3 hrs (or 10800 secs)
            dff = df[0]
            dff = dff[dff['time_difference'] < 10800]

            dfs = []
            d1 = dff.groupby(by=['time', 'barcode_group']).sum().reset_index()
            d1.rename(columns={'time_difference': 'value'}, inplace=True)
            d1['report_level'] = 'total error time'

            d2 = dff.groupby(by=['time', 'barcode_group']).mean().reset_index()
            d2.rename(columns={'time_difference': 'value'}, inplace=True)
            d2['report_level'] = 'resolution time'

            final = pd.concat([ideal_dataset, d1, d2])
            return final


    # ------------xxxxxx----------------
    def hr_func(self,date):
        datem = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        return datem.hour


    def butlerpercentages(self,client, start_date, end_date):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'hour', 'report_level', 'value_s'])
        sd = start_date.split(' ')[0]
        ed = end_date.split(' ')[0]
        # sd = start_date.strftime('%Y-%m-%d')
        # ed = end_date.strftime('%Y-%m-%d')

        qry = "select (sum(low_charged_paused)/sum(total_kpi))*100 as low_charge_pause_per,(sum(move_task)/sum(total_kpi))*100 as move_task_per, (sum(manual_paused)/sum(total_kpi))*100 as manual_pause_per, (sum(error_time)/sum(total_kpi))*100 as error_per, (sum(idle_time)/sum(total_kpi))*100 as idle_per, (sum(pps_task_time)/sum(total_kpi))*100 as pps_per, (sum(charging_time)/sum(total_kpi))*100 as charging_per , (sum(offline_time)/sum(total_kpi))*100 as offline_per from butler_uptime_ayx where time>='" + sd + "' and time<'" + ed + "' group by charger_id,time(1h)"  # add ofline
        # pps_per+idle_per_char_per + move_per

        data_dict = self.fetch_data(client, qry)

        error_data = {'Error Message': [], 'Query': []}
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            print(error_data)
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            return ideal_dataset
        else:
            d['time'] = d['time'].apply(lambda x: x.replace('T', ' ').replace('Z', ''))
            d = d.melt(['time'], var_name='kpi')
            d['hour'] = d['time'].apply(self.hr_func)
            d['report_level'] = 'bot availability'
            return d


    # ------------xxxxxx----------------
    def from_kpi_tables(self,client, start_date, end_date):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'value_s'])
        sd = start_date
        ed = end_date
        qrylist = [
            f"select entity_picked,entity_put,order_breached_completed,orders_completed,barcode_travelled,charger_uptime,OWT_Pick,OWT_Put from OneView_data_v2 where site='" +
            self.site.get('name') + f"' and time>='{sd}' and time<'{ed}'",
            f"select total_put_expectations as put_exceptions,active_bots,inducted_bots,Bots_in_maintenance,active_chargers,total_chargers,SUM_UPH_60min,SUM_put_UPH_60min,COUNT_UPH_60min,COUNT_put_UPH_60min,port_entry_errors, port_exit_errors, port_entry_totes_presented, port_exit_totes_presented  from daily_site_kpi_ayx where site='" +
            self.site.get('name') + f"' and time>='{sd}' and time<'{ed}'"
            ]

        dfs = []
        error_data = {'Error Message': [], 'Query': []}
        for i in qrylist:
            data_dict = self.fetch_data(client, i)
            if 'error' in data_dict[0].keys():
                error_data['Error Message'].append(data_dict[0]['error'])
                error_data['Query'].append(data_dict[0]['query'])
            else:
                d = pd.DataFrame(data_dict)
                dfs.append(d)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            print(error_data)
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            return ideal_dataset
        else:
            if len(dfs) > 0:
                final = reduce(lambda left, right: pd.merge(left, right, on=['time'], how='inner'), dfs)
                final['inducted_bots'] = final['inducted_bots']
                final['time'] = final['time'].apply(lambda x: x.replace('T', ' ').replace('Z', ''))
                final = final.melt(['time'], var_name='kpi')
                final['report_level'] = 'kpi tables'
                return final
            else:
                return ideal_dataset


    # ------------xxxxxx----------------
    def bot_stops(self,df, df2):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'value_s'])
        if df[1] == 0:
            print('please check query')
            print(df[2])
            return ideal_dataset
        else:
            dff = df[0]
            dfs = []
            # total
            bot_stops_df = pd.DataFrame(dff.groupby('time').size()).reset_index()
            bot_stops_df.rename(columns={0: 'bot_stops'}, inplace=True)
            dfs.append(bot_stops_df)

            # exc HE and SS
            hess_df = pd.DataFrame(
                dff[(dff['L1'] != 'Hard Emg error') & (dff['L1'] != 'Server Safety')].groupby('time').size()).reset_index()
            hess_df.rename(columns={0: 'bot_stops_exc_HE_and_SS'}, inplace=True)
            dfs.append(hess_df)

            # exc ss
            he_df = pd.DataFrame(dff[dff['L1'] != 'Hard Emg error'].groupby('time').size()).reset_index()
            he_df.rename(columns={0: 'bot_stops_exc_HE'}, inplace=True)
            dfs.append(he_df)

            if df2[1] != 0:
                mi_df = pd.DataFrame(df2[0].groupby('time').size()).reset_index()
                mi_df.rename(columns={0: 'bot_stops_manual_intervention'}, inplace=True)
                dfs.append(mi_df)

            if len(dfs) > 0:
                final = reduce(lambda left, right: pd.merge(left, right, on=['time'], how='outer'), dfs)
                final.fillna(0, inplace=True)
                final = final.melt(['time'], var_name='kpi')
                final['report_level'] = 'bot stops'
                return final
            else:
                return ideal_dataset


    # ------------xxxxxx----------------
    def bot_maintenance_data(self,client, start_date, end_date):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'value_s'])
        sd = start_date.split(' ')[0]
        ed = end_date.split(' ')[0]
        # sd = start_date.strftime('%Y-%m-%d')
        # ed = end_date.strftime('%Y-%m-%d')
        qry = f"select Maintenance_to_Ops, Ops_to_Maintenance,BFR,updated_bots_bfr from bot_maintenance_data where time>='{sd}' and time<'{ed}'"

        data_dict = self.fetch_data(client, qry)

        error_data = {'Error Message': [], 'Query': []}
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            print(error_data)
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            return ideal_dataset
        else:
            d['time'] = d['time'].apply(lambda x: x.replace('T', ' ').replace('Z', ''))
            bfr = d['BFR'][0]
            d = d.melt(['time'], var_name='kpi')
            d['value_s'] = d['value']
            d["value"] = np.where(d["value"] == bfr, 0, d['value'])
            d['report_level'] = 'bot maintenance data'
            return d


    def warehouse_occupancy_data(self,client, start_date, end_date):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'value_s'])
        sd = start_date.split(' ')[0]
        ed = end_date.split(' ')[0]
        # sd = start_date.strftime('%Y-%m-%d')
        # ed = end_date.strftime('%Y-%m-%d')
        qry = "select value	 as warehouse_space_value ,warehouse_space_total,	warehouse_space_used from warehouse_occupancy where time>='" + sd + "' and time<'" + ed + "' order by time desc limit 1"

        data_dict = self.fetch_data(client, qry)

        error_data = {'Error Message': [], 'Query': []}
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            print(error_data)
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            return ideal_dataset
        else:
            d['time'] = d['time'].apply(lambda x: x.replace('T', ' ').replace('Z', ''))
            d = d.melt(['time'], var_name='kpi')
#            d['value_s'] = d['value']
            d['report_level'] = 'kpi tables'
            return d


    def query1(self,client, sd,ed):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'charger_id', 'value_s'])
        databases = client.get_list_database()
        database_exists = any(db['name'] == 'gmc' for db in databases)
        if database_exists:
            qrylist = f"select charger_id as val,charger_id  from gmc..chargetask_events where time>='{sd}' and time<'{ed}' and event='docking_success'"
        else:
            qrylist = f"select charger_id as val,charger_id  from GreyOrange..chargetask_events where time>='{sd}' and time<'{ed}' and event='docking_success'"
        error_data = {'Error Message': [], 'Query': []}
        data_dict = self.fetch_data(client, qrylist)
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            print(error_data)
            return ideal_dataset
        else:
            d = d.groupby(['charger_id']).agg(time=('time', 'min'), value=('val', 'count')).reset_index()
            d['time'] = d['time'].apply(lambda x: x.replace('T', ' ').replace('Z', ''))
            d['report_level'] = 'charger data'
            d['kpi'] = 'charger_attempts'
            d['value_s'] = d['value'].astype('str')
            return d

    def query2(self,client, sd,ed):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'charger_id', 'value_s'])
        qrylist = f"select charger_id as val,reason as error_category, charger_id from charger_events where time>='{sd}' and time<'{ed}'  and mode='error'"
        error_data = {'Error Message': [], 'Query': []}
        data_dict = self.fetch_data(client, qrylist)
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            print(error_data)
            return ideal_dataset
        else:
            d1 = d.groupby(['charger_id']).agg(time=('time', 'min'), value=('val', 'count')).reset_index()
            d1['kpi'] = 'total_charger_errors'
            d['error_category'] = d['error_category'].fillna('')
            d2 = d.groupby(['charger_id','error_category']).agg(time=('time', 'min'), value=('val', 'count')).reset_index()
            d2['kpi'] = 'charger_errors'
            print(d2)
            d = pd.concat([d1,d2])
            print(d)
            d['time'] = d['time'].apply(lambda x: x.replace('T', ' ').replace('Z', ''))
            d['report_level'] = 'charger data'
            d1['value_s'] = d1['value'].astype('str')
            return d


    def query3(self,client, sd,ed):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'charger_id', 'value_s'])
        qrylist = f"select value as val,charger_id from charger_events where time>='{sd}' and time<'{ed}'  and " + '"' + "name" + '"' + " ='stopping_charging' and value>0"
        error_data = {'Error Message': [], 'Query': []}
        data_dict = self.fetch_data(client, qrylist)
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            print(error_data)
            return ideal_dataset
        else:
            d = d.groupby(['charger_id']).agg(time=('time', 'min'), value=('val', 'count')).reset_index()
            d['time'] = d['time'].apply(lambda x: x.replace('T', ' ').replace('Z', ''))
            d['report_level'] = 'charger data'
            d['kpi'] = 'charger_success'
            d['value_s'] = d['value'].astype('str')
            return d

    def query4(self,client, sd,ed):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'charger_id', 'value_s'])
        qrylist = f"select value as val,charger_id  from charger_events where time>='{sd}' and time<'{ed}'  and " + '"' + "name" + '"' + " ='stopping_charging' and value>0"
        error_data = {'Error Message': [], 'Query': []}
        data_dict = self.fetch_data(client, qrylist)
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            print(error_data)
            return ideal_dataset
        else:
            d = d.groupby(['charger_id']).agg(time=('time', 'min'), value=('val', 'mean')).reset_index()
            d['time'] = d['time'].apply(lambda x: x.replace('T', ' ').replace('Z', ''))
            d['report_level'] = 'charger data'
            d['kpi'] = 'avg_charge_pushed'
            d['value_s'] = d['value'].astype('str')
            return d

    def query5(self,client, sd,ed):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'charger_id', 'value_s'])
        qrylist = f"select charger_id as val,charger_id  from charger_events where time>='{sd}' and time<'{ed}'  and " + '"' + "name" + '"' + " ='contactor_on'"
        error_data = {'Error Message': [], 'Query': []}
        data_dict = self.fetch_data(client, qrylist)
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data['Error Message']):
            print('please check query')
            errortable = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
            print(error_data)
            return ideal_dataset
        else:
            d = d.groupby(['charger_id']).agg(time=('time', 'min'), value=('val', 'count')).reset_index()
            d['time'] = d['time'].apply(lambda x: x.replace('T', ' ').replace('Z', ''))
            d['report_level'] = 'charger data'
            d['kpi'] = 'charger_contactor_on'
            d['value_s'] = d['value'].astype('str')
            return d

    # ------------xxxxxx----------------
    def charging_success_rate(self,client, start_date, end_date, h, m):
        sd = start_date
        ed = end_date
        d1 = self.query1(client, sd, ed)
        d2 = self.query2(client, sd, ed)
        d3 = self.query3(client, sd, ed)
        d4 = self.query4(client, sd, ed)
        d5 = self.query5(client, sd, ed)
        df = pd.concat([d1,d2,d3,d4,d5])
        return df




    def main_errors(self,cursor, sd, ed):
        ideal_dataset = pd.DataFrame(columns=['time', 'kpi', 'value', 'report_level', 'value_s'])
        error_data = {'Error Message': [], 'Query': []}

        # cursor = client[0].cursor()
        try:
            cursor.execute(
                "SELECT error_reason as error_name,count(error_reason) as error_count FROM device_manager_butlermaintenance where created between '" + sd + "' and '" + ed + "' group by error_reason ")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=["kpi", "value"])
            df['report_level'] = 'maintenance_errors'
            #df['time'] = sd.replace('T', ' ').replace('Z', '')
            df['time'] = sd
            return df
        except:
            error_data['Error Message'].append('error in device_manager_butlermaintenance (tower db)')
            error_data['Query'].append(
                "SELECT error_reason as error_name,count(error_reason) as error_count FROM device_manager_butlermaintenance where created between '" + sd + "' and '" + ed + "' group by error_reason")
            df = pd.DataFrame(error_data)
            subject = 'consolidated_data (' + self.site.get('name') + ')'
            self.send_mail(df.to_html(), subject, sender, receiver_list)
            return ideal_dataset


    # print(Yesterday+' '+sites['hour'][i]+':'+sites['min'][i]+':00',dby+' '+sites['hour'][i]+':'+sites['min'][i]+':00')

    receiver_list = ['ankush.j@greyorange.com']
    sender = 'analytics@greyorange.com'

    # creating date
    # Today = time.strftime('%Y-%m-%d')
    # Kal = datetime.today() - timedelta(days=1)
    # Parso = datetime.today() - timedelta(days=2)
    # Yesterday = Kal.strftime('%Y-%m-%d')
    # dby = Parso.strftime('%Y-%m-%d')
    # old_data = datetime.today() - timedelta(days=15)
    # old_data = old_data.strftime('%Y-%m-%d')

    # connection error message
    con_error = {'Error': ['DB Connection Error'], 'Messaage': ["Could'nt establish connection"]}

    # fetch all sites
    d = {'port': 'object', 'tower_port': 'object', 'hour': 'object', 'min': 'object'}
    # sites = pd.read_csv(
    #     '/home/gor/Documents/analytics_dashboard_scripts/Misc_Scripts/Consolidated/Site_Excel/all_site_excel.csv', dtype=d)

    # ideal dataset
    ideal_dataset = pd.DataFrame(
        columns=['time', 'kpi', 'value', 'report_level', 'error_category', 'barcode_group', 'value_s'])


    def execute_values(self,conn, df, table):
        tuples = [tuple(x) for x in df.to_numpy()]

        cols = ','.join(list(df.columns))
        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("the dataframe is inserted")
        cursor.close()


    def delete_data(self,conn, start_date, table, site):
        # SQL query to execute
        query = f"DELETE FROM {table} WHERE date='{start_date}' and site = '{site}';"
        cursor = conn.cursor()
        print(query)
        try:
            cursor.execute(query)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("the dataframe is delete data")
        cursor.close()


    def delete_data_old_data(self, conn, day14_old_data, table,site):
        # SQL query to execute
        query = f"DELETE FROM {table} WHERE date<='{day14_old_data}' and site = '{site}';"
        print(query)
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("the dataframe is delete data")
        cursor.close()

    def consolidate_kpi1(self):
        # create dataset for all sites
        all_dfs = []
        print(self.site.get("influx_ip"))
        print(self.site.get('influx_port'))
        print(self.site.get("out_db_name"))
        dashboard_name= self.site.get("displayname")
        # client_tuple=InfluxData(host=self.site.get("influx_ip"),port=self.site.get('influx_port'),db=self.site.get("out_db_name"))
        # client_tuple_Alteryx=InfluxData(host=self.site.get("influx_ip"),port=self.site.get('influx_port'),db=self.site.get("alteryx_out_db_name"))
        client_tuple = self.connection_to_influx(self.site.get('influx_ip'), self.site.get('influx_port'), 'gor', 'Grey()orange', 'GreyOrange',
                                            self.site.get('name'))
        client_tuple_Alteryx = self.connection_to_influx(self.site.get('influx_ip'), self.site.get('influx_port'), 'gor', 'Grey()orange', 'Alteryx',
                                                        self.site.get('name'))
        client_kpi = self.connection_to_central_influx(self.site.get('central_influx'), '8086', 'altryx', 'Z20tcH!vZC$jb3V&wYW$ZGFlZ3U', self.site.get('alteryx_out_db_name'))
        # client_kpi = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'),
        #                                              self.site.get('central_influx'))
        # client_tower = self.connection_to_tower(self.sites['tower_ip'][i], self.sites['tower_port'][i], self.sites['site'][i], 'tower',
        #                                    self.sites['tower_password'][i], self.sites['ssl_root'][i], self.sites['ssl_cert'][i],
        #                                    self.sites['ssl_key'][i])

        client_tower = postgres_connection(database='tower', user=self.site.get('tower_user'), \
                                         sslrootcert=self.site.get('sslrootcert'),
                                         sslcert=self.site.get('sslcert'), \
                                         sslkey=self.site.get('sslkey'),
                                         host=self.site.get('postgres_tower'), \
                                         port=self.site.get('postgres_tower_port'),\
                                         password=self.site.get('tower_password'),
                                         dag_name=os.path.basename(__file__), site_name=self.site.get('name'))

        client_tower_cursor =client_tower.pg_curr

        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        if client_tuple == 0 or client_tuple_Alteryx == 0 or client_kpi == 0 or client_tower == 0:
            print(con_error)
            errortable = pd.DataFrame(con_error)
            subject = 'Script (consolidated_data) didnt run for (' + self.site.get('name') + ')'
            self.send_mail(errortable.to_html(), subject, sender, receiver_list)
        else:
            # manually set time interval - can only take 1 day interval at a time
            # sd = '2022-09-05T'+sites['hour'][i]+':'+sites['min'][i]+':00Z'
            # ed = '2022-09-06T'+sites['hour'][i]+':'+sites['min'][i]+':00Z'
            # automatic time interval
            sd = self.start_date
            ed = self.end_date

        df = self.fetch_butler_nav_incidents(client_tuple, sd, ed, hour, minute)
        df2 = self.fetch_butler_nav_error(client_tuple,client_tuple_Alteryx, sd, ed, hour, minute)
        f1 = self.error_per_category(df)
        f2 = self.topNbutlers(df, n=10)
        f3 = self.topNbarcodes(df, n=10)
        f13 = self.topN_Mi_nbutlers(df2, n=10)
        f4 = self.butlerpercentages(client_tuple_Alteryx, sd, ed)
        f5 = self.from_kpi_tables(client_kpi, sd, ed)
        f6 = self.bot_stops(df,df2)
        f7 = self.error_and_res_time(df)
        f8 = self.bot_maintenance_data(client_tuple, sd, ed)
        f10 = self.charging_success_rate(client_tuple, sd, ed, hour, minute)
        f11 = self.top5perbots(df)
        f12 = self.main_errors(client_tower_cursor, sd, ed)
        f14 = self.warehouse_occupancy_data(client_tuple, sd, ed)
        f15 = self.fetch_manual_intervention(client_tuple, client_tuple_Alteryx, sd, ed, hour, minute)
        data = {'time':self.orignal_start_date,'report_level':'site_name','error_category':'','kpi':'site_name','value':0,'value_s':dashboard_name}
        df_dashboard_name = pd.DataFrame(data,index=[0])

        final = pd.concat([ideal_dataset, f1, f2, f3, f4, f5, f6, f7, f8, f10, f11, f12,f13,f14,f15, df_dashboard_name])


        # if len(final_df) > 0:
        #     final_df['site'] = self.site.get('name')
        #     all_dfs.append(final_df)
        # else:
        #     print('No data created for ', sites['site'][i])

        # check final result and push to tableau
        if len(final) > 0:

            # automatic
            dte = self.orignal_start_date.strftime('%Y-%m-%d')
            # manual
            # dte = '2022-09-05'

            print(dte)
            final['date'] = dte
            final['time'] = dte
            final = final.reset_index()
            final = final.set_index(pd.to_datetime(np.arange(len(final)), unit='s', origin=final['time'][1]))
            final.drop(['index', 'time'], axis=1, inplace=True)
            final['site']=self.site.get('name')
            final['value'].astype('float')
            final['barcode_group'] = final['barcode_group'].fillna('')
            final['charger_id'] = final['charger_id'].fillna('')
            final['error_category'] = final['error_category'].fillna('')
            final['hour'] = final['hour'].fillna(0)
            final['kpi'] = final['kpi'].fillna('')
            final['report_level'] = final['report_level'].fillna('')
            final['value'] = final['value'].fillna(0)
            final['value_s'] = final['value_s'].fillna('')
            print(final)
            # import os
            # path = f'{self.tenant_info["Name"]}_test.csv'
            # path = os.path.join(CONFFILES_DIR, path)

            # final.to_csv(path, index=False, header=True)
            # push data to tableau
            #    client = DataFrameClient(host='172.16.2.68', port=8086, username='altryx', password='Z20tcH!vZC$jb3V&wYW$ZGFlZ3U', ssl=True, verify_ssl=False)
            #    client.write_points(final,'consolidated_data',database='Alteryx',protocol='line',time_precision='s')
            consolidate_db = self.connection_to_consolidatedb()
            self.delete_data(consolidate_db, dte, 'consolidate_table',site=self.site.get('name'))
            self.execute_values(consolidate_db, final, 'consolidate_table')
            self.delete_data_old_data(consolidate_db, self.old_data, 'consolidate_table',site=self.site.get('name'))
            client_tower.close()
            print('Data inserted - ', len(final))
        else:
            print('final df is empty')


    # send mail
