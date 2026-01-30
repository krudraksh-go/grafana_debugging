import pandas as pd
from datetime import datetime, timedelta, timezone

from utils.CommonFunction import InfluxData, MultiTenantDAG, CommonFunction, postgres_connection, SingleTenantDAGBase, tableau_postgres_setup
from airflow.models.dag import DAG
import urllib3
import json
import os
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


dag = DAG(
    "user_session",
    start_date=datetime.strptime('07/27/23 13:55:26', '%m/%d/%y %H:%M:%S'),
    schedule_interval='4 9 * * *',
    default_args=CommonFunction().get_default_args_for_dag(),
    catchup=False,
)

receiver_list = ['ankush.j@greyorange.com']
sender = 'ankush.j@greyorange.com'

# connection error message
con_error = {'Error': ['DB Connection Error'], 'Messaage': ["Could'nt establish connection"]}

ideal_dataset = pd.DataFrame(
    columns=['id', 'app_name', 'context', 'is_session_active', 'login_time', 'logout_time', 'station_id', 'username'])


class UserSession(SingleTenantDAGBase):

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df

    def get_datetime_interval2(self,consolidate_db):
        # influx = self.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        # start_datetime = influx.fetch_one("""select * from user_session
        #               where site='{site}'  order by time desc limit 1""".format(
        #     site=self.site.get('name')))
        query="""select time from user_session
                      where site='{site}'  order by time desc limit 1""".format(
            site=self.site.get('name'))
        cursor=consolidate_db.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        start_datetime = pd.DataFrame(data, columns=['time'])
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime['time'][0], utc=True)
            start_datetime = start_datetime + timedelta(days=1)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=30)
        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = datetime.now(timezone.utc)

        timeseriesdata = self.create_time_series_data2(start_datetime, end_datetime, '1d')
        return timeseriesdata

    def user_session(self):
        self.tableau_func = tableau_postgres_setup()
        consolidate_db = self.tableau_func.connection_to_consolidatedb()
        try:
            daterange = self.get_datetime_interval2(consolidate_db)
            for i in daterange.index:
                self.start_date = daterange['start_date'][i]
                self.end_date = daterange['end_date'][i]
                UserSession.user_session_1(self, self.start_date, self.end_date)
        except Exception as e:
            print(f"error:{e}")

    def user_session_1(self, start_date, end_date):

        # self.orignal_start_date= pd.to_datetime(start_date,utc=True)
        # self.end_date =pd.to_datetime(end_date,utc=True)
        # self.start_date=self.orignal_start_date.strftime("%Y-%m-%d %H:%M:%S")
        # self.end_date=self.end_date.strftime("%Y-%m-%d %H:%M:%S")

        self.commonFunction = CommonFunction()

        self.start_date = pd.to_datetime(start_date, utc=True).strftime("%Y-%m-%d %H:%M:%S")
        self.end_date = pd.to_datetime(end_date, utc=True).strftime("%Y-%m-%d %H:%M:%S")
        report_date = pd.to_datetime(start_date, utc=True).strftime("%Y-%m-%d")

        self.postgres_conn = postgres_connection(database='platform_security',
                                                 user=self.site.get('postgres_pf_user'), \
                                                 sslrootcert=self.site.get('sslrootcert'), \
                                                 sslcert=self.site.get('sslcert'), \
                                                 sslkey=self.site.get('sslkey'),
                                                 host=self.site.get('postgres_pf'), \
                                                 port=self.site.get('postgres_pf_port'),
                                                 password=self.site.get('postgres_pf_password'),
                                                 dag_name=os.path.basename(__file__), site_name=self.site.get('name'))

        client_cursor = self.postgres_conn.pg_curr

        if self.postgres_conn == 0:
            subject = 'Script (user_session) didnt run for (' + self.site.get('name') + ')'
            errortable = pd.DataFrame(con_error)
            self.commonFunction.send_mail(errortable.to_html(), subject, sender, receiver_list)
        else:
            sd = self.start_date
            ed = self.end_date
            f1 = self.create_data(client_cursor, sd, ed)
            final = pd.concat([ideal_dataset, f1])
            final['report_date']= report_date
        self.postgres_conn.close()
        if len(final) > 0:

            final['login_time'] = final['login_time'].astype('str')
            final['logout_time'] = final['logout_time'].astype('str')
            # df = final.copy()
            # df['time'] = df['time'].astype('str')

            # print(df)
            # final = final.set_index('time')
            # write_influx = self.get_central_influx_client(self.site.get('alteryx_out_db_name'),
            #                                               self.site.get('central_influx'))
            # isvalid = write_influx.is_influx_reachable()
            # if isvalid:
            #     success = write_influx.write_all(final, 'user_session', tag_columns=["site"])

            # self.tableau_func =tableau_postgres_setup()
            consolidate_db = self.tableau_func.connection_to_consolidatedb()
            #self.tableau_func.delete_data(consolidate_db, dte, 'user_session',site=self.site.get('name'))
            self.tableau_func.execute_values(consolidate_db, final, 'user_session')
            consolidate_db.close()
            #self.tableau_func.delete_data_old_data(consolidate_db, self.old_data, 'user_session',site=self.site.get('name'))

        else:
            print('final df is empty')

    def create_data(self, cursor, sd, ed):

        ideal_dataset = pd.DataFrame(
            columns=['id', 'app_name', 'context', 'is_session_active', 'login_time', 'logout_time', 'station_id',
                     'username'])
        error_data = {'Error Message': [], 'Query': []}

        try:
            cursor.execute(
                "SELECT id,app_name, context,is_session_active,login_time,logout_time,station_id,username FROM user_session where login_time>='" + sd + "' and login_time<'" + ed + "' and app_name!='unknown'")
            data = cursor.fetchall()
            df = pd.DataFrame(data,
                              columns=['id', 'app_name', 'context', 'is_session_active', 'login_time', 'logout_time',
                                       'station_id', 'username'])
            df['time'] = df['login_time']
            df['site'] = self.site.get('name')
            df['context'] = df['context'].apply(json.dumps)
            return df
        except Exception as ex:
            error_data['Error Message'].append(f'Please check query (platfor_security db):{ex}')
            error_data['Query'].append(
                "SELECT id,app_name,context,is_session_active,login_time,logout_time,station_id,username FROM user_session where login_time>='" + sd + "' and login_time<'" + ed + "' and app_name!='unknown'")

            errortable = pd.DataFrame(error_data)
            subject = 'user_session ('+self.site.get('name')+')'
            self.commonFunction.send_mail(errortable.to_html(), subject, sender, receiver_list)
            return ideal_dataset


MultiTenantDAG(
    dag,
    [
        'user_session',
    ],
    [],
    UserSession
).create()
