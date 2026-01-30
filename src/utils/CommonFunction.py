from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
from datetime import datetime, timezone, timedelta
import numpy as np
import math
import pytz
import psycopg2
import requests
import os
import time
import json
from urllib.parse import urlparse
from airflow.operators.python import PythonOperator
import airflow
import gspread
import psycopg2.extras as extras
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.DagIssueAlertUtils import log_failures, log_dag_failure, DbConnectionError, DbWriteError
from google.cloud import storage
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import KafkaError, UnknownTopicOrPartitionError
from pymongo import MongoClient

Site = os.environ.get('Site', 'Site')
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
Postgres_ButlerDev = os.environ.get('Postgres_ButlerDev', Butler_ip)
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
elastic_ip = os.environ.get('ELASTIC_HOSTNAME', 'localhost')
elastic_port = os.environ.get('ELASTIC_PORT', '9200')
write_influx_ip = os.environ.get('write_influx_ip', '10.11.4.23')
Write_influx_port = os.environ.get('Write_influx_port', '8086')
db_name = os.environ.get('Out_db_name', 'GreyOrange')
gmc_db_name = os.environ.get('Out_gmc_db_name', 'gmc')
TimeZone = os.environ.get('TimeZone', 'PDT')
StartTime = os.environ.get('StartTime', '7:00:00')
Postgres_pf = os.environ.get('Postgres_pf', 'localhost')
Postgres_pf_port = os.environ.get('Postgres_pf_port', 5432)
Postgres_pf_password = os.environ.get('Postgres_pf_password', "YLafmbfuji$@#45!")
Postgres_pf_user = os.environ.get('Postgres_pf_user', "altryx_read")

Postgres_tower = os.environ.get('Postgres_tower', "localhost")
Postgres_tower_port = os.environ.get('Postgres_tower_port', 5432)
Tower_user = os.environ.get('Tower_user', "altryx_read")
Tower_password = os.environ.get('Tower_password', "YLafmbfuji$@#45!")
sslrootcert = os.environ.get('sslrootcert', "")
sslcert = os.environ.get('sslcert', "")
sslkey = os.environ.get('sslkey', "")
Postgres_butler_port = os.environ.get('Postgres_butler_port', 5432)
Postgres_butler_password = os.environ.get('Postgres_butler_password', "YLafmbfuji$@#45!")
Postgres_butler_user = os.environ.get('Postgres_butler_user', "altryx_read")
streaming = os.environ.get('streaming', "N")
alteryx_out_db_name = os.environ.get('alteryx_out_db_name', 'Alteryx')
Butler_port = os.environ.get('Butler_port', "8181")
Butler_interface_port = os.environ.get('Butler_interface_port', "8181")
is_ttp_setup = os.environ.get('is_ttp_setup', 'N')


from config import (
    CENTRAL_INFLUX_USERNAME,
    CENTRAL_INFLUX_PASSWORD,
    GOOGLE_TENANTS_FILE_PATH,
    GOOGLE_ERROR_FILE_PATH,
    GOOGLE_Warehouse_Manager_FILE_PATH,
    GOOGLE_Warehouse_Manager_site_ops_houe_FILE_PATH,
    GOOGLE_AUTH_JSON,
    CONFFILES_DIR,
    GLOBAL_SITE_HARDWARE_FILE_PATH,
    AIRFLOW_SETUP_SHEET_FILE_PATH,
    GOOGLE_BUCKET_AUTH_JSON,
    GOOGLE_BUCKET_NAME,
    GOOGLE_TENANTS_FILE,
    GOOGLE_ERROR_FILE,
    GLOBAL_SITE_HARDWARE_FILE,
    AIRFLOW_SETUP_SHEET_FILE,
    GOOGLE_Warehouse_Manager_FILE,
    GOOGLE_Warehouse_Manager_site_ops_houe_FILE,
    DJANGO_SERVER,
    MongoDbServer,
    rp_seven_days,
    HOST
)


class Elasticsearch:
    def __init__(self, host='localhost', port='9200', dag_name="", site_name=""):
        self.elastic_ip = host
        self.elastic_port = port
        self.dag_name = dag_name
        self.site_name = site_name

    def check_elasticsearch_reachability(self):
        try:
            response = requests.get(f"http://{self.elastic_ip}:{self.elastic_port}", timeout=120)
            if response.status_code == 200:
                return True
            else:
                log_failures('db_connection_failure', self.dag_name, self.site_name, "elasticsearch", str(ex))
                return False
        except Exception as ex:
            print(ex)
            log_failures('db_connection_failure', self.dag_name, self.site_name, "elasticsearch", str(ex))
            return False
    
    def fetch_doc_with_payload(self, index_name, payload, batch_size):
        if not self.check_elasticsearch_reachability():
            raise DbConnectionError("Elasticsearch server is not reachable.")

        search_url = f"http://{self.elastic_ip}:{self.elastic_port}/{index_name}/_search?scroll=1m&size={batch_size}"
        headers = {
            'Content-Type': 'application/json'
            }

        try:
            response = requests.request("GET", search_url, headers=headers, data=payload)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to fetch data: {str(e)}")

        data = response.json()

        if "hits" not in data or "hits" not in data["hits"]:
            raise ValueError("Invalid response format from Elasticsearch.")        

        scroll_id = data["_scroll_id"]
        hits = data["hits"]["hits"]
        all_hits = hits      

        while len(hits) > 0:
            scroll_url = f"http://{self.elastic_ip}:{self.elastic_port}/_search/scroll?scroll_id={scroll_id}&scroll=1m"

            try:
                response = requests.get(scroll_url, timeout=120)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                raise ConnectionError(f"Failed to fetch scroll data: {str(e)}")

            data = response.json()

            if "hits" not in data or "_scroll_id" not in data:
                raise ValueError("Invalid response format from Elasticsearch.")

            hits = data["hits"]["hits"]
            all_hits.extend(hits)
            scroll_id = data["_scroll_id"]

        return all_hits
    
    def fetch_all_docs(self, index_name, batch_size=100):
        if not self.check_elasticsearch_reachability():
            raise DbConnectionError("Elasticsearch server is not reachable.")

        initial_search_url = f"http://{self.elastic_ip}:{self.elastic_port}/{index_name}/_search?scroll=1m&size={batch_size}"

        try:
            response = requests.get(initial_search_url, timeout=120)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to fetch initial data: {str(e)}")

        data = response.json()

        if "hits" not in data or "_scroll_id" not in data:
            raise ValueError("Invalid response format from Elasticsearch.")

        scroll_id = data["_scroll_id"]
        hits = data["hits"]["hits"]
        all_hits = hits

        while len(hits) > 0:
            scroll_url = f"http://{self.elastic_ip}:{self.elastic_port}/_search/scroll?scroll_id={scroll_id}&scroll=1m"

            try:
                response = requests.get(scroll_url, timeout=120)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                raise ConnectionError(f"Failed to fetch scroll data: {str(e)}")

            data = response.json()

            if "hits" not in data or "_scroll_id" not in data:
                raise ValueError("Invalid response format from Elasticsearch.")

            hits = data["hits"]["hits"]
            all_hits.extend(hits)
            scroll_id = data["_scroll_id"]

        return all_hits


class InfluxData:

    def __init__(self, *args, **kwargs):
        self.site_name = ''
        self.dag_name = ''
        print("kwargs.get('host')", kwargs.get('host'), kwargs.get('port', 8086), kwargs.get('db'))
        self.influx_client = InfluxDBClient(
            kwargs.get('host'),
            kwargs.get('port', 8086)
            ,
            # username='altryx',
            # password='Z20tcH!vZC$jb3V&wYW$ZGFlZ3U',
            # ssl=True,
            retries=2,
            timeout=kwargs.get('timeout', 180)
        )
        try:
            InfluxDBClient.ping(self.influx_client)
            # self.influx_client.switch_database(kwargs.get('db'))
            # print("\nConnection to " + db + " InfluxDB successful (" + site + ")\n")
            # return (client, site)
        except:
            print("\nFailed to establish connection")
            # return (0, 0)
        # try:
        #     self.influx_client.create_database('airflow')
        # except:
        #     pass
        self.influx_client.switch_database(kwargs.get('db'))

    def query(self, query, retries=0):
        try:
            return self.influx_client.query(query, chunked=True, chunk_size=10000)
        except airflow.exceptions.AirflowTaskTimeout:
            raise
        except Exception as ex:
            if retries < 3:
                time.sleep(10)
                return self.query(query, retries=retries + 1)
            else:
                print(ex)
                error_message = str(ex)
                log_failures('db_connection_failure', self.dag_name, self.site_name, "influxdb", error_message)
                raise DbConnectionError("[InfluxDB not connected] " + error_message)

    def fetch_data(self, client, query):
        try:
            datadict = client.query(query, chunked=True, chunk_size=10000)
        except:
            return [{'error': 'Please check the query', 'query': query}]

        data = list(datadict.get_points())
        if len(data) <= 0:
            return [{'error': 'No data fetched', 'query': query}]

        # returns a list of dictionaries where 'key' is column name and 'value' is data
        return data

    def get_list_database(self):
        return self.influx_client.get_list_database()

    def setup_database(self, host="localhost", port=8086):
        # client = InfluxDBClient(host , port)
        try:
            self.influx_client.create_database('Alteryx')
        except:
            pass

        try:
            rp_name = rp_seven_days
            query = f"SHOW RETENTION POLICIES ON GreyOrange"

            # Execute the query
            result = pd.DataFrame(self.client.query(query).get_points())

            # Check if the retention policy exists
            #exists = any(rp['name'] == rp_name for rp in result['name'])
            exists = rp_name in result['name'].values
            if not exists:
                try:
                    rp_query = f""" CREATE RETENTION POLICY {rp_name} ON GreyOrange DURATION 7d REPLICATION 1 """
                    self.client.query(rp_query)
                except:
                    pass

            rp_name = rp_seven_days
            query = f"SHOW RETENTION POLICIES ON Alteryx"

            # Execute the query
            result =pd.DataFrame(self.influx_client.query(query).get_points())

            # Check if the retention policy exists
            #exists = any(rp['name'] == rp_name for rp in result['name'])
            exists = rp_name in result['name'].values
            if exists:
                print(f"Retention policy '{rp_name}' exists.")
            else:
                print(f"Retention policy '{rp_name}' does not exist.")
                rp_query = f""" CREATE RETENTION POLICY {rp_seven_days}
                ON Alteryx 
                DURATION 7d 
                REPLICATION 1 
                """
                self.influx_client.query(rp_query)

            # Close the connection
            #client.close()
        except:
            pass
        return None

    def is_influx_reachable(self, host="localhost", port=8086, dag_name="", site_name="", retries=0):
        self.site_name = site_name
        self.dag_name = dag_name
        try:
            q = f'SHOW MEASUREMENTS'
            check = pd.DataFrame(self.influx_client.query(q).get_points())
        except airflow.exceptions.AirflowTaskTimeout:
            raise
        except Exception as ex:
            print(ex)
            error_message = str(ex)
            log_failures('db_connection_failure', dag_name, site_name, "influxdb", error_message)
            raise DbConnectionError("[InfluxDB not connected] " + error_message)
        return True

    def connect_database(self, host="localhost", port=8086):
        try:
            return self.influx_client
        except:
            pass
        return None

    def is_table_present(self, table_name):
        q = f"select * from {table_name} where time > now()-7d order by time desc limit 1"
        cron_run_at = pd.DataFrame(self.influx_client.query(q).get_points())
        if not cron_run_at.empty:
            return True

        q = f"select * from {table_name} where time > now()-120d order by time desc limit 1"
        cron_run_at = pd.DataFrame(self.influx_client.query(q).get_points())
        if not cron_run_at.empty:
            return True

        q = f"select * from {table_name} order by time desc limit 1"
        cron_run_at = pd.DataFrame(self.influx_client.query(q).get_points())
        if not cron_run_at.empty:
            return True

        return False

    def find_last_entry_point(self, table_name, where_clause_list=None):
        # Construct the base query
        base_query = f'select * from {table_name}'

        # Add additional WHERE clauses if provided
        if where_clause_list:
            where_clauses = ' and '.join(where_clause_list)
            base_query = f'{base_query} where {where_clauses}'
            q = f'{base_query} and time > now()-1d order by time desc limit 1'
            q1 = f'{base_query} and time > now()-7d order by time desc limit 1'
            q2 = f'{base_query} and time > now()-30d order by time desc limit 1'
        else:
            q = f'{base_query} where time > now()-1d order by time desc limit 1'
            q1 = f'{base_query} where time > now()-7d order by time desc limit 1'
            q2 = f'{base_query} where time > now()-30d order by time desc limit 1'

        # First query: Last 1 day
        result_df = pd.DataFrame(self.influx_client.query(q).get_points())

        if not result_df.empty:
            return result_df

        # Second query: Last 7 days
        result_df = pd.DataFrame(self.influx_client.query(q1).get_points())

        if not result_df.empty:
            return result_df

        # Third query: Last 30 days
        result_df = pd.DataFrame(self.influx_client.query(q2).get_points())

        return result_df

    def hour_rounder(self, t):
        # Rounds to nearest hour by adding a timedelta hour if minute >= 30
        return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
                + timedelta(hours=t.minute // 30))

    def fetch_last_run(self, table_name, site, installation_id):
        try:
            connection_string = MongoDbServer
            database_name = "GreyOrange"
            collection_name = 'last_dag_run'
            self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
            query = {'site': site, 'table':table_name}
            data = self.cls_mdb.get_data(query,None)
            self.cls_mdb.close_connection()
            if data:
                df = pd.DataFrame(data)
                last_run = df['last_run'][0]
            else:
                last_run = (datetime.now() - timedelta(days=1000)).strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception as ex:
            last_run = (datetime.now()-timedelta(days=1000)).strftime('%Y-%m-%dT%H:%M:%SZ')
        return last_run

    def get_start_date(self, table_name, tenant_info, where_clause_list=None):
        site = tenant_info['Name']
        installation_id = tenant_info['installation_id']
        is_production = tenant_info['is_production']
        IsIndivisualInflux = tenant_info['IsIndivisualInflux']
        if is_production =='N' and IsIndivisualInflux =='Y' and not where_clause_list:
            where_clause_list = [f" installation_id = '{installation_id}' "]

        site = tenant_info['Name']
        start_date = datetime.now(timezone.utc)
        start_date = start_date - timedelta(hours=100)
        start_date = start_date.replace(second=0, microsecond=0)
        start_date = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            df_time = self.find_last_entry_point(table_name,where_clause_list)
            if df_time.empty:
                time = datetime.now(timezone.utc)
                time = time - timedelta(hours=100)
            else:
                df_time['time'] = pd.to_datetime(df_time['time'])
                time = df_time['time'][0].to_pydatetime()
            dag_last_run = pd.to_datetime(self.fetch_last_run(table_name,site,installation_id),utc=True).to_pydatetime()
            time = max(time, dag_last_run)
            if not pd.isna(time):
                return time.strftime("%Y-%m-%d %H:%M:%S.%f")
            else:
                start_date
        except:
            pass
        return start_date
    


    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df

    # def get_datetime_interval_new(self,table_name,interval,starttime,back_date_datageneration_hours=1,max_range_hours=1):
    #     q = f'select * from {table_name} order by time desc limit 1'
    #     df_time = pd.DataFrame(self.influx_client.query(q).get_points())
    #     if not df_time.empty:
    #         start_date = pd.to_datetime(df_time['time'], utc=True)
    #         start_date = start_date[0].to_pydatetime()
    #         start_date = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")
    #     else:
    #         start_date = datetime.now(timezone.utc)
    #         start_date = start_date - timedelta(hours=back_date_datageneration_hours)
    #         start_date = start_date.replace(second=0, microsecond=0)
    #         start_date = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")
    #
    #     end_datetime = datetime.now(timezone.utc)
    #     end_datetime = end_datetime.replace(second=0, microsecond=0)
    #     timeseriesdata = self.create_time_series_data2(start_date, end_datetime, max_range_hours)
    #     return timeseriesdata

    def get_datetime_interval2(self, table_name, interval, starttime):
        df_time = self.find_last_entry_point(table_name)
        if not df_time.empty:
            start_datetime = pd.to_datetime(df_time['time'], utc=True)
            start_datetime = start_datetime[0].to_pydatetime()
            hour, minute, second = map(int, starttime.split(':'))
            start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=1)
            hour, minute, second = map(int, starttime.split(':'))
            start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = datetime.now(timezone.utc)
        end_datetime = end_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        timeseriesdata = self.create_time_series_data2(start_datetime, end_datetime, interval)
        return timeseriesdata
    
    def get_datetime_interval_from_last_run(self, table_name, interval, site, installation_id):
        dag_last_run = pd.to_datetime(self.fetch_last_run(table_name,site, installation_id), utc=True).to_pydatetime()
        four_days_before = datetime.now(timezone.utc) - timedelta(hours=100)
        start_datetime = max(dag_last_run, four_days_before)
        end_datetime = datetime.now(timezone.utc)
        timeseriesdata = self.create_time_series_data2(start_datetime, end_datetime, interval)
        return timeseriesdata

    def get_datetime_interval3(self, table_name, interval, tenant_info):
        site = tenant_info['Name']
        installation_id = tenant_info['installation_id']
        is_production = tenant_info['is_production']
        IsIndivisualInflux = tenant_info['IsIndivisualInflux']
        where_clause_list= None
        if is_production =='N' and IsIndivisualInflux =='Y':
            where_clause_list = [f" installation_id = '{installation_id}' "]

        df_time = self.find_last_entry_point(table_name,where_clause_list)
        if not df_time.empty:
            start_datetime = pd.to_datetime(df_time['time'], utc=True)
            start_datetime = start_datetime[0].to_pydatetime()
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(hours=100)

        end_datetime = datetime.now(timezone.utc)
        dag_last_run = pd.to_datetime(self.fetch_last_run(table_name,site,installation_id), utc=True).to_pydatetime()
        start_datetime = max(start_datetime, dag_last_run)
        timeseriesdata = self.create_time_series_data2(start_datetime, end_datetime, interval)
        return timeseriesdata

    def get_start_date_with_site(self, table_name, site):
        start_date = datetime.now(timezone.utc)
        start_date = start_date - timedelta(hours=25)
        start_date = start_date.replace(second=0, microsecond=0)
        start_date = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            df_time = self.find_last_entry_point(table_name, where_clause_list=[f"site={site}"])
            df_time['time'] = pd.to_datetime(df_time['time'])
            time = df_time['time'][0].to_pydatetime()
            if not pd.isna(time):
                return time.strftime("%Y-%m-%d %H:%M:%S.%f")
            else:
                start_date
        except:
            pass
        return start_date

    def write_database(self, host="localhost", port=8086, username='gor', password='Grey()orange', timeout=180):
        try:
            return DataFrameClient(host=host, port=port, username=username, password=password,
                                   retries=2,
                                   timeout=timeout)
        except:
            pass
        return None

    def write_points(self, df, table_name, db_name, dag_name='', site_name='',retention_policy="autogen"):
        try:
            self.influx_client.switch_database(db_name)
            self.influx_client.write_points(df, table_name, database=db_name, protocol='line', time_precision='ms',retention_policy=retention_policy)
        except Exception as ex1:
            error_message1 = str(ex1)
            log_failures('influxdb_write_issue', dag_name, site_name, "influxdb", error_message1)
            raise DbWriteError("[InfluxDB write issue] " + error_message1)

    def write_points(self, jsondatavalue='', database='', dag_name='', site_name='',retention_policy="autogen"):
        try:
            self.influx_client.switch_database(database)
            self.influx_client.write_points(jsondatavalue,retention_policy=retention_policy)
        except Exception as ex1:
            error_message1 = str(ex1)
            log_failures('influxdb_write_issue', dag_name, site_name, "influxdb", error_message1)
            raise DbWriteError("[InfluxDB write issue] " + error_message1)

    def get_central_influx_client(self, database, host=None, timeout=180):
        return InfluxDataV2(
            host=host if host else 'alteryx-influx.greymatter.greyorange.com',
            port=8086,
            username=CENTRAL_INFLUX_USERNAME,
            password=CENTRAL_INFLUX_PASSWORD,
            ssl=True,
            database=database,
            retries=2,
            timeout=timeout

        )


class Write_InfluxData:

    def __init__(self, *args, **kwargs):
        self.write_data = DataFrameClient(host=kwargs.get('host'), port=kwargs.get('port', 8086),
                                          username=kwargs.get('username', 'gor'),
                                          password=kwargs.get('username', 'Grey()orange'),
                                          retries=2,
                                          timeout=kwargs.get('timeout', 180))

    def writepoints(self, df, table_name, db_name, tag_columns, dag_name="", site_name="",retention_policy="autogen"):
        try:
            self.write_data.switch_database(db_name)
            i = math.ceil(df.shape[0] / 1000)
            i = 1 if i <= 0 else i
            list_df = np.array_split(df, i)
            for chunk in list_df:
                self.write_data.write_points(chunk, table_name, database=db_name, tag_columns=tag_columns,
                                             protocol='line', time_precision='n',retention_policy=retention_policy)
        except Exception as ex1:
            error_message1 = str(ex1)
            log_failures('influxdb_write_issue', dag_name, site_name, "influxdb", error_message1)
            raise DbWriteError("[InfluxDB write issue] " + error_message1)

class CommonFunction:
    def __init__(self):
        self.gcs = GCS()

    def send_mail(self, html_error_table, subject, sender, receiver_list):
        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = sender
        message['To'] = ', '.join(receiver_list)
        receivers = receiver_list

        body_content = html_error_table
        message.attach(MIMEText(body_content, "html"))
        msg_body = message.as_string()

        server = SMTP("email-smtp.us-east-1.amazonaws.com", 25)
        server.starttls()
        server.login('AKIA3FSTBJC6M5VPXEY3', 'BG8L0fFue6AvXFfXO4lUZMA2LoLhiq+kqyWNYmoyG+1D')
        server.sendmail(message['From'], receivers, msg_body)
        server.quit()
        print('Error Mail Sent!')

    def send_alert_mail(self, alert_html, sender, receiver_list):
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d")
        subject = formatted_datetime + " [Alert] - Airflow Dag Issues"
        self.send_mail(alert_html, subject, sender, receiver_list)

    def create_startdate_enddate_frame(self, start_date, end_date):
        # interval_start_date = start_date.replace(minute=0,second=0)
        interval_start_date = start_date
        interval_end_date = end_date.replace(second=0, microsecond=0)
        df = pd.DataFrame({"start_date": [start_date], "end_date": [end_date], \
                           "interval_start_date": [interval_start_date], \
                           "interval_end_date": [interval_end_date]})
        return df

    def create_time_series_data(self, start_date, end_date, interval):
        start_date_new = start_date.replace(second=0, microsecond=0)
        start_date1 = pd.date_range(start_date_new, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date_new, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["interval_end"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'interval_start'}, inplace=True)
        df['ceil'] = np.arange(1, (df.shape[0] + 1), 1)
        df['key'] = 1
        df = df[df["interval_end"] == df["interval_end"]]
        return df

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["interval_end"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'interval_start'}, inplace=True)
        df = df[df["interval_end"] == df["interval_end"]]
        return df

    def calc_uom_quanity(self, x):
        if x['uom_quantity_int']:
            val = x['uom_quantity_int']
        elif x['uom_quantity']:
            val = x['uom_quantity']
        else:
            val = x['value']
        if not pd.isna(val):
            return int(val)
        else:
            return 0

    def calc_flowevent_quanity(self, x):
        try:
            if x['quantity_int']:
                val = x['quantity_int']
            elif x['quantity']:
                val = x['quantity']
            else:
                val = 0
        except:
            val = x['quantity']
        return val

    def ceil_data(self, x):
        diff = (x['time'].tz_convert(tz=pytz.UTC) - x['interval_start_date'].tz_convert(
            tz=pytz.UTC)).total_seconds() / 60.0
        # diff = (x['time']-x['interval_start_date'].tz_localize(tz=pytz.UTC)).total_seconds() / 60.0
        # diff= self.value_in_sec(x['time'],x['interval_start_date']).total_seconds() / 60.0
        val = math.ceil((diff) / 3)
        if val == 0:
            val = 1
        return val

    def date_compare(self, x):
        diff = (x['start'].tz_convert(tz=pytz.UTC) - x['interval_start'].tz_localize(tz=pytz.UTC)).total_seconds()
        # diff = (x['start'].tz_localize(tz=pytz.UTC)-x['interval_start'].tz_localize(tz=pytz.UTC)).total_seconds()
        if diff <= 0:
            return True
        return False

    def upper_shift_data(self, df):
        df['event_name_upper'] = df['event_name'].shift(-1)
        df['pps_id_upper'] = df['pps_id'].shift(-1)
        df['installation_id_upper'] = df['installation_id'].shift(-1)
        return df

    def lower_shift_data(self, df):
        df['event_name_lower'] = df['event_name'].shift(+1)
        df['pps_id_lower'] = df['pps_id'].shift(+1)
        df['installation_id_lower'] = df['installation_id'].shift(+1)
        # df['startdate_lower'] = df['startdate'].shift(-1)
        return df

    def lower_shift_data_list(self, df, collist, shift_pos=1):
        for col in collist:
            lower_col = col + '_lower'
            df[lower_col] = df[col].shift(+shift_pos)
        return df

    def upper_shift_data_list(self, df, collist, shift_pos=1):
        for col in collist:
            lower_col = col + '_upper'
            df[lower_col] = df[col].shift(-shift_pos)
        return df

    def divide_slotref(self, df):
        df[['rack', 'face', 'slots', 'info4', 'info5']] = df['slotref'].str.split('.', expand=True)
        return df

    def update_station_type(self, df, ttp_setup):
        station = 'ttp' if ttp_setup else 'rtp'
        if 'station_type' not in df.columns:
            df['station_type'] = station

        df['station_type'] = df['station_type'].fillna(method='ffill')
        df['station_type'] = df['station_type'].apply(lambda x: station if pd.isna(x) else x)
        return df

    def update_storage_from_station(self, station, storage):
        if pd.isna(storage):
            if station == 'ttp':
                return 'tsu'
            else:
                return 'msu'
        else:
            return storage

    def update_storage_type(self, df, ttp_setup):
        storage = 'tsu' if ttp_setup else 'msu'
        if 'storage_type' not in df.columns:
            if 'storagetype' not in df.columns:
                df['storage_type'] = storage
            else:
                df.rename(columns={'storagetype': 'storage_type'}, inplace=True)

        if 'storagetype' in df.columns:
            df['storage_type'] = df.apply(
                lambda x: x['storagetype'] if pd.isna(x['storage_type']) else x['storage_type'], axis=1)

        df['storage_type'] = df['storage_type'].fillna(method='ffill')
        df['storage_type'] = df.apply(lambda x: self.update_storage_from_station(x['station_type'], x['storage_type']),
                                      axis=1)
        return df
    
    def create_cols_in_df(self, df, cols):
        for col in cols:
            df[col] = np.nan
        return df

    def str_typecast(self,df,cols):
        for col in cols:
            if col in df.columns:
                df[col] = df[col].astype(str, errors='ignore')
        return df

    def float_typecast(self,df,cols):
        for col in cols:
            if col in df.columns:
                df[col] = df[col].astype(float, errors='ignore')
        return df
    
    def handle_multiple_dtype(self, client, res, table, field):
        q = f"SHOW FIELD KEYS FROM {table}"
        df = pd.DataFrame(client.query(q).get_points())
        if not df.empty:
            field_type = df[df['fieldKey'] == field].reset_index()
            if not field_type.empty:
                field_type = field_type['fieldType'][0]
                if field_type=='float':
                    res[field] = res[field].astype(float)
                elif field_type=='integer':
                    res[field] = res[field].astype(int)
                else:
                    res[field] = res[field].astype(str)
        return res


    def create_unique_id(self, df):
        df = df.reset_index().set_index('pps_id')
        df['unique_id'] = df.index.get_level_values('pps_id').astype(str) + '_' + df['rack'] + '.' + df['face']
        return df

    def value_in_millisec(self, date1, date2):
        if pd.isna(date1) or pd.isna(date2):
            return None
        else:
            diff = (pd.to_datetime(date1) - (pd.to_datetime(date2)))
            if pd.isna(diff):
                return None
            else:
                return round((pd.to_datetime(date1) - (pd.to_datetime(date2))).total_seconds() * 1000)

    def value_in_sec(self, date1, date2):
        if pd.isna(date1) or pd.isna(date2):
            return None
        else:
            diff = (pd.to_datetime(date1) - (pd.to_datetime(date2)))
            if pd.isna(diff):
                return None
            else:
                return round((pd.to_datetime(date1) - (pd.to_datetime(date2))).total_seconds())

    def datediff_in_millisec(self, df, firstColumn, secondColumn, newfieldname):
        df[newfieldname] = df.apply(lambda x: self.value_in_millisec(x[firstColumn], x[secondColumn]) if pd.isna(
            x[newfieldname]) and not pd.isna(x[firstColumn]) and not pd.isna(x[secondColumn]) else x[newfieldname],
                                    axis=1)
        return df

    def datediff_in_sec(self, df, firstColumn, secondColumn, newfieldname):
        df[newfieldname] = df.apply(
            lambda x: self.value_in_sec(x[firstColumn], x[secondColumn]) if pd.isna(x[newfieldname]) and not pd.isna(
                x[firstColumn]) and not pd.isna(x[secondColumn]) else x[newfieldname], axis=1)
        return df

    def get_epoch_time(self, df, date1):
        x = float(pd.to_datetime(date1).strftime('%s'))
        df = df.reset_index()
        df["time_ns"] = x
        df['row_num'] = df.index.tolist()
        df['time_ns'] = df['row_num'].apply(lambda idx: datetime.fromtimestamp(idx * 0.00001 + x))
        del df['row_num']
        return df

    def create_event_name_unique(self, df):
        df["event_name_unique"] = df['event_name'].replace(
            ['pick_front_no_free_bin', 'first_item_pick', 'more_items', 'empty', 'msu_wait', 'system_idle'],
            [1, 2, 3, 4, 5, 6])
        df = df[df["event_name_unique"].isin([1, 2, 3, 4, 5, 6])]
        return df

    def merge_dataframes(self, df1, df2):
        df_merge = pd.concat([df1, df2], ignore_index=True)
        return df_merge

    def update_dag_last_run(self, query, update_values):
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        collection_name = 'last_dag_run'
        mongo_db_client = MongoDBManager(connection_string, database_name, collection_name)
        mongo_db_client.update_data(query, update_values)
        mongo_db_client.close_connection()

    def parse_timestamp(self,ts):
        try:
            if str(ts).isdigit():
                return pd.to_datetime(int(ts), unit='ms').strftime('%Y-%m-%d %H:%M:%S')
            else:
                return pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Failed to parse: {ts}  with exception — {e}")
            return ''
    
    def convert_to_nanosec(self, timestamp):
        base = timestamp.strftime('%Y-%m-%dT%H:%M:%S.')
        micros = str(timestamp.microsecond).zfill(6)
        nanos = str(timestamp.nanosecond).zfill(3)
        return f"{base}{micros}{nanos}Z"

    def reset_index(selfs, df):
        if 'level_0' in df.columns:
            del df['level_0']
        df = df.reset_index()
        return df

    def get_timezoneinfodata(self, timez):
        if timez == 'JST':
            h = 9
            m = 0
        elif timez == 'EDT':
            h = -5
            m = 0
        elif timez == 'EST':
            h = -4
            m = 0
        elif timez == 'IST':
            h = 5
            m = 30
        elif timez == 'ACT':
            h = 10
            m = 0
        elif timez == 'CET':
            h = 2
            m = 0
        elif timez == 'PDT':
            h = -8
            m = 0
        elif timez == 'PST':
            h = -7
            m = 0
        elif timez == 'AST':
            h = -4
            m = 0
        elif timez == 'CST':
            h = -3
            m = 0
        else:
            h = 0
            m = 0
        return (h, m)

    def flow_event(self, df, site_name=''):
        if df.empty:
            return df
        site_name = site_name.upper()
        if 'DISNEY' in site_name:
            df['order_flow'] = df['order_flow_name'].apply(lambda x: 'retail' if 'RETAIL' in x.upper() else 'online')
        elif 'PROJECT' in site_name or 'HNMCANADA' in site_name:
            df['order_flow'] = df['order_flow_name'].apply(lambda x: 'online' if 'ONLINE' in x.upper() else 'None')
            df['order_flow'] = df.apply(
                lambda x: 'retail' if 'GROUP' in x['order_id'].upper() and 'TOTE' not in x['uom_requested'].upper() and
                                      x['order_flow'] == 'None' else x['order_flow'], axis=1)
            df = df[(df['order_flow'] != 'None')]
        elif 'SECTOR' in site_name or 'ADIDAS' in site_name:
            df['order_flow'] = df['order_flow_name'].apply(lambda x: 'Sector6' if 'SECTOR6' in x.upper() else x)
            df['order_flow'] = df.apply(
                lambda x: 'Sector5' if 'SECTOR5' in x['order_flow_name'].upper() else x['order_flow'], axis=1)
            df['order_flow'] = df.apply(
                lambda x: 'Sector4' if 'SECTOR4' in x['order_flow_name'].upper() else x['order_flow'], axis=1)
            df['order_flow'] = df.apply(
                lambda x: 'Sector3' if 'SECTOR3' in x['order_flow_name'].upper() else x['order_flow'], axis=1)
            df['order_flow'] = df.apply(
                lambda x: 'Sector2' if 'SECTOR2' in x['order_flow_name'].upper() else x['order_flow'], axis=1)
            df['order_flow'] = df.apply(
                lambda x: 'Sector1' if 'SECTOR1' in x['order_flow_name'].upper() else x['order_flow'], axis=1)
        elif 'MSIO' in site_name:
            df['order_flow'] = df['order_flow_name'].apply(lambda x: 'non-msio' if 'SINGLE' in x.upper() else 'msio')
        elif 'XPO_HNM' in site_name:
            df['order_flow'] = df['order_flow_name'].apply(lambda
                                                               x: 'online' if 'ONLINE' in x.upper() or 'SECTOR5' in x.upper() or 'SECTOR6' in x.upper() or 'EXPEDITE' in x.upper() or 'AGED' in x.upper() or 'DEFAULTE' in x.upper() or 'SINGLE' in x.upper() else 'retail')
        else:
            df['order_flow'] = df['order_flow_name'].apply(lambda
                                                               x: 'online' if 'ONLINE' in x.upper() or 'SECTOR7' in x.upper() or 'SECTOR8' in x.upper() else 'retail')

        return df

    def flow_event_for_profile(self, df, site_name=''):
        if df.empty:
            return df
        site_name = site_name.upper()
        if 'DISNEY' in site_name:
            df['order_flow'] = df['profile'].apply(lambda x: 'retail' if 'RETAIL' in x.upper() else 'online')
        elif 'ADIDAS' in site_name:
            df['order_flow'] = df['profile']
        elif 'XPO_HNM' in site_name:
            df['order_flow'] = df['profile'].apply(lambda
                                                       x: 'online' if 'ONLINE' in x.upper() or 'SECTOR 5' in x.upper() or 'SECTOR 6' in x.upper() or 'SECTOR-5' in x.upper() or 'SECTOR-6' in x.upper() else 'retail')
            df['order_flow'] = df.apply(lambda x: 'default' if 'DEFAULT' in x['profile'].upper() else x['order_flow'],
                                        axis=1)
            df['order_flow'] = df.apply(
                lambda x: 'ecom' if x['order_flow'].upper() == 'RETAIL' and 'ECOM' in x['profile'].upper() else x[
                    'order_flow'], axis=1)
        elif 'PROJECT' in site_name:
            df['order_flow'] = df['mtu_type_id'].apply(lambda x: 'online' if x.upper() == 'MTU_2' else 'retail')
        elif 'HNMCANADA' in site_name:
            df['order_flow'] = df['mtu_type_id'].apply(lambda x: 'online' if 'ONLINE' in x.upper() else 'retail')
        del df['mtu_type_id']
        return df

    def get_all_site_data_config(self):
        df_sheet = self.get_all_site_data_config_sheet()
        csvReader = df_sheet.to_dict('records')
        return csvReader

    def get_all_site_data_config_sheet(self):  # multitenant
        try:
            gcs = GCS();
            if HOST not in ['sl12-airflow-onprem', 'Airflow_10_11_9_10']:
                gcs.download_from_bucket(GOOGLE_BUCKET_NAME, GOOGLE_TENANTS_FILE, GOOGLE_TENANTS_FILE_PATH)
            if os.path.isfile(GOOGLE_TENANTS_FILE_PATH):
                df_sheet = pd.read_csv(GOOGLE_TENANTS_FILE_PATH)
            else:
                gcs.download_from_bucket(GOOGLE_BUCKET_NAME, GOOGLE_TENANTS_FILE, GOOGLE_TENANTS_FILE_PATH)
                if os.path.isfile(GOOGLE_TENANTS_FILE_PATH):
                    df_sheet = pd.read_csv(GOOGLE_TENANTS_FILE_PATH)
                else:
                    df_sheet = self.refresh_airflow_setup_sheet()
        except Exception as ex:
            print(ex)
            df_sheet = pd.read_csv(GOOGLE_TENANTS_FILE_PATH)
        return df_sheet

    def get_sheet_from_airflow_setup_excel(self, sheet):
        try:
            # self.refresh_airflow_setup_sheet()
            # self.gcs.download_from_bucket(GOOGLE_BUCKET_NAME, AIRFLOW_SETUP_SHEET_FILE, AIRFLOW_SETUP_SHEET_FILE_PATH)
            if os.path.isfile(AIRFLOW_SETUP_SHEET_FILE_PATH):
                df_sheet = pd.read_excel(AIRFLOW_SETUP_SHEET_FILE_PATH, sheet_name=sheet)
            else:
                self.refresh_airflow_setup_sheet()
                time.sleep(10)
                df_sheet = pd.read_excel(AIRFLOW_SETUP_SHEET_FILE_PATH, sheet_name=sheet)
        except Exception as ex:
            print(ex)
            df_sheet = pd.read_excel(AIRFLOW_SETUP_SHEET_FILE_PATH, sheet_name=sheet)
        return df_sheet

    def save_sheet1_to_csv(self):
        try:
            host_name = os.environ.get('GM_WORKER_NAME', 'dev')
            api = f"http://{DJANGO_SERVER}/api/tenants/?server={host_name}"
            response = requests.get(api)
            data = response.json()
            df_sheet = pd.DataFrame(data)
            # df_sheet = df_sheet[(df_sheet["Hostname"] == host_name)].reset_index()
            df_sheet["Postgres_butler_port"] = df_sheet["Postgres_butler_port"].apply(
                lambda x: 5432 if pd.isna(x) or x == '' else int(x))
            df_sheet["influx_port"] = df_sheet["influx_port"].fillna(8086)
            df_sheet["elastic_port"] = df_sheet["elastic_port"].fillna(9200)
            df_sheet["write_influx_port"] = df_sheet["write_influx_port"].fillna(8086)
            df_sheet["Postgres_pf_port"] = df_sheet["Postgres_pf_port"].apply(
                lambda x: 5432 if pd.isna(x) or x == '' else int(x))
            df_sheet["Postgres_tower_port"] = df_sheet["Postgres_tower_port"].apply(
                lambda x: 5432 if pd.isna(x) or x == '' else int(x))
            df_sheet["Butler_port"] = df_sheet["Butler_port"].apply(lambda x: 8181 if pd.isna(x) or x == '' else int(x))
            df_sheet["Postgres_butler_port"] = df_sheet["Postgres_butler_port"].astype(int)
            df_sheet["influx_port"] = df_sheet["influx_port"].astype(int)
            df_sheet["elastic_port"] = df_sheet["elastic_port"].astype(int)
            df_sheet["write_influx_port"] = df_sheet["write_influx_port"].astype(int)
            df_sheet["Postgres_pf_port"] = df_sheet["Postgres_pf_port"].astype(int)
            df_sheet["Postgres_tower_port"] = df_sheet["Postgres_tower_port"].astype(int)
            df_sheet["Butler_port"] = df_sheet["Butler_port"].astype(int)
            df_sheet["Butler_interface_port"] = df_sheet["Butler_interface_port"].astype(int)
            df_sheet.to_csv(GOOGLE_TENANTS_FILE_PATH, index=False, header=True)
            self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, GOOGLE_TENANTS_FILE_PATH, GOOGLE_TENANTS_FILE)

        except Exception as ex:
            print(f"unable to save gtenant file: {ex}")

    def save_worksheet_from_api(self, worksheet_name):
        api = f"http://{DJANGO_SERVER}/api/insight_suggestion"
        response = requests.get(api)
        data = response.json()
        df_sheet = pd.DataFrame(data)
        df_sheet = df_sheet.fillna('')
        try:
            with pd.ExcelWriter(AIRFLOW_SETUP_SHEET_FILE_PATH, engine='openpyxl', mode='a',
                                if_sheet_exists='replace') as writer:
                df_sheet.to_excel(writer, sheet_name=worksheet_name, index=False)
        except FileNotFoundError:
            print(f"Unable to save {worksheet_name} file to .xlsx")

    def refresh_airflow_setup_sheet(self):
        SHEET_ID = '1qpqYrz1aUniuoXVvjl_I5G9bgEb1uvZH4V4FCJU1AMw'
        gc = gspread.service_account(GOOGLE_AUTH_JSON)
        self.gcs = GCS()
        self.save_sheet1_to_csv()
        spreadsheet = gc.open_by_key(SHEET_ID)
        worksheets = spreadsheet.worksheets()
        for worksheet in worksheets:
            data = worksheet.get_all_values()
            df_sheet = pd.DataFrame(data)
            expected_headers = df_sheet.iloc[0]
            df_sheet.columns = expected_headers
            df_sheet = df_sheet.drop([0])
            df_sheet = df_sheet.reset_index()
            if worksheet.title == 'insight_suggestion':
                self.save_worksheet_from_api(worksheet.title)
            else:
                try:
                    with pd.ExcelWriter(AIRFLOW_SETUP_SHEET_FILE_PATH, engine='openpyxl', mode='a',
                                        if_sheet_exists='replace') as writer:
                        df_sheet.to_excel(writer, sheet_name=worksheet.title, index=False)
                except FileNotFoundError:
                    with pd.ExcelWriter(AIRFLOW_SETUP_SHEET_FILE_PATH, engine='openpyxl') as writer:
                        df_sheet.to_excel(writer, sheet_name=worksheet.title, index=False)
        self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, AIRFLOW_SETUP_SHEET_FILE_PATH, AIRFLOW_SETUP_SHEET_FILE)

    def get_hardware_sheet_data(self):
        try:
            self.gcs.download_from_bucket(GOOGLE_BUCKET_NAME, GLOBAL_SITE_HARDWARE_FILE, GLOBAL_SITE_HARDWARE_FILE_PATH)
            if os.path.isfile(GLOBAL_SITE_HARDWARE_FILE_PATH):
                df_sheet = pd.read_csv(GLOBAL_SITE_HARDWARE_FILE_PATH)
            else:
                df_sheet = self.refresh_site_hardware_detail()
        except Exception as ex:
            print(ex)
            df_sheet = pd.read_csv(GLOBAL_SITE_HARDWARE_FILE_PATH)
        return df_sheet

    def refresh_site_hardware_detail(self):
        SHEET_ID = '1qpiibVAT8xZ3LLS7sP8xQ6h1t4FY2B0eX8oOb_F9jS0'
        SHEET_NAME = 'Site Hardware_RTP'
        gc = gspread.service_account(GOOGLE_AUTH_JSON)
        spreadsheet = gc.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_values()
        df_sheet = pd.DataFrame(data)
        expected_headers = df_sheet.iloc[1]
        df_sheet.columns = expected_headers
        df_sheet = df_sheet.drop([0, 1])
        df_sheet = df_sheet[['SiteName used in Airflow', 'Spare Bots', 'Total Chargers']]
        df_sheet = df_sheet[(df_sheet['SiteName used in Airflow'] != '')]
        df_sheet = df_sheet[(pd.isna(df_sheet['SiteName used in Airflow']) == False)]
        df_sheet = df_sheet.reset_index()
        df_sheet = df_sheet[['SiteName used in Airflow', 'Spare Bots', 'Total Chargers']]
        df_sheet['Spare Bots'] = df_sheet['Spare Bots'].apply(lambda x: '0' if x == '' else x)
        df_sheet['Total Chargers'] = df_sheet['Total Chargers'].apply(lambda x: '0' if x == '' else x)
        df_sheet.to_csv(GLOBAL_SITE_HARDWARE_FILE_PATH, index=False, header=True)
        self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, GLOBAL_SITE_HARDWARE_FILE_PATH, GLOBAL_SITE_HARDWARE_FILE)
        return df_sheet

    def get_error_data_flag_sheet(self):
        try:
            # self.gcs.download_from_bucket(GOOGLE_BUCKET_NAME, GOOGLE_ERROR_FILE, GOOGLE_ERROR_FILE_PATH)
            if os.path.isfile(GOOGLE_ERROR_FILE_PATH):
                df_sheet = pd.read_csv(GOOGLE_ERROR_FILE_PATH)
            else:
                self.gcs.download_from_bucket(GOOGLE_BUCKET_NAME, GOOGLE_ERROR_FILE, GOOGLE_ERROR_FILE_PATH)
                time.sleep(10)
                if not os.path.isfile(GOOGLE_ERROR_FILE_PATH):
                    df_sheet = self.refresh_error_data_flag_sheet()
                else:
                    df_sheet = pd.read_csv(GOOGLE_ERROR_FILE_PATH)
        except Exception as ex:
            print(ex)
            df_sheet = pd.read_csv(GOOGLE_ERROR_FILE_PATH)
        return df_sheet

    def refresh_error_data_flag_sheet(self):
        SHEET_ID = '1zxuoKDL8VG51B-8DLTK8TK4eY3A6VEQoaEdP1gK9vMo'
        SHEET_NAME = 'Error Flag Details'
        gc = gspread.service_account(GOOGLE_AUTH_JSON)
        spreadsheet = gc.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        rows = worksheet.get_all_values()
        df_sheet = pd.DataFrame(rows)
        expected_headers = df_sheet.iloc[1]
        df_sheet.columns = expected_headers
        df_sheet = df_sheet[(df_sheet['L1'] != '')]
        df_sheet = df_sheet[(pd.isna(df_sheet['L1']) == False)]
        df_sheet.to_csv(GOOGLE_ERROR_FILE_PATH, index=False, header=True)
        self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, GOOGLE_ERROR_FILE_PATH, GOOGLE_ERROR_FILE)
        return df_sheet

    def get_Warehouse_Manager_sheet(self):
        try:
            t = datetime.now(timezone.utc)
            flag = False
            if os.path.isfile(GOOGLE_Warehouse_Manager_FILE_PATH):
                t1 = os.path.getmtime(GOOGLE_Warehouse_Manager_FILE_PATH)
                t1 = datetime.datetime.fromtimestamp(t1)
                flag = True
            else:
                t1 = t

            if not (t1.day == t.day and flag):
                SHEET_ID = '1BXMBCv7FN7-C1tHATkpvFUUlb-fOf_dr6VQj3ul-zsw'
                SHEET_NAME = 'site_mappings'
                gc = gspread.service_account(GOOGLE_AUTH_JSON)
                spreadsheet = gc.open_by_key(SHEET_ID)
                worksheet = spreadsheet.worksheet(SHEET_NAME)
                rows = worksheet.get_all_values()
                df_sheet = pd.DataFrame(rows)
                expected_headers = df_sheet.iloc[1]
                df_sheet.columns = expected_headers
                df_sheet = df_sheet[(df_sheet['excel_names'] != '')]
                df_sheet = df_sheet[(pd.isna(df_sheet['excel_names']) == False)]
                df_sheet.to_csv(GOOGLE_ERROR_FILE_PATH, index=False, header=True)
            else:
                df_sheet = pd.read_csv(GOOGLE_Warehouse_Manager_FILE_PATH)
        except Exception as ex:
            print(ex)
            df_sheet = pd.read_csv(GOOGLE_Warehouse_Manager_FILE_PATH)
        return df_sheet

    def get_Warehouse_Manager_sheet_SiteOpshours(self):
        try:
            t = datetime.now(timezone.utc)
            flag = False
            if os.path.isfile(GOOGLE_Warehouse_Manager_site_ops_houe_FILE_PATH):
                t1 = os.path.getmtime(GOOGLE_Warehouse_Manager_site_ops_houe_FILE_PATH)
                t1 = datetime.datetime.fromtimestamp(t1)
                flag = True
            else:
                t1 = t

            if not (t1.day == t.day and flag):
                SHEET_ID = '1BXMBCv7FN7-C1tHATkpvFUUlb-fOf_dr6VQj3ul-zsw'
                SHEET_NAME = 'Site Ops hours'
                gc = gspread.service_account(GOOGLE_AUTH_JSON)
                spreadsheet = gc.open_by_key(SHEET_ID)
                worksheet = spreadsheet.worksheet(SHEET_NAME)
                rows = worksheet.get_all_values()
                df_sheet = pd.DataFrame(rows)
                expected_headers = df_sheet.iloc[1]
                df_sheet.columns = expected_headers
                df_sheet = df_sheet[(df_sheet['excel_names'] != '')]
                df_sheet = df_sheet[(pd.isna(df_sheet['excel_names']) == False)]
                df_sheet.to_csv(GOOGLE_ERROR_FILE_PATH, index=False, header=True)
            else:
                df_sheet = pd.read_csv(GOOGLE_Warehouse_Manager_site_ops_houe_FILE_PATH)
        except Exception as ex:
            print(ex)
            df_sheet = pd.read_csv(GOOGLE_Warehouse_Manager_site_ops_houe_FILE_PATH)
        return df_sheet

    def get_default_args_for_dag(self):
        default_args = {
            'owner': 'airflow',
            'start_date': datetime.strptime('04/23/23 13:55:26', '%m/%d/%y %H:%M:%S'),
            'depends_on_past': False,
            'email': ['ankush.j@greyorange.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'on_failure_callback': log_dag_failure,
            'dagrun_timeout': timedelta(hours=2)
        }
        return default_args

    def get_tenant_info(self):
        StartTime, TimeZone = self.get_start_time(Site)
        tenant = {
            "Name": Site,
            "Butler_ip": Butler_ip,
            "Postgres_ButlerDev": Postgres_ButlerDev,
            "influx_ip": influx_ip,
            "influx_port": influx_port,
            "elastic_ip": elastic_ip,
            "elastic_port": elastic_port,
            "write_influx_ip": write_influx_ip,
            "write_influx_port": Write_influx_port,
            "out_db_name": db_name,
            "out_gmc_db_name": gmc_db_name,
            "TimeZone": TimeZone,
            "StartTime": StartTime,
            "Postgres_pf": Postgres_pf,
            "Postgres_pf_port": Postgres_pf_port,
            "Postgres_pf_password": Postgres_pf_password,
            "Postgres_pf_user": Postgres_pf_user,
            "Postgres_tower": Postgres_tower,
            "Postgres_tower_port": Postgres_tower_port,
            "Tower_user": Tower_user,
            "Tower_password": Tower_password,
            "sslrootcert": sslrootcert,
            "sslcert": sslcert,
            "sslkey": sslkey,
            "Postgres_butler_port": Postgres_butler_port,
            "Postgres_butler_password": Postgres_butler_password,
            "Postgres_butler_user": Postgres_butler_user,
            "streaming": streaming,
            "alteryx_out_db_name": alteryx_out_db_name,
            "Butler_port": int(Butler_port),
            "Butler_interface_port": int(Butler_interface_port),
            "is_ttp_setup": is_ttp_setup
        }

        return tenant

    def get_site_read_influx_client(self, site, database, timeout=300):
        influx_config = {
            "host": site.get('influx_ip'),
            "port": site.get('influx_port'),
            "database": database,
            "retries": 2,
            "timeout": timeout,
        }
        return InfluxDataV2(**influx_config)

    def get_site_write_influx_client(self, site, database=None):
        if not database:
            database = site.get('out_db_name')

        influx_config = {
            "host": site.get('write_influx_ip'),
            "port": int(site.get('write_influx_port')),
            "database": database,
        }
        return InfluxDataV2(**influx_config)

    def get_central_influx_client(self, database, host=None, timeout=180):
        return InfluxDataV2(
            host=host if host else 'alteryx-influx.greymatter.greyorange.com',
            port=8086,
            username=CENTRAL_INFLUX_USERNAME,
            password=CENTRAL_INFLUX_PASSWORD,
            ssl=True,
            database=database,
            retries=2,
            timeout=timeout
        )

    def get_start_time(self, Site):
        data = [['cnc9', 'CST', '7:00:00'],
                ['Simulation', 'CST', '7:00:00'],
                ['Coupang', 'JST', '15:00:00'],
                ['Goldbond', 'CET', '22:00:00'],
                ['HnMCanada', 'EST', '4:00:00'],
                ['JYSK', 'CET', '22:00:00'],
                ['Macy', 'EST', '4:00:00'],
                ['Sams_Club', 'PDT', '7:00:00'],
                ['SBS', 'EDT', '5:00:00'],
                ['Sodimac_Columbia', 'EDT', '5:00:00'],
                ['Sodimac_CnC', 'EDT', '4:00:00'],
                ['Sodimac_CnC_florida', 'EDT', '4:00:00'],
                ['sodimac_cnc3', 'EDT', '4:00:00'],
                ['Sodimac_CnC5', 'EDT', '4:00:00'],
                ['Sodimac_CnC4', 'EDT', '4:00:00'],
                ['Sodimac_CnC7', 'EDT', '4:00:00'],
                ['Sodimac_CnC8', 'EDT', '4:00:00'],
                ['Sodimac_CnC9', 'EDT', '4:00:00']]
        df = pd.DataFrame(data, columns=['site', 'TimeZone', 'StartTime'])
        if Site in df['site'].values:
            site_data = df[df['site'] == Site][['TimeZone', 'StartTime']]
            return site_data['TimeZone'].values[0], site_data['StartTime'].values[0]
        else:
            return 'PST', '7:00:00'


class GCS:
    def __init__(self, host='localhost', port='9200', dag_name="", site_name=""):
        self.gcs_client = storage.Client.from_service_account_json(GOOGLE_BUCKET_AUTH_JSON)

    def upload_to_bucket(self, bucket_name, source_file_name, destination_blob_name):
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")

    def download_from_bucket(self, bucket_name, source_blob_name, destination_file_name):
        try:
            bucket = self.gcs_client.bucket(bucket_name)
            blob = bucket.get_blob(source_blob_name)
            if blob!=None:
                blob.download_to_filename(destination_file_name)
                print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")
        except:
            print(f"unable to download file Blob {source_blob_name}:{destination_file_name}")


class postgres_connection:
    def __init__(self, *args, **kwargs):
        self.dag_name = kwargs.get('dag_name', '')
        self.site_name = kwargs.get('site_name', '')
        if 'retries' not in kwargs:
            kwargs['retries'] = 0
        try:
            if not pd.isna(kwargs.get('sslrootcert')):
                self.sslrootcert = os.path.join(CONFFILES_DIR, kwargs.get('sslrootcert'))
                self.sslcert = os.path.join(CONFFILES_DIR, kwargs.get('sslcert'))
                self.sslkey = os.path.join(CONFFILES_DIR, kwargs.get('sslkey'))
                self.dbname = kwargs.get('database')
                self.user = kwargs.get('user')
                self.host = kwargs.get('host')
                self.port = kwargs.get('port')
                self.password = kwargs.get('password')
                self.pg_conn = psycopg2.connect(dbname=self.dbname,
                                                user=self.user,
                                                sslrootcert=self.sslrootcert,
                                                sslcert=self.sslcert,
                                                sslkey=self.sslkey,
                                                host=self.host,
                                                port=self.port,
                                                password=self.password)
                self.pg_curr = self.pg_conn.cursor()
            else:
                self.sslrootcert = None
                self.sslcert = None
                self.sslkey = None
                self.dbname = kwargs.get('database')
                self.user = kwargs.get('user')
                self.host = kwargs.get('host')
                self.port = kwargs.get('port')
                self.password = kwargs.get('password')
                self.pg_conn = psycopg2.connect(user=self.user,
                                                password=self.password,
                                                host=self.host,
                                                port=self.port,
                                                database=self.dbname)
                self.pg_curr = self.pg_conn.cursor()
        except Exception as ex:
            print(ex)
            if kwargs['retries'] < 3:
                kwargs['retries'] += 1
                print("retrying after 1 min...")
                time.sleep(60)
                postgres_connection(*args, **kwargs)
            else:
                error_message = str(ex)
                if 'data_tote' not in error_message:
                    log_failures('db_connection_failure', self.dag_name, self.site_name, "postgres",
                             error_message)
                raise DbConnectionError("[Postgres not connected] " + error_message)

    def close(self):
        if self.pg_curr:
            self.pg_curr.close()

        if self.pg_conn:
            self.pg_conn.close()

    def create_new_db_connection(self, retries=0):
        try:
            print("create_new_db_connection")
            print(self.port)
            if not pd.isna(self.sslrootcert):
                self.pg_conn = psycopg2.connect(dbname=self.dbname,
                                                user=self.user,
                                                sslrootcert=self.sslrootcert,
                                                sslcert=self.sslcert,
                                                sslkey=self.sslkey,
                                                host=self.host,
                                                port=self.port,
                                                password=self.password)
                self.pg_curr = self.pg_conn.cursor()
            else:
                self.pg_conn = psycopg2.connect(user=self.user,
                                                password=self.password,
                                                host=self.host,
                                                port=self.port,
                                                database=self.dbname)
                self.pg_curr = self.pg_conn.cursor()
        except airflow.exceptions.AirflowTaskTimeout:
            raise
        except Exception as ex:
            print(ex)
            if retries < 3:
                retries += 1
                print("retrying after 1 min...")
                time.sleep(60)
                self.create_new_db_connection(retries=retries + 1)
            else:
                error_message = str(ex)
                raise DbConnectionError("[Postgres not connected] " + error_message)

    def query_exec(self, query, retries=0):
        try:
            self.pg_conn.execute(query)
        except airflow.exceptions.AirflowTaskTimeout:
            raise
        except Exception as ex:
            if retries < 3:
                time.sleep(60)
                self.query_exec(query, retries=retries + 1)
            else:
                raise ValueError(str(ex))

    def fetch_postgres_data_in_chunk(self, query, retries=0, *args, **kwargs):
        try:
            self.pg_curr.execute(query)
            output = self.pg_curr.fetchmany(size=2000)
            while (True):
                records = self.pg_curr.fetchmany(size=2000)
                if not records:
                    break
                output = output + records
            self.pg_curr.close()
            return output
        except airflow.exceptions.AirflowTaskTimeout:
            raise
        except Exception as ex:
            if retries < 3:
                time.sleep(60)
                print(ex)
                self.postgres_conn = self.create_new_db_connection()
                return self.fetch_postgres_data_in_chunk(query, retries=retries + 1)
            else:
                error_message = str(ex)
                if 'data_tote' not in error_message:                
                    log_failures('db_connection_failure', self.dag_name, self.site_name, "postgres",
                                    error_message)
                raise DbConnectionError("[Postgres not connected] " + error_message)


class tableau_postgres_setup:
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

    def delete_data(self, conn, start_date, table, site):
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

    def delete_data_old_data(self, conn, day14_old_data, table, site):
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

    def execute_values(self, conn, df, table):
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


class Butler_api:
    def __init__(self, *args, **kwargs):
        self.host_name = kwargs.get('host_name')
        self.butler_ip =kwargs.get('host')
        self.butler_api = "http://" + kwargs.get('host') + ":" + str(kwargs.get('port'))
        self.rack_api = self.butler_api + "/api/v2/racktype_rec"
        self.rack_storage_utilization = self.butler_api + "/rack_storage_utilization/_search?size=100"
        self.rack_volumn_utilization = self.butler_api + "/api/storage_availability"

        self.butleruptime_api = self.butler_api + "/api/v2/butlerinfo"
        self.chargeruptime_api = self.butler_api + "/api/v2/chargerinfo"
        self.ppsinfo_api = self.butler_api + "/api/v2/ppsinfo"
        self.gridinfo_api = self.butler_api + "/api/v2/gridinfo"
        self.storage_info_api = self.butler_api + "/api/v2/storage_info"
        self.tote_storage_api = self.butler_api + "/api/v2/tote_storage"
        self.tote_info_api = self.butler_api + "/api/v2/tote_info"
        self.invetory_api = self.butler_api + "/api/inventory/v2/all?details=true&storage_level=slot"
        self.rack_popularity_api = self.butler_api + "/api/storage/rack/score"
        self.location_category_api = self.butler_api + "/api/mhs/v2/tote_location/popularity/all"
        self.tote_location_mapping_api = self.butler_api + "/api/mhs/entity_location_mapping"
        self.rack_location_popularity_api = self.butler_api + "/api/mhs/storable_location_scores"
        self.item_popularity_api = self.butler_api + "/api/put/v1/item/popularity"

    def fetch_item_popularity_data(self):
        print(self.item_popularity_api)
        item_popularity_reponse = requests.get(self.item_popularity_api)
        print(item_popularity_reponse.status_code)
        if item_popularity_reponse.status_code == 200:
            data = item_popularity_reponse.json()
            item_popularity = pd.DataFrame(data)[['item_id', 'seed_bucket']]
            item_popularity = item_popularity.rename(columns={'seed_bucket': 'popularity_bucket'})
        else:
            print(f"Failed to fetch item popularity data: {item_popularity_reponse.status_code}")
            item_popularity = pd.DataFrame(columns=['item_id', 'popularity_bucket'])
        item_popularity['item_id'] = item_popularity['item_id'].astype(str)
        return item_popularity[['item_id', 'popularity_bucket']]
        
    def fetch_rack_popularity_data(self):
        rack_popularity_reponse = requests.get(self.rack_popularity_api)
        if rack_popularity_reponse.status_code == 200:
            data = rack_popularity_reponse.json()  
            rack_popularity = pd.DataFrame(data)
        else:
            print(f"Failed to fetch rack popularity data: {rack_popularity_reponse.status_code}")
            rack_popularity = pd.DataFrame(columns=['rack_score', 'category', 'rack_id', 'rack_popularity', 'popularity_category'])
        return rack_popularity

    def fetch_rack_location_popularity(self):
        rack_location_popularity_response = requests.get(self.rack_location_popularity_api)
        if rack_location_popularity_response.status_code == 200:
            data = rack_location_popularity_response.json()  
            rack_location_popularity =pd.DataFrame(list(data.items()), columns=['location', 'score'])
        else:
            print(f"Failed to fetch rack popularity data: {rack_location_popularity_response.status_code}")
            rack_location_popularity = pd.DataFrame(columns=['location', 'score'])
        return rack_location_popularity        


    def fetch_tote_location_popularity(self):
        headers = {
            "Content-Type": "application/json"
        }
        location_popularity_reposnse = requests.post(self.location_category_api, headers=headers)
        if location_popularity_reposnse.status_code == 200:
            location_popularity_data = location_popularity_reposnse.json()  
            location_popularity = pd.DataFrame(location_popularity_data['data'])
        else:
            print(f"Failed to fetch location category data: {location_popularity_reposnse.status_code}")
            location_popularity = pd.DataFrame(columns=['category', 'idc', 'io_point', 'location', 'score'])
        return location_popularity

    def fetch_location_mapping_data(self):
        tote_location_mapping_reponse = requests.get(self.tote_location_mapping_api)
        if tote_location_mapping_reponse.status_code == 200:
            data = tote_location_mapping_reponse.json()  
            tote_location_mapping = pd.DataFrame(data['data'])
        else:
            print(f"Failed to fetch tote location mapping data: {tote_location_mapping_reponse.status_code}")
            tote_location_mapping = pd.DataFrame(columns=['location','tote_id'])
        return tote_location_mapping    

    def fetch_rack_data(self):
        rack_info_req = requests.get(self.rack_api)
        self.rack_data = rack_info_req.json()

    def fetch_rack_storage_utilization(self):
        rack_info_req = requests.get(self.rack_storage_utilization)
        self.rack_storage_utilization_data = rack_info_req.json()

    def fetch_rack_volume_utilization(self):
        rack_info_req = requests.get(self.rack_volumn_utilization)
        self.rack_volumn_utilization_data = rack_info_req.json()

    def fetch_butleruptime_data(self):
        butler_data_req = requests.get(self.butleruptime_api, timeout=30)
        butler_data = butler_data_req.json()
        json_butler_data = []
        if 'butlerinfo_list' in butler_data:
            butler_data = butler_data['butlerinfo_list']
        for butler in butler_data:
            indo = {
                "measurement": "butler_uptime",
                "tags": {
                    "bot_id": butler['id'],
                },
                "time": time.strftime("%Y-%m-%dT%X"),
                "fields": {
                    "Butler_ip": self.butler_ip ,
                    "tasktype": butler['tasktype'],
                    "power_state": butler['state'],
                    "bot_id_int": butler['id'],
                    "manual_paused": butler['pause_status']['manual'],
                    "low_charged_paused": butler['pause_status']['low_charge'],
                    "nav_status_state": butler['navstatus'],
                    "bot_state": butler['status'],
                    "address": butler['address'],
                    "category": butler['category'] or butler.get('version') or None
                }
            }
            json_butler_data.append(indo)
        return json_butler_data

    def fetch_chargeruptime_data(self):
        charger_data_req = requests.get(self.chargeruptime_api, timeout=30)
        charger_data = charger_data_req.json()
        json_charger_data = []
        if 'chargerinfo_list' in charger_data:
            charger_data = charger_data['chargerinfo_list']

        for charger in charger_data:
            indo = {
                "measurement": "charger_uptime",
                "tags": {
                    "charger_id": charger['charger_id'],
                },
                "time": time.strftime("%Y-%m-%dT%X"),
                "fields": {
                    "connectivity": charger['status'],
                    "state": charger['mode'],
                    "error_condition": charger['error_reason'],
                }
            }
            json_charger_data.append(indo)
        return json_charger_data

    def fetch_ppsinfo_data(self):
        pps_info_req = requests.get(self.ppsinfo_api)
        json_pps_data = []
        if pps_info_req:
            pps_data = pps_info_req.json()

            if 'ppsinfo_list' in pps_data:
                pps_data = pps_data['ppsinfo_list']

            for i in pps_data:
                indo = {
                    "measurement": "pps_info",
                    "tags": {
                        "pps_id": i['pps_id'],
                    },
                    "time": time.strftime("%Y-%m-%dT%X"),
                    "fields": {
                        "host" :self.host_name,
                        "location": i['location'],
                        "status": i['status'],
                        "queue_barcodes": ','.join(map(str, i['queue_barcodes'])),
                    }
                }
                json_pps_data.append(indo)
        return json_pps_data

    def fetch_gridinfo_data(self, retries=0):
        try:
            grid_info_req = requests.get(self.gridinfo_api)
            json_grid_data = []
            if grid_info_req:
                grid_data = grid_info_req.json()
                if 'gridinfo_list' in grid_data:
                    grid_data = grid_data['gridinfo_list']

                for i in grid_data:
                    if 'floor' in i:
                        indo = {
                            "measurement": "grid_info",
                            "tags": {
                                "barcode": i['barcode'],
                            },
                            "time": time.strftime("%Y-%m-%dT%X"),
                            "fields": {
                                "host": self.host_name,
                                "coordinate": i['coordinate'],
                                "zone": i['zone'],
                                "floor": i['floor']
                            }
                        }
                    else:
                        indo = {
                            "measurement": "grid_info",
                            "tags": {
                                "barcode": i['barcode'],
                            },
                            "time": time.strftime("%Y-%m-%dT%X"),
                            "fields": {
                                "host": self.host_name,
                                "coordinate": i['coordinate'],
                                "zone": i['zone']
                            }
                        }
                    json_grid_data.append(indo)
            return json_grid_data
        except Exception as ex:
            print(ex)
            if retries < 3:
                print("retrying after 1 min .....")
                time.sleep(60)
                return self.fetch_gridinfo_data(retries=retries + 1)
            else:
                raise ex

    def fetch_storageinfo_data(self):
        storage_info_req = requests.get(self.storage_info_api)
        storage_data = storage_info_req.json()
        json_storage_data = []
        if 'storage_info_list' in storage_data:
            storage_data = storage_data['storage_info_list']

        for i in storage_data:
            indo = {
                "measurement": "storage_info",
                "tags": {
                    "rack_id": i['rack_id'],
                },
                "time": time.strftime("%Y-%m-%dT%X"),
                "fields": {
                    "host": self.host_name,
                    "storage_id": i['storage_id'],
                    "storage_status": i['storage_status'],
                }
            }
            json_storage_data.append(indo)
        return json_storage_data

    def fetch_toteinfo_data(self):
        try:
            storage_info_req = requests.get(self.tote_storage_api)
            storage_data = storage_info_req.json()
            json_storage_data = []
            if 'tote_info_list' in storage_data:
                storage_data = storage_data['tote_info_list']

            for i in storage_data:
                indo = {
                    "measurement": "tote_info",
                    "tags": {
                        "rack_id": i['tote_id'],
                    },
                    "time": time.strftime("%Y-%m-%dT%X"),
                    "fields": {
                        "host": self.host_name,
                        "storage_id": str(i['io_point']['coordinate']),
                        "storage_status": i['status'],
                    }
                }
                json_storage_data.append(indo)
            return json_storage_data
        except Exception as ex:
            return {}

    def fetch_toteinfo_data2(self):
        try:
            storage_info_req = requests.get(self.tote_info_api)
            storage_data = storage_info_req.json()
            json_storage_data = []
            if 'tote_info_list' in storage_data:
                storage_data = storage_data['tote_info_list']

            for i in storage_data:
                indo = {
                    "measurement": "tote_info2",
                    "tags": {
                        "rack_id": i['tote_id'],
                    },
                    "time": time.strftime("%Y-%m-%dT%X"),
                    "fields": {
                        "host": self.host_name,
                        "location_type": str(i['location_details']['location_type']),
                        "location": str(i['location_details']['location']),
                        "storage_status": i['status'],
                        "tote_type": i['tote_type'],
                    }
                }
                json_storage_data.append(indo)
            return json_storage_data
        except Exception as ex:
            return {}

    def fetch_chargerinfo_data(self):
        charger_info_req = requests.get(self.chargeruptime_api)
        charger_data = charger_info_req.json()
        json_charger_data = []
        if 'charger_info_list' in charger_data:
            charger_data = charger_data['charger_info_list']

        for i in charger_data:
            indo = {
                "measurement": "charger_info",
                "tags": {
                    "charger_id": i['charger_id'],
                },
                "time": time.strftime("%Y-%m-%dT%X"),
                "fields": {
                    "host": self.host_name,
                    "charger_location": i['charger_location'],
                    "entry_point_location": i['entry_point_location'],
                    "reinit_point_location": i['reinit_point_location']
                }
            }
            json_charger_data.append(indo)
        return json_charger_data

    def fetch_inventoryinfo_data(self):
        inventoryinfo_req = requests.get(self.invetory_api)
        storage_data = inventoryinfo_req.json()
        # Check if JSON is not empty and 'slot' key exists and is not empty
        if storage_data and 'slot' in storage_data and storage_data['slot']:
            df2 = pd.DataFrame(storage_data['slot'])
            if 'tag_values' in df2.columns:
                del df2['tag_values']
            if not df2.empty:
                l = ['product_uid', 'quantity', 'rack_type', 'slot_id', 'type']
                for col in l:
                    if col not in df2.columns:
                        df2[col]=''

            df2 = df2[['product_uid', 'quantity', 'rack_type', 'slot_id', 'type']]
            df2['floor'] = df2['slot_id'].str.split('.', expand=True)[2]
            df2['face'] = df2['slot_id'].str.split('.', expand=True)[1]
            df2_1st = df2.groupby(['slot_id', 'floor', 'rack_type', 'face'],
                                  as_index=False).agg(unique_skus=('product_uid', 'nunique'),
                                                      sku_quanity=('quantity', 'sum')
                                                      )
        else:
            print("JSON is blank or 'slot' key is missing or empty.")
            df2_1st = pd.DataFrame()  # optional: create an empty DataFrame to avoid crashes later

        return df2_1st



class InfluxDataV2:
    """It will return result either in pandas dataframe or in Json format"""

    def __init__(self, **config):
        self.client = DataFrameClient(**config)
        self.dag_name = ''
        self.site_name = ''

    def is_influx_reachable(self, dag_name="", site_name="", retries=0):
        self.site_name = site_name
        self.dag_name = dag_name
        try:
            q = f'SHOW MEASUREMENTS'
            check = pd.DataFrame(self.client.query(q).get_points())
        except airflow.exceptions.AirflowTaskTimeout:
            raise
        except Exception as ex:
            print(ex)
            if retries < 3:
                print("retrying after 1 min...")
                time.sleep(60)
                self.is_influx_reachable(dag_name=dag_name, site_name=site_name, retries=retries + 1)
            else:
                error_message = str(ex)
                log_failures('db_connection_failure', dag_name, site_name, "influxdb", error_message)
                raise DbConnectionError("[InfluxDB not connected] " + error_message)
        return True

    def fetch_all(self, query, format="dataframe", get_all_data=False):
        """Use it when expected more than one rows in result"""
        try:
            result = self.client.query(query, chunked=True, chunk_size=10000)

            if len(result.values()) == 0:
                if format == "dataframe":
                    return pd.DataFrame()
                elif format == "json":
                    return []

            try:
                if get_all_data:
                    return result
                else:   
                    result = list(result.values())[0]
            except:
                if format == "dataframe":
                    return pd.DataFrame()
                elif format == "json":
                    return []

            if format == "dataframe":
                return result
            elif format == "json":
                result['time'] = result.index
                data = json.loads(result.to_json(orient='records'))
                return data

        except Exception as ex:
            print(ex)
            error_message = str(ex)
            log_failures('db_connection_failure', self.dag_name, self.site_name, "influxdb", error_message)
            raise DbConnectionError("[InfluxDB not connected] " + error_message)

    def fetch_one(self, query, format="dataframe"):
        """Use it when expected only one row in result"""
        try:
            result = self.fetch_all(query, format=format)

            if format == "json":
                return result[0] if len(result) != 0 else {}
            return result

        except Exception as ex:
            print(ex)
            error_message = str(ex)
            log_failures('db_connection_failure', self.dag_name, self.site_name, "influxdb", error_message)
            raise DbConnectionError("[InfluxDB not connected] " + error_message)

    def write_all(self, write_data, measurement, format="dataframe", dag_name="", site_name="", retention_policy="autogen", **write_point_kwargs):
        try:
            if format == "dataframe":
                return self.client.write_points(write_data, measurement,retention_policy=retention_policy, **write_point_kwargs)
        except Exception as ex1:
            error_message1 = str(ex1)
            log_failures('influxdb_write_issue', dag_name, site_name, "influxdb", error_message1)
            raise DbWriteError("[InfluxDB write issue] " + error_message1)

    @property
    def query(self):
        return self.client.query


class ExcelConfig:
    def __init__(self, excel_file, sheet):
        self.excel_file = excel_file
        self.sheet = sheet
        self.config_df = self.load_config()

    def load_config(self):
        return pd.read_excel(pd.ExcelFile(self.excel_file), self.sheet)

    def get_excel_config_by_valid_flag(self, valid):
        return json.loads(self.config_df[self.config_df["Valid"] == valid].iloc[0].to_json())

    def get_excel_config(self, key, value):
        print("conffg df", self.config_df[self.config_df[key] == value])
        return json.loads(self.config_df[self.config_df[key] == value].iloc[0].to_json())

    def get_influx_config(self, valid=None, key=None, value=None):
        if valid:
            config = self.get_excel_config_by_valid_flag(valid)

        if key and value:
            config = self.get_excel_config(key, value)

        url = urlparse(config.get("InfluxURL"))
        influx_config = {
            "host": url.hostname,
            "port": url.port,
            "ssl": True if url.scheme == 'https' else False,
            "database": config.get("db"),
        }
        if url.hostname == 'alteryx-influx.greymatter.greyorange.com':
            influx_config['username'] = CENTRAL_INFLUX_USERNAME
            influx_config['password'] = CENTRAL_INFLUX_PASSWORD

        return influx_config

    def get_influx_client(self, valid=None):
        influx_config = self.get_excel_config_by_valid_flag(valid)
        return InfluxDataV2(**influx_config)


class SingleTenantDAGBase:
    def __init__(self, dag, site, tasks, dependencies):
        self.dag = dag
        self.site = site
        self.tasks_data = {}
        self.data = {}

        self.task_instance_map = {}

        for task in tasks:
            self.task_instance_map[task] = PythonOperator(
                task_id=self.get_task_id(task, site),
                python_callable=getattr(self, task),
                dag=dag
            )

        for dependency in dependencies:
            task1, task2 = dependency.replace(" ", "").split(">>")
            self.task_instance_map[task1] >> self.task_instance_map[task2]

    def get_task_id(self, task_name, site):
        return f"{task_name}_for_{site.get('name')}"

    def get_site_read_influx_client(self, database):
        influx_config = {
            "host": self.site.get('influx_ip'),
            "port": self.site.get('influx_port'),
            "database": database,
        }
        return InfluxDataV2(**influx_config)

    def get_site_write_influx_client(self, database=None):
        if not database:
            database = self.site.get('out_db_name')

        influx_config = {
            "host": self.site.get('write_influx_ip'),
            "port": int(self.site.get('write_influx_port')),
            "database": database,
        }
        return InfluxDataV2(**influx_config)

    def get_central_influx_client(self, database, host=None, timeout=180):
        return InfluxDataV2(
            host=host if host else 'alteryx-influx.greymatter.greyorange.com',
            port=8086,
            username=CENTRAL_INFLUX_USERNAME,
            password=CENTRAL_INFLUX_PASSWORD,
            ssl=True,
            database=database,
            retries=2,
            timeout=timeout
        )

    def get_task_data(self, task_name, **kwargs):
        task_instance = kwargs["task_instance"]
        return task_instance.xcom_pull(
            task_ids=self.get_task_id(task_name, self.site)
        )


class MultiTenantDAG:
    def __init__(self, dag, tasks, dependencies, tenant_class, combined_tasks_functions=None):
        self.dag = dag
        self.tasks = tasks
        self.dependencies = dependencies
        self.tenant_class = tenant_class
        self.combined_tasks_functions = combined_tasks_functions
        self.tenants = self.get_tenants()
        self.tenant_instances = []

    def get_tenants(self):
        if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'false':
            tenants = [CommonFunction().get_tenant_info()]
        else:
            df = CommonFunction().get_all_site_data_config_sheet()
            df = df[(df["Active"] == 'Y') & (df[self.dag.dag_id] == 'Y')]
            tenants = json.loads(df.to_json(orient='records'))

        # Reformatting dict key according to PEP8 guidelines
        formatted_tenants = []
        for tenant in tenants:
            _dict = {}
            for key, value in tenant.items():
                _dict[key.lower()] = value
            formatted_tenants.append(_dict)
        return formatted_tenants

    def create(self):
        for site in self.tenants:
            self.tenant_instances.append(self.tenant_class(
                self.dag,
                site,
                self.tasks,
                self.dependencies
            ))
        if self.combined_tasks_functions:
            self.add_combined_tasks()

    def add_combined_tasks(self):
        first_task_name = self.combined_tasks_functions[0].__name__
        first_combined_task = PythonOperator(
            task_id=first_task_name,
            python_callable=self.combined_tasks_functions[0],
            dag=self.dag,
            op_kwargs={
                'multi_tenant_instance': self,
            }
        )

        # Getting last task of all tenants for setting dependencies
        last_task_name = self.tasks[-1]
        for tenant_instance in self.tenant_instances:
            tenant_instance.task_instance_map[last_task_name] >> first_combined_task

        for func in self.combined_tasks_functions[1:]:
            task = PythonOperator(
                task_id=func.__name__,
                python_callable=func,
                dag=self.dag,
                op_kwargs={
                    'multi_tenant_instance': self,
                }
            )

            first_combined_task >> task

    def get_task_data(self, task_id, **kwargs):
        task_instance = kwargs["task_instance"]
        return task_instance.xcom_pull(
            task_ids=task_id
        )


def average(_list):
    if not _list:
        return 0
    return sum(_list) / len(_list)


class KafkaManager:
    def __init__(self, bootstrap_servers):
        """
        Initialize KafkaManager with the provided Kafka bootstrap servers.
        """
        self.bootstrap_servers = bootstrap_servers

        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize JSON to UTF-8
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )

        # Initialize Kafka Admin Client
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        self.group_id = 'my-group'

    def push_key_value(self, topic, datevalue, keyvalue):
        """
        Produce a message to a specified Kafka topic.

        Args:
            topic (str): The Kafka topic to send the message to.
            key (str): The key of the message.
            value (str): The value of the message.
        """
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,  # Replace with your Kafka broker(s)
            key_serializer=lambda k: k.encode('utf-8'),  # Serialize keys as UTF-8 strings
            value_serializer=lambda v: v.encode('utf-8')  # Serialize values as UTF-8 strings
        )

        try:

            producer.send(topic, key=keyvalue, value=datevalue)
            producer.flush()  # Wait for all messages to be sent
            print(f"Message sent to topic")
        except KafkaError as e:
            print(f"Failed to produce message: {e}")

    def get_keyvalue(self, topic, target_key, auto_offset_reset='latest'):
        """
        Consume messages from a Kafka topic.

        Args:
            topic (str): The Kafka topic to consume messages from.
            auto_offset_reset (str): Where to start reading messages (e.g., 'earliest' or 'latest').
            max_messages (int, optional): Maximum number of messages to consume (None for unlimited).

        Returns:
            list: A list of consumed messages (deserialized JSON records).
        """
        consumer = KafkaConsumer(
            # topic,
            bootstrap_servers=self.bootstrap_servers,
            # group_id=self.group_id,
            key_deserializer=lambda k: k.decode('utf-8'),
            value_deserializer=lambda v: v.decode('utf-8'),
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            consumer_timeout_ms=30000
        )
        messages = []
        message_value = ''
        try:
            partitions = consumer.partitions_for_topic(topic)
            print(f"partitions:{partitions}:{topic}")
            if partitions:
                topic_partitions = [TopicPartition(topic, p) for p in partitions]
                print(f"topic_partitions:{topic_partitions}")
                consumer.assign(topic_partitions)

                # Determine end offsets for all partitions
                end_offsets = consumer.end_offsets(topic_partitions)

                # Fetch Messages
                # messages = []
                print("Consuming messages...")
                # consumer.poll(timeout_ms=1000)
                for message in consumer:
                    print(message)
                    # Process message
                    if message.key == target_key:
                        print(f"Found record with key: {message.key}, value: {message.value}")
                        message_value = message.value
                        break
                    if message.offset == end_offsets[TopicPartition(message.topic, message.partition)] - 1:
                        print(f"Reached end of partition {message.partition}")
                        break
        except KafkaError as e:
            print(f"Failed to consume messages: {e}")
        finally:
            consumer.close()
        consumer.close()
        return message_value

    def produce_message(self, topic, table_data, key=''):
        """
        Produce a message to a specified Kafka topic.

        Args:
            topic (str): The Kafka topic to send the message to.
            key (str): The key of the message.
            value (str): The value of the message.
        """

        try:
            for index, row in table_data.iterrows():
                message = row.to_dict()
                valid_json_string = json.dumps(message)
                data = f'{valid_json_string}'
                if key == '':
                    key = topic
                self.producer.send(topic, key=key, value=data)
                # self.producer.produce(topic, data.encode('utf-8'),
                #                      callback=lambda err, msg: print(f"Produced: {msg.value()}"))
                # elf.producer.flush()  # Wait for all messages to be sent
            self.producer.flush()  # Wait for all messages to be sent
            print(f"Message sent to topic")
        except KafkaError as e:
            print(f"Failed to produce message: {e}")

    def delete_topic(self, topic):
        """
        Delete a Kafka topic.

        Args:
            topic (str): The Kafka topic to delete.
        """
        try:
            self.admin_client.delete_topics([topic])
            print(f"Topic '{topic}' deleted successfully.")
            return True
        except KafkaError as e:
            print(f"Failed to delete topic '{topic}': {e}")
        return False

    def consume_messages(self, topic, auto_offset_reset='earliest', max_messages=10000):
        """
        Consume messages from a Kafka topic.

        Args:
            topic (str): The Kafka topic to consume messages from.
            auto_offset_reset (str): Where to start reading messages (e.g., 'earliest' or 'latest').
            max_messages (int, optional): Maximum number of messages to consume (None for unlimited).

        Returns:
            list: A list of consumed messages (deserialized JSON records).
        """
        consumer = KafkaConsumer(
            # topic,
            bootstrap_servers=self.bootstrap_servers,
            # group_id=self.group_id,
            # value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # Deserialize JSON from UTF-8
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False
        )

        messages = []
        try:
            # while True:
            #     msg = consumer.poll(1.0)  # Poll with a 1-second timeout
            #     print(msg)
            #     if msg is None or pd.isna(msg) or msg=={}:
            #         continue
            #     if msg.error():
            #         if msg.error().code() == KafkaError._PARTITION_EOF:
            #             print("End of partition reached")
            #             break
            #         else:
            #             print(f"Error: {msg.error()}")
            #         continue
            #     messages.append(msg.value)
            partitions = consumer.partitions_for_topic(topic)
            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(topic_partitions)

            # Determine end offsets for all partitions
            end_offsets = consumer.end_offsets(topic_partitions)

            # Fetch Messages
            # messages = []
            print("Consuming messages1...")
            for message in consumer:
                # Process message
                print(json.loads(message.value.decode('utf-8')))
                print(type(json.loads(message.value.decode('utf-8'))))
                messages.append(json.loads(message.value.decode('utf-8')))
                #     {
                #     'key': message.key.decode('utf-8') if message.key else None,
                #     'value': message.value.decode('utf-8') if message.value else None,
                #     'partition': message.partition,
                #     'offset': message.offset,
                #     'timestamp': message.timestamp
                # }
                # )
                if message.offset == end_offsets[TopicPartition(message.topic, message.partition)] - 1:
                    print(f"Reached end of partition {message.partition}")
                    break  # Exit loop when end of partition is reached
        except KafkaError as e:
            print(f"Failed to consume messages: {e}")
        finally:
            consumer.close()

        return messages

    def create_topic(self, topic, num_partitions=1, replication_factor=1):
        """
        Create a Kafka topic.

        Args:
            topic (str): The Kafka topic to create.
            num_partitions (int): Number of partitions.
            replication_factor (int): Replication factor.
        """
        new_topic = NewTopic(
            name=topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        try:
            self.admin_client.create_topics([new_topic])
            print(f"Topic '{topic}' created successfully.")
        except KafkaError as e:
            print(f"Failed to create topic '{topic}': {e}")

    def topic_exists(self, topic):
        """
        Check if a Kafka topic exists.
        :param topic_name: The name of the topic to check.
        :return: True if the topic exists, False otherwise.
        """
        try:
            # Use KafkaAdminClient to fetch the list of topics
            topics = self.admin_client.list_topics()
            self.admin_client.close()
            return topic in topics
        except KafkaError as e:
            print(f"Error checking topic existence: {e}")
            return False


class MongoDBManager:
    def __init__(self, connection_string, database_name, collection_name):
        """
        Initialize the MongoDBManager.

        :param connection_string: MongoDB connection string
        :param database_name: Name of the database
        :param collection_name: Name of the collection
        """
        self.client = MongoClient(connection_string)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

    def get_data(self, query=None, projection=None):
        """
        Retrieve data from the collection.

        :param query: Filter to find documents (default: None for all documents)
        :param projection: Fields to include or exclude (default: None for all fields)
        :return: List of documents
        """
        cursor = self.collection.find(query or {}, projection)
        data= list(cursor)
        if len(data) == 0:
            return None
        else:
            return data
        return None

    def get_two_days_data(self, start_date, projection=None):
        two_days_before = (pd.to_datetime(start_date)-timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        query = { 'time': { '$gt': two_days_before } }
        cursor = self.collection.find(query or {}, projection)
        data= list(cursor)
        if len(data) == 0:
            return None
        else:
            return data
        return None

    def get_previous_hours_data(self, start_date, hours=48, projection=None):
        back_date = (pd.to_datetime(start_date)-timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        query = { 'time': { '$gt': back_date } }
        cursor = self.collection.find(query or {}, projection)
        data= list(cursor)
        if len(data) == 0:
            return None
        else:
            return data

    def insert_data(self, data):
        """
        Insert data into the collection.

        :param data: Dictionary or list of dictionaries to insert
        :return: Inserted IDs
        """
        if isinstance(data, list):
            result = self.collection.insert_many(data)
            return result.inserted_ids
        else:
            result = self.collection.insert_one(data)
            return result.inserted_id

    def update_data(self, query, update_values, multiple=False):
        """
        Update data in the collection.

        :param query: Filter to find documents to update
        :param update_values: Dictionary with the updated values
        :param multiple: If True, updates multiple documents, otherwise updates one
        :return: Update result
        """
        update_query = {"$set": update_values}
        if multiple:
            result = self.collection.update_many(query, update_query)
        else:
            result = self.collection.update_one(query, update_query, upsert=True)
        return result.raw_result

    def delete_data(self, query, multiple=False):
        """
        Delete data from the collection.

        :param query: Filter to find documents to delete
        :param multiple: If True, deletes multiple documents, otherwise deletes one
        :return: Delete result
        """
        if multiple:
            result = self.collection.delete_many(query)
        else:
            result = self.collection.delete_one(query)
        return result.raw_result

    def close_connection(self):
        """
        Close the connection to MongoDB.
        """
        self.client.close()

#
# # Example usage
# if __name__ == "__main__":
#     connection_string = "mongodb://localhost:27017/"
#     database_name = "test_db"
#     collection_name = "test_collection"
#
#     # Initialize the MongoDBManager
#     db_manager = MongoDBManager(connection_string, database_name, collection_name)
#
#     # Insert data
#     data_to_insert = {"name": "John Doe", "age": 30, "city": "New York"}
#     inserted_id = db_manager.insert_data(data_to_insert)
#     print(f"Inserted ID: {inserted_id}")
#
#     # Update data
#     query = {"name": "John Doe"}
#     update_values = {"age": 31}
#     update_result = db_manager.update_data(query, update_values)
#     print(f"Update Result: {update_result}")
#
#     # Delete data
#     delete_query = {"name": "John Doe"}
#     delete_result = db_manager.delete_data(delete_query)
#     print(f"Delete Result: {delete_result}")

    # Close the connection
