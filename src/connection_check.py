## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, postgres_connection,Butler_api
from pandasql import sqldf
from utils.DagIssueAlertUtils import push_failure_to_central_influx_mails
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')
receiver_list = ['analytics-team@greyorange.com']
sender = 'analytics@greyorange.com'
from config import (
    rp_seven_days,
    rp_one_year
)

# dag = DAG(
#     'orderline_transactions',
#     default_args = default_args,
#     description = 'calculation of orderline_transactions GM-44036',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class cls_connection_check:

    def create_retention_policy(self, name, duration, db):
        rp_name = name
        query = f"SHOW RETENTION POLICIES ON {db}"

        # Execute the query
        result = pd.DataFrame(self.client.query(query).get_points())

        # Check if the retention policy exists
        #exists = any(rp['name'] == rp_name for rp in result['name'])
        exists = rp_name in result['name'].values
        if not exists:
            try:
                rp_query = f""" CREATE RETENTION POLICY {rp_name} ON {db} DURATION {duration} REPLICATION 1 """
                self.client.query(rp_query)
            except Exception as e:
                print(e)
                pass

    def def_connection_check(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.utilfunction = CommonFunction()
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        try:
            isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"])
        except:
            isvalid=False

        if not isvalid:
            error_data = {'Error Message': []}
            error_data['Error Message'].append("Unable to Connect influx server("+ str(self.tenant_info["write_influx_ip"])+":" +str(self.tenant_info["write_influx_port"]) +")")
            errortable = pd.DataFrame(error_data)
            subject = 'Client Side Connection issue (' + self.tenant_info["Name"] + ')'
            self.utilfunction.send_mail(errortable.to_html(), subject, sender, receiver_list)
            data = {"time": formatted_datetime, "subject": subject, "html_body": errortable,'category': 'Connection Check'}
            push_failure_to_central_influx_mails(data)
            return
        self.altery_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        isvalid = self.altery_client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"])
        if not isvalid:
            try:
                self.influx_client.create_database('Alteryx')
            except:
                pass

        self.create_retention_policy(name=rp_seven_days, duration='7d', db = 'Alteryx')
        self.create_retention_policy(name=rp_seven_days, duration='7d', db = 'GreyOrange')
        self.create_retention_policy(name=rp_one_year, duration='365d', db = 'Alteryx')

        if self.tenant_info['Postgres_pf_user'] != "" and self.tenant_info['Postgres_pf_user']==self.tenant_info['Postgres_pf_user'] :
            try:
                self.postgres_conn = postgres_connection(database='platform_srms',
                                                         user=self.tenant_info['Postgres_pf_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_pf'], \
                                                         port=self.tenant_info['Postgres_pf_port'],
                                                         password=self.tenant_info['Postgres_pf_password'],
                                                         dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                self.postgres_conn.close()
            except Exception as e:
                error_data = {'Error Message': [], 'Error': []}
                error_data['Error Message'].append(
                    "Unable to Connect platform postgres server(" + str(self.tenant_info['Postgres_pf']) + ":" + str(self.tenant_info['Postgres_pf_port']) + ")")
                error_data['Error'].append(str(e))
                errortable = pd.DataFrame(error_data)
                subject = 'Client Side Connection issue (' + self.tenant_info["Name"] + ')'
                self.utilfunction.send_mail(errortable.to_html(), subject, sender, receiver_list)
                data = {"time": formatted_datetime, "subject": subject, "html_body": errortable,'category':'Connection Check'}
                push_failure_to_central_influx_mails(data)

        if self.tenant_info['Postgres_pf_user'] != "" and (not pd.isna(self.tenant_info['Postgres_pf_user'])) and self.tenant_info['Postgres_pf_user']==self.tenant_info['Postgres_pf_user']:
            try:
                self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_tower'], \
                                                         port=self.tenant_info['Postgres_tower_port'],
                                                         password=self.tenant_info['Tower_password'],
                                                         dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                self.postgres_conn.close()
            except Exception as e:
                error_data = {'Error Message': [], 'Error': []}
                error_data['Error Message'].append(
                    "Unable to Connect tower postgres server(" + str(self.tenant_info['Postgres_tower']) + ":" + str(self.tenant_info['Postgres_tower_port']) + ")")
                error_data['Error'].append(str(e))
                errortable = pd.DataFrame(error_data)
                subject = 'Client Side Connection issue (' + self.tenant_info["Name"] + ')'
                self.utilfunction.send_mail(errortable.to_html(), subject, sender, receiver_list)
                data = {"time": formatted_datetime, "subject": subject, "html_body": errortable,'category':'Connection Check'}
                push_failure_to_central_influx_mails(data)

        if self.tenant_info['Postgres_butler_user'] != "" and (not pd.isna(self.tenant_info['Postgres_butler_user'])) and self.tenant_info['Postgres_butler_user']==self.tenant_info['Postgres_butler_user']:
            try:
                self.postgres_conn = postgres_connection(database='butler_dev',
                                                         user=self.tenant_info['Postgres_butler_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_ButlerDev'], \
                                                         port=self.tenant_info['Postgres_butler_port'],
                                                         password=self.tenant_info['Postgres_butler_password'],
                                                         dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                self.postgres_conn.close()
            except Exception as e:
                error_data = {'Error Message': [], 'Error': []}
                error_data['Error'].append(str(e))
                error_data['Error Message'].append(
                    "Unable to Connect tower postgres server(" + str(self.tenant_info['Postgres_tower']) + ":" + str(self.tenant_info['Postgres_tower_port']) + ")")
                errortable = pd.DataFrame(error_data)
                subject = 'Client Side Connection issue (' + self.tenant_info["Name"] + ')'
                self.utilfunction.send_mail(errortable.to_html(), subject, sender, receiver_list)
                data = {"time": formatted_datetime, "subject": subject, "html_body": errortable,'category':'Connection Check'}
                push_failure_to_central_influx_mails(data)

        if self.tenant_info['Butler_ip']!='' and (not pd.isna(self.tenant_info['Butler_ip'])) and self.tenant_info['Butler_ip']==self.tenant_info['Butler_ip']:
            self.butler = Butler_api(host=self.tenant_info['Butler_ip'],port=self.tenant_info['Butler_port'])
            self.butler.fetch_rack_data()
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'Connection_check_setup',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of Orderline transactions',
    schedule_interval = '22 * * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y":
                Connection_check_final_task = PythonOperator(
                    task_id='Connection_check_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(cls_connection_check().def_connection_check,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )

    else:
        # tenant = {"Name":"project", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Connection_check_final_task = PythonOperator(
            task_id='Connection_check_final',
            provide_context=True,
            #python_callable=ButlerUptime().butler_uptime_final,
            python_callable=functools.partial(cls_connection_check().def_connection_check,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

