## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, Butler_api, postgres_connection
from pandasql import sqldf
import time
import requests
from airflow.exceptions import AirflowTaskTimeout

## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
Today = time.strftime('%Y-%m-%d')
Kal = datetime.today() - timedelta(days=1)
Yesterday = Kal.strftime('%Y-%m-%d')


class cls_bot_maintenance_data:
    def calculate_type(self,df2,df, field_name,direction_type):
        new_direction_type=direction_type
        if not df2.empty:
            direction_type2 = df2[df2['fieldKey'] == field_name]
            if not direction_type2.empty:
                direction_type2 = direction_type2.reset_index()
                if direction_type2.shape[0] > 1:
                    new_direction_type = direction_type
                else:
                    new_direction_type = direction_type2['fieldType'][0]
            else:
                new_direction_type = direction_type

        if new_direction_type=='string':
            df[f'{field_name}'] = df[f'{field_name}'].astype(str)
        elif new_direction_type=='float':
            df[f'{field_name}'] = df[f'{field_name}'].astype(float)
        elif new_direction_type == 'integer':
            df[f'{field_name}'] = df[f'{field_name}'].astype(int)
        else:
            df[f'{field_name}'] = df[f'{field_name}'].astype(str)
        return df

    def bot_maintenance_data_func(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        try:
            daterange = self.client.get_datetime_interval2("bot_maintenance_data", '1d',self.tenant_info['StartTime'])
            for i in daterange.index:
                    self.start_date = daterange['start_date'][i]
                    self.end_date = daterange['end_date'][i]
                    self.bot_maintenance_data_func1(Yesterday=self.start_date,Today=self.end_date, **kwargs)
        except AirflowTaskTimeout as timeout_exception:
            raise timeout_exception
        except Exception as e:
            print(f"error:{e}")
            raise e


    def bot_maintenance_data_func1(self,Yesterday, Today, **kwargs):
        #self.tenant_info = tenant_info['tenant_info']
        if self.tenant_info['Tower_user'] != "":
            connection = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                     sslrootcert=self.tenant_info['sslrootcert'],
                                                     sslcert=self.tenant_info['sslcert'], \
                                                     sslkey=self.tenant_info['sslkey'],\
                                                     host=self.tenant_info['Postgres_tower'], \
                                                     port=self.tenant_info['Postgres_tower_port'],
                                                     password=self.tenant_info['Tower_password'],
                                                     dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            connection = connection.pg_curr
            cursor1 = connection
            cursor2 = connection
            cursor3 = connection
            cursor4 = connection
            cursor5 = connection
            cursor6 = connection
            cursor7 = connection
            cursor8 = connection
            cursor9 = connection
            cursor10 = connection
            cursor11 = connection
            cursor12 = connection
            cursor1.execute(
                "SELECT count(butler_id) as Bots_in_maintenance FROM device_manager_butlermaintenance where is_resolved=false and butler_id in (select device_id from dm_device)")
            Bots_in_main = cursor1.fetchall()
            cursor2.execute(
                "SELECT count(butler_id) as Ops_to_maintenance FROM device_manager_butlermaintenance where"
                " created>'%s' and created<'%s'" % (Yesterday, Today))
            Ops_to_main = cursor2.fetchall()
            cursor3.execute(
                "SELECT count(butler_id) as Maintenance_to_Ops FROM device_manager_butlermaintenance where"
                " is_resolved=true and modified>'%s' and modified<'%s'" % (Yesterday, Today))
            Main_to_ops = cursor3.fetchall()
            cursor4.execute(
                "SELECT avg(EXTRACT(EPOCH FROM (modified - created))/3600) as Time_in_maintenance FROM device_manager_butlermaintenance where"
                " is_resolved=true and modified>'%s' and modified<'%s' " % (Yesterday, Today))
            Time_in_main = cursor4.fetchall()
            cursor5.execute("SELECT count(status) AS upgrade_attempts  FROM public.dm_updaterun"
                            " where start_time>'%s' and start_time<'%s' " % (Yesterday, Today))
            upgrade_attempts = cursor5.fetchall()
            cursor6.execute(
                "SELECT count(status) AS upgrade_success FROM public.dm_updaterun where status='success'"
                " and start_time>'%s' and start_time<'%s' " % (Yesterday, Today))
            upgrade_success = cursor6.fetchall()
            cursor7.execute("SELECT count(status) AS upgrade_failed FROM public.dm_updaterun where status='failed'"
                            " and start_time > '%s' and start_time < '%s' " % (Yesterday, Today))
            upgrade_failed = cursor7.fetchall()
            cursor8.execute(
                "SELECT count(status) AS upgrade_timeout FROM public.dm_updaterun where status='timeout'"
                " and start_time>'%s' and start_time<'%s' " % (Yesterday, Today))
            upgrade_timeout = cursor8.fetchall()
            cursor9.execute("select count(butler_id) as inducted_bots from data_butler")
            inducted_bots = cursor9.fetchall()
            cursor10.execute("select count(*) as inducted_chargers from data_charger")
            inducted_chargers = cursor10.fetchall()
            cursor11.execute(
                "select count(dm_device.build_id) as updated_bots_bfr, (dm_build.name) as BFR from dm_device,"
                "dm_build where dm_device.build_id=dm_build.id  group by dm_build.name,created_date order by created_date desc limit 1")
            bfr = cursor11.fetchall()
            cursor12.execute("select count(device_id) as total_bots from dm_device")
            total_bots = cursor12.fetchall()
            connection.close()
            df1 = pd.DataFrame(Bots_in_main, columns=["Bots_in_maintenance"])
            df1['time'] = '%s' % (Yesterday)
            df2 = pd.DataFrame(Ops_to_main, columns=['Ops_to_Maintenance'])
            df2['time'] = '%s' % (Yesterday)
            df3 = pd.DataFrame(Main_to_ops, columns=['Maintenance_to_Ops'])
            df3['time'] = '%s' % (Yesterday)
            df4 = pd.DataFrame(Time_in_main, columns=["Time_in_maintenance"])
            df4['time'] = '%s' % (Yesterday)
            df5 = pd.DataFrame(upgrade_attempts, columns=['upgrade_attempts'])
            df5['time'] = '%s' % (Yesterday)
            df6 = pd.DataFrame(upgrade_success, columns=['upgrade_success'])
            df6['time'] = '%s' % (Yesterday)
            df7 = pd.DataFrame(upgrade_failed, columns=['upgrade_failed'])
            df7['time'] = '%s' % (Yesterday)
            df8 = pd.DataFrame(upgrade_timeout, columns=['upgrade_timeout'])
            df8['time'] = '%s' % (Yesterday)
            df9 = pd.DataFrame(inducted_bots, columns=['inducted_bots'])
            df9['time'] = '%s' % (Yesterday)
            df10 = pd.DataFrame(inducted_chargers, columns=['inducted_chargers'])
            df10['time'] = '%s' % (Yesterday)
            df11 = pd.DataFrame(bfr, columns=['updated_bots_bfr', 'BFR'])
            df11['time'] = '%s' % (Yesterday)
            df12 = pd.DataFrame(total_bots, columns=['total_bots'])
            df12['time'] = '%s' % (Yesterday)
            df= pd.merge(df1,df2 ,on=['time'],how ='left')
            df = pd.merge(df, df3, on=['time'], how='left')
            df = pd.merge(df, df4, on=['time'], how='left')
            df = pd.merge(df, df5, on=['time'], how='left')
            df = pd.merge(df, df6, on=['time'], how='left')
            df = pd.merge(df, df7, on=['time'], how='left')
            df = pd.merge(df, df8, on=['time'], how='left')
            df = pd.merge(df, df9, on=['time'], how='left')
            df = pd.merge(df, df10, on=['time'], how='left')
            df = pd.merge(df, df11, on=['time'], how='left')
            df = pd.merge(df, df12, on=['time'], how='left')

            self.client = InfluxData(host=self.tenant_info["write_influx_ip"],
                                     port=self.tenant_info["write_influx_port"],
                                     db=self.tenant_info["out_db_name"])
            qry3 = "SHOW FIELD KEYS FROM  bot_maintenance_data "
            df2 = pd.DataFrame(self.client.query(qry3).get_points())

            df = self.calculate_type(df2, df,'Time_in_maintenance','float')
            df = self.calculate_type(df2, df, 'Bots_in_maintenance', 'float')
            df = self.calculate_type(df2, df, 'Maintenance_to_Ops', 'float')
            df = self.calculate_type(df2, df, 'inducted_bots', 'float')
            df = self.calculate_type(df2, df, 'inducted_chargers', 'float')
            df = self.calculate_type(df2, df, 'total_bots', 'float')
            df = self.calculate_type(df2, df, 'updated_bots_bfr', 'float')
            df = self.calculate_type(df2, df, 'upgrade_attempts', 'float')
            df = self.calculate_type(df2, df, 'upgrade_success', 'float')
            df = self.calculate_type(df2, df, 'upgrade_timeout', 'float')
            df = self.calculate_type(df2, df, 'Ops_to_Maintenance', 'float')
            df = self.calculate_type(df2, df, 'upgrade_failed', 'float')
            if 'Time_in_maintenance' in df.columns:
                del df['Time_in_maintenance']
            df['host']= self.tenant_info['host_name']
            df['installation_id'] = self.tenant_info['installation_id']
            df['host'] = df['host'].astype(str)
            df['installation_id'] = df['installation_id'].astype(str)
            df.time = pd.to_datetime(df.time)
            df = df.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df, "bot_maintenance_data", db_name=self.tenant_info["out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'bot_maintenance_data_only_for_on_cloud_sites',
    default_args = {**CommonFunction().get_default_args_for_dag(), **{'retries':3, 'retry_delay':timedelta(minutes=1)}},
    description = 'calculation of bot_maintenance_data',
    schedule_interval = '0 23,0 * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        print("only valid for cloud sites")
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['bot_maintenance_data'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                bot_maintenance_data_final_task = PythonOperator(
                    task_id='bot_maintenance_data_final_taskfinal{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(cls_bot_maintenance_data().bot_maintenance_data_func,
                                                        tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        bot_maintenance_data_final_task = PythonOperator(
            task_id='bot_maintenance_data_final',
            provide_context=True,
            python_callable=functools.partial(cls_bot_maintenance_data().bot_maintenance_data_func,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

