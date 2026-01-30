## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, postgres_connection
import numpy as np
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class Slotvacancytrends:

    def divide_slotref(self, df):
        df['rack_id'] = df['slot'].str.split('.', expand=True)[0]
        df['rack_face'] = df['slot'].str.split('.', expand=True)[1]
        # df['slot'] = df['slot_id'].str.split('.', 2, expand=True)[2]
        return df

    def fetch_tote_info_from_tower(self):
        try:
            self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                     sslrootcert=self.tenant_info['sslrootcert'],
                                                     sslcert=self.tenant_info['sslcert'], \
                                                     sslkey=self.tenant_info['sslkey'],
                                                     host=self.tenant_info['Postgres_tower'], \
                                                     port=self.tenant_info['Postgres_tower_port'],
                                                     password=self.tenant_info['Tower_password'],
                                                     dag_name=os.path.basename(__file__),
                                                     site_name=self.tenant_info['Name'])
            totalcount = self.postgres_conn.fetch_postgres_data_in_chunk("select count(*) from data_tote")
            self.postgres_conn.close()
            if totalcount[0][0] == 0:
                tote_flow = True

            f = True
            time_offset = 0
            l = 2000
            print(f"total rows:{totalcount[0][0]}")
            while f == True:
                if time_offset <= totalcount[0][0]:
                    self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                             sslrootcert=self.tenant_info['sslrootcert'],
                                                             sslcert=self.tenant_info['sslcert'], \
                                                             sslkey=self.tenant_info['sslkey'],
                                                             host=self.tenant_info['Postgres_tower'], \
                                                             port=self.tenant_info['Postgres_tower_port'],
                                                             password=self.tenant_info['Tower_password'],
                                                             dag_name=os.path.basename(__file__),
                                                             site_name=self.tenant_info['Name'])
                    qry = "select tote_id, tote_type from data_tote order by tote_id offset "
                    qry = qry + str(time_offset) + " limit " + str(l)
                    print(qry)
                    temp_data = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
                    self.postgres_conn.close()
                    temp_data = pd.DataFrame(temp_data, columns=["rack_id", "rack_type"])
                    if time_offset == 0:
                        tower_rack = temp_data
                    else:
                        tower_rack = pd.concat([tower_rack, temp_data])
                    time_offset = time_offset + l
                else:
                    f = False
            return tower_rack
        except:
            return pd.DataFrame(columns=["rack_id", "rack_type"])

    def fetch_rack_info_from_tower(self):
        self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                 sslrootcert=self.tenant_info['sslrootcert'],
                                                 sslcert=self.tenant_info['sslcert'], \
                                                 sslkey=self.tenant_info['sslkey'],
                                                 host=self.tenant_info['Postgres_tower'], \
                                                 port=self.tenant_info['Postgres_tower_port'],
                                                 password=self.tenant_info['Tower_password'],
                                                 dag_name=os.path.basename(__file__),
                                                 site_name=self.tenant_info['Name'])
        totalcount = self.postgres_conn.fetch_postgres_data_in_chunk("select count(*) from data_rack")
        self.postgres_conn.close()

        f = True
        time_offset = 0
        l = 2000
        print(f"total rows:{totalcount[0][0]}")
        while f == True:
            if time_offset <= totalcount[0][0]:
                self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_tower'], \
                                                         port=self.tenant_info['Postgres_tower_port'],
                                                         password=self.tenant_info['Tower_password'],
                                                         dag_name=os.path.basename(__file__),
                                                         site_name=self.tenant_info['Name'])
                qry = "select rack_id, rack_type from data_rack order by rack_id offset "
                qry = qry + str(time_offset) + " limit " + str(l)
                print(qry)
                temp_data = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
                self.postgres_conn.close()
                temp_data = pd.DataFrame(temp_data, columns=["rack_id", "rack_type"])
                if time_offset == 0:
                    tower_rack = temp_data
                else:
                    tower_rack = pd.concat([tower_rack, temp_data])
                time_offset = time_offset + l
            else:
                f = False
        try:
            tote_df = self.fetch_tote_info_from_tower()
            if not tote_df.empty:
                tower_rack = pd.concat([tower_rack, tote_df])
        except:
            pass
        return tower_rack

    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        #        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")
        self.utilfunction = CommonFunction()

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("slot_vacancy_trends", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        self.postgres_conn = postgres_connection(database='butler_dev',
                                                 user=self.tenant_info['Postgres_butler_user'], \
                                                 sslrootcert=self.tenant_info['sslrootcert'],
                                                 sslcert=self.tenant_info['sslcert'], \
                                                 sslkey=self.tenant_info['sslkey'],
                                                 host=self.tenant_info['Postgres_ButlerDev'], \
                                                 port=self.tenant_info['Postgres_butler_port'],
                                                 password=self.tenant_info['Postgres_butler_password'],
                                                 dag_name=os.path.basename(__file__),
                                                 site_name=self.tenant_info['Name'])
        # cursor = self.postgres_conn.pg_curr
        q = f"""
        SELECT *
        FROM inventory_transaction_archives
        WHERE time > '{check_start_date}'
          AND time <= '{check_end_date}'
          AND new_uom = '{{"Item": 0}}'
        LIMIT 1;
        """

        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        self.postgres_conn.close()
        if not data:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.utilfunction.create_time_series_data2(check_start_date, check_end_date, '1h')
                for i in daterange.index:
                    end_date = daterange['interval_end'][i]
                    self.final_call1(end_date)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("slot_occupancy_history", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date: {self.end_date}")


        self.postgres_conn = postgres_connection(database='butler_dev',
                                                 user=self.tenant_info['Postgres_butler_user'], \
                                                 sslrootcert=self.tenant_info['sslrootcert'],
                                                 sslcert=self.tenant_info['sslcert'], \
                                                 sslkey=self.tenant_info['sslkey'],
                                                 host=self.tenant_info['Postgres_ButlerDev'], \
                                                 port=self.tenant_info['Postgres_butler_port'],
                                                 password=self.tenant_info['Postgres_butler_password'],
                                                 dag_name=os.path.basename(__file__),
                                                 site_name=self.tenant_info['Name'])
        # cursor = self.postgres_conn.pg_curr
        q = f"""
                SELECT id, external_service_request_id, transaction_type, request_id, event_name, user_name, slot, item_id, time, pps_id, ppsbin_id,old_uom,	delta_uom,	new_uom
                FROM inventory_transaction_archives
                WHERE time > '{self.start_date}'
                  AND time <= '{self.end_date}'
                  AND (new_uom = '{{"Item": 0}}' or new_uom = '{{}}' or old_uom = '{{}}' or old_uom = '{{"Item": 0}}');
                """
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        self.postgres_conn.close()
        cols = ['id','external_service_request_id','transaction_type','request_id','event_name','user_name',
                'slot','item_id','time','pps_id','ppsbin_id','old_uom','delta_uom','new_uom']
        df = pd.DataFrame(data, columns=cols)
        if df.empty:
            self.utilfunction.update_dag_last_run({"site": self.tenant_info['Name'], "table": "slot_occupancy_history"},
                                                  {"last_run": self.end_date})
            return None

        df['old_uom'] = df.old_uom.astype(str).str.extract('(\d+)}$', expand=False)
        df['new_uom'] = df.new_uom.astype(str).str.extract('(\d+)}$', expand=False)
        df['delta_uom'] = df.delta_uom.astype(str).str.extract('(\d+)}$', expand=False)
        df['host'] = self.tenant_info['host_name']
        df['installation_id'] = self.tenant_info['installation_id']
        df['host'] = df['host'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)
        df['old_uom'] = df['old_uom'].fillna(0).astype(int)
        df['new_uom'] = df['new_uom'].fillna(0).astype(int)
        df['delta_uom'] = df['delta_uom'].fillna(0).astype(int)



        for col in ['id','external_service_request_id','transaction_type','request_id','event_name','user_name',
                'slot,item_id','time','pps_id','ppsbin_id']:
            if col in df.columns:
                df[col] = df[col].astype('str', errors='ignore')
        df = self.divide_slotref(df)
        rackinfo = self.fetch_rack_info_from_tower()
        df = pd.merge(df, rackinfo, on=["rack_id"])
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "slot_occupancy_history", db_name=self.tenant_info["alteryx_out_db_name"],
                                      tag_columns=['pps_id','rack_type'],
                                      dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        print("inserted")
        return None


with DAG(
        'slot_occupancy_history',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Empty Slot information',
        schedule_interval='48 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(seconds=1200),
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['slot_occupancy_history'] == "Y" and (
                    tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='slot_occupancy_history_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(Slotvacancytrends().final_call,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=3600))
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='picks_per_rack_final',
            provide_context=True,
            python_callable=functools.partial(Slotvacancytrends().final_call,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

