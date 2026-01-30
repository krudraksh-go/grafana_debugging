
## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, Butler_api, postgres_connection
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name =os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class rack_volume_utlization:
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

    def rack_volume_utlization_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.utilfunction = CommonFunction()
        self.butler_interface = Butler_api(host=self.tenant_info['Butler_ip'], port=self.tenant_info['Butler_interface_port'])
        self.butler_interface.fetch_rack_volume_utilization()
       # rack_api = "http://192.168.32.86:30408/rack_storage_utilization/_search?size=100"
       # rack_info_req = requests.get(rack_api)
        rack_data = self.butler_interface.rack_volumn_utilization_data
#        print(rack_data)
        dataframe = pd.json_normalize(rack_data)
        if not dataframe.empty and 'dimensions' in dataframe:
            dataframe['total_volume'] = dataframe['dimensions'].apply(
                lambda x: x[0] * x[1] * x[2])
            dataframe['free_slots'] = dataframe['volume_occupied'].apply(
                lambda x: 1 if pd.isna(x) or x==0 else 0)
            dataframe['occupied_slots'] = dataframe['volume_occupied'].apply(
                lambda x: 1 if x!=0 else 0)
            dataframe['rack_id'] = dataframe['slot_id'].apply(
                lambda x: x.split('.')[0])
            dataframe['rack_face'] = dataframe['slot_id'].apply(
                lambda x: x.split('.')[1])
            dataframe['shelf_floor'] = dataframe['slot_id'].apply(
                lambda x: x.split('.')[2])
            dataframe['avg_utilization'] =dataframe.apply(
                lambda x: x['volume_occupied']/x['total_volume'], axis=1)
            dataframe['dimensions'] = dataframe['dimensions'].apply(lambda x: str(x[0]) + "*" + str(x[1]) + "*" + str(x[2]))
            rackinfo = self.fetch_rack_info_from_tower()
            dataframe = pd.merge(dataframe, rackinfo, on=["rack_id"])
            final_df = dataframe.groupby(['rack_type','slot_name','rack_face','shelf_floor','dimensions'], as_index=False).agg(free_slots=('free_slots','sum'),occupied_slots=('occupied_slots','sum'),avg_utilization=('avg_utilization','mean'),total_slots=('slot_id','count'))

            cron_run_at = datetime.now()
            final_df['cron_ran_at'] = cron_run_at.strftime('%Y-%m-%dT%H:%M:%S')
            final_df['free_slots']= final_df['free_slots'].apply(int)
            final_df['occupied_slots'] = final_df['occupied_slots'].apply(int)
            final_df['total_slots'] = final_df['total_slots'].apply(int)
            final_df['avg_utilization'] = final_df['avg_utilization'].apply(float)
            final_df['slot_name'] = final_df['slot_name'].apply(str)

            final_df = self.utilfunction.get_epoch_time(final_df, date1=cron_run_at)
            final_df['time'] = pd.to_datetime(final_df.time_ns)
            final_df=final_df.drop(columns=['time_ns','index'])
            final_df['host'] = self.tenant_info['host_name']
            final_df['installation_id'] = self.tenant_info['installation_id']
            final_df['host'] = final_df['host'].astype(str)
            final_df['installation_id'] = final_df['installation_id'].astype(str)
            final_df = final_df.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(final_df, "rack_volume_utlization", db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['rack_type'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None


with DAG(
    'rack_volume_utlization',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'rack_volume_utlization',
    schedule_interval = '12 * * * *',
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
            if tenant['Active'] == "Y" and tenant['rack_volume_utilization'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                final_task = PythonOperator(
                    task_id='barcode_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(rack_volume_utlization().rack_volume_utlization_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='rack_volume_utlization_final',
            provide_context=True,
            python_callable=functools.partial(rack_volume_utlization().rack_volume_utlization_final,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )
