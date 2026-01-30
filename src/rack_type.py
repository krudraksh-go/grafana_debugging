## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient
import numpy as np
import pandas as pd

#from dags.utils.CommonFunction import InfluxData
from utils.CommonFunction import InfluxData,Write_InfluxData, postgres_connection, Butler_api,CommonFunction
from pandasql import sqldf
import json
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

# import os
# Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
# influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
# write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
# influx_port = os.environ.get('INFLUX_PORT', '8086')
# Postgres_tower=os.environ.get('Postgres_tower', "localhost")
# Postgres_tower_port=os.environ.get('Postgres_tower_port', 5432)
# tower_ip = os.environ.get('tower_ip', 'localhost')
# tower_port = os.environ.get('8086', 5432)
# db_name =os.environ.get('Out_db_name', 'airflow')
# sslrootcert=os.environ.get('sslrootcert', "")
# sslcert=os.environ.get('sslcert', "")
# sslkey=os.environ.get('sslkey', "")



## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class RacktypeSlotProcessor:
    def __init__(self, racktype_data):
        self._racktype_data = racktype_data
        self._floor_structure = self._racktype_data["floor_structure"]
        self._slot_data = {}
        self._rack_face = {}
        self._process_racktype_slots()

    def _process_racktype_slots(self):
        for floor_data in self._floor_structure:
            for base_data in floor_data["same_base_structure"]:
                for rack_face in base_data["rack_face_structure"]:
                    if 'rack_face' in self._rack_face:
                        if self._rack_face["rack_face"] <= len(rack_face):
                            self._rack_face["rack_face"] = len(rack_face)
                    else:
                        self._rack_face["rack_face"] = len(rack_face)
                    for slot in rack_face["slot_structure"]:
                        if slot['slot_type'] == 'drawer':
                            slot_length = 0
                            slot_depth = 0
                            for drawer_data in slot["drawer_structure"]:
                                slot_length += drawer_data['length']
                                slot_depth = max(drawer_data['depth'], slot_depth)
                        else:
                            slot_length = slot['length']
                            slot_depth = slot['depth']
                        slot_data = (slot_length, slot['height'], slot_depth, slot['slot_type'])
                        try:
                            self._slot_data[slot_data] += 1
                        except KeyError:
                            self._slot_data[slot_data] = 1
        return True

    def get_total_number_of_slots(self):
        total_num = sum(list(self._slot_data.values()))
        return total_num

    def get_num_diff_slots(self):
        return len(self._slot_data.keys())

    def get_slot_data(self):
        return self._slot_data

    def get_rack_faces(self):
        total_faces = max(list(self._rack_face.values()))
        return total_faces

class Racktype:
    def get_racktypes_details(self, input_racktype_json):
        racktypes_details = []
        temp_input_racktype_json=''
        flag=False
        if 'racktype_rec_list' in input_racktype_json:
            flag= True
            temp_input_racktype_json=input_racktype_json["racktype_rec_list"]
        else:
            temp_input_racktype_json = input_racktype_json["data"]

        for r in temp_input_racktype_json:
            if flag:
                r =json.loads(json.loads(r))
            racktype_id = r["racktype_id"]
            slot_processor = RacktypeSlotProcessor(r)
            total_num_slots = slot_processor.get_total_number_of_slots()
            num_diff_slots = slot_processor.get_num_diff_slots()
            slot_data = slot_processor.get_slot_data()
            rack_faces = slot_processor.get_rack_faces()
            slot_details = []
            for slot_detail, count in slot_data.items():
                slot_length, slot_height, slot_depth, slot_type = slot_detail
                slot_details.append({
                    "slot_length": slot_length,
                    "slot_height": slot_height,
                    "slot_depth": slot_depth,
                    "slot_type": slot_type,
                    "count": count,
                })

            racktypes_details.append(
                {
                    "racktype_id": r["racktype_id"],
                    "total_val": total_num_slots,
                    "num_slot_types": num_diff_slots,
                    "no_of_rack_faces": rack_faces
                }
            )
        self.rack_df = pd.DataFrame(racktypes_details,columns =['racktype_id', 'total_val', 'num_slot_types', 'no_of_rack_faces'])

    def fetch_rack_info_from_tower(self):
        self.postgres_conn.pg_curr.execute("select rack_type,count(rack_id) as total_racks from data_rack group by rack_type")
        data = self.postgres_conn.pg_curr.fetchall()
        self.tower_rack = pd.DataFrame(data, columns=["rack_type","total_racks"])
        try:
            self.postgres_conn.pg_curr.execute(
                "select tote_type,count(tote_id) as total_racks from data_tote group by tote_type")
            data = self.postgres_conn.pg_curr.fetchall()
            tote_df = pd.DataFrame(data, columns=["rack_type", "total_racks"])
            if not tote_df.empty:
                self.tower_rack =pd.concat([self.tower_rack,tote_df])
        except:
            pass


    def Rack_type_info_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.end_date = datetime.now()
        self.postgres_conn= postgres_connection(database='tower',user=self.tenant_info['Tower_user'],\
                                                sslrootcert=self.tenant_info['sslrootcert'],sslcert=self.tenant_info['sslcert'],\
                                                sslkey=self.tenant_info['sslkey'],host=self.tenant_info['Postgres_tower'],\
                                                port=self.tenant_info['Postgres_tower_port'],password=self.tenant_info['Tower_password'],
                                                dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        self.butler = Butler_api(host=self.tenant_info['Butler_ip'],port=self.tenant_info['Butler_port'])
        self.butler.fetch_rack_data()
        self.get_racktypes_details(self.butler.rack_data)
        self.fetch_rack_info_from_tower()
        rack_df = self.rack_df
        tower_rack=self.tower_rack
        finaldf =sqldf("select b.racktype_id as racktype, a.total_racks ,b.no_of_rack_faces, b.total_val as total from rack_df as b left join  tower_rack as a on a.rack_type= b.racktype_id")
        finaldf['no_of_rack_faces'].fillna(0, inplace=True)
        finaldf['total'].fillna(0, inplace=True)
        finaldf['total_racks'].fillna(0, inplace=True)
        finaldf["total_slots"] = finaldf['total']*finaldf['total_racks']
        finaldf["cron_ran_at"] = self.end_date
        finaldf["time"] = self.end_date
        finaldf['record_id'] = np.arange(1, (finaldf.shape[0] + 1), 1)
        #self.client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["write_influx_port"],db='Alteryx')

        if not finaldf.empty:
            finaldf.time = pd.to_datetime(finaldf.time)
            finaldf = finaldf.set_index('time')
            finaldf['cron_ran_at'] = finaldf['cron_ran_at'].astype(str)
            finaldf['no_of_rack_faces'] = finaldf['no_of_rack_faces'].astype(float)
            finaldf['racktype'] = finaldf['racktype'].astype(str)
            finaldf['record_id'] = finaldf['record_id'].astype(str)
            finaldf['total'] = finaldf['total'].astype(float)
            finaldf['total_racks'] = finaldf['total_racks'].astype(float)
            finaldf['total_slots'] = finaldf['total_slots'].astype(float)
            finaldf['host'] = self.tenant_info['host_name']
            finaldf['installation_id'] = self.tenant_info['installation_id']
            finaldf['host'] = finaldf['host'].astype(str)
            finaldf['installation_id'] = finaldf['installation_id'].astype(str)
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(finaldf, "racktype_info_ayx", db_name=self.tenant_info["out_db_name"],tag_columns=['record_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'Rack_type_info',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of Racktype',
    schedule_interval = timedelta(hours=8),
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
            if tenant['Active'] == "Y" and tenant['Rack_type'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                Rack_type_info_final_task = PythonOperator(
                    task_id='Rack_type_info_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(Racktype().Rack_type_info_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant =CommonFunction().get_tenant_info()
        # tenant = {"Name": "site", "Butler_ip": Butler_ip, "influx_ip": influx_ip, "influx_port": influx_port, \
        #           "write_influx_ip": write_influx_ip, "write_influx_port": influx_port, \
        #           "out_db_name": db_name,\
        #           "Tower_user": "remote", "Tower_password": "apj0702", "sslrootcert" : sslrootcert, "sslcert" : sslcert,\
        #           "sslkey" : sslkey, "Postgres_tower":Postgres_tower, "Postgres_tower_port":Postgres_tower_port
        # }
        tenant = CommonFunction().get_tenant_info()
        Rack_type_info_final_task = PythonOperator(
            task_id='Rack_type_info_final',
            provide_context=True,
            python_callable=functools.partial(Racktype().Rack_type_info_final,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )



