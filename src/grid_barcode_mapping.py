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

from pandasql import sqldf
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, Butler_api
from config import (
    rp_seven_days
)
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

class GridBarcodeMapping:
    def fetch_grid_info(self):
        gridinfo_data=self.butler.fetch_gridinfo_data()
        grid_info = pd.json_normalize(gridinfo_data)
        if 'measurement' in grid_info.columns:
            del grid_info['measurement']
        grid_info = grid_info.rename(columns={'tags.barcode':'barcode',  'fields.host':'host', 'fields.coordinate':'coordinate', 'fields.zone':'zone','fields.floor':'floor'})
        return grid_info

    def fetch_storage_info(self):
        # q = f"select * from storage_info"
        storageinfo_data = self.butler.fetch_storageinfo_data()
        storage_info = pd.json_normalize(storageinfo_data)
        if not storage_info.empty:
            if 'measurement' in storage_info.columns:
                del storage_info['measurement']
            storage_info = storage_info.rename(columns={'tags.rack_id':'rack_id','fields.host':'host', 'fields.storage_id':'storage_id', 'fields.storage_status':'storage_status'})
            storage_info.columns = ['time', 'rack_id','host', 'storage_id', 'storage_status']
            lamdatest = lambda x: list(map(int,x.replace('[','').replace(']','').split(',')))
            storage_info['storage_id_list'] = storage_info['storage_id'].apply(lamdatest)
            storage_info['x'] = storage_info['storage_id_list'].apply(lambda x :x[0])
            storage_info['y'] = storage_info['storage_id_list'].apply(lambda x :x[1])
            storage_info['store_cordinate'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x'])+"," + str(a['y']) + "]", axis=1)
            storage_info['io_point_up'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x'])+"," + str(a['y']+1) + "]", axis=1)
            storage_info['io_point_down'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x'])+"," + str(a['y']-1) + "]", axis=1)
            storage_info['io_point_left'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x']+1)+"," + str(a['y']) + "]", axis=1)
            storage_info['io_point_right'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x']-1)+"," + str(a['y']) + "]", axis=1)
            storage_info = storage_info.drop(['storage_id_list','x','y'], axis=1)
        else:
            storage_info = pd.DataFrame([],columns=['host','storage_id','io_point_up','io_point_down','io_point_left','io_point_right','store_cordinate'])
        return storage_info

    def fetch_tote_info(self):
        # q = f"select * from tote_info"
        tote_data = self.butler.fetch_toteinfo_data()
        storage_info = pd.json_normalize(tote_data)
        if not storage_info.empty:
            if 'measurement' in storage_info.columns:
                del storage_info['measurement']
            storage_info = storage_info.rename(columns={'tags.rack_id':'rack_id', 'fields.host':'host', 'fields.storage_id':'storage_id', 'fields.storage_status':'storage_status'})
            lamdatest = lambda x: list(map(int,x.replace('[','').replace(']','').split(',')))
            storage_info['storage_id_list'] = storage_info['storage_id'].apply(lamdatest)
            storage_info['x'] = storage_info['storage_id_list'].apply(lambda x :x[0])
            storage_info['y'] = storage_info['storage_id_list'].apply(lambda x :x[1])
            storage_info['store_cordinate'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x'])+"," + str(a['y']) + "]", axis=1)
            storage_info['io_point_up'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x'])+"," + str(a['y']+1) + "]", axis=1)
            storage_info['io_point_down'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x'])+"," + str(a['y']-1) + "]", axis=1)
            storage_info['io_point_left'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x']+1)+"," + str(a['y']) + "]", axis=1)
            storage_info['io_point_right'] = storage_info[['x','y']].apply(lambda a: "["+ str(a['x']-1)+"," + str(a['y']) + "]", axis=1)
            storage_info = storage_info.drop(['storage_id_list','x','y'], axis=1)
        else:
            storage_info = pd.DataFrame([],columns=['host','storage_id','io_point_up','io_point_down','io_point_left','io_point_right','store_cordinate'])
        return storage_info

    def fetch_pps_info(self):
        # q = f"select * from pps_info"
        ppsinfo_data=self.butler.fetch_ppsinfo_data()
        pps_info = pd.json_normalize(ppsinfo_data)
        col_list1 = []
        if not pps_info.empty:
            if 'measurement' in pps_info.columns:
                del pps_info['measurement']
            pps_info = pps_info.rename(columns={'tags.pps_id':'pps_id', 'fields.host':'host', 'fields.location':'location', 'fields.status':'status', 'fields.queue_barcodes':'queue_barcodes'})
            if 'queue_barcodes' in pps_info:
                pps_info['queue_barcodes_list'] = pps_info['queue_barcodes'].str.split(',')
                col_list = pps_info.queue_barcodes_list.values.tolist()
                for l1 in col_list:
                    if not l1==None:
                        for element in l1:
                            col_list1.append(element)

            if 'location' in pps_info:
                for l1 in  pps_info.location.values.tolist():
                    col_list1.append(l1)
            unique_barcode =set(col_list1)
            pps_barcode = pd.DataFrame(unique_barcode, columns=['Barcodes'])
            pps_barcode['host']= self.tenant_info['host_name']
        else:
            pps_barcode = pd.DataFrame([], columns=['host','Barcodes'])
        return pps_barcode

    def fetch_charger_info(self):
        # q = f"select * from charger_info"
        chargerinfo_data = self.butler.fetch_chargerinfo_data()
        charger_info = pd.json_normalize(chargerinfo_data)
        if not charger_info.empty:
            if 'measurement' in charger_info.columns:
                del charger_info['measurement']
            charger_info = charger_info.rename(columns={'tags.charger_id':'charger_id','fields.host':'host',  'fields.charger_location':'charger_location', 'fields.entry_point_location':'entry_point_location','fields.reinit_point_location':'reinit_point_location'})
            charger_barcodes = charger_info.charger_location.values.tolist()
            charger_barcodes = charger_barcodes+ charger_info.entry_point_location.values.tolist()
            charger_barcodes = charger_barcodes + charger_info.reinit_point_location.values.tolist()
            unique_charger_barcodes=[]
            unique_charger_barcodes = set(charger_barcodes)
            charger_info = pd.DataFrame(unique_charger_barcodes, columns=['Barcodes'])
            charger_info['host'] = self.tenant_info['host_name']
        else:
            charger_info = pd.DataFrame([], columns=['host','Barcodes'])
        return charger_info

    def fetch_elevator_info(self):
        try:
            q = f"select * from elevator_info"
            elevator_info = pd.DataFrame(self.client.query(q).get_points())
            return elevator_info
        except:
            elevator_info = pd.DataFrame({'elevator_id': [None],
                           'key': [None]}) #find from dag
            return elevator_info

    def final_data(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.utilfunction = CommonFunction()
        self.client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["write_influx_port"],db="Alteryx")
        self.butler = Butler_api(host=self.tenant_info['Butler_ip'], port=self.tenant_info['Butler_port'], host_name = self.tenant_info['host_name'])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        grid_info = self.fetch_grid_info()
        storage_info = self.fetch_storage_info()
        pps_info = self.fetch_pps_info()
        tote_info = self.fetch_tote_info()
        #---------------------------------#
        #read csv file also(pending)
        #---------------------------------#
        charger_info = self.fetch_charger_info()
        #elevator_info = fetch_elevator_info(client)
        if grid_info.empty or (storage_info.empty and tote_info.empty) or pps_info.empty or charger_info.empty:
            return None

        final_storage = pd.concat([storage_info, tote_info])
        final_df = sqldf("select a.*, b.storage_id,b.io_point_up,b.io_point_down,b.io_point_left,b.io_point_right from grid_info a left join final_storage b on a.host = b.host and (a.barcode= b.storage_id or a.coordinate= b.storage_id or  a.coordinate= b.store_cordinate)")
        final_df = sqldf("select a.*,b.Barcodes as charge_barcode from final_df a left join charger_info b on a.host = b.host and a.barcode= b.Barcodes")
        final_df = sqldf("select a.*,b.Barcodes as pps_barcode from final_df a left join pps_info b on a.host = b.host and a.barcode= b.Barcodes")

        storage_info_cords = storage_info.store_cordinate.values.tolist()
        storage_info_cords = storage_info_cords + storage_info.io_point_up.values.tolist()
        storage_info_cords = storage_info_cords + storage_info.io_point_down.values.tolist()
        storage_info_cords = storage_info_cords + storage_info.io_point_left.values.tolist()
        storage_info_cords = storage_info_cords + storage_info.io_point_right.values.tolist()
        unique_storage_info = set(storage_info_cords)
        unique_storage = pd.DataFrame(unique_storage_info, columns=['Barcodes'])
        unique_storage['category'] ='Aisle'
        final_df = sqldf('select a.* , b.category  from final_df a left join unique_storage b on a.coordinate = b.Barcodes')
        final_df.category = final_df[['storage_id','category']].apply(lambda x: 'storable' if x['storage_id'] is not None else x['category'], axis=1)
        final_df.category = final_df[['charge_barcode','category']].apply(lambda x: 'Charger' if x['charge_barcode'] is not None else x['category'], axis=1)
        final_df.category = final_df[['pps_barcode','category']].apply(lambda x: 'PPS' if x['pps_barcode'] is not None else x['category'], axis=1)
        final_df.category = final_df['category'].apply(lambda x: 'Highway' if x is None else x)
        final_df['RecordID'] = np.arange(1, (final_df.shape[0] + 1), 1)
        end_date = datetime.now()
        end_date = end_date.replace(microsecond=0)
        final_df['cron_ran_at'] = end_date
        final_df=self.utilfunction.reset_index(final_df)
        final_df=self.utilfunction.get_epoch_time(final_df,date1=end_date)
        final_df.time = pd.to_datetime(final_df.time_ns)
        final_df=final_df.drop(columns=['time_ns','index'])
        final_df = final_df.drop(['io_point_up','io_point_down','io_point_left','io_point_right','storage_id','charge_barcode','pps_barcode'], axis=1)
        final_df.time = pd.to_datetime(final_df.time)
        final_df['cron_ran_at'] = final_df['cron_ran_at'].astype(str)
        final_df['floor'] = final_df['floor'].astype(str)
        final_df = final_df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(final_df, "grid_barcode_mapping", db_name=self.tenant_info["alteryx_out_db_name"],
                                      tag_columns=['RecordID'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'],retention_policy =rp_seven_days)
        return None

with DAG(
    'Grid_Barcode_Mapping',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Grid Barcode Mapping',
    schedule_interval = '16 1 * * *',
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
            if tenant['Active'] == "Y" and tenant['Grid_Barcode_Mapping'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                final_task = PythonOperator(
                    task_id='grid_barcode_mapping_final_data_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(GridBarcodeMapping().final_data,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='grid_barcode_mapping_final_data',
            provide_context=True,
            python_callable=functools.partial(GridBarcodeMapping().final_data,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

