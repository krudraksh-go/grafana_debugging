## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------
import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime, timezone
import pandas as pd
import gspread
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
from config import (
    GOOGLE_AUTH_JSON,
    rp_seven_days
)
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

class kpi_document_airflow:

    def refresh_kpi_document(self):
        SHEET_ID = '1qEA9QP8XP0LJ3IdHqNw4hbxEqzd1EHsdm2L51OiLSgg'
        gc = gspread.service_account(GOOGLE_AUTH_JSON)
        spreadsheet = gc.open_by_key(SHEET_ID)
        sheet_names = [s.title for s in spreadsheet.worksheets()]
        final_sheet= pd.DataFrame()
        for sheet_name in sheet_names:
            worksheet = spreadsheet.worksheet(sheet_name)
            rows = worksheet.get_all_values()
            df_sheet = pd.DataFrame(rows)
            expected_headers = df_sheet.iloc[0]
            df_sheet.columns = expected_headers
            df_sheet["Dashboard"]= sheet_name

            if df_sheet.shape[1] == 9:
                df_sheet=df_sheet[1:]
                if final_sheet.empty:
                    final_sheet=df_sheet
                else:
                    final_sheet = pd.concat([final_sheet,df_sheet])
        return final_sheet

    def insert_kpi_document(self, tenant ,df ):
        self.read_client = InfluxData(host=tenant["influx_ip"], port=tenant["influx_port"], db="Alteryx")
        isvalid = self.read_client.is_influx_reachable(host=tenant["influx_ip"],
                                                  port=tenant["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=tenant['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        # try:
        #     self.read_client.query("drop measurement kpi_document_airflow")
        # except:
        #     pass
        try:
            self.write_client = Write_InfluxData(host=tenant["write_influx_ip"],
                                                 port=tenant["write_influx_port"])

            self.write_client.writepoints(df, "kpi_document_airflow",
                                          db_name=tenant["alteryx_out_db_name"],
                                          tag_columns=[], dag_name=os.path.basename(__file__),
                                          site_name=tenant['Name'],retention_policy =rp_seven_days)
        except:
            pass
    def kpi_document_airflow_final(self, **kwargs):
        self.utilfunction = CommonFunction()
        df = self.refresh_kpi_document()
        cron_run_at= datetime.now()
        cron_run_at = cron_run_at.replace(microsecond=0)
        df['cron_ran_at'] = cron_run_at
        df=self.utilfunction.reset_index(df)
        df=self.utilfunction.get_epoch_time(df,date1=cron_run_at)
        df.rename(columns={'time_ns': 'time'}, inplace=True)
        if not df.empty:
            l = ['time_ns', 'index','cron_ran_at', 'index','level_0']
            for col in l:
                if col in df.columns:
                    del df[col]
        df = df.set_index('time')

        if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
            csvReader = CommonFunction().get_all_site_data_config()
            for tenant in csvReader:
                if tenant['Active'] == "Y" and tenant['kpi_document_airflow'] == "Y":
                    self.insert_kpi_document(tenant,df)
        else:
            tenant = CommonFunction().get_tenant_info()
            self.insert_kpi_document(tenant,df)

        return


with DAG(
        'kpi_document_airflow',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='kpi_document_airflow',
        schedule_interval='56 5 * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['kpi_document_airflow'] == "Y":
                final_task = PythonOperator(
                    task_id='kpi_document_airflow_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(kpi_document_airflow().kpi_document_airflow_final),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        final_task = PythonOperator(
            task_id='kpi_document_airflow_final',
            provide_context=True,
            python_callable=functools.partial(kpi_document_airflow().kpi_document_airflow_final),
            execution_timeout=timedelta(seconds=3600),
        )
