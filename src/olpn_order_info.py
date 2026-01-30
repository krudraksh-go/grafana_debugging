import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction,postgres_connection
from pandasql import sqldf
import pytz
import time
import numpy as np
import os
import psycopg2
from influxdb import InfluxDBClient,DataFrameClient


class OlpnOrdersInfo():
    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.connection = postgres_connection(database='platform_srms',
                                                         user=self.tenant_info['Postgres_pf_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_pf'], \
                                                         port=self.tenant_info['Postgres_pf_port'],
                                                         password=self.tenant_info['Postgres_pf_password'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        
        cursor = self.connection.pg_curr

        check_start_date = self.client.get_start_date("olpn_orders_info", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("select * from service_request where updated_on>'%s' and updated_on<'%s' limit 1"%(check_end_date,check_start_date))
        olpn = cursor.fetchall()
        df = pd.DataFrame(olpn)
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("olpn_orders_info", '1d', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except Exception as e:
                print(f"error:{e}")

    def final_call1(self, end_date, **kwargs):
        self.end_date = end_date
        self.start_date = self.client.get_start_date("olpn_orders_info", self.tenant_info)
        self.start_date=pd.to_datetime(self.start_date)
        self.start_date=self.start_date.strftime('%Y-%m-%d')
        self.end_date = self.end_date.strftime('%Y-%m-%d')
        start_time=self.tenant_info['StartTime']
        start = self.start_date+" "+start_time
        end = self.end_date+" "+start_time
        cursor0 = self.connection.pg_curr # single_olpn_created # Orders created(Disney)
        cursor1 = self.connection.pg_curr # multi_olpn_created
        cursor2 = self.connection.pg_curr # single_olpn_completed
        cursor3 = self.connection.pg_curr # multi_olpn_completed
        cursor4 = self.connection.pg_curr # single_olpn_breached
        cursor5 = self.connection.pg_curr # multi_olpn_breached
        cursor6 = self.connection.pg_curr # single_olpn_created_entities
        cursor7 = self.connection.pg_curr # Cancelled_olpns (day end olpns which were cancelled)
        cursor8 = self.connection.pg_curr  # Unfulfillable_olpns (day end olpns which were failed)
        cursor9 = self.connection.pg_curr # created_olpns(day end olpns which were in created state)
        cursor10 = self.connection.pg_curr # processing_olpns(day end olpns which were in procesing state)
        cursor11 = self.connection.pg_curr # multi_olpn_created_entities
        cursor12 = self.connection.pg_curr # single_olpn_completed_entities
        cursor13 = self.connection.pg_curr # multi_olpn_completed_entities


        cursor0.execute("select count(id) as single_olpn_created from service_request as sr left join service_request_children as src on sr.id=src.service_request_id left "
                   "join service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' and sr.state='complete' "
                   "AND sr.attributes->>'has_parent'='false' and sr.attributes->>'order_type'='STREAMING_STORE_ORDER' and sr.updated_on>'%s' and sr.updated_on<='%s' " %(start,end))

        cursor1.execute("select count(id) as multi_olpn_created from service_request as sr left join service_request_children as src on sr.id=src.service_request_id left "
                    "join service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' and sr.state='complete' "
                    "AND sr.attributes->>'has_parent'='false' and sr.attributes->>'order_type'='REPLENISHMENT_ORDER' and sr.updated_on>'%s' and sr.updated_on<='%s' " %(start,end))

        cursor2.execute("select count(id) as single_olpn from service_request as sr left join service_request_children as src on sr.id=src.service_request_id left join "
                    "service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' and sr.state='complete' and "
                    "sr.attributes->>'order_type'='STREAMING_STORE_ORDER' AND sr.attributes->>'has_parent'='false' and sr.updated_on>'%s' and "
                        "sr.updated_on<='%s' "%(start,end))

        cursor3.execute("select count(id) as multi_olpn from service_request as sr left join service_request_children as src on sr.id=src.service_request_id left "
                    "join service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' and sr.state='complete' and "
                    "sr.attributes->>'order_type'='REPLENISHMENT_ORDER' AND sr.attributes->>'has_parent'='false' and sr.updated_on>'%s' and sr.updated_on<='%s'"%(start,end))

        cursor4.execute("select count(id) as single_olpn_breached from service_request as sr left join service_request_children as src on "
                    "sr.id=src.service_request_id left join service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' "
                    "and sr.state='complete' and (sr.attributes->>'order_type'='STREAMING_STORE_ORDER') AND "
                    "sr.attributes->>'has_parent'='false' and (extract(epoch from ((attributes::json->>'pick_before_time')::timestamp "
                    "at time zone 'UTC' )-((updated_on)::timestamp at time zone 'UTC' ))/3600)<0 and sr.updated_on>'%s' and sr.updated_on<='%s'"%(start,end))

        cursor5.execute("select count(id) as multi_olpn_breached from service_request as sr left join service_request_children as src on "
                    "sr.id=src.service_request_id left join service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' "
                    "and sr.state='complete' and (sr.attributes->>'order_type'='REPLENISHMENT_ORDER') AND "
                    "sr.attributes->>'has_parent'='false' and (extract(epoch from ((attributes::json->>'pick_before_time')::timestamp "
                    "at time zone 'UTC' )-((updated_on)::timestamp at time zone 'UTC' ))/3600)<0 and sr.updated_on>'%s' and sr.updated_on<='%s'"%(start,end))


        cursor6.execute("select  sum(stock_unit.quantity) as single_olpn_created_entities from "
                        "service_request, service_request_children  , container_stock_units, stock_unit,service_request_actuals,container where s"
                        "ervice_request .id = service_request_children.service_request_id and service_request_children.servicerequests_id = "
                        "service_request_actuals.service_request_id and container_stock_units.stockunits_id = stock_unit.id and service_request_actuals.actuals_id=container_stock_units.container_id and service_request_actuals.actuals_id=container.id "
                        "and service_request.type in ('PICK', 'ITEM_RECALL') and (service_request.attributes->>'order_type'='STREAMING_STORE_ORDER') and service_request.created_on>'%s' and service_request.created_on<='%s'"%(start,end))

        cursor7.execute("select count(id) as cancelled_status from service_request as sr left join service_request_children as src on sr.id=src.service_request_id left "
                    "join service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' and sr.status='CANCELLED' "
                    "AND sr.attributes->>'has_parent'='false' and sr.updated_on>'%s' and sr.updated_on<='%s' "%(start,end))

        cursor8.execute("select count(id) as Unfulfillable_status from service_request as sr left join service_request_children as src on sr.id=src.service_request_id left "
                    "join service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' and sr.status='FAILED' "
                    "AND sr.attributes->>'has_parent'='false' and sr.updated_on>'%s' and sr.updated_on<='%s'"%(start,end))

        cursor9.execute("select count(id) as created_status from service_request as sr left join service_request_children as src on sr.id=src.service_request_id left "
                    "join service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' and sr.status='CREATED' "
                    "AND sr.attributes->>'has_parent'='false' and sr.updated_on>'%s' and sr.updated_on<='%s'"%(start,end))

        cursor10.execute("select count(id) as processing_status from service_request as sr left join service_request_children as src on sr.id=src.service_request_id left "
                    "join service_request_children as src2 on src.servicerequests_id=src2.service_request_id where sr.type='PICK' and sr.status='PROCESSING' "
                    "AND sr.attributes->>'has_parent'='false' and sr.updated_on>'%s' and sr.updated_on<='%s'"%(start,end))


        cursor11.execute("select  sum(stock_unit.quantity) as multi_olpn_created_entities from "
                        "service_request, service_request_children  , container_stock_units, stock_unit,service_request_actuals,container where s"
                        "ervice_request .id = service_request_children.service_request_id and service_request_children.servicerequests_id = "
                        "service_request_actuals.service_request_id and container_stock_units.stockunits_id = stock_unit.id and service_request_actuals.actuals_id=container_stock_units.container_id and service_request_actuals.actuals_id=container.id "
                        "and service_request.type in ('PICK', 'ITEM_RECALL') and (service_request.attributes->>'order_type'='REPLENISHMENT_ORDER') and service_request.created_on>'%s' and service_request.created_on<='%s'"%(start,end))

        cursor12.execute("select  sum(stock_unit.quantity) as single_olpn_completed_entities "
                        "from service_request, service_request_children  , container_stock_units, stock_unit,service_request_actuals,container where "
                        "service_request .id = service_request_children.service_request_id and service_request_children.servicerequests_id = "
                        "service_request_actuals.service_request_id and container_stock_units.stockunits_id = stock_unit.id "
                        "and service_request_actuals.actuals_id =container_stock_units.container_id and service_request_actuals.actuals_id=container.id"
                        " and service_request.type in ('PICK', 'ITEM_RECALL') and service_request.state='complete' and "
                        "(service_request.attributes->>'order_type'='STREAMING_STORE_ORDER') and service_request.updated_on>'%s' and "
                        "service_request.updated_on<='%s'"%(start,end))

        cursor13.execute("select  sum(stock_unit.quantity) as multi_olpn_completed_entities "
                        "from service_request, service_request_children  , container_stock_units, stock_unit,service_request_actuals,container where "
                        "service_request .id = service_request_children.service_request_id and service_request_children.servicerequests_id = "
                        "service_request_actuals.service_request_id and container_stock_units.stockunits_id = stock_unit.id "
                        "and service_request_actuals.actuals_id =container_stock_units.container_id and service_request_actuals.actuals_id=container.id"
                        " and service_request.type in ('PICK', 'ITEM_RECALL') and service_request.state='complete' and service_request.updated_on>'%s' and "
                        "service_request.updated_on<='%s' "%(start,end))
        
        
        Single_olpn_created = cursor0.fetchall()
        Multi_olpn_created = cursor1.fetchall()
        Single_OLPN = cursor2.fetchall()
        Multi_OLPN = cursor3.fetchall()
        Single_olpn_breach = cursor4.fetchall()
        Multi_olpn_breach = cursor5.fetchall()
        Created_single_olpn_entities=cursor6.fetchall()
        Cancelled = cursor7.fetchall()
        Failed = cursor8.fetchall()
        Created = cursor9.fetchall()
        Processing = cursor10.fetchall()
        Created_multi_olpn_entities = cursor11.fetchall()
        Single_olpn_completed_entities = cursor12.fetchall()
        Multi_olpn_completed_entities = cursor13.fetchall()
        self.connection.close()
        df0 = pd.DataFrame(Single_olpn_created, columns=['single_olpn_created'])
        df1 = pd.DataFrame(Multi_olpn_created, columns=['multi_olpn_created'])
        df2 = pd.DataFrame(Single_OLPN, columns=['single_olpn_completed'])
        df3 = pd.DataFrame(Multi_OLPN, columns=['multi_olpn_completed'])
        df4 = pd.DataFrame(Single_olpn_breach, columns=['single_olpn_breach'])
        df5 = pd.DataFrame(Multi_olpn_breach, columns=['multi_olpn_breached'])
        df6 = pd.DataFrame(Created_single_olpn_entities, columns=['single_olpn_created_entities'])
        df7 = pd.DataFrame(Cancelled, columns=['cancelled_status'])
        df8 = pd.DataFrame(Failed, columns=['unfulfillable_status'])
        df9 = pd.DataFrame(Created, columns=['created_status'])
        df10 = pd.DataFrame(Processing, columns=['processing_status'])
        df11 = pd.DataFrame(Created_multi_olpn_entities, columns=['multi_olpn_created_entities'])
        df12 = pd.DataFrame(Single_olpn_completed_entities, columns=['single_olpn_completed_entities'])
        df13 = pd.DataFrame(Multi_olpn_completed_entities, columns=['multi_olpn_completed_entities'])
        dff=pd.concat([df0,df1,df2,df3,df4,df5,df6,df11,df12,df13],sort=False)

        dff['Site'] = self.tenant_info["Name"]
        dff['time'] = self.end_date

        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db=self.tenant_info["out_db_name"])
        
        somelist = []
        querylist = [
        "select sum(value) as single_item_picked from item_picked where  order_flow_name='STORE_STREAMING_EACH_ORDER' and time>='%s' and time<='%s'" % (
            start, end),
        "select sum(value) as multi_item_picked from item_picked where  order_flow_name='replenishment_flow'  and time>='%s' and time<='%s'" % (
            start, end)]
        
        for q in querylist:
            df1=pd.DataFrame(self.read_client.query(q).get_points())
            if not df1.empty:
                somelist.append(df1)
        
        df2 = pd.concat(somelist, sort=False)
        df2['time'] = self.end_date

        df=pd.concat([dff,df2],sort = False)
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "olpn_orders_info", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=None, dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        print("data inserted")
        return None
        
with DAG(
    'olpn_orders_info',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'olpn orders ino',
    schedule_interval = timedelta(days=1),
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
            if tenant['Active'] == "Y" and tenant['olpn_orders_info'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                olpn_orders_info_final_task = PythonOperator(
                    task_id='olpn_orders_info_final{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(OlpnOrdersInfo().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":"dev_postgres", "influx_ip":"dev_influxdb", "influx_port":8086,\
        #           "write_influx_ip":"dev_influxdb","write_influx_port":8086, \
        #           "out_db_name":"airflow", "alteryx_out_db_name": "airflow"}
        tenant = CommonFunction().get_tenant_info()
        olpn_orders_info_final_task = PythonOperator(
            task_id='olpn_orders_info_final',
            provide_context=True,
            python_callable=functools.partial(OlpnOrdersInfo().final_call,tenant_info={'tenant_info': tenant}),
            # op_kwargs={
            #     'tenant_info': tenant,
            # },
            execution_timeout=timedelta(seconds=3600),
        )

