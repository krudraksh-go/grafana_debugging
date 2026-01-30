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


class cls_system_order_stats:
    def system_order_stats_func(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        if self.tenant_info['is_ttp_setup']=='Y':
            QUERIES = {
            'remaining_orders': "select station_type,storage_type,count(*) from service_request where status in ('CREATED') and type='PICK' GROUP BY station_type,storage_type;",
            'remaining_orderlines': "select station_type,storage_type, count(*) from service_request where status in ('CREATED') and type='PICK_LINE' GROUP BY station_type,storage_type;",
            'processing_orders': "select station_type,storage_type, count(*) from service_request where status in ('PROCESSING') and type='PICK' GROUP BY station_type,storage_type;",
            'processing_orderlines': "select station_type,storage_type, count(*) from service_request where status in ('PROCESSING') and type='PICK_LINE' GROUP BY station_type,storage_type;",
            'paused_order': "select station_type,storage_type, count(*) from service_request where status in ('PAUSED','WAITING') and type='PICK' GROUP BY station_type,storage_type;",
            'paused_orderlines': "select station_type,storage_type, count(*) from service_request where status in ('PAUSED','WAITING') and type='PICK_LINE' GROUP BY station_type,storage_type;",
            'completed_order': "select station_type,storage_type, count(*) from service_request where status in ('PROCESSED') and type='PICK' and updated_on > (CURRENT_DATE) GROUP BY station_type,storage_type;",
            'completed_orderlines': "select station_type,storage_type, count(*) from service_request where status in ('PROCESSED') and type='PICK_LINE' and updated_on > (CURRENT_DATE) GROUP BY station_type,storage_type;",
            'breached_orders': "select station_type,storage_type, count(id) from service_request where status in ('CREATED','PROCESSING','WAITING') and type='PICK' and (((attributes::json->>'pick_before_time')::timestamp)<now()::timestamp at time zone 'UTC') GROUP BY station_type,storage_type;",
            'remaining_items': "select foo.station_type,foo.storage_type,sum1 - sum2 as picks_remaining \
                            from (select station_type,storage_type,coalesce(sum(quantity),0) as sum1 from service_request left join service_request_expectations on service_request.id=service_request_expectations.service_request_id left join container_stock_units on container_stock_units.container_id=service_request_expectations.expectations_id left join stock_unit on stock_unit.id=container_stock_units.stockunits_id where service_request.type='PICK_LINE' and service_request.status IN ('PROCESSING', 'WAITING', 'CREATED') GROUP BY station_type,storage_type) as foo,\
                            (select station_type,storage_type, coalesce(sum(quantity),0) as sum2 from service_request left join service_request_actuals on service_request.id=service_request_actuals.service_request_id left join container_stock_units on container_stock_units.container_id=service_request_actuals.actuals_id left join stock_unit on stock_unit.id=container_stock_units.stockunits_id where service_request.type='PICK_LINE' and service_request.status IN ('PROCESSING', 'WAITING', 'CREATED') GROUP BY station_type,storage_type) as bar on foo.station_type=bar.station_type and foo.storage_type=bar.storage_type",\
            'qty_per_orderline': "select station_type,storage_type, avg(stock_unit.quantity) from service_request , service_request_children , service_request_expectations , container_stock_units , stock_unit where service_request .id = service_request_children.service_request_id and service_request_children.servicerequests_id = service_request_expectations.service_request_id and service_request_expectations.expectations_id = container_stock_units.container_id and container_stock_units.stockunits_id = stock_unit.id and service_request.status in ('CREATED') GROUP BY station_type,storage_type;",
            'qty_per_orderline_completed': "select station_type,storage_type, coalesce(avg(stock_unit.quantity),0) from service_request , service_request_children , service_request_expectations , container_stock_units , stock_unit where service_request .id = service_request_children.service_request_id and service_request_children.servicerequests_id = service_request_expectations.service_request_id and service_request_expectations.expectations_id = container_stock_units.container_id and container_stock_units.stockunits_id = stock_unit.id and service_request.status in ('PROCESSED') and service_request.updated_on > (CURRENT_DATE) GROUP BY station_type,storage_type;",
            # 'pick': "select type as \"Type\",count(*) as \"Count\" from service_request where status in ('CREATED','PROCESSING') and type in ('PICK','PICK_LINE');"
        }
        else:
            QUERIES = {
            'remaining_orders': "select count(*) from service_request where status in ('CREATED') and type='PICK' ;",
            'remaining_orderlines': "select count(*) from service_request where status in ('CREATED') and type='PICK_LINE';",
            'processing_orders': "select count(*) from service_request where status in ('PROCESSING') and type='PICK' ;",
            'processing_orderlines': "select count(*) from service_request where status in ('PROCESSING') and type='PICK_LINE';",
            'paused_order': "select count(*) from service_request where status in ('PAUSED','WAITING') and type='PICK' ;",
            'paused_orderlines': "select count(*) from service_request where status in ('PAUSED','WAITING') and type='PICK_LINE';",
            'completed_order': "select count(*) from service_request where status in ('PROCESSED') and type='PICK' and updated_on > (CURRENT_DATE) ;",
            'completed_orderlines': "select count(*) from service_request where status in ('PROCESSED') and type='PICK_LINE' and updated_on > (CURRENT_DATE);",
            'breached_orders': "select count(id) from service_request where status in ('CREATED','PROCESSING','WAITING') and type='PICK' and (((attributes::json->>'pick_before_time')::timestamp)<now()::timestamp at time zone 'UTC') ;",
            'remaining_items': "select sum1 - sum2 as picks_remaining from (select coalesce(sum(quantity),0) as sum1 from service_request left join service_request_expectations on service_request.id=service_request_expectations.service_request_id left join container_stock_units on container_stock_units.container_id=service_request_expectations.expectations_id left join stock_unit on stock_unit.id=container_stock_units.stockunits_id where service_request.type='PICK_LINE' and service_request.status IN ('PROCESSING', 'WAITING', 'CREATED')) as foo, (select coalesce(sum(quantity),0) as sum2 from service_request left join service_request_actuals on service_request.id=service_request_actuals.service_request_id left join container_stock_units on container_stock_units.container_id=service_request_actuals.actuals_id left join stock_unit on stock_unit.id=container_stock_units.stockunits_id where service_request.type='PICK_LINE' and service_request.status IN ('PROCESSING', 'WAITING', 'CREATED')) as bar;",
            'qty_per_orderline': "select avg(stock_unit.quantity) from service_request , service_request_children , service_request_expectations , container_stock_units , stock_unit where service_request .id = service_request_children.service_request_id and service_request_children.servicerequests_id = service_request_expectations.service_request_id and service_request_expectations.expectations_id = container_stock_units.container_id and container_stock_units.stockunits_id = stock_unit.id and service_request.status in ('CREATED');",
            'qty_per_orderline_completed': "select coalesce(avg(stock_unit.quantity),0) from service_request , service_request_children , service_request_expectations , container_stock_units , stock_unit where service_request .id = service_request_children.service_request_id and service_request_children.servicerequests_id = service_request_expectations.service_request_id and service_request_expectations.expectations_id = container_stock_units.container_id and container_stock_units.stockunits_id = stock_unit.id and service_request.status in ('PROCESSED') and service_request.updated_on > (CURRENT_DATE);",
            # 'pick': "select type as \"Type\",count(*) as \"Count\" from service_request where status in ('CREATED','PROCESSING') and type in ('PICK','PICK_LINE');"
        }

        if self.tenant_info['Postgres_pf_user'] != "":
            conn = postgres_connection(database='platform_srms',
                                                     user=self.tenant_info['Postgres_pf_user'], \
                                                     sslrootcert=self.tenant_info['sslrootcert'],
                                                     sslcert=self.tenant_info['sslcert'], \
                                                     sslkey=self.tenant_info['sslkey'],
                                                     host=self.tenant_info['Postgres_pf'], \
                                                     port=self.tenant_info['Postgres_pf_port'],
                                                     password=self.tenant_info['Postgres_pf_password'],
                                                     dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        cur = conn.pg_curr
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        data = {}
        for item, query in QUERIES.items():
            cur.execute(query)
            if self.tenant_info['is_ttp_setup']=='Y':
                data[item] = cur.fetchone()[2]
                # data['station_type'] = cur.fetchone()[0]
                # data['storage_type'] = cur.fetchone()[1]
            else:
                data[item] = cur.fetchone()[0]
                # data['station_type'] = 'rtp'
                # data['storage_type'] = 'msu'

        #logger.warning(current_time + str(data))

        params = list(map(lambda k: '{}={}'.format(k[0], k[1]), data.items()))
        #logger.warning(current_time + str(params))
        data_string = 'system_order_stats %s' % (','.join(params))
        INFLUX_URL = "http://" + self.tenant_info["write_influx_ip"] + ":" + str(self.tenant_info["write_influx_port"])
        url_string = INFLUX_URL + f'/write?db={self.tenant_info["out_db_name"]}'
        #logger.warning(current_time + str(data_string))
        response = requests.post(url_string, data=data_string)
        #logger.warning(current_time + str(response.text))
        #logger.warning(current_time + str(response.status_code))
        cur.close()
        conn.close()
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'system_order_stats_only_for_on_cloud_sites',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of system_order_stats',
    schedule_interval = '*/1 * * * * ',
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
            if tenant['Active'] == "Y" and tenant['system_order_stats'] == "Y":
                system_order_stats_final_task = PythonOperator(
                    task_id='system_order_stats_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(cls_system_order_stats().system_order_stats_func,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        system_order_stats_final_task = PythonOperator(
            task_id='system_order_stats_final',
            provide_context=True,
            python_callable=functools.partial(cls_system_order_stats().system_order_stats_func,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=600),
        )

