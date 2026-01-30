from unittest.mock import patch, MagicMock
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from put_per_rack_face import put_per_rack_face
from utils.CommonFunction import CommonFunction, InfluxData ,Write_InfluxData
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from influxdb.resultset import ResultSet
from test_cases.unit_test_cases.setup import TestSetupDBDag

DEFAULT_DATE = datetime.now(timezone.utc)-timedelta(days=1)

class TestPut_per_rack_face(TestSetupDBDag):
    def method_to_mock_Write_InfluxData(*args, **kwargs):
        return ''
    def my_function_mock(*args, **kwargs):
        if 'select * from item_put' in args[0]:
            return ResultSet({})
        elif 'select installation_id,pps_id' in args[0]:
            data = [{'name': 'item_put', 'columns': ['time', 'installation_id', 'pps_id', 'uom_quantity_int', 'slotref', 'value', 'item_id', 'order_id', 'uom_quantity'], 'values': [['2023-11-24T05:50:54.968000698Z', 'butler_demo', '18', 4, '13669.1.A.04-A.05-A.06', 4, '303542', None, None]]}]

            #return [{'time': '2023-11-21T12:25:56.315691979Z',  'installation_id': 'butler_demo', 'item_id': '347150',  'order_id': '4370220',  'pps_id': '11',  'slotref': '356.3.E.03-E.04',  'uom_quantity_int': 1,  'value': 1 }]
            return ResultSet({'series': data})
        

    @patch("utils.CommonFunction.Write_InfluxData.writepoints",side_effect=method_to_mock_Write_InfluxData)
    @patch("utils.CommonFunction.InfluxData.get_datetime_interval3")
    @patch("utils.CommonFunction.InfluxData.query",side_effect=my_function_mock)
    @patch("utils.CommonFunction.InfluxData.get_start_date")
    @patch("utils.CommonFunction.InfluxData.is_influx_reachable")
    @patch("utils.CommonFunction.CommonFunction.get_tenant_info")
    def test_pick_per_rack_face(self,  mock_get_tenant_info,mock_is_influx_reachable, mock_get_start_date,mock_query, mock_get_datetime_interval3,mock_writepoints):
        # Mock the data you expect to receive from InfluxDB
        import json
        mock_get_tenant_info.return_value = self.expected_influx_data
        mock_is_influx_reachable.return_value = True
        mock_get_start_date.return_value=datetime.now(timezone.utc)
        #mock_query.return_value = json.dumps({"('item_picked', None)": [{'time': '2023-11-21T12:25:56.315691979Z', 'bin_barcode': '', 'bin_id': '5', 'bintags': '-', 'face_id': '356.3', 'fulfilment_area': 'gtp', 'host': 'butler-core-dillardprod-prod', 'installation_id': 'butler_demo', 'item_id': '347150', 'mass_kg': 0.18, 'order_flow_name': 'default', 'order_id': '4370220', 'physical_rack_type': 'msu', 'pick_qty_int': 1, 'piggyback': 'undefined', 'popularity_bucket': 'default', 'popularity_score': 0.15, 'pps_id': '11', 'product_tags': '{}', 'rack_id': '356', 'racktype': 'Dillards_MSU_Type_1', 'slotref': '356.3.E.03-E.04', 'uom_picked': 'Item', 'uom_quantity_int': 1, 'uom_requested': 'undefined', 'user_id': '909639523', 'value': 1, 'volume_cm3': 1622.32}, {'time': '2023-11-21T12:25:42.793805913Z', 'bin_barcode': '', 'bin_id': '34', 'bintags': '-', 'face_id': '388.1', 'fulfilment_area': 'gtp', 'host': 'butler-core-dillardprod-prod', 'installation_id': 'butler_demo', 'item_id': '404705', 'mass_kg': 0.11, 'order_flow_name': 'default', 'order_id': '4370261', 'physical_rack_type': 'msu', 'pick_qty_int': 1, 'piggyback': 'undefined', 'popularity_bucket': 'C', 'popularity_score': 0.41, 'pps_id': '11', 'product_tags': '{}', 'rack_id': '388', 'racktype': 'Dillards_MSU_Type_1', 'slotref': '388.1.F.03', 'uom_picked': 'Item', 'uom_quantity_int': 1, 'uom_requested': 'undefined', 'user_id': '909639523', 'value': 1, 'volume_cm3': 1238.45}]})
        data=[{'time': '2023-11-21T12:25:56.315691979Z', 'bin_barcode': '', 'bin_id': '5', 'bintags': '-', 'face_id': '356.3', 'fulfilment_area': 'gtp', 'host': 'butler-core-dillardprod-prod', 'installation_id': 'butler_demo', 'item_id': '347150', 'mass_kg': 0.18, 'order_flow_name': 'default', 'order_id': '4370220', 'physical_rack_type': 'msu', 'pick_qty_int': 1, 'piggyback': 'undefined', 'popularity_bucket': 'default', 'popularity_score': 0.15, 'pps_id': '11', 'product_tags': '{}', 'rack_id': '356', 'racktype': 'Dillards_MSU_Type_1', 'slotref': '356.3.E.03-E.04', 'uom_picked': 'Item', 'uom_quantity_int': 1, 'uom_requested': 'undefined', 'user_id': '909639523', 'value': 1, 'volume_cm3': 1622.32}, {'time': '2023-11-21T12:25:42.793805913Z', 'bin_barcode': '', 'bin_id': '34', 'bintags': '-', 'face_id': '388.1', 'fulfilment_area': 'gtp', 'host': 'butler-core-dillardprod-prod', 'installation_id': 'butler_demo', 'item_id': '404705', 'mass_kg': 0.11, 'order_flow_name': 'default', 'order_id': '4370261', 'physical_rack_type': 'msu', 'pick_qty_int': 1, 'piggyback': 'undefined', 'popularity_bucket': 'C', 'popularity_score': 0.41, 'pps_id': '11', 'product_tags': '{}', 'rack_id': '388', 'racktype': 'Dillards_MSU_Type_1', 'slotref': '388.1.F.03', 'uom_picked': 'Item', 'uom_quantity_int': 1, 'uom_requested': 'undefined', 'user_id': '909639523', 'value': 1, 'volume_cm3': 1238.45}]
        results = [
            ResultSet(result, raise_errors=True)
            for result in data
        ]
        mock_query.return_value =results
        mock_get_datetime_interval3.return_value={}
        mock_writepoints.return_value=''
        with DAG(
                'Put_per_rack_face',
                default_args=CommonFunction().get_default_args_for_dag(),
                description='calculation of picks per rack face GM-44025',
                catchup=False,
                schedule=None,
        ) as dag:
            import csv
            import os
            import functools

            tenant = CommonFunction().get_tenant_info()
            PythonOperator(
                task_id='puts_per_rack_final',
                python_callable=functools.partial(put_per_rack_face().puts_per_rack_final,
                                                  tenant_info={'tenant_info': tenant}),
                execution_timeout=timedelta(seconds=3600),
            )
        dagrun = dag.create_dagrun(state=DagRunState.RUNNING,
                                   execution_date=DEFAULT_DATE,
                                   # data_interval=DEFAULT_DATE,
                                   start_date=DEFAULT_DATE,
                                   run_type=DagRunType.MANUAL)
        ti = dagrun.get_task_instance(task_id='puts_per_rack_final')
        ti.task = dag.get_task(task_id='puts_per_rack_final')
        ti.run(ignore_ti_state=True)
