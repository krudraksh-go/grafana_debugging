from unittest.mock import patch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from custom_item_exceptions_with_sector import custom_item_exceptions_with_sector
from utils.CommonFunction import CommonFunction
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from influxdb.resultset import ResultSet
from test_cases.unit_test_cases.setup import TestSetupDBDag

DEFAULT_DATE = datetime.now(timezone.utc)-timedelta(days=1)

class Test_custom_item_exception_with_sector(TestSetupDBDag):

    def method_to_mock_query(*args,**kwargs):
        if 'select *  from item_picked' in args[0]:
            return ResultSet({})
        
        elif 'select order_id, installation_id,order_flow_name,uom_requested from item_picked' in args[0]:
            data = [{'name': 'item_picked',
                    'columns': ['time', 'order_id','installation_id','order_flow_name','uom_requested'],
                    'values': [['2023-11-28T08:00:25.561527486Z','59811632','butler_demo','Sector1','Item']]}]
            return ResultSet({'series': data})
        
        elif 'select * from item_exceptions' in args[0]:

            data = [{'name': 'item_exception',
                    'columns': ['time', 'bin_id','host','installation_id','inventory_adjusted','item_id','mode','order_id','pps_id','quantity','rack_id','slotref','type','value'],
                    'values': [['2023-11-28T07:49:09.157014Z','22','tsgvm02363','butler_demo','true','763483','pick','59811021','38','1','14751','14751.0.C.05-C.06','missing',1]]}]
            return ResultSet({'series': data})


    @patch("utils.CommonFunction.Write_InfluxData.writepoints")
    @patch("utils.CommonFunction.InfluxData.query",side_effect=method_to_mock_query)
    @patch("utils.CommonFunction.InfluxData.get_start_date")
    @patch("utils.CommonFunction.InfluxData.is_influx_reachable")
    @patch("utils.CommonFunction.CommonFunction.get_tenant_info")
    def test_custom_item_exceptions_with_sector(self,mock_get_tenant_info,mock_is_influx_reachable,mock_get_start_date,mock_query,mock_writepoints):
        mock_get_tenant_info.return_value = self.expected_influx_data
        mock_is_influx_reachable.return_value = True
        mock_get_start_date.return_value = datetime.now(timezone.utc)
        mock_writepoints.return_value=''

        with DAG(
                'custom_item_exception_with_sector',
                default_args=CommonFunction().get_default_args_for_dag(),
                description='custom item exception',
                catchup=False,
                schedule=None,
        ) as dag:
            import functools
            tenant = CommonFunction().get_tenant_info()
            PythonOperator(
                task_id='custom_item_exception_final',
                python_callable=functools.partial(custom_item_exceptions_with_sector().custom_item_exceptions_with_sector,
                                                  tenant_info={'tenant_info': tenant}),
                execution_timeout=timedelta(seconds=3600),
            )
        dagrun = dag.create_dagrun(state=DagRunState.RUNNING,
                                   execution_date=DEFAULT_DATE,
                                   start_date=DEFAULT_DATE,
                                   run_type=DagRunType.MANUAL)
        ti = dagrun.get_task_instance(task_id='custom_item_exception_final')
        ti.task = dag.get_task(task_id='custom_item_exception_final')
        ti.run(ignore_ti_state=True)
