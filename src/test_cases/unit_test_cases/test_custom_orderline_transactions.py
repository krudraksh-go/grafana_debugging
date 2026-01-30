from unittest.mock import patch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from custom_orderline_transactions import custom_Orderline_transactions
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
        
        elif 'select order_id,item_id,pps_id, installation_id,host,order_flow_name,uom_requested from item_picked' in args[0]:
            data = [{'name': 'item_picked',
                    'columns': ['time', 'order_id','item_id','pps_id','installation_id','host','order_flow_name','uom_requested'],
                    'values': [['2023-11-28T08:00:25.561527486Z','59811632','191997','30','butler_demo','tsgvm02363','Sector1','Item']]}]
            return ResultSet({'series': data})

    @patch("utils.CommonFunction.Write_InfluxData.writepoints")
    @patch("utils.CommonFunction.InfluxData.query",side_effect=method_to_mock_query)
    @patch("utils.CommonFunction.InfluxData.get_start_date")
    @patch("utils.CommonFunction.InfluxData.is_influx_reachable")
    @patch("utils.CommonFunction.CommonFunction.get_tenant_info")
    def test_custom_orderline_transactions_sector(self,mock_get_tenant_info,mock_is_influx_reachable,mock_get_start_date,mock_query,mock_writepoints):
        mock_get_tenant_info.return_value = self.expected_influx_data
        mock_is_influx_reachable.return_value = True
        mock_get_start_date.return_value = datetime.now(timezone.utc)
        mock_writepoints.return_value=''

        with DAG(
                'custom_Orderline_transactions',
                default_args=CommonFunction().get_default_args_for_dag(),
                description='calculation of Orderline transactions',
                catchup=False,
                schedule=None,
        ) as dag:
            import functools
            tenant = CommonFunction().get_tenant_info()
            PythonOperator(
                task_id='custom_Orderline_transactions_final',
                python_callable=functools.partial(custom_Orderline_transactions().custom_Orderline_transactions,
                                                  tenant_info={'tenant_info': tenant}),
                execution_timeout=timedelta(seconds=3600),
            )
        dagrun = dag.create_dagrun(state=DagRunState.RUNNING,
                                   execution_date=DEFAULT_DATE,
                                   start_date=DEFAULT_DATE,
                                   run_type=DagRunType.MANUAL)
        ti = dagrun.get_task_instance(task_id='custom_Orderline_transactions_final')
        ti.task = dag.get_task(task_id='custom_Orderline_transactions_final')
        ti.run(ignore_ti_state=True)
