from unittest.mock import patch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from completion_to_release import CompletionToRelease
from utils.CommonFunction import CommonFunction
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from influxdb.resultset import ResultSet
from test_cases.unit_test_cases.setup import TestSetupDBDag

DEFAULT_DATE = datetime.now(timezone.utc)-timedelta(days=1)

class Test_completion_to_release(TestSetupDBDag):

    def method_to_mock_query(*args,**kwargs):

        if 'select * from order_status_log' and 'limit 1' in args[0]:
            return ResultSet({})
        
        elif 'select * from order_status_log' in args[0]:
            data = [{'name': 'order_status_log',
                     'columns': ['time', 'bin_id','host','installation_id','order_id','order_status','pps_id','value'],
                       'values': [['2023-03-14T15:59:47.279355Z','19','tsgvm02363','butler_demo','48070498','complete','51',0]]}]
            return ResultSet({'series': data})
        
        elif 'select * from ppsbin_log' in args[0]:
            data = [{'name': 'ppsbin_log',
                     'columns': ['time', 'bin_id','bin_status','host','installation_id','order_id','pps_id','value'],
                       'values': [['2023-03-14T15:59:52.3714Z','24','free','tsgvm02363','butler_demo','48077205','41',0]]}]
            return ResultSet({'series': data})

    @patch("utils.CommonFunction.Write_InfluxData.writepoints")
    @patch("utils.CommonFunction.InfluxData.query",side_effect=method_to_mock_query)
    @patch("utils.CommonFunction.InfluxData.get_start_date")
    @patch("utils.CommonFunction.InfluxData.is_influx_reachable")
    @patch("utils.CommonFunction.CommonFunction.get_tenant_info")
    def test_completion_to_release(self,mock_get_tenant_info,mock_is_influx_reachable,mock_get_start_date,mock_query,mock_writepoints):
        mock_get_tenant_info.return_value = self.expected_influx_data
        mock_is_influx_reachable.return_value = True
        mock_get_start_date.return_value = datetime.now(timezone.utc)
        mock_writepoints.return_value=''

        with DAG(
                'completion_to_release',
                default_args=CommonFunction().get_default_args_for_dag(),
                description='completion to release',
                catchup=False,
                schedule=None,
        ) as dag:
            import functools
            tenant = CommonFunction().get_tenant_info()
            PythonOperator(
                task_id='completion_to_release_final',
                python_callable=functools.partial(CompletionToRelease().final_call,
                                                  tenant_info={'tenant_info': tenant}),
                execution_timeout=timedelta(seconds=3600),
            )
        dagrun = dag.create_dagrun(state=DagRunState.RUNNING,
                                   execution_date=DEFAULT_DATE,
                                   start_date=DEFAULT_DATE,
                                   run_type=DagRunType.MANUAL)
        ti = dagrun.get_task_instance(task_id='completion_to_release_final')
        ti.task = dag.get_task(task_id='completion_to_release_final')
        ti.run(ignore_ti_state=True)


