from unittest.mock import patch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from put_events import put_events
from utils.CommonFunction import CommonFunction
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from influxdb.resultset import ResultSet
from test_cases.unit_test_cases.setup import TestSetupDBDag

DEFAULT_DATE = datetime.now(timezone.utc)-timedelta(days=1)


class Test_put_events(TestSetupDBDag):


    def method_to_mock_query(*args,**kwargs):
        if 'select *  from put_events' and 'limit 1' in args[0]:
            return ResultSet({})
        elif  'select *  from put_events' and 'order by time desc' in args[0]:
            data = [{'name': 'put_events',
                    'columns': ['time', 'event_name','externalServiceRequestId','host','installation_id','item_id','possible_amount','put_id','put_quantity','storage_node_type','value'],
                    'values': [['2023-11-28T08:00:25.561527486Z','put_calculated',None,'tsgvm02363','butler_demo',None,'2','59854694',None,None,0]]}]
            return ResultSet({'series': data})



    @patch("utils.CommonFunction.Write_InfluxData.writepoints")
    @patch("utils.CommonFunction.InfluxData.query",side_effect=method_to_mock_query)
    @patch("utils.CommonFunction.InfluxData.get_start_date")
    @patch("utils.CommonFunction.InfluxData.is_influx_reachable")
    @patch("utils.CommonFunction.CommonFunction.get_tenant_info")
    def test_put_events(self,mock_get_tenant_info,mock_is_influx_reachable,mock_get_start_date,mock_query,mock_writepoints):
        mock_get_tenant_info.return_value = self.expected_influx_data
        mock_is_influx_reachable.return_value = True
        mock_get_start_date.return_value = datetime.now(timezone.utc)
        mock_writepoints.return_value=''

        with DAG(
                'test_put_events',
                default_args=CommonFunction().get_default_args_for_dag(),
                description='put events',
                catchup=False,
                schedule=None,
        ) as dag:
            import functools
            tenant = CommonFunction().get_tenant_info()
            PythonOperator(
                task_id='put_events_final',
                python_callable=functools.partial(put_events().put_events_final,
                                                  tenant_info={'tenant_info': tenant}),
                execution_timeout=timedelta(seconds=3600),
            )
        dagrun = dag.create_dagrun(state=DagRunState.RUNNING,
                                   execution_date=DEFAULT_DATE,
                                   start_date=DEFAULT_DATE,
                                   run_type=DagRunType.MANUAL)
        ti = dagrun.get_task_instance(task_id='put_events_final')
        ti.task = dag.get_task(task_id='put_events_final')
        ti.run(ignore_ti_state=True)
