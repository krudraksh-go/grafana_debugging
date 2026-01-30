from unittest.mock import patch,Mock
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from butler_charger_uptime import butler_charger_uptime
from utils.CommonFunction import CommonFunction
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from test_cases.unit_test_cases.setup import TestSetupDBDag


DEFAULT_DATE = datetime.now(timezone.utc)-timedelta(days=1)

class Test_butler_charger_uptime(TestSetupDBDag):
    @patch("utils.CommonFunction.InfluxData.write_points")
    @patch("utils.CommonFunction.Butler_api.fetch_chargeruptime_data")
    @patch("utils.CommonFunction.Butler_api.fetch_butleruptime_data")
    @patch("utils.CommonFunction.InfluxData.is_influx_reachable")
    @patch("utils.CommonFunction.CommonFunction.get_tenant_info")
    def test_butler_charger_uptime(self, mock_get_tenant_info,mock_is_influx_reachable,mock_fetch_butleruptime_data,mock_fetch_chargeruptime_data,mock_writepoints):
        mock_get_tenant_info.return_value = self.expected_influx_data
        mock_is_influx_reachable.return_value = True
        mock_fetch_butleruptime_data.return_value = {'measurement': 'butler_uptime', 'tags': {'bot_id': 7073}, 'time': '2023-12-01T19:33:45', 'fields': {'tasktype': 'null', 'power_state': 'online', 'bot_id_int': 7073, 'manual_paused': False, 'low_charged_paused': False, 'nav_status_state': 'info', 'bot_state': 'ready', 'address': '172.25.105.90'}}
        mock_fetch_chargeruptime_data.return_value = [{'measurement': 'charger_uptime', 'tags': {'charger_id': 15}, 'time': '2023-12-03T18:13:59', 'fields': {'connectivity': 'connected', 'state': 'auto', 'error_condition': 'undefined'}}]
        mock_writepoints.return_value=''

        with DAG(
                'butler_charger_uptime_only_for_on_cloud_sites',
                default_args=CommonFunction().get_default_args_for_dag(),
                description='calculation of butler_charger_uptime',
                catchup=False,
                schedule=None,
        ) as dag:
            import functools
            tenant = CommonFunction().get_tenant_info()
            PythonOperator(
                task_id='butler_charger_uptime_final',
                provide_context=True,
                python_callable=functools.partial(butler_charger_uptime().butler_charger_uptime,tenant_info={'tenant_info': tenant}),
                op_kwargs={
                    'tenant_info1': tenant,
                },
                execution_timeout=timedelta(seconds=3600),
            )
        dagrun = dag.create_dagrun(state=DagRunState.RUNNING,
                                   execution_date=DEFAULT_DATE,
                                   start_date=DEFAULT_DATE,
                                   run_type=DagRunType.MANUAL)
        ti = dagrun.get_task_instance(task_id='butler_charger_uptime_final')
        ti.task = dag.get_task(task_id='butler_charger_uptime_final')
        ti.run(ignore_ti_state=True)