from unittest.mock import patch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from cell_voltages_ayx_logic import cell_voltages
from utils.CommonFunction import CommonFunction
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from influxdb.resultset import ResultSet
from test_cases.unit_test_cases.setup import TestSetupDBDag
DEFAULT_DATE = datetime.now(timezone.utc)-timedelta(days=1)

class Test_cell_voltages_ayx(TestSetupDBDag):

    def method_to_mock_query(*args,**kwargs):
        if 'limit 1' in args[0]:
            return ResultSet({})

        elif 'SHOW FIELD KEYS FROM' in args[0]:
            data = [{'name': 'call_voltage_field',
                     'columns': ['fieldKey', 'fieldType'], 'values': [
                    ['host','string'],['installation_id','string'],['max_cell_voltage','string'],['min_cell_voltage','string'],['value','float']]}]
            return ResultSet({'series': data})

    def method_to_mock_fetch_data(*args,**kwargs):
        if 'select * from cell_voltages_delta_alert' in args[1]:
            data = [{'time': '2023-11-26T15:51:59.986205Z', 'butler_id': '5851', 'host': 'tsgvm02363', 'installation_id': 'butler_demo', 'is_charging': 'true', 'max_cell_voltage': '3474', 'min_cell_voltage': '3437', 'value': 0}]
            return data
        else:
            pass
            

    @patch("utils.CommonFunction.Write_InfluxData.writepoints")
    @patch("utils.CommonFunction.InfluxData.fetch_data",side_effect=method_to_mock_fetch_data)
    @patch("utils.CommonFunction.InfluxData.query",side_effect=method_to_mock_query)
    @patch("utils.CommonFunction.InfluxData.get_start_date")
    @patch("utils.CommonFunction.InfluxData.is_influx_reachable")
    @patch("utils.CommonFunction.CommonFunction.get_tenant_info")
    def test_cell_voltages_ayx(self,mock_get_tenant_info,mock_is_influx_reachable,mock_get_start_date,mock_query, mock_fetch_data,mock_writepoints):
        mock_get_tenant_info.return_value = self.expected_influx_data
        mock_is_influx_reachable.return_value = True
        mock_get_start_date.return_value = datetime.now(timezone.utc)
        mock_writepoints.return_value=''

        with DAG(
                'cell_voltages',
                default_args=CommonFunction().get_default_args_for_dag(),
                description='calculation of cell voltage',
                catchup=False,
                schedule=None,
        ) as dag:
            import functools
            tenant = CommonFunction().get_tenant_info()
            PythonOperator(
                task_id='cell_voltages_final',
                python_callable=functools.partial(cell_voltages().cell_voltages,
                                                  tenant_info={'tenant_info': tenant}),
                execution_timeout=timedelta(seconds=3600),
            )
        dagrun = dag.create_dagrun(state=DagRunState.RUNNING,
                                   execution_date=DEFAULT_DATE,
                                   start_date=DEFAULT_DATE,
                                   run_type=DagRunType.MANUAL)
        ti = dagrun.get_task_instance(task_id='cell_voltages_final')
        ti.task = dag.get_task(task_id='cell_voltages_final')
        ti.run(ignore_ti_state=True)



