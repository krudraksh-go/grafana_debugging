from unittest.mock import patch, MagicMock
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from barcode_travelled import barcode_travelled
from utils.CommonFunction import CommonFunction, InfluxData ,Write_InfluxData
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from influxdb.resultset import ResultSet
from test_cases.unit_test_cases.setup import TestSetupDBDag
import pandas as pd

DEFAULT_DATE = datetime.now(timezone.utc)-timedelta(days=1)

class Test_barcode_travelled(TestSetupDBDag):
    
    def method_to_mock_query(*args, **kwargs):
        if 'SHOW FIELD KEYS' in args[0]:
            data = [{'name': 'barcode_travelled',
                     'columns': ['fieldKey', 'fieldType'], 'values': [
                    ['Butler Id','integer'],['barcode','integer'],['butler_id','integer']]}]
            return ResultSet({'series': data})
        elif 'select count(barcode) as barcode' in args[0]:
            data = [{'name': 'butler_step_logs',
                     'columns': ['time', 'barcode'], 'values': [
                    ['2023-11-23T07:00:00Z','6367156']]}]
            return ResultSet({'series': data})

        
    def method_to_mock_get_datetime_interval2(*args,**kwargs):
        start = pd.to_datetime('2023-11-23 07:00:00+00:00')
        end = pd.to_datetime('2023-11-24 07:00:00+00:00')
        data = [{'start_date': start, 'end_date':end}]
        df =  pd.DataFrame(data)
        print(f"data: {df}")
        return df
    
    @patch("utils.CommonFunction.Write_InfluxData.writepoints")
    @patch("utils.CommonFunction.InfluxData.get_datetime_interval2",side_effect = method_to_mock_get_datetime_interval2)
    @patch("utils.CommonFunction.InfluxData.query",side_effect=method_to_mock_query)
    @patch("utils.CommonFunction.InfluxData.is_influx_reachable")
    @patch("utils.CommonFunction.CommonFunction.get_tenant_info")
    def test_barcode_travelled(self,mock_get_tenant_info,mock_is_influx_reachable,mock_query, mock_get_datetime_interval2,mock_writepoints):
        mock_get_tenant_info.return_value = self.expected_influx_data
        mock_is_influx_reachable.return_value = True
        mock_writepoints.return_value=''

        with DAG(
                'barcode_travelled',
                default_args=CommonFunction().get_default_args_for_dag(),
                description='barcode travelled',
                catchup=False,
                schedule=None,
        ) as dag:
            import functools
            tenant = CommonFunction().get_tenant_info()
            PythonOperator(
                task_id='barcode_travelled_final',
                python_callable=functools.partial(barcode_travelled().barcode_travelled_final,
                                                  tenant_info={'tenant_info': tenant}),
                execution_timeout=timedelta(seconds=3600),
            )
        dagrun = dag.create_dagrun(state=DagRunState.RUNNING,
                                   execution_date=DEFAULT_DATE,
                                   # data_interval=DEFAULT_DATE,
                                   start_date=DEFAULT_DATE,
                                   run_type=DagRunType.MANUAL)
        ti = dagrun.get_task_instance(task_id='barcode_travelled_final')
        ti.task = dag.get_task(task_id='barcode_travelled_final')
        ti.run(ignore_ti_state=True)
