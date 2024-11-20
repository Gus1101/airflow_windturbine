from airflow.models import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id="main_trigger_dag",
    description="Dag that triggers the process",
    start_date=datetime(2024,11,19),
    schedule_interval="*/10 * * * *",
    catchup=False,
    default_view="graph",
) as dag:
    
    dummy_start_task = DummyOperator(
        task_id="dummy_start_task",
    )

    trigger_turbinedata = TriggerDagRunOperator(
        task_id="trigger_turbinedata",
        trigger_dag_id="create_and_saves_turbine_data_dag",
    )

    trigger_windturbine = TriggerDagRunOperator(
        task_id="trigger_windturbine",
        trigger_dag_id="windturbine_dag",
    )

    dummy_start_task >> trigger_turbinedata >> trigger_windturbine