from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os

default_args = {"depends_on_past":False,
                "email":"augustosantosb60@gmail.com",
                "email_on_failure":True,
                "email_on_retry":False,
                "retries":1,
                "retry_delay":timedelta(seconds=10),}

file_path = Variable.get("file_path")

def get_data(**kwargs):
    
    with open(file_path) as f:
        data = json.load(f)
        kwargs["task_instance"].xcom_push(key="temperature",value=data["temperatura"])
        kwargs["task_instance"].xcom_push(key="timestamp",value=data["timestamp"])
    os.remove("file_path")

def check_temperature(**kwargs) -> float:

    value = kwargs["task_instance"].xcom_pull(key="temperature", task_ids=["get_data_task"])
    
    if value >= 35.0:
        return "danger"
    else:
        return "normal"

with DAG (
    dag_id="windturbine_dag",
    description="Verify turbine and sends an email depending on temperature",
    start_date=None,
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    default_view="graph",
    doc_md="## Verify turbine data and sends an email depending on temperature"
) as dag:
    
    file_sensor_task = FileSensor(
        task_id="file_sensor_task",
        fs_conn_id="file_conn",
        filepath = file_path,
        poke_interval=60,
        timeout=360,
        mode="poke",
    )

    get_data_task = PythonOperator(
        task_id="get_data_task",
        python_callable=get_data,
        provide_context=True,
    )

    with TaskGroup(
        "GroupCheckTemp"
    ) as check_temp_group:
        
        dummy_start_task = DummyOperator(
            task_id="dummy_start_task",
        )
        
        branch_check_temperature_task = BranchPythonOperator(
            task_id="brach_check_temperature_task",
            python_callable=check_temperature,
        )

        normal_temp_email_task = EmailOperator(
            task_id="normal_temp_email_task",
            to="email@email.com",
            subject="Everthing is ok with the turbine",
            html_content="<h3>Hey, don't worry! Everthing is ok.</h3>",
        )

        danger_temp_email_task = EmailOperator(
            task_id="danger_temp_email_task",
            to="email@email.com",
            subject="Turbine is in danger",
            html_content="<h3>Hey! Better do something.</h3>",
        )

        dummy_start_task >> branch_check_temperature_task
        branch_check_temperature_task >> normal_temp_email_task
        branch_check_temperature_task >> danger_temp_email_task


    with TaskGroup(
        "PersistToPostgres"
    ) as persist_to_postgres_group:
        
        dummy_start_task = DummyOperator(
            task_id="dummy_start_task",
        )

        create_table_task = PostgresOperator(
            task_id="create_table_task",
            postgres_conn_id="postgres_conn",
            sql="""
        CREATE TABLE IF NOT EXISTS sensor (
            timestamp TIMESTAMP,
            temperature FLOAT
        );
        """,
        )

        insert_data_task = PostgresOperator(
            task_id="insert_data_task",
            postgres_conn_id="postgres_conn",
            parameters=(
                '{{task_instance.xcom_pull(task_ids="get_data_task", key="timestamp")}}',
                '{{task_instance.xcom_pull(task_ids="get_data_task", key="temperature")}}',
            ),
            sql=""" 
        INSERT INTO sensor (
            timestamp,temperature
        ) 
        VALUES (
        %s, %s
        );
        """,
        )

        dummy_start_task >> create_table_task >> insert_data_task

    file_sensor_task >> get_data_task >> [check_temp_group, persist_to_postgres_group]