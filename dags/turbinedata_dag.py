from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from typing import Dict
import random
import json

def create_turbine_data() -> Dict:

    turbine_data = {
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "id_turbina": random.randint(1, 100),  
        "velocidade_vento_mps": round(random.uniform(2.0, 25.0), 2),  
        "direcao_vento_graus": random.randint(0, 360),  
        "produz_energia_kw": round(random.uniform(0.0, 1500.0), 2),  
        "temperatura": round(random.uniform(-5.0, 40.0), 2),
    }

    return turbine_data

def save_turbine_data() -> None:

    data = create_turbine_data()

    file_path = Variable.get("file_path")

    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Dados da turbina eólica salvos em {file_path}")

with DAG(
    dag_id="create_and_saves_turbine_data_dag",
    description="Dag that creates and saves turbine data",
    start_date=None,
    schedule_interval=None,
    catchup=False,
    default_view="graph",
) as dag:
    
    create_turbine_data_task = PythonOperator(
        task_id="create_turbine_data_task",
        python_callable=create_turbine_data,
    )

    save_turbine_data_task = PythonOperator(
        task_id="save_turbine_data_task",
        python_callable=save_turbine_data,
    )

    create_turbine_data_task >> save_turbine_data_task