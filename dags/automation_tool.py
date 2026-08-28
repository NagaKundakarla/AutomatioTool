from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import time


def hello():
    time.sleep(20)
    print("Hello! My first Airflow DAG is working!")


with DAG(
    dag_id="sample1_dag",
    start_date=datetime(2026, 8, 25),
    schedule=None,
    catchup=False,
) as dag:

    A = PythonOperator(
        task_id="Task_A",
        python_callable=hello,
    )

    B = PythonOperator(
        task_id="Task_B",
        python_callable=hello,
    )

    C = PythonOperator(
        task_id="Task_C",
        python_callable=hello,
    )

    D = PythonOperator(
        task_id="Task_D",
        python_callable=hello,
    )

    E = PythonOperator(
        task_id="Task_E",
        python_callable=hello,
    )

    A >> B
    A >> C
    C >> D
    B >> E

