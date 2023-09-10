from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


# DAGの設定
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
}

def my_python_function():
    raise Exception("Task failed intentionally.")

with DAG(
    dag_id='sample-exponential-backoff',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    # DAGのタスクを定義
    start_task = DummyOperator(
        task_id='start_task',
        dag=dag,
    )

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=my_python_function,
        dag=dag,
    )

    end_task = DummyOperator(
        task_id='end_task',
        dag=dag,
    )

    # タスク間の依存関係を設定
    start_task >> python_task >> end_task
