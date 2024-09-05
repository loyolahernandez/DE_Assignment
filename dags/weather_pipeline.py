from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from plugins.utils import fetch_weather_data, transform_observations

default_args = {
    'owner': 'IgnacioLoyola',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 9, 5),
}

# Define el DAG
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A pipeline that fetches and processes weather data',
    schedule_interval=timedelta(days=1),
)

# Definir las tareas
fetch_data_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    op_args=['0112W'],  # Asumiendo que quieres datos de la estación 0112W
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_observations',
    python_callable=transform_observations,
    provide_context=True,
    dag=dag,
)

# Configurar dependencias
fetch_data_task >> transform_data_task
