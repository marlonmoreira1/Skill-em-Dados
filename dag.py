from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from script_datajobs import extrair_dados, transformar_dados, carregar_dados
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 28),
    'depends_on_past': False,
    'email': ['marlonm.almeida@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'etl_vagas_dados',
    default_args=default_args,
    description='DAG para extrair, transformar e carregar dados no BigQuery',
    schedule_interval=timedelta(days=1),
)

extrair = PythonOperator(
    task_id='extrair_dados',
    python_callable=extrair_dados,
    provide_context=True,
    dag=dag,
)

transformar = PythonOperator(
    task_id='transformar_dados',
    python_callable=transformar_dados,
    provide_context=True,
    dag=dag,
)

carregar = PythonOperator(
    task_id='carregar_dados',
    python_callable=carregar_dados,
    provide_context=True,
    dag=dag,
)

extrair >> transformar >> carregar

    