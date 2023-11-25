from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.get_despesas_stg import extractDespesas
from scripts.etl import getDespesaStg, transformDespesas, loadDespesasFinal

# Definindo as configurações do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}


# Definindo o DAG
dag = DAG(
    'dag_extract_despesas',
    default_args=default_args,
    start_date=datetime.now(),
    schedule_interval=timedelta(minutes=1),
    catchup=False
)


#Definindo as tasks
task_extract_despesas = PythonOperator(
	task_id='task_extract_despesas',
	python_callable=extractDespesas,
	provide_context=True,
	dag=dag
)

task_get_despesas_stg = PythonOperator(
	task_id='task_get_despesas_stg',
	python_callable=getDespesaStg,
	provide_context=True,
	dag=dag
)

task_transform_despesas = PythonOperator(
	task_id='task_transform_despesas',
	python_callable=transformDespesas,
	provide_context=True,
    dag=dag
)

task_load_despesas = PythonOperator(
    task_id='task_load_despesas',
    python_callable=loadDespesasFinal,
    provide_context=True,
    dag=dag
)


task_remove_duplicate_despesas = PostgresOperator(
    task_id='task_remove_duplicate_despesas',
    postgres_conn_id='postgres-dept',
    sql="sql/despesas_schema.sql"
)


task_extract_despesas >> task_get_despesas_stg >> task_transform_despesas >> task_load_despesas >> task_remove_duplicate_despesas