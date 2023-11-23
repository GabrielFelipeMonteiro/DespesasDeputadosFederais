from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from scripts.get_despesas_stg import extractDespesas
from scripts.etl import getDespesaStg, transformDespesas, loadDespesasFinal


#Definindo as configurações do DAG
default_args = {
	'owner':'gabs',
	'depends_on_past':False,
	'start_date':datetime.now(),
	'email_on_failure':False,
	'email_on_retry':False,
	'retries':3,
	'retry_delay':timedelta(minutes=3)
}


#Definindo o DAG
dag = DAG(
	'dag_extract_despesas',
	default_args,
	description='DAG para execução do script de extração de despesas dos deputados',
	schedule_interval=timedelta(minutes=1)
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
    provide_contex=True,
    dag=dag
)