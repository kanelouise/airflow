#code from https://towardsdatascience.com/managed-apache-airflow-on-aws-new-aws-service-for-data-pipelines-91908ee9e5fc
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

args = {'owner': 'kane', 'start_date': '2021-08-26'}


def say_hi():
    print('Hello from MWAA!')


with DAG(dag_id='ex_dag', default_args=args,
         schedule_interval=None, tags=['medium']) as dag:
    hw_task = PythonOperator(task_id='hw_task', provide_context=True,
                             python_callable=say_hi, dag=dag)

    ls_task = BashOperator(task_id='ls_task', bash_command='ls')

    hw_task >> ls_task