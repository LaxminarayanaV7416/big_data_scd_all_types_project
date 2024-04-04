from airflow import DAG
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd


DAG_ARGS = {
    'dag_id': 'BigDataProjectSCD_2',
    'description': 'this Dag will query the database and put it into a file!',
    # 'schedule' : timedelta(days=1),
    'start_date': datetime(2024, 4, 3),
    'catchup': False,
    'tags': ['BigDataProject'],
    'default_args': {'depends_on_past': False,
                     'email': ['laxminarayana.vadnala@slu.edu'],
                     'email_on_failure': False,
                     'email_on_retry': False,
                     'retries': 0,
                     'retry_delay': timedelta(minutes=5)}
}


def save_file():

    step_1 = PostgresOperator(
        task_id="QueyingTransactionalDB",
        sql="SELECT * FROM test",
        postgres_conn_id="transactional_db_uri"
    )

    pg_result = step_1.execute(context={})
    df = pd.DataFrame(pg_result)
    output_file_path = "raw_test.csv"
    df.to_csv(output_file_path, index=False)
    print("Data saved to:", output_file_path)


def last_step():
    print("Hello World am Done!")


with DAG(**DAG_ARGS) as dag:

    step_2 = PythonOperator(
        task_id="queryPostgresSaveFile",
        python_callable=save_file,
        # op_kwargs = {"command" : , "start_date" : "{{ ds }}"}
    )

    step_3 = PythonOperator(
        task_id="TestPrint",
        python_callable=last_step
    )

step_2 >> step_3
