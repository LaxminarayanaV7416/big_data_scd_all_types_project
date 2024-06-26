from airflow import DAG
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os

RAW_PATH = "SCDs/SCD_0/raw_data"
FINAL_PATH = "SCDs/SCD_0/final_data"
COLUMNS = ["id", "fax", "email", "domain_name"]

DAG_ARGS = {
    'dag_id': 'BigDataProjectSCD_0',
    'description': 'this Dag will query the database and put it into a file!',
    # 'schedule' : timedelta(days=1),
    'start_date': datetime(2024, 4, 4),
    'catchup': False,
    'tags': ['BigDataProject'],
    'default_args': {'depends_on_past': False,
                     'email': ['laxminarayana.vadnala@slu.edu'],
                     'email_on_failure': False,
                     'email_on_retry': False,
                     'retries': 0,
                     'retry_delay': timedelta(minutes=5)}
}


def save_file(todays_date):

    step_1 = PostgresOperator(
        task_id="QueyingTransactionalDBForSCD0",
        sql="SELECT * FROM company_info",
        postgres_conn_id="transactional_db_uri"
    )

    pg_result = step_1.execute(context={})
    df = pd.DataFrame(pg_result, columns=COLUMNS)
    PATH = os.path.join(RAW_PATH, todays_date)
    df.to_csv(PATH, index=False)
    print("Data saved to:", PATH)

def last_step(todays_date):
    if not os.path.exists(os.path.join(FINAL_PATH, "final_results.csv")):
        PATH = os.path.join(RAW_PATH, todays_date)
        df = pd.read_csv(PATH)
        df.to_csv(os.path.join(FINAL_PATH, "final_results.csv"), index = False)
    print("Hurrey we are up to date!")


with DAG(**DAG_ARGS) as dag:

    step_2 = PythonOperator(
        task_id="queryPostgresSaveFile",
        python_callable=save_file,
        op_kwargs = { "todays_date" : "{{ ts_nodash }}"}
    )

    step_3 = PythonOperator(
        task_id="UpdateTheQueriedFilesAsPerSCD_0",
        python_callable=last_step,
        op_kwargs = { "todays_date" : "{{ ts_nodash }}"}
    )

step_2 >> step_3
