from airflow import DAG
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os

RAW_PATH = "SCDs/SCD_1/raw_data"
FINAL_PATH = "SCDs/SCD_1/final_data"

COLUMNS = ["sales_id", "item", "qunatity", "sale_date"]


DAG_ARGS = {
    'dag_id': 'BigDataProjectSCD_1',
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

    date_time_from_previous = Variable.get("SCD_1_last_update", default_var="2024-04-04 11:05:00")
    print(date_time_from_previous, "BEFORE MAXXXX DATE IS")
    step_1 = PostgresOperator(
        task_id = "QueyingTransactionalDB",
        sql = f"SELECT * FROM sales_info where sale_date > '{date_time_from_previous}'",
        postgres_conn_id = "transactional_db_uri"
    )

    pg_result = step_1.execute(context={})
    df = pd.DataFrame(pg_result, columns=COLUMNS)
    # get the max date such that we use that to query next data
    max_datetime = max(pd.to_datetime(df["sale_date"]).dt.strftime(r"%Y-%m-%d %H:%M:%s"))
    max_datetime = max_datetime[:19]
    print(max_datetime, "MAXXXX DATE IS")
    Variable.set(key = "SCD_1_last_update", value=max_datetime)

    PATH = os.path.join(RAW_PATH, todays_date)
    df.to_csv(PATH, index=False)
    print("Data saved to:", PATH)


def last_step(todays_date):
    PATH = os.path.join(RAW_PATH, todays_date)
    df = pd.read_csv(PATH)

    if os.path.exists(os.path.join(FINAL_PATH, "final_results.csv")):
        final_df = pd.read_csv(os.path.join(FINAL_PATH, "final_results.csv"))
        
        final_sales_ids = final_df["sales_id"].values.tolist()
        now_sales_ids = df["sales_id"].values.tolist()

        updating_records = []
        inserting_records = []
        for i in now_sales_ids:
            if i in final_sales_ids:
                updating_records.append(i)
            else:
                inserting_records.append(i)
        
        stge_1 = final_df[~final_df["sales_id"].isin(updating_records)]
        saving_df = pd.concat([stge_1, df], ignore_index=True)

        saving_df.to_csv(os.path.join(FINAL_PATH, "final_results.csv"), index = False)
    else:
        df.to_csv(os.path.join(FINAL_PATH, "final_results.csv"), index = False)
    print("Hurrey we are up to date!")


with DAG(**DAG_ARGS) as dag:

    step_2 = PythonOperator(
        task_id="queryPostgresSaveFileAsPerSCD1",
        python_callable=save_file,
        op_kwargs = { "todays_date" : "{{ ts_nodash }}"}
    )

    step_3 = PythonOperator(
        task_id="UpdateAsPerSCD1",
        python_callable=last_step,
        op_kwargs = { "todays_date" : "{{ ts_nodash }}"}
    )

step_2 >> step_3
