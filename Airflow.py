from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from Quality_Checks import Quality_Check
from spark_etl import get_latest_fact_id,read_incremental_from_oltp,clean_and_transform_data,write_to_olap



# Define default_args and other DAG configurations
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'your_etl_pipeline',
    default_args=default_args,
    description='Your ETL Pipeline',
    schedule_interval=timedelta(days=1),
)

# Task to get latest fact id
def get_latest_fact_id_task():
    # Your data extraction logic here
    fact_id = PythonOperator(
        task_id='fact_id',
        python_callable=get_latest_fact_id,
        op_kwargs = {"fact_table" : "fact_table"},
        dag=dag,
    )

# Task to transform data
def extract_incremental_task():
# Your data extract logic here
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=read_incremental_from_oltp,
        op_kwargs = {"table_name":"table_name", "max_fact_id":"max_fact_id"},
        dag=dag,
    )

def transform_data_task():
# Your data transformation logic here
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=clean_and_transform_data,
        op_kwargs = {"table_name":"table_name"},
        dag=dag,
    )


# Task to load data
def load_data_task():
    # Your data loading logic here\
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=write_to_olap,
        op_kwargs = {df_with_keys:"df",your_table:"table"},
        dag=dag,
    )


quality_check_task = PythonOperator(
    task_id='perform_quality_checks',
    python_callable=Quality_Check,
    dag=dag,
)

# Define task dependencies
get_latest_fact_id_task >> extract_incremental_task >> transform_data_task >> load_data_task << quality_check_task
