from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'job_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    scraping = BashOperator(
        task_id='scraping',
        bash_command='python /opt/airflow/scraper/emploi_scraper.py'
    )

    upload = BashOperator(
        task_id='upload',
        bash_command='python /opt/airflow/scraper/upload_to_minio.py'
    )

    silver = BashOperator(
        task_id='silver',
        bash_command='python /opt/airflow/spark_jobs/silver_processing.py'
    )

    gold = BashOperator(
        task_id='gold',
        bash_command='python /opt/airflow/spark_jobs/gold_processing.py'
    )

    scraping >> upload >> silver >> gold