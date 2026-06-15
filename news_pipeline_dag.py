from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'news_scraping_pipeline',
    default_args=default_args,
    description='Scraping des news financières depuis différentes sources',
    # "0 */6 * * *" = Minute 0 passée de toutes les 6 heures (00h, 06h, 12h, 18h)
    schedule_interval='0 */6 * * *',
    catchup=False,
    tags=['finance', 'scraping'],
) as dag:

    # Le BashOperator permet d'exécuter simplement ton script Python existant
    scrape_and_publish_task = BashOperator(
        task_id='scrape_and_send_to_kafka',
        bash_command='python /opt/airflow/scrapers/news_scraper.py',
    )