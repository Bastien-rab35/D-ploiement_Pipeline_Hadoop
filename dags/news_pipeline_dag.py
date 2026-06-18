from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajout du dossier racine au PYTHONPATH pour permettre l'import du module scraper
sys.path.append('/opt/airflow')
from scrapers.news_scraper import run_all_scrapers

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
    schedule_interval='0 */6 * * *',
    catchup=False,
    tags=['finance', 'scraping'],
) as dag:

    # Utilisation d'un PythonOperator au lieu d'un BashOperator (Recommandation Professeur)
    scrape_and_publish_task = PythonOperator(
        task_id='scrape_and_send_to_kafka',
        python_callable=run_all_scrapers,
    )

    # Tâche de surveillance et de validation de la qualité des données (EID4.4)
    def run_pipeline_monitor():
        import subprocess
        print("Démarrage de la surveillance du pipeline...")
        result = subprocess.run(
            ['python', '/opt/airflow/jobs/pipeline_monitor.py'],
            capture_output=True,
            text=True
        )
        print("STDOUT du moniteur:")
        print(result.stdout)
        if result.returncode != 0:
            print("STDERR du moniteur:")
            print(result.stderr)
            raise Exception("La surveillance du pipeline a détecté une anomalie critique ou un service en panne.")
        print("Surveillance terminée avec succès !")

    monitor_pipeline_task = PythonOperator(
        task_id='monitor_and_validate_data',
        python_callable=run_pipeline_monitor,
    )

    # Définition de l'ordre d'exécution
    scrape_and_publish_task >> monitor_pipeline_task