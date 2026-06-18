import pyspark
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, year, month, dayofmonth, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# Force PySpark à utiliser le même interpréteur Python pour le Driver et les Workers
if 'pyspark' in sys.modules:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

@udf(returnType=BooleanType())
def detect_fake_news_udf(title, summary, source):
    """
    UDF PySpark : Application de l'API de Machine Learning de manière distribuée sur le cluster.
    """
    # Import de requests localement pour éviter les erreurs de sérialisation
    import requests 
    import time
    
    text_to_analyze = f"{title}. {summary}"
    
    try:
        from deep_translator import GoogleTranslator
        text_to_analyze = GoogleTranslator(source='auto', target='en').translate(text_to_analyze)
    except Exception as e:
        print(f"Translation error: {str(e)}")
    
    max_retries = 2
    for attempt in range(max_retries):
        try:
            # Interrogation du modèle ML via son API (Nom DNS Docker)
            response = requests.post(
                "http://fake-news-detector:5000/detect_json",
                json={"text": text_to_analyze},
                timeout=15
            )
            if response.status_code == 200:
                result = response.json()
                
                # Recherche de "fake" uniquement dans les prédictions pour éviter les faux positifs
                if isinstance(result, dict):
                    prediction_values = [str(v).lower() for k, v in result.items() if k != "text"]
                    return any("fake" in v for v in prediction_values)
                return "fake" in str(result).lower()
            else:
                print(f"ML API Erreur HTTP {response.status_code} (Tentative {attempt+1}/{max_retries})")
                time.sleep(2)
        except Exception as e:
            print(f"ML API Exception: {str(e)} (Tentative {attempt+1}/{max_retries})")
            time.sleep(2)
            
    # Par défaut, la news est considérée comme vraie si toutes les tentatives échouent
    return False

def create_spark_session():
    """
    Initialise la session Spark avec les packages pour Kafka, Postgres et MinIO.
    """
    spark_version = pyspark.__version__
    
    scala_version = "2.13" if int(spark_version.split('.')[0]) >= 4 else "2.12"
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
    
    postgres_package = "org.postgresql:postgresql:42.6.0"
    
    # Adaptation des librairies AWS selon la version de PySpark installée
    if int(spark_version.split('.')[0]) >= 4:
        hadoop_aws_version = "3.4.2"
        aws_sdk_version = "1.12.767"
    else:
        hadoop_aws_version = "3.3.4"
        aws_sdk_version = "1.12.262"

    aws_packages = f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}"

    return SparkSession.builder \
        .appName("NewsStreamingProcessor") \
        .config("spark.jars.packages", f"{kafka_package},{postgres_package},{aws_packages}") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .remote("sc://localhost:15002") \
        .getOrCreate()

def write_to_sinks(df, epoch_id):
    """
    Fonction utilisée dans le foreachBatch pour écrire simultanément dans PostgreSQL et MinIO.
    """
    if df.count() == 0:
        return

    try:
        # Note d'architecture : Spark tournant en local sur l'hôte (master="local[*]"),
        # on utilise 'localhost' pour atteindre le port exposé par le conteneur Docker.
        # (Si Spark était "dockerizé", on utiliserait le nom de service 'postgres')
        # Écriture dans la base de données relationnelle
        # 1. Écriture dans la base de données relationnelle (sans les colonnes de partition)
        df_postgres = df.select("source", "title", "summary", "event_date", "publish_date", "is_fake")
        df_postgres.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5433/finance_news") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "news_articles") \
            .option("user", os.getenv("POSTGRES_USER", "airflow")) \
            .option("password", os.getenv("POSTGRES_PASSWORD", "airflow")) \
            .mode("append") \
            .save()

        # 2. Écriture de l'historique brut dans le Data Lake (S3/MinIO) partitionné par date (EID4.5)
        df.write \
            .format("json") \
            .partitionBy("year", "month", "day") \
            .mode("append") \
            .save("s3a://data-lake/raw-news/")
        print(f"INFO: Batch {epoch_id} écrit avec succès dans Postgres et MinIO (partitionné).")
    except Exception as e:
        print(f"ERREUR CRITIQUE sur le batch {epoch_id} : Impossible d'écrire les données. Raison : {str(e)}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage du job Spark... En attente de messages depuis Kafka...")

    # Définition du schéma du JSON reçu
    news_schema = StructType([
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("event_date", StringType(), True),
        StructField("publish_date", StringType(), True)
    ])

    # Lecture du flux Kafka depuis le début (earliest) pour respecter la contrainte du sujet
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", "raw_news") \
        .option("startingOffsets", "latest") \
        .load()

    # Transformation des données brutes en colonnes
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), news_schema).alias("data")) \
        .select("data.*")

    # Application du modèle de ML via l'UDF
    df_processed = df_parsed.withColumn(
        "is_fake", detect_fake_news_udf(col("title"), col("summary"), col("source"))
    )

    # Ajout des colonnes de partitionnement pour le Data Lake MinIO (EID4.5)
    df_partitioned = df_processed \
        .withColumn("timestamp", to_timestamp(col("event_date"))) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .drop("timestamp")

    query = df_partitioned.writeStream \
        .foreachBatch(write_to_sinks) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()