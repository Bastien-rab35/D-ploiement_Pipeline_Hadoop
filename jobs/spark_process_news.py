import pyspark
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# --- Correction du conflit de version Python sur l'environnement local ---
# Force PySpark à utiliser le même interpréteur Python pour le processus
# principal (Driver) et les processus distribués (Workers).
if 'pyspark' in sys.modules:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# --- Définition de l'UDF pour le Machine Learning ---
@udf(returnType=BooleanType())
def detect_fake_news_udf(title, summary, source):
    """
    User Defined Function (UDF) PySpark :
    Permet d'appliquer une fonction Python classique (ici, l'appel à notre API de Machine Learning)
    de manière distribuée sur tous les workers du cluster Spark.
    """
    # Import local à l'UDF pour éviter les erreurs de sérialisation sur les workers Spark
    import requests 
    
    text_to_analyze = f"{title}. {summary}"
    
    try:
        # Appel à l'API REST locale fournie par le conteneur Docker
        response = requests.post(
            "http://localhost:5001/detect_json",
            json={"text": text_to_analyze},
            timeout=5 # Timeout pour ne pas bloquer Spark indéfiniment
        )
        if response.status_code == 200:
            result = response.json()
            
            # Amélioration : on cherche "fake" uniquement dans les prédictions
            # pour éviter les faux positifs si l'API nous renvoie le texte d'origine.
            if isinstance(result, dict):
                prediction_values = [str(v).lower() for k, v in result.items() if k != "text"]
                return any("fake" in v for v in prediction_values)
            return "fake" in str(result).lower()
    except Exception:
        pass # Si l'API est inaccessible, on ignore l'erreur
        
    # En cas d'erreur ou si la news est vraie
    return False

def create_spark_session():
    """
    Initialise la session Spark avec les dépendances (packages) nécessaires
    pour communiquer avec Kafka, PostgreSQL et AWS S3 (MinIO).
    """
    # Récupération dynamique de la version de PySpark installée
    spark_version = pyspark.__version__
    
    # Spark 4+ utilise Scala 2.13 par défaut, Spark 3 utilise Scala 2.12
    scala_version = "2.13" if int(spark_version.split('.')[0]) >= 4 else "2.12"
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
    # Ajout du driver JDBC PostgreSQL
    postgres_package = "org.postgresql:postgresql:42.6.0"
    
    # Adaptation dynamique des librairies AWS/Hadoop selon la version de PySpark
    if int(spark_version.split('.')[0]) >= 4:
        hadoop_aws_version = "3.4.2"
        aws_sdk_version = "1.12.767"
    else:
        hadoop_aws_version = "3.3.4"
        aws_sdk_version = "1.12.262"

    # Ajout des librairies Hadoop-AWS pour communiquer avec MinIO (S3)
    aws_packages = f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}"

    return SparkSession.builder \
        .appName("NewsStreamingProcessor") \
        .config("spark.jars.packages", f"{kafka_package},{postgres_package},{aws_packages}") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .master("local[*]") \
        .getOrCreate()

def write_to_sinks(df, epoch_id):
    """
    Fonction utilisée par le 'foreachBatch' de Spark Streaming.
    Elle permet d'écrire le même micro-batch de données vers plusieurs destinations (Sinks)
    simultanément : PostgreSQL (pour la B.I.) et MinIO (pour le Data Lake).
    """
    # On ignore le batch s'il est vide (pas de nouvelles données)
    if df.count() == 0:
        return

    try:
        # 1. Écriture dans la base de données relationnelle
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5433/finance_news") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "news_articles") \
            .option("user", os.getenv("POSTGRES_USER", "airflow")) \
            .option("password", os.getenv("POSTGRES_PASSWORD", "airflow")) \
            .mode("append") \
            .save()

        # 2. Écriture de l'historique brut dans le Data Lake (MinIO) au format JSON
        df.write \
            .format("json") \
            .mode("append") \
            .save("s3a://data-lake/raw-news/")
        print(f"INFO: Batch {epoch_id} écrit avec succès dans Postgres et MinIO.")
    except Exception as e:
        print(f"ERREUR CRITIQUE sur le batch {epoch_id} : Impossible d'écrire les données. Raison : {str(e)}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN") # Pour cacher les dizaines de logs d'information de Spark

    print("Démarrage du job Spark... En attente de messages depuis Kafka...")

    # 1. Définition du schéma correspondant à l'objet JSON envoyé par notre scraper
    news_schema = StructType([
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("event_date", StringType(), True),
        StructField("publish_date", StringType(), True)
    ])

    # 2. Lecture du flux Kafka
    # L'option "startingOffsets": "earliest" est cruciale ici.
    # Elle répond à la contrainte du sujet : "Permettre de retraiter très rapidement 
    # toutes les informations collectées depuis le début".
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", "raw_news") \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Transformation des données brutes en colonnes structurées
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), news_schema).alias("data")) \
        .select("data.*")

    # 3.5. Application du modèle de Machine Learning (ajout de la colonne is_fake)
    df_processed = df_parsed.withColumn(
        "is_fake", detect_fake_news_udf(col("title"), col("summary"), col("source"))
    )

    # 4. Écriture des données structurées dans PostgreSQL et S3
    query = df_processed.writeStream \
        .foreachBatch(write_to_sinks) \
        .start()

    query.awaitTermination() # Maintient l'application ouverte en continu

if __name__ == "__main__":
    main()