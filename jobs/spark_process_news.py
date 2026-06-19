import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, year, month, dayofmonth, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# ─────────────────────────────────────────────────────────────────────────────
# UDF de détection de fake news (appelée localement via l'API du conteneur ML)
# ─────────────────────────────────────────────────────────────────────────────
@udf(returnType=BooleanType())
def detect_fake_news_udf(title, summary, source):
    """Appelle l'API ML Flask pour classifier la news."""
    import requests
    import time

    text = f"{title}. {summary}"

    # Tentative de traduction en anglais (le modèle ML est entraîné en anglais)
    try:
        from deep_translator import GoogleTranslator
        text = GoogleTranslator(source='auto', target='en').translate(text)
    except Exception:
        pass

    for _ in range(2):
        try:
            # Le conteneur fake-news-detector est exposé sur le port 5001 de localhost
            resp = requests.post(
                "http://localhost:5001/detect_json",
                json={"text": text},
                timeout=15
            )
            if resp.status_code == 200:
                result = resp.json()
                if isinstance(result, dict):
                    values = [str(v).lower() for k, v in result.items() if k != "text"]
                    return any("fake" in v for v in values)
                return "fake" in str(result).lower()
        except Exception:
            time.sleep(2)

    return False  # Par défaut : vraie news si l'API est inaccessible


# ─────────────────────────────────────────────────────────────────────────────
# Session Spark locale (s'exécute directement sur le Mac, pas dans Docker)
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session():
    """
    Crée une session Spark locale avec les JARs pour Kafka, PostgreSQL et MinIO.
    Le mode local[*] utilise tous les cœurs du Mac — aucun serveur Docker requis.
    """
    spark_version = pyspark.__version__
    scala_version = "2.13" if int(spark_version.split('.')[0]) >= 4 else "2.12"

    packages = ",".join([
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
        "org.postgresql:postgresql:42.6.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])

    spark = (
        SparkSession.builder
        .appName("NewsStreamingProcessor")
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "admin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "password"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

    # Suppression des avertissements répétitifs de KafkaDataConsumer (inoffensifs)
    log4j = spark._jvm.org.apache.log4j
    log4j.LogManager.getLogger("org.apache.spark.sql.kafka010.KafkaDataConsumer").setLevel(log4j.Level.ERROR)

    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Écriture dans les deux sinks : PostgreSQL et MinIO
# ─────────────────────────────────────────────────────────────────────────────
def write_to_sinks(df, epoch_id):
    """Écrit chaque micro-batch dans PostgreSQL (JDBC) et MinIO (S3A)."""
    if df.count() == 0:
        return

    try:
        # 1. PostgreSQL — exposé sur localhost:5433 côté Mac (cf. compose.yaml)
        (
            df.select("source", "title", "summary", "event_date", "publish_date", "is_fake")
            .write
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5433/finance_news")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", "news_articles")
            .option("user", os.getenv("POSTGRES_USER", "airflow"))
            .option("password", os.getenv("POSTGRES_PASSWORD", "airflow"))
            .mode("append")
            .save()
        )

        # 2. MinIO (Data Lake) — partitionné par date pour répondre à EID4.5
        (
            df.write
            .format("json")
            .partitionBy("year", "month", "day")
            .mode("append")
            .save("s3a://data-lake/raw-news/")
        )

        print(f"INFO: Batch {epoch_id} écrit dans PostgreSQL et MinIO.")
    except Exception as e:
        print(f"ERREUR batch {epoch_id}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark_session()

    print("Démarrage du job Spark... En attente de messages depuis Kafka...")

    news_schema = StructType([
        StructField("source",       StringType(), True),
        StructField("title",        StringType(), True),
        StructField("summary",      StringType(), True),
        StructField("event_date",   StringType(), True),
        StructField("publish_date", StringType(), True),
    ])

    # Lecture depuis Kafka (broker exposé sur localhost:9093 côté Mac)
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9093")
        .option("subscribe", "raw_news")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Décodage JSON
    df_parsed = (
        df_kafka
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), news_schema).alias("data"))
        .select("data.*")
    )

    # Détection de fake news via l'UDF
    df_processed = df_parsed.withColumn(
        "is_fake", detect_fake_news_udf(col("title"), col("summary"), col("source"))
    )

    # Ajout des colonnes de partition temporelle (EID4.5)
    df_partitioned = (
        df_processed
        .withColumn("timestamp", to_timestamp(col("event_date")))
        .withColumn("year",  year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day",   dayofmonth(col("timestamp")))
        .drop("timestamp")
    )

    # Lancement du streaming en continu
    query = df_partitioned.writeStream.foreachBatch(write_to_sinks).start()
    query.awaitTermination()


if __name__ == "__main__":
    main()