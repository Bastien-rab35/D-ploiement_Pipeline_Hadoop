import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_session():
    # Récupération dynamique de la version de PySpark installée
    spark_version = pyspark.__version__
    
    # Spark 4+ utilise Scala 2.13 par défaut, Spark 3 utilise Scala 2.12
    scala_version = "2.13" if int(spark_version.split('.')[0]) >= 4 else "2.12"
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
    # Ajout du driver JDBC PostgreSQL
    postgres_package = "org.postgresql:postgresql:42.6.0"
    # Ajout des librairies Hadoop-AWS pour communiquer avec MinIO (S3)
    aws_packages = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

    return SparkSession.builder \
        .appName("NewsStreamingProcessor") \
        .config("spark.jars.packages", f"{kafka_package},{postgres_package},{aws_packages}") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .master("local[*]") \
        .getOrCreate()

def write_to_sinks(df, epoch_id):
    """Fonction exécutée pour chaque micro-batch afin d'écrire dans PostgreSQL ET MinIO."""
    # On ignore le batch s'il est vide (pas de nouvelles données)
    if df.count() == 0:
        return

    try:
        # 1. Écriture dans la base de données relationnelle
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/finance_news") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "news_articles") \
            .option("user", "airflow") \
            .option("password", "airflow") \
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
    # "startingOffsets" = "earliest" permet de relire tous les messages déjà présents dans Kafka
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

    # 4. Écriture des données structurées dans PostgreSQL et S3
    query = df_parsed.writeStream \
        .foreachBatch(write_to_sinks) \
        .start()

    query.awaitTermination() # Maintient l'application ouverte en continu

if __name__ == "__main__":
    main()