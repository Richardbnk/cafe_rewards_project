import os
import time
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Configurações
PROJECT_ID = "gcp-project-463802"
DATASET_ID = "raw"
GOOGLE_APPLICATION_CREDENTIALS = "credentials/gcp-project.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

FILES = {
    "customers": "data/raw/customers.csv",
    # adicione outros arquivos se quiser
}

def start_spark():
    spark = SparkSession.builder \
        .appName("BigQueryIngest") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
        .config("temporaryGcsBucket", "bucket-project-20250622") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
        .getOrCreate()
    return spark

def transform_customers(df):
    return df.filter(col("income").isNotNull()) \
             .withColumn("age", col("age").cast("integer")) \
             .withColumn("income", col("income").cast("float")) \
             .withColumn("became_member_on", to_date(col("became_member_on"), "yyyyMMdd"))

def delete_table_if_exists(client, dataset_id, table_name):
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    try:
        client.delete_table(table_id)
        print(f"Tabela {table_id} deletada.")
        time.sleep(5)  # Espera para consistência
    except Exception as e:
        if "Not found" in str(e):
            print(f"Tabela {table_id} não existe, nada a deletar.")
        else:
            raise

def ingest_csv_to_bigquery(spark, client):
    for table_name, filepath in FILES.items():
        print(f"Iniciando ingestão do arquivo {filepath} para tabela {DATASET_ID}.{table_name}")
        
        df = spark.read.option("header", "true").csv(filepath)

        if table_name == "customers":
            df = transform_customers(df)

        delete_table_if_exists(client, DATASET_ID, table_name)

        df.write.format("bigquery") \
            .option("table", f"{PROJECT_ID}:{DATASET_ID}.{table_name}") \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()

        print(f"Tabela {DATASET_ID}.{table_name} criada e carregada com sucesso.")

if __name__ == "__main__":
    client = bigquery.Client(project=PROJECT_ID)
    spark = start_spark()

    ingest_csv_to_bigquery(spark, client)

    spark.stop()
