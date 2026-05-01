import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, IntegerType, TimestampType, StringType
)
from spark_utils import get_spark_session
from load_env import load_env
from s3_utils import upload_folder_to_s3, verify_s3_connection
load_env()

RAW_DATA_PATH = "data/raw/"
BRONZE_OUTPUT_PATH = "data/bronze/"

TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
])


def read_raw_data(spark, path, schema):
    return spark.read \
        .option("mergeSchema", "true") \
        .schema(schema) \
        .parquet(path)


def add_ingestion_metadata(df):
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    df = df.withColumn("source_file", F.lit("nyc_yellow_taxi_2024_q1"))
    return df


def write_bronze(df, output_path):
    os.makedirs(output_path, exist_ok=True)
    df.write \
        .mode("overwrite") \
        .partitionBy("VendorID") \
        .parquet(output_path)
    print(f"Bronze layer written to: {output_path}")
    print(f"Total rows written: {df.count():,}")


def main():
    spark = get_spark_session("NYC Taxi Extract")
    spark.sparkContext.setLogLevel("ERROR")

    print("Reading raw parquet files...")
    df = read_raw_data(spark, RAW_DATA_PATH, TAXI_SCHEMA)

    print("Adding ingestion metadata...")
    df = add_ingestion_metadata(df)

    print("Writing bronze layer...")
    write_bronze(df, BRONZE_OUTPUT_PATH)

    spark.stop()
    print("Extract complete.")

    if verify_s3_connection():
        upload_folder_to_s3(BRONZE_OUTPUT_PATH, "bronze")


if __name__ == "__main__":
    main()