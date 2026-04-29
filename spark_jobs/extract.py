import os
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")
os.environ["HADOOP_OPTS"] = "-Djava.library.path=C:/hadoop/bin"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, IntegerType, TimestampType, StringType
)
import os

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


def create_spark_session():
    return SparkSession.builder \
        .appName("NYC Taxi Extract") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()


def read_raw_data(spark, path, schema):
    df = spark.read \
        .option("mergeSchema", "true") \
        .schema(schema) \
        .parquet(path)
    return df


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
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("Reading raw parquet files...")
    df = read_raw_data(spark, RAW_DATA_PATH, TAXI_SCHEMA)

    print("Adding ingestion metadata...")
    df = add_ingestion_metadata(df)

    print("Writing bronze layer...")
    write_bronze(df, BRONZE_OUTPUT_PATH)

    spark.stop()
    print("Extract complete.")


if __name__ == "__main__":
    main()