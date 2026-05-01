import os
from pyspark.sql import functions as F
from spark_utils import get_spark_session
from load_env import load_env
from s3_utils import upload_folder_to_s3, verify_s3_connection
load_env()

BRONZE_PATH = "data/bronze/"
SILVER_PATH = "data/silver/"


def read_bronze(spark, path):
    return spark.read.parquet(path)


def clean_data(df):
    original_count = df.count()
    print(f"Rows before cleaning: {original_count:,}")

    df = df.filter(
        (F.col("fare_amount") > 0) &
        (F.col("trip_distance") > 0) &
        (F.col("trip_distance") < 500) &
        (F.col("total_amount") > 0) &
        (F.col("total_amount") < 2000) &
        (F.col("passenger_count") >= 1) &
        (F.col("passenger_count") <= 8) &
        (F.col("tpep_pickup_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) &
        (F.col("PULocationID").isNotNull()) &
        (F.col("DOLocationID").isNotNull())
    )

    cleaned_count = df.count()
    removed = original_count - cleaned_count
    print(f"Rows after cleaning: {cleaned_count:,}")
    print(f"Rows removed: {removed:,} ({removed/original_count*100:.2f}%)")

    return df


def engineer_features(df):
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60
    )

    df = df.withColumn(
        "speed_mph",
        F.when(
            F.col("trip_duration_minutes") > 0,
            F.col("trip_distance") / (F.col("trip_duration_minutes") / 60)
        ).otherwise(None)
    )

    df = df.withColumn(
        "pickup_hour",
        F.hour("tpep_pickup_datetime")
    )

    df = df.withColumn(
        "pickup_day_of_week",
        F.dayofweek("tpep_pickup_datetime")
    )

    df = df.withColumn(
        "pickup_month",
        F.month("tpep_pickup_datetime")
    )

    df = df.withColumn(
        "time_of_day",
        F.when((F.col("pickup_hour") >= 6) & (F.col("pickup_hour") < 12), "morning")
         .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 17), "afternoon")
         .when((F.col("pickup_hour") >= 17) & (F.col("pickup_hour") < 21), "evening")
         .otherwise("night")
    )

    df = df.withColumn(
        "is_weekend",
        F.when(F.col("pickup_day_of_week").isin([1, 7]), True).otherwise(False)
    )

    df = df.withColumn(
        "fare_per_mile",
        F.when(
            F.col("trip_distance") > 0,
            F.round(F.col("fare_amount") / F.col("trip_distance"), 2)
        ).otherwise(None)
    )

    df = df.withColumn(
        "tip_percentage",
        F.when(
            F.col("fare_amount") > 0,
            F.round((F.col("tip_amount") / F.col("fare_amount")) * 100, 2)
        ).otherwise(0.0)
    )

    df = df.withColumn(
        "payment_type_desc",
        F.when(F.col("payment_type") == 1, "Credit Card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No Charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .otherwise("Unknown")
    )

    df = df.filter(
        (F.col("trip_duration_minutes") > 0) &
        (F.col("trip_duration_minutes") < 180) &
        (F.col("speed_mph") < 150)
    )

    return df


def write_silver(df, output_path):
    os.makedirs(output_path, exist_ok=True)
    df.write \
        .mode("overwrite") \
        .partitionBy("pickup_month") \
        .parquet(output_path)
    print(f"Silver layer written to: {output_path}")
    print(f"Total rows in silver: {df.count():,}")


def main():
    spark = get_spark_session("NYC Taxi Silver Transform")
    spark.sparkContext.setLogLevel("ERROR")

    print("Reading bronze layer...")
    df = read_bronze(spark, BRONZE_PATH)

    print("Cleaning data...")
    df = clean_data(df)

    print("Engineering features...")
    df = engineer_features(df)

    print("Writing silver layer...")
    write_silver(df, SILVER_PATH)

    spark.stop()
    print("Silver transformation complete.")

    if verify_s3_connection():
        upload_folder_to_s3(SILVER_PATH, "silver")


if __name__ == "__main__":
    main()