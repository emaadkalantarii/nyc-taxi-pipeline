import os
from pyspark.sql import functions as F
from spark_utils import get_spark_session
from load_env import load_env
from s3_utils import upload_folder_to_s3, verify_s3_connection
load_env()

SILVER_PATH = "data/silver/"
GOLD_PATH = "data/gold/"


def read_silver(spark, path):
    return spark.read.parquet(path)


def create_hourly_stats(df):
    return df.groupBy(
        "pickup_month",
        "pickup_day_of_week",
        "pickup_hour",
        "time_of_day",
        "is_weekend"
    ).agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
        F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph")
    ).orderBy("pickup_month", "pickup_day_of_week", "pickup_hour")


def create_location_stats(df):
    return df.groupBy(
        "PULocationID"
    ).agg(
        F.count("*").alias("total_pickups"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue")
    ).orderBy(F.desc("total_pickups"))


def create_payment_stats(df):
    return df.groupBy(
        "payment_type_desc",
        "pickup_month"
    ).agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue")
    ).orderBy("pickup_month", F.desc("total_trips"))


def create_daily_summary(df):
    return df.groupBy(
        F.to_date("tpep_pickup_datetime").alias("trip_date"),
        "pickup_month"
    ).agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("daily_revenue"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.countDistinct("PULocationID").alias("active_pickup_zones")
    ).orderBy("trip_date")


def write_gold_table(df, output_path, table_name):
    full_path = os.path.join(output_path, table_name)
    os.makedirs(full_path, exist_ok=True)
    df.write \
        .mode("overwrite") \
        .parquet(full_path)
    print(f"Gold table '{table_name}' written: {df.count():,} rows")


def main():
    spark = get_spark_session("NYC Taxi Gold Transform")
    spark.sparkContext.setLogLevel("ERROR")

    print("Reading silver layer...")
    df = read_silver(spark, SILVER_PATH)

    print("Creating gold tables...")

    hourly_stats = create_hourly_stats(df)
    write_gold_table(hourly_stats, GOLD_PATH, "hourly_stats")

    location_stats = create_location_stats(df)
    write_gold_table(location_stats, GOLD_PATH, "location_stats")

    payment_stats = create_payment_stats(df)
    write_gold_table(payment_stats, GOLD_PATH, "payment_stats")

    daily_summary = create_daily_summary(df)
    write_gold_table(daily_summary, GOLD_PATH, "daily_summary")

    spark.stop()
    print("Gold transformation complete.")

    if verify_s3_connection():
        upload_folder_to_s3(GOLD_PATH, "gold")


if __name__ == "__main__":
    main()