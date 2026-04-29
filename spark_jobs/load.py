import os
from pyspark.sql import functions as F
from spark_utils import get_spark_session

GOLD_PATH = "data/gold/"
JDBC_URL = "jdbc:postgresql://localhost:5432/nyc_taxi"
JDBC_DRIVER = "org.postgresql.Driver"
JDBC_JAR = "jars/postgresql-42.7.3.jar"

DB_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": JDBC_DRIVER
}


def get_spark_with_jdbc(app_name):
    return get_spark_session.__wrapped__(app_name) if hasattr(get_spark_session, '__wrapped__') else (
        __import__('pyspark').sql.SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.driver.extraClassPath", JDBC_JAR)
        .config("spark.executor.extraClassPath", JDBC_JAR)
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin")
        .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin")
        .getOrCreate()
    )


def create_spark_with_jdbc(app_name):
    os.environ["HADOOP_HOME"] = "C:/hadoop"
    os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")
    os.environ["HADOOP_OPTS"] = "-Djava.library.path=C:/hadoop/bin"

    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.driver.extraClassPath", JDBC_JAR) \
        .config("spark.executor.extraClassPath", JDBC_JAR) \
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
        .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
        .getOrCreate()


def load_table(df, table_name, mode="overwrite"):
    print(f"Loading {table_name} into PostgreSQL...")
    row_count = df.count()

    df.write \
        .jdbc(
            url=JDBC_URL,
            table=table_name,
            mode=mode,
            properties=DB_PROPERTIES
        )

    print(f"Loaded {row_count:,} rows into '{table_name}'")


def rename_columns_for_postgres(df, table_name):
    if table_name == "location_stats":
        df = df.withColumnRenamed("PULocationID", "pu_location_id")
    return df


def main():
    spark = create_spark_with_jdbc("NYC Taxi Load")
    spark.sparkContext.setLogLevel("ERROR")

    print("Loading Gold tables into PostgreSQL...")
    print("-" * 40)

    hourly_stats = spark.read.parquet(f"{GOLD_PATH}hourly_stats/")
    hourly_stats = rename_columns_for_postgres(hourly_stats, "hourly_stats")
    load_table(hourly_stats, "hourly_stats")

    location_stats = spark.read.parquet(f"{GOLD_PATH}location_stats/")
    location_stats = rename_columns_for_postgres(location_stats, "location_stats")
    load_table(location_stats, "location_stats")

    payment_stats = spark.read.parquet(f"{GOLD_PATH}payment_stats/")
    payment_stats = rename_columns_for_postgres(payment_stats, "payment_stats")
    load_table(payment_stats, "payment_stats")

    daily_summary = spark.read.parquet(f"{GOLD_PATH}daily_summary/")
    daily_summary = rename_columns_for_postgres(daily_summary, "daily_summary")
    load_table(daily_summary, "daily_summary")

    print("-" * 40)
    print("All tables loaded successfully.")
    spark.stop()


if __name__ == "__main__":
    main()