import os
import tempfile

os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")
os.environ["HADOOP_OPTS"] = "-Djava.library.path=C:/hadoop/bin"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("NYC Taxi Explorer") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.hadoop.home.dir", "C:/hadoop") \
    .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
    .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/raw/")

print("=== SCHEMA ===")
df.printSchema()

print("=== ROW COUNT ===")
print(f"Total rows: {df.count():,}")

print("=== FIRST 5 ROWS ===")
df.show(5, truncate=False)

print("=== BASIC STATISTICS ===")
df.select(
    "fare_amount",
    "trip_distance",
    "passenger_count",
    "tip_amount",
    "total_amount"
).describe().show()

print("=== NULL COUNTS ===")
null_counts = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
])
null_counts.show()

spark.stop()