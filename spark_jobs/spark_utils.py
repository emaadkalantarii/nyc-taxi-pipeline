import os
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")
os.environ["HADOOP_OPTS"] = "-Djava.library.path=C:/hadoop/bin"


def get_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
        .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
        .getOrCreate()