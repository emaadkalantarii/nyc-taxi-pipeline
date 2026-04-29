import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver")
GOLD_PATH = os.path.join(BASE_DIR, "data", "gold")
REPORT_PATH = os.path.join(BASE_DIR, "data_quality", "reports")

JDBC_URL = "jdbc:postgresql://localhost:5432/nyc_taxi"
JDBC_JAR = os.path.join(BASE_DIR, "jars", "postgresql-42.7.3.jar")

DB_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}