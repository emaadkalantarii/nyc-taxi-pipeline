import os
import pandas as pd
from sqlalchemy import create_engine, text

LOCAL_DB = "postgresql://airflow:airflow@localhost:5432/nyc_taxi"
NEON_DB = "postgresql://neondb_owner:password@ep-morning-wind-al5prp8z-pooler.c-3.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

tables = [
    ("public", "hourly_stats"),
    ("public", "daily_summary"),
    ("public", "location_stats"),
    ("public", "payment_stats"),
    ("analytics", "mart_peak_hours"),
    ("analytics", "mart_revenue_trends"),
    ("analytics", "mart_location_performance"),
    ("analytics", "mart_payment_insights"),
]

local_engine = create_engine(LOCAL_DB)
neon_engine = create_engine(NEON_DB)

with neon_engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
    conn.commit()

for schema, table in tables:
    print(f"Migrating {schema}.{table}...")
    df = pd.read_sql(f"SELECT * FROM {schema}.{table}", local_engine)
    df.to_sql(table, neon_engine, schema=schema, if_exists="replace", index=False)
    print(f"  Done — {len(df):,} rows")

print("All tables migrated to Neon.")