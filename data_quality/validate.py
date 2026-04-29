import os
import sys
import json
import pandas as pd
from datetime import datetime

os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")
os.environ["HADOOP_OPTS"] = "-Djava.library.path=C:/hadoop/bin"

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import SparkSession

SILVER_PATH = "data/silver/"
GOLD_PATH = "data/gold/"
REPORT_PATH = "data_quality/reports/"


def get_spark():
    return SparkSession.builder \
        .appName("NYC Taxi Data Quality") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
        .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
        .getOrCreate()


def load_sample(spark, path, sample_size=50000):
    df = spark.read.parquet(path)
    total = df.count()
    fraction = min(sample_size / total, 1.0)
    sample_df = df.sample(fraction=fraction, seed=42).toPandas()
    print(f"Loaded {len(sample_df):,} rows as sample from {total:,} total rows")
    return sample_df


def validate_silver(df):
    print("\n--- Validating Silver Layer ---")
    results = []

    def check(name, condition, passing_count, total_count):
        pct = (passing_count / total_count) * 100
        passed = condition
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {name}: {passing_count:,}/{total_count:,} ({pct:.2f}%)")
        results.append({
            "check": name,
            "status": status,
            "passing_rows": passing_count,
            "total_rows": total_count,
            "pass_rate_pct": round(pct, 2)
        })

    total = len(df)

    valid_fare = (df["fare_amount"] > 0).sum()
    check("fare_amount is positive", valid_fare == total, valid_fare, total)

    valid_distance = (df["trip_distance"] > 0).sum()
    check("trip_distance is positive", valid_distance == total, valid_distance, total)

    valid_distance_max = (df["trip_distance"] < 500).sum()
    check("trip_distance under 500 miles", valid_distance_max == total, valid_distance_max, total)

    valid_passengers = ((df["passenger_count"] >= 1) & (df["passenger_count"] <= 8)).sum()
    check("passenger_count between 1 and 8", valid_passengers == total, valid_passengers, total)

    valid_total = (df["total_amount"] > 0).sum()
    check("total_amount is positive", valid_total == total, valid_total, total)

    no_null_pickup = df["tpep_pickup_datetime"].notna().sum()
    check("tpep_pickup_datetime not null", no_null_pickup == total, no_null_pickup, total)

    no_null_dropoff = df["tpep_dropoff_datetime"].notna().sum()
    check("tpep_dropoff_datetime not null", no_null_dropoff == total, no_null_dropoff, total)

    valid_duration = (df["trip_duration_minutes"] > 0).sum()
    check("trip_duration_minutes is positive", valid_duration == total, valid_duration, total)

    valid_duration_max = (df["trip_duration_minutes"] < 180).sum()
    check("trip_duration under 180 minutes", valid_duration_max == total, valid_duration_max, total)

    valid_speed = (df["speed_mph"] < 150).sum()
    check("speed_mph under 150", valid_speed == total, valid_speed, total)

    no_null_pu = df["PULocationID"].notna().sum()
    check("PULocationID not null", no_null_pu == total, no_null_pu, total)

    valid_payment = df["payment_type"].isin([1, 2, 3, 4, 5, 6]).sum()
    check("payment_type is valid code", valid_payment / total >= 0.99, valid_payment, total)

    valid_tip_pct = ((df["tip_percentage"] >= 0) & (df["tip_percentage"] <= 200)).sum()
    check("tip_percentage between 0 and 200", valid_tip_pct == total, valid_tip_pct, total)

    return results


def validate_gold_daily(df):
    print("\n--- Validating Gold: daily_summary ---")
    results = []

    def check(name, condition, detail=""):
        status = "PASS" if condition else "FAIL"
        print(f"  [{status}] {name} {detail}")
        results.append({"check": name, "status": status, "detail": detail})

    check("No null trip_date", df["trip_date"].notna().all())
    check("total_trips always positive", (df["total_trips"] > 0).all())
    check("daily_revenue always positive", (df["daily_revenue"] > 0).all())
    check("avg_fare is reasonable", ((df["avg_fare"] > 0) & (df["avg_fare"] < 500)).all())
    check("No duplicate dates",
          not df["trip_date"].duplicated().any(),
          f"({df['trip_date'].nunique()} unique dates)")

    return results


def generate_html_report(silver_results, gold_results):
    os.makedirs(REPORT_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    silver_passed = sum(1 for r in silver_results if r["status"] == "PASS")
    silver_total = len(silver_results)
    gold_passed = sum(1 for r in gold_results if r["status"] == "PASS")
    gold_total = len(gold_results)
    total_passed = silver_passed + gold_passed
    total_checks = silver_total + gold_total

    def row_color(status):
        return "#d4edda" if status == "PASS" else "#f8d7da"

    def badge(status):
        color = "#28a745" if status == "PASS" else "#dc3545"
        return f'<span style="background:{color};color:white;padding:3px 10px;border-radius:12px;font-size:12px;font-weight:bold;">{status}</span>'

    silver_rows = ""
    for r in silver_results:
        silver_rows += f"""
        <tr style="background:{row_color(r['status'])}">
            <td>{r['check']}</td>
            <td>{badge(r['status'])}</td>
            <td>{r['passing_rows']:,} / {r['total_rows']:,}</td>
            <td>{r['pass_rate_pct']}%</td>
        </tr>"""

    gold_rows = ""
    for r in gold_results:
        gold_rows += f"""
        <tr style="background:{row_color(r['status'])}">
            <td>{r['check']}</td>
            <td>{badge(r['status'])}</td>
            <td colspan="2">{r.get('detail', '')}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>NYC Taxi Pipeline — Data Quality Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
        .container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; }}
        .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
        .stat-box {{ flex: 1; padding: 20px; border-radius: 8px; text-align: center; }}
        .stat-box.good {{ background: #d4edda; border: 1px solid #28a745; }}
        .stat-box.info {{ background: #d1ecf1; border: 1px solid #17a2b8; }}
        .stat-number {{ font-size: 36px; font-weight: bold; color: #2c3e50; }}
        .stat-label {{ font-size: 14px; color: #666; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th {{ background: #2c3e50; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #ddd; }}
        .timestamp {{ color: #888; font-size: 13px; margin-top: 20px; }}
    </style>
</head>
<body>
<div class="container">
    <h1>NYC Taxi Pipeline — Data Quality Report</h1>
    <p class="timestamp">Generated: {timestamp}</p>

    <div class="summary">
        <div class="stat-box good">
            <div class="stat-number">{total_passed}/{total_checks}</div>
            <div class="stat-label">Total Checks Passed</div>
        </div>
        <div class="stat-box info">
            <div class="stat-number">{silver_passed}/{silver_total}</div>
            <div class="stat-label">Silver Layer Checks</div>
        </div>
        <div class="stat-box info">
            <div class="stat-number">{gold_passed}/{gold_total}</div>
            <div class="stat-label">Gold Layer Checks</div>
        </div>
    </div>

    <h2>Silver Layer Validation</h2>
    <table>
        <tr>
            <th>Check</th>
            <th>Status</th>
            <th>Passing Rows</th>
            <th>Pass Rate</th>
        </tr>
        {silver_rows}
    </table>

    <h2>Gold Layer Validation — daily_summary</h2>
    <table>
        <tr>
            <th>Check</th>
            <th>Status</th>
            <th colspan="2">Detail</th>
        </tr>
        {gold_rows}
    </table>
</div>
</body>
</html>"""

    report_file = os.path.join(REPORT_PATH, "data_quality_report.html")
    with open(report_file, "w") as f:
        f.write(html)

    print(f"\nHTML report saved to: {report_file}")
    return report_file


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("Loading silver sample...")
    silver_df = load_sample(spark, SILVER_PATH)

    print("Loading gold daily_summary...")
    gold_df = spark.read.parquet(f"{GOLD_PATH}daily_summary/").toPandas()

    silver_results = validate_silver(silver_df)
    gold_results = validate_gold_daily(gold_df)

    total_checks = len(silver_results) + len(gold_results)
    total_passed = sum(1 for r in silver_results + gold_results if r["status"] == "PASS")

    print(f"\n{'='*40}")
    print(f"TOTAL: {total_passed}/{total_checks} checks passed")
    print(f"{'='*40}")

    generate_html_report(silver_results, gold_results)

    spark.stop()


if __name__ == "__main__":
    main()