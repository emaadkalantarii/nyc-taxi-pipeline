import os
import sys
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPARK_JOBS_DIR = os.path.join(BASE_DIR, "spark_jobs")
DATA_QUALITY_DIR = os.path.join(BASE_DIR, "data_quality")


def run_script(script_path):
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        cwd=BASE_DIR
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Script failed: {script_path}\n{result.stderr}")
    return result.stdout


def task_extract():
    print("Starting Extract task...")
    run_script(os.path.join(SPARK_JOBS_DIR, "extract.py"))
    print("Extract task complete.")


def task_transform_silver():
    print("Starting Silver Transform task...")
    run_script(os.path.join(SPARK_JOBS_DIR, "transform_silver.py"))
    print("Silver Transform task complete.")


def task_transform_gold():
    print("Starting Gold Transform task...")
    run_script(os.path.join(SPARK_JOBS_DIR, "transform_gold.py"))
    print("Gold Transform task complete.")


def task_validate():
    print("Starting Data Quality Validation task...")
    run_script(os.path.join(DATA_QUALITY_DIR, "validate.py"))
    print("Validation task complete.")


def task_load():
    print("Starting Load task...")
    run_script(os.path.join(SPARK_JOBS_DIR, "load.py"))
    print("Load task complete.")