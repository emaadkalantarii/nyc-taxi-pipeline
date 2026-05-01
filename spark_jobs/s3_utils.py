import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "eu-west-1"),
    )


def use_s3():
    return os.environ.get("USE_S3", "false").lower() == "true"


def get_bucket():
    return os.environ.get("S3_BUCKET", "nyc-taxi-pipeline-emad")


def upload_folder_to_s3(local_folder, s3_prefix):
    if not use_s3():
        print(f"S3 disabled — skipping upload of {local_folder}")
        return

    client = get_s3_client()
    bucket = get_bucket()
    uploaded = 0

    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_folder)
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")

            try:
                client.upload_file(local_path, bucket, s3_key)
                uploaded += 1
            except (ClientError, NoCredentialsError) as e:
                print(f"Failed to upload {local_path}: {e}")

    print(f"Uploaded {uploaded} files to s3://{bucket}/{s3_prefix}/")


def download_folder_from_s3(s3_prefix, local_folder):
    if not use_s3():
        print(f"S3 disabled — using local folder {local_folder}")
        return

    client = get_s3_client()
    bucket = get_bucket()
    os.makedirs(local_folder, exist_ok=True)

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=s3_prefix)

    downloaded = 0
    for page in pages:
        for obj in page.get("Contents", []):
            s3_key = obj["Key"]
            relative = os.path.relpath(s3_key, s3_prefix)
            local_path = os.path.join(local_folder, relative)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            client.download_file(bucket, s3_key, local_path)
            downloaded += 1

    print(f"Downloaded {downloaded} files from s3://{bucket}/{s3_prefix}/")


def list_s3_prefix(s3_prefix):
    client = get_s3_client()
    bucket = get_bucket()
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=s3_prefix)

    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def verify_s3_connection():
    try:
        client = get_s3_client()
        bucket = get_bucket()
        client.head_bucket(Bucket=bucket)
        print(f"S3 connection verified — bucket '{bucket}' is accessible")
        return True
    except (ClientError, NoCredentialsError) as e:
        print(f"S3 connection failed: {e}")
        return False