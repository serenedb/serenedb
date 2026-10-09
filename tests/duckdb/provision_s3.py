import os
import sys

import boto3
from botocore.client import Config

endpoint, duckdb_data, generated = sys.argv[1:4]

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id="duckdb_minio_admin",
    aws_secret_access_key="duckdb_minio_admin_password",
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

existing = {bucket["Name"] for bucket in s3.list_buckets().get("Buckets", [])}
for bucket in ("test-bucket", "test-bucket-2", "test-bucket-public"):
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
s3.put_bucket_versioning(Bucket="test-bucket", VersioningConfiguration={"Status": "Enabled"})

uploads = {
    "phonenumbers.csv": os.path.join(duckdb_data, "csv", "phonenumbers.csv"),
    "t1.parquet": os.path.join(duckdb_data, "parquet-testing", "glob", "t1.parquet"),
    "lineitem_large.parquet": os.path.join(generated, "presigned-url-lineitem.parquet"),
    "attach.db": os.path.join(generated, "attach.db"),
    "lineitem_sf1.db": os.path.join(generated, "lineitem_sf1.db"),
}
for name, path in uploads.items():
    s3.upload_file(path, "test-bucket", f"presigned/{name}")

presigned = {
    "S3_SMALL_CSV_PRESIGNED_URL": "phonenumbers.csv",
    "S3_SMALL_PARQUET_PRESIGNED_URL": "t1.parquet",
    "S3_LARGE_PARQUET_PRESIGNED_URL": "lineitem_large.parquet",
    "S3_ATTACH_DB_PRESIGNED_URL": "attach.db",
}
for variable, name in presigned.items():
    url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": "test-bucket", "Key": f"presigned/{name}"}, ExpiresIn=7 * 24 * 3600
    )
    print(f"{variable}={url}")
