import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

storage = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    region_name="us-east-1",
    aws_access_key_id="username",
    aws_secret_access_key="password",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)


def storage_kv_set(bucket: str, key: str, data: bytes) -> str | None:
    try:
        storage.create_bucket(Bucket=bucket)
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code", "")
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise

    resp = storage.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
    )
    return resp.get("ETag")


def storage_kv_get(bucket: str, key: str) -> bytes:
    try:
        obj = storage.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code", "")
        if code in ("NoSuchKey", "NotFound", "404"):
            raise FileNotFoundError(key) from e
        raise


def storage_kv_del(bucket: str, key: str) -> None:
    try:
        storage.delete_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code", "")
        if code not in ("NoSuchKey", "NotFound", "404"):
            raise


def storage_kv_scan(bucket: str, prefix: str):
    paginator = storage.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]
