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


def storage_kv_set(key: str, data: bytes) -> str | None:
    try:
        storage.create_bucket(Bucket="scenes")
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code", "")
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise

    resp = storage.put_object(
        Bucket="scenes",
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
    )
    return resp.get("ETag")


def storage_kv_get(key: str) -> bytes:
    try:
        obj = storage.get_object(Bucket="scenes", Key=key)
        return obj["Body"].read()
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code", "")
        if code in ("NoSuchKey", "NotFound", "404"):
            raise FileNotFoundError(key) from e
        raise


def storage_kv_del(key: str) -> None:
    try:
        storage.delete_object(Bucket="scenes", Key=key)
    except ClientError as e:
        code = (e.response.get("Error", {}) or {}).get("Code", "")
        if code not in ("NoSuchKey", "NotFound", "404"):
            raise
