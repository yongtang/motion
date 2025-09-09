import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

storage = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    region_name="us-east-1",
    aws_access_key_id="username",
    aws_secret_access_key="password",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 5, "mode": "standard"},
        connect_timeout=3,
        read_timeout=10,
    ),
)


def storage_kv_set(bucket: str, key: str, data: bytes) -> str | None:
    # Try to create the bucket; ignore races/already-exists.
    try:
        storage.create_bucket(Bucket=bucket)
        log.info(f"[KV {bucket}] Created bucket")
    except ClientError as e:
        code = str(((e.response or {}).get("Error") or {}).get("Code", ""))
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            log.debug(f"[KV {bucket}] Bucket exists ({code})")
        else:
            log.error(f"[KV {bucket}] Bucket create failed: {code}")
            raise

    resp = storage.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
    )
    etag = resp.get("ETag")
    if isinstance(etag, str):
        etag = etag.strip('"')
    log.info(f"[KV {bucket}/{key}] Stored {len(data)} bytes (ETag={etag})")
    return etag


def storage_kv_get(bucket: str, key: str) -> bytes:
    try:
        resp = storage.get_object(Bucket=bucket, Key=key)
        with resp["Body"] as body:
            data = body.read()
        log.info(f"[KV {bucket}/{key}] Loaded {len(data)} bytes")
        return data
    except ClientError as e:
        code = str(((e.response or {}).get("Error") or {}).get("Code", ""))
        if code in ("NoSuchKey", "NoSuchBucket", "NotFound", "404"):
            log.warning(f"[KV {bucket}/{key}] Not found ({code})")
            raise FileNotFoundError(f"{bucket}/{key}") from e
        log.error(f"[KV {bucket}/{key}] Get failed: {code}")
        raise


def storage_kv_del(bucket: str, key: str) -> None:
    try:
        storage.delete_object(Bucket=bucket, Key=key)
        log.info(f"[KV {bucket}/{key}] Deleted")
    except ClientError as e:
        code = str(((e.response or {}).get("Error") or {}).get("Code", ""))
        if code in ("NoSuchKey", "NoSuchBucket", "NotFound", "404"):
            log.info(f"[KV {bucket}/{key}] Delete skipped (missing: {code})")
            return
        log.error(f"[KV {bucket}/{key}] Delete failed: {code}")
        raise


def storage_kv_scan(bucket: str, prefix: str):
    log.info(f"[KV {bucket}/{prefix}] Scan start")
    paginator = storage.get_paginator("list_objects_v2")
    found = False
    try:
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig={"PageSize": 1000},
        ):
            for resp in page.get("Contents", []) or []:
                found = True
                yield resp["Key"]
        if not found:
            log.info(f"[KV {bucket}/{prefix}] Scan empty")
        else:
            log.info(f"[KV {bucket}/{prefix}] Scan complete")
    except ClientError as e:
        code = str(((e.response or {}).get("Error") or {}).get("Code", ""))
        if code in ("NoSuchBucket", "NotFound", "404"):
            log.info(f"[KV {bucket}/{prefix}] Scan skipped (bucket missing: {code})")
            return
        log.error(f"[KV {bucket}/{prefix}] Scan failed: {code}")
        raise
