import collections
import contextlib
import logging

import boto3
import botocore
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

storage = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    region_name="us-east-1",
    aws_access_key_id="username",
    aws_secret_access_key="password",
    config=botocore.config.Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 5, "mode": "standard"},
        connect_timeout=3,
        read_timeout=10,
    ),
)


def storage_kv_set(bucket: str, key: str, data: bytes) -> str | None:
    try:
        storage.create_bucket(Bucket=bucket)
        log.info(f"[KV {bucket}] Created bucket")
    except ClientError as e:
        match str(((e.response or {}).get("Error") or {}).get("Code", "")):
            case "BucketAlreadyOwnedByYou" | "BucketAlreadyExists":
                log.debug(f"[KV {bucket}] Bucket exists")
            case code:
                log.error(f"[KV {bucket}] Bucket create failed: {code}")
                raise

    if not isinstance(data, (bytes, bytearray, memoryview)):
        data.seek(0)
        storage.upload_fileobj(
            Fileobj=data,
            Bucket=bucket,
            Key=key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
        head = storage.head_object(Bucket=bucket, Key=key)
        etag = head.get("ETag")
        if isinstance(etag, str):
            etag = etag.strip('"')
        size = head.get("ContentLength")
        log.info(
            f"[KV {bucket}/{key}] Streamed upload complete ({size} bytes, ETag={etag})"
        )
        return etag

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


def storage_kv_get(bucket: str, key: str):
    def f(body):
        with contextlib.closing(body):
            for chunk in body.iter_chunks(chunk_size=1024 * 1024):
                yield chunk
        log.info(f"[KV {bucket}/{key}] Stream closed")

    try:
        resp = storage.get_object(Bucket=bucket, Key=key)
        log.info(f"[KV {bucket}/{key}] Stream start")
        return f(resp["Body"])
    except ClientError as e:
        match str(((e.response or {}).get("Error") or {}).get("Code", "")):
            case "NoSuchKey" | "NoSuchBucket" | "NotFound" | "404":
                log.warning(f"[KV {bucket}/{key}] Not found")
                raise FileNotFoundError(f"{bucket}/{key}") from e
            case code:
                log.error(f"[KV {bucket}/{key}] Get failed: {code}")
                raise


def storage_kv_del(bucket: str, key: str) -> None:
    try:
        storage.delete_object(Bucket=bucket, Key=key)
        log.info(f"[KV {bucket}/{key}] Deleted")
    except ClientError as e:
        match str(((e.response or {}).get("Error") or {}).get("Code", "")):
            case "NoSuchKey" | "NoSuchBucket" | "NotFound" | "404":
                log.info(f"[KV {bucket}/{key}] Delete skipped (missing)")
                return
            case code:
                log.error(f"[KV {bucket}/{key}] Delete failed: {code}")
                raise


def storage_kv_scan(
    bucket: str, prefix: str
) -> collections.abc.Generator[str, None, None]:
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
        match str(((e.response or {}).get("Error") or {}).get("Code", "")):
            case "NoSuchBucket" | "NotFound" | "404":
                log.info(f"[KV {bucket}/{prefix}] Scan skipped (bucket missing)")
                return
            case code:
                log.error(f"[KV {bucket}/{prefix}] Scan failed: {code}")
                raise
