import boto3
import botocore
import botocore.stub
import pytest

import server.storage


@pytest.fixture
def endpoint(docker_compose, monkeypatch):
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{docker_compose['motion']}:9000",
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
    monkeypatch.setattr(server.storage, "storage", client)
    return client


@pytest.fixture
def freeze_time(monkeypatch):
    fixed_now = 1_700_000_000
    monkeypatch.setattr(server.storage.time, "time", lambda: fixed_now)
    return fixed_now


def test_storage_acquire(freeze_time, endpoint):
    bucket = "bkt"
    client = endpoint

    # Case A: first-time acquire
    key_a = "vm-001"
    deadline_a = str(freeze_time + 300)
    with botocore.stub.Stubber(client) as stub:
        stub.add_response(
            "put_object",
            service_response={"ETag": '"etag-a1"'},
            expected_params={
                "Bucket": bucket,
                "Key": key_a,
                "Body": b"",
                "ContentType": "application/octet-stream",
                "Metadata": {"deadline": deadline_a},
            },
        )
        assert server.storage.storage_kv_acquire(bucket, key_a, ttl=300) is True

    # Case B: busy (not expired)
    key_b = "vm-002"
    deadline_b_new = str(freeze_time + 300)
    deadline_b_existing = str(freeze_time + 120)
    with botocore.stub.Stubber(client) as stub:
        stub.add_client_error(
            "put_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params={
                "Bucket": bucket,
                "Key": key_b,
                "Body": b"",
                "ContentType": "application/octet-stream",
                "Metadata": {"deadline": deadline_b_new},
            },
        )
        stub.add_response(
            "head_object",
            service_response={
                "ETag": '"etag-b-old"',
                "ContentLength": 0,
                "Metadata": {"deadline": deadline_b_existing},
            },
            expected_params={"Bucket": bucket, "Key": key_b},
        )
        assert server.storage.storage_kv_acquire(bucket, key_b, ttl=300) is False

    # Case C: expired -> reclaim
    key_c = "vm-003"
    deadline_c_new = str(freeze_time + 300)
    deadline_c_existing = str(freeze_time - 10)
    with botocore.stub.Stubber(client) as stub:
        stub.add_client_error(
            "put_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params={
                "Bucket": bucket,
                "Key": key_c,
                "Body": b"",
                "ContentType": "application/octet-stream",
                "Metadata": {"deadline": deadline_c_new},
            },
        )
        stub.add_response(
            "head_object",
            service_response={
                "ETag": '"etag-c-old"',
                "ContentLength": 0,
                "Metadata": {"deadline": deadline_c_existing},
            },
            expected_params={"Bucket": bucket, "Key": key_c},
        )
        stub.add_response(
            "put_object",
            service_response={"ETag": '"etag-c-new"'},
            expected_params={
                "Bucket": bucket,
                "Key": key_c,
                "Body": b"",
                "ContentType": "application/octet-stream",
                "Metadata": {"deadline": str(freeze_time + 300)},
            },
        )
        assert server.storage.storage_kv_acquire(bucket, key_c, ttl=300) is True


def test_storage_release(endpoint):
    bucket = "bkt"
    key = "vm-rel"
    client = endpoint

    with botocore.stub.Stubber(client) as stub:
        stub.add_response(
            "delete_object",
            service_response={},
            expected_params={"Bucket": bucket, "Key": key},
        )
        stub.add_client_error(
            "delete_object",
            service_error_code="NoSuchKey",
            http_status_code=404,
            expected_params={"Bucket": bucket, "Key": key},
        )

        server.storage.storage_kv_release(bucket, key)
        server.storage.storage_kv_release(bucket, key)  # should not raise
