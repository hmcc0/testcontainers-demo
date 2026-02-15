"""
Testcontainers fixtures for Fibonacci Queue Worker tests.

Demonstrates:
- Multi-container orchestration (Postgres, Redis, NATS, LocalStack)
- Session-scoped containers (start once, reuse across tests)
- Function-scoped clients (fresh state per test with cleanup)
- Dynamic port mapping
- Built-in helpers
"""

import pytest
import pytest_asyncio
import psycopg
import redis
import boto3
from pathlib import Path

from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer
from testcontainers.nats import NatsContainer
from testcontainers.localstack import LocalStackContainer


# Session-scoped containers - start once, reuse across all tests


def get_postgres_url(container) -> str:
    """Convert testcontainers URL to psycopg-compatible format."""
    # testcontainers returns: postgresql+psycopg2://user:pass@host:port/db
    # psycopg expects: postgresql://user:pass@host:port/db
    url = container.get_connection_url()
    return url.replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="session")
def postgres_container():
    """
    Session-scoped Postgres container.

    Demonstrates:
    - Using get_connection_url() for connection strings
    - Running initialization SQL on startup
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        # Initialize schema
        schema_path = Path(__file__).parent.parent / "schema.sql"
        conn = psycopg.connect(get_postgres_url(postgres))
        with conn.cursor() as cur:
            cur.execute(schema_path.read_text())
        conn.commit()
        conn.close()
        yield postgres


@pytest.fixture(scope="session")
def redis_container():
    """
    Session-scoped Redis container.

    Demonstrates:
    - Simple container startup with defaults
    - Using get_container_host_ip() and get_exposed_port()
    """
    with RedisContainer("redis:7-alpine") as redis_cont:
        yield redis_cont


@pytest.fixture(scope="session")
def nats_container():
    """
    Session-scoped NATS container.

    Demonstrates:
    - Message queue container for async communication
    """
    with NatsContainer("nats:2.10-alpine") as nats:
        yield nats


@pytest.fixture(scope="session")
def localstack_container():
    """
    Session-scoped LocalStack container for S3.

    Demonstrates:
    - AWS service emulation
    - Service-specific endpoint URLs
    """
    with LocalStackContainer("localstack/localstack:3") as localstack:
        yield localstack


# Function-scoped clients - fresh state per test


@pytest.fixture
def db_connection(postgres_container):
    """
    Function-scoped database connection with automatic cleanup.

    Demonstrates:
    - Creating fresh connections per test
    - Cleaning up test data between tests
    """
    conn = psycopg.connect(get_postgres_url(postgres_container))
    yield conn

    # Cleanup: delete all test data
    with conn.cursor() as cur:
        cur.execute("DELETE FROM jobs")
    conn.commit()
    conn.close()


@pytest.fixture
def redis_client(redis_container):
    """
    Function-scoped Redis client with automatic cleanup.

    Demonstrates:
    - Building connection URL from host/port
    - Flushing cache between tests
    """
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    url = f"redis://{host}:{port}"

    client = redis.from_url(url, decode_responses=True)
    yield client

    # Cleanup: flush all keys
    client.flushall()
    client.close()


@pytest.fixture
def s3_client(localstack_container):
    """
    Function-scoped S3 client.

    Demonstrates:
    - Configuring boto3 for LocalStack
    - Getting endpoint URL from container
    """
    endpoint = localstack_container.get_url()

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    yield client


@pytest.fixture
def s3_bucket(s3_client):
    """
    Function-scoped S3 bucket with cleanup.

    Demonstrates:
    - Creating test buckets
    - Cleaning up objects between tests
    """
    bucket_name = "test-fibonacci-results"

    # Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass

    yield bucket_name

    # Cleanup: delete all objects in bucket
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            for obj in response["Contents"]:
                s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    except Exception:
        pass


@pytest_asyncio.fixture
async def nats_client(nats_container):
    """
    Function-scoped NATS client.

    Demonstrates:
    - Async client creation
    - Getting connection URL from container
    """
    import nats as nats_lib

    host = nats_container.get_container_host_ip()
    port = nats_container.get_exposed_port(4222)
    url = f"nats://{host}:{port}"

    nc = await nats_lib.connect(url)
    yield nc
    await nc.close()


# Convenience fixtures for URLs


@pytest.fixture
def postgres_url(postgres_container):
    """Get Postgres connection URL."""
    return get_postgres_url(postgres_container)


@pytest.fixture
def redis_url(redis_container):
    """Get Redis connection URL."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}"


@pytest.fixture
def nats_url(nats_container):
    """Get NATS connection URL."""
    host = nats_container.get_container_host_ip()
    port = nats_container.get_exposed_port(4222)
    return f"nats://{host}:{port}"


@pytest.fixture
def s3_endpoint(localstack_container):
    """Get S3 endpoint URL."""
    return localstack_container.get_url()
