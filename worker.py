"""
Fibonacci Queue Worker

Demonstrates testcontainers with Python using NATS, Redis, Postgres, and LocalStack (S3).
Computes Fibonacci numbers with Redis caching and stores results in S3/Postgres.
"""

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Callable

import boto3
import nats
import psycopg
import redis


# Configuration from environment
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
POSTGRES_URL = os.getenv(
    "POSTGRES_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "fibonacci-results")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

JOBS_SUBJECT = "jobs.fibonacci"
NOTIFICATIONS_SUBJECT = "notifications.fibonacci"


def get_redis_client(url: str | None = None) -> redis.Redis:
    """Create a Redis client."""
    return redis.from_url(url or REDIS_URL, decode_responses=True)


def get_s3_client(endpoint: str | None = None):
    """Create an S3 client configured for LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint or S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def get_db_connection(url: str | None = None) -> psycopg.Connection:
    """Create a Postgres connection."""
    return psycopg.connect(url or POSTGRES_URL)


def ensure_bucket_exists(s3_client, bucket: str) -> None:
    """Create the S3 bucket if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=bucket)
    except s3_client.exceptions.ClientError:
        s3_client.create_bucket(Bucket=bucket)


def compute_fibonacci(
    n: int,
    cache: redis.Redis | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> int:
    """
    Compute the nth Fibonacci number using Redis for memoization.

    Args:
        n: The Fibonacci number to compute
        cache: Redis client for caching intermediate results
        progress_callback: Optional callback(current_n, target_n) for progress updates

    Returns:
        The nth Fibonacci number
    """
    if n <= 1:
        return n

    # Check cache first
    if cache:
        cached = cache.get(f"fib:{n}")
        if cached is not None:
            return int(cached)

    # Iterative computation with caching
    a, b = 0, 1
    start = 2

    # Find highest cached value to start from
    if cache:
        for i in range(n, 1, -1):
            cached_a = cache.get(f"fib:{i - 1}")
            cached_b = cache.get(f"fib:{i}")
            if cached_a is not None and cached_b is not None:
                a, b = int(cached_a), int(cached_b)
                start = i + 1
                break

    for i in range(start, n + 1):
        a, b = b, a + b

        # Cache intermediate results
        if cache:
            cache.set(f"fib:{i}", b)

        # Report progress every 10% for large computations
        if progress_callback and n >= 100 and i % max(1, n // 10) == 0:
            progress_callback(i, n)

    return b


def save_result_to_s3(
    s3_client,
    bucket: str,
    job_id: str,
    n: int,
    result: int,
) -> str:
    """
    Save computation result as JSON to S3.

    Returns:
        S3 URI of the saved object
    """
    key = f"results/{job_id}.json"
    data = {
        "job_id": job_id,
        "n": n,
        "result": result,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )

    return f"s3://{bucket}/{key}"


def save_job_to_db(
    conn: psycopg.Connection,
    job_id: str,
    n: int,
    result: int,
    s3_uri: str,
) -> None:
    """Save job result to Postgres."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (id, n, result, s3_uri, completed_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (job_id, n, result, s3_uri, datetime.now(timezone.utc)),
        )
    conn.commit()


def get_job_from_db(conn: psycopg.Connection, job_id: str) -> dict | None:
    """Retrieve a job from Postgres."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, n, result, s3_uri, completed_at, created_at FROM jobs WHERE id = %s",
            (job_id,),
        )
        row = cur.fetchone()
        if row:
            return {
                "id": row[0],
                "n": row[1],
                "result": row[2],
                "s3_uri": row[3],
                "completed_at": row[4],
                "created_at": row[5],
            }
    return None


async def publish_notification(
    nc: nats.NATS,
    job_id: str,
    status: str,
    extra: dict | None = None,
) -> None:
    """Publish a job notification to NATS."""
    message = {
        "job_id": job_id,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        message.update(extra)

    await nc.publish(NOTIFICATIONS_SUBJECT, json.dumps(message).encode())


async def process_job(
    job_data: dict,
    nc: nats.NATS,
    cache: redis.Redis,
    s3_client,
    db_conn: psycopg.Connection,
    bucket: str = S3_BUCKET,
) -> dict:
    """
    Process a Fibonacci computation job.

    Args:
        job_data: Dict with 'job_id' and 'n'
        nc: NATS connection for notifications
        cache: Redis client for caching
        s3_client: S3 client for storing results
        db_conn: Postgres connection for persistence
        bucket: S3 bucket name

    Returns:
        Job result dict
    """
    job_id = job_data["job_id"]
    n = job_data["n"]

    # Notify job started
    await publish_notification(nc, job_id, "started", {"n": n})

    # Progress callback for large computations
    async def progress_callback(current: int, target: int):
        await publish_notification(
            nc,
            job_id,
            "progress",
            {
                "current": current,
                "target": target,
                "percent": int(current / target * 100),
            },
        )

    # Sync wrapper for async progress callback
    progress_events = []

    def sync_progress_callback(current: int, target: int):
        progress_events.append((current, target))

    # Compute Fibonacci
    result = compute_fibonacci(n, cache, sync_progress_callback)

    # Send accumulated progress notifications
    for current, target in progress_events:
        await publish_notification(
            nc,
            job_id,
            "progress",
            {
                "current": current,
                "target": target,
                "percent": int(current / target * 100),
            },
        )

    # Save to S3
    ensure_bucket_exists(s3_client, bucket)
    s3_uri = save_result_to_s3(s3_client, bucket, job_id, n, result)

    # Save to database
    save_job_to_db(db_conn, job_id, n, result, s3_uri)

    # Notify completion
    await publish_notification(
        nc, job_id, "completed", {"result": result, "s3_uri": s3_uri}
    )

    return {"job_id": job_id, "n": n, "result": result, "s3_uri": s3_uri}


async def run_worker():
    """Main worker loop - subscribe to NATS and process jobs."""
    print(f"Connecting to NATS at {NATS_URL}...")
    nc = await nats.connect(NATS_URL)

    print(f"Connecting to Redis at {REDIS_URL}...")
    cache = get_redis_client()

    print(f"Connecting to Postgres at {POSTGRES_URL}...")
    db_conn = get_db_connection()

    print(f"Connecting to S3 at {S3_ENDPOINT}...")
    s3_client = get_s3_client()

    print(f"Subscribing to {JOBS_SUBJECT}...")

    async def message_handler(msg):
        try:
            job_data = json.loads(msg.data.decode())
            print(f"Received job: {job_data}")

            result = await process_job(job_data, nc, cache, s3_client, db_conn)
            print(f"Completed job: {result}")

        except Exception as e:
            print(f"Error processing job: {e}")
            raise

    sub = await nc.subscribe(JOBS_SUBJECT, cb=message_handler)

    print("Worker ready, waiting for jobs...")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await sub.unsubscribe()
        await nc.close()
        db_conn.close()
        cache.close()


if __name__ == "__main__":
    asyncio.run(run_worker())
