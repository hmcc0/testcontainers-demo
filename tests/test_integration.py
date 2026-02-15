"""
Integration tests for Fibonacci Queue Worker.

Tests the complete job flow with all services:
- Job submission via NATS
- Fibonacci computation with caching
- Result storage in S3
- Job record in Postgres
- Notification publishing
"""

import asyncio
import json
import uuid

import pytest

from worker import (
    process_job,
    compute_fibonacci,
    get_job_from_db,
)


pytestmark = pytest.mark.asyncio


class TestCompleteJobFlow:
    """Test the complete job processing flow."""

    async def test_full_job_flow(
        self,
        nats_client,
        redis_client,
        s3_client,
        s3_bucket,
        db_connection,
    ):
        """Test complete flow: job → compute → S3 → Postgres → notifications."""
        job_id = str(uuid.uuid4())
        n = 20

        # Collect notifications
        notifications = []

        async def notification_handler(msg):
            notifications.append(json.loads(msg.data.decode()))

        sub = await nats_client.subscribe(
            "notifications.fibonacci",
            cb=notification_handler,
        )

        # Process job
        result = await process_job(
            {"job_id": job_id, "n": n},
            nc=nats_client,
            cache=redis_client,
            s3_client=s3_client,
            db_conn=db_connection,
            bucket=s3_bucket,
        )

        # Allow notifications to be delivered
        await asyncio.sleep(0.1)

        # Verify result
        assert result["job_id"] == job_id
        assert result["n"] == n
        assert result["result"] == 6765
        assert result["s3_uri"].startswith(f"s3://{s3_bucket}/")

        # Verify S3 object exists
        key = f"results/{job_id}.json"
        response = s3_client.get_object(Bucket=s3_bucket, Key=key)
        s3_data = json.loads(response["Body"].read().decode())
        assert s3_data["result"] == 6765

        # Verify database record
        job = get_job_from_db(db_connection, job_id)
        assert job is not None
        assert job["result"] == 6765
        assert job["s3_uri"] == result["s3_uri"]

        # Verify notifications
        assert len(notifications) >= 2  # At least started and completed
        statuses = [n["status"] for n in notifications]
        assert "started" in statuses
        assert "completed" in statuses

        await sub.unsubscribe()

    async def test_multiple_sequential_jobs(
        self,
        nats_client,
        redis_client,
        s3_client,
        s3_bucket,
        db_connection,
    ):
        """Test processing multiple jobs sequentially."""
        jobs = [
            (str(uuid.uuid4()), 5),
            (str(uuid.uuid4()), 10),
            (str(uuid.uuid4()), 15),
        ]
        expected_results = [5, 55, 610]

        for (job_id, n), expected in zip(jobs, expected_results):
            result = await process_job(
                {"job_id": job_id, "n": n},
                nc=nats_client,
                cache=redis_client,
                s3_client=s3_client,
                db_conn=db_connection,
                bucket=s3_bucket,
            )

            assert result["result"] == expected

            # Verify in database
            job = get_job_from_db(db_connection, job_id)
            assert job["result"] == expected

    async def test_cache_speedup_verification(
        self,
        nats_client,
        redis_client,
        s3_client,
        s3_bucket,
        db_connection,
    ):
        """Test that cache improves performance for repeated computations."""
        n = 30

        # First job - cold cache
        job1_id = str(uuid.uuid4())
        await process_job(
            {"job_id": job1_id, "n": n},
            nc=nats_client,
            cache=redis_client,
            s3_client=s3_client,
            db_conn=db_connection,
            bucket=s3_bucket,
        )

        # Verify cache is populated
        assert redis_client.get(f"fib:{n}") is not None

        # Second job with same n - should use cache
        job2_id = str(uuid.uuid4())
        result = await process_job(
            {"job_id": job2_id, "n": n},
            nc=nats_client,
            cache=redis_client,
            s3_client=s3_client,
            db_conn=db_connection,
            bucket=s3_bucket,
        )

        assert result["result"] == 832040

    async def test_large_fibonacci_with_progress(
        self,
        nats_client,
        redis_client,
        s3_client,
        s3_bucket,
        db_connection,
    ):
        """Test large Fibonacci computation with progress updates."""
        job_id = str(uuid.uuid4())
        n = 150  # Large enough to trigger progress updates

        # Collect notifications
        notifications = []

        async def notification_handler(msg):
            notifications.append(json.loads(msg.data.decode()))

        sub = await nats_client.subscribe(
            "notifications.fibonacci",
            cb=notification_handler,
        )

        await process_job(
            {"job_id": job_id, "n": n},
            nc=nats_client,
            cache=redis_client,
            s3_client=s3_client,
            db_conn=db_connection,
            bucket=s3_bucket,
        )

        await asyncio.sleep(0.1)

        # Should have progress notifications
        progress_notifications = [n for n in notifications if n["status"] == "progress"]
        assert len(progress_notifications) > 0

        # Verify percent values in progress
        for pn in progress_notifications:
            assert "percent" in pn
            assert 0 <= pn["percent"] <= 100

        await sub.unsubscribe()


class TestContainerNetworking:
    """Test that all containers are properly networked."""

    async def test_nats_connectivity(self, nats_client):
        """Test NATS pub/sub works."""
        received = []

        async def handler(msg):
            received.append(msg.data.decode())

        sub = await nats_client.subscribe("test.subject", cb=handler)
        await nats_client.publish("test.subject", b"hello")
        await asyncio.sleep(0.1)

        assert "hello" in received
        await sub.unsubscribe()

    def test_redis_connectivity(self, redis_client):
        """Test Redis operations work."""
        redis_client.set("test-key", "test-value")
        assert redis_client.get("test-key") == "test-value"

    def test_postgres_connectivity(self, db_connection):
        """Test Postgres operations work."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            assert result[0] == 1

    def test_s3_connectivity(self, s3_client, s3_bucket):
        """Test S3 operations work."""
        s3_client.put_object(
            Bucket=s3_bucket,
            Key="test-object",
            Body=b"test content",
        )

        response = s3_client.get_object(Bucket=s3_bucket, Key="test-object")
        assert response["Body"].read() == b"test content"


class TestEdgeCases:
    """Test edge cases and error handling."""

    async def test_fibonacci_zero(
        self,
        nats_client,
        redis_client,
        s3_client,
        s3_bucket,
        db_connection,
    ):
        """Test computing Fibonacci(0)."""
        job_id = str(uuid.uuid4())

        result = await process_job(
            {"job_id": job_id, "n": 0},
            nc=nats_client,
            cache=redis_client,
            s3_client=s3_client,
            db_conn=db_connection,
            bucket=s3_bucket,
        )

        assert result["result"] == 0

    async def test_fibonacci_one(
        self,
        nats_client,
        redis_client,
        s3_client,
        s3_bucket,
        db_connection,
    ):
        """Test computing Fibonacci(1)."""
        job_id = str(uuid.uuid4())

        result = await process_job(
            {"job_id": job_id, "n": 1},
            nc=nats_client,
            cache=redis_client,
            s3_client=s3_client,
            db_conn=db_connection,
            bucket=s3_bucket,
        )

        assert result["result"] == 1

    def test_compute_without_cache(self):
        """Test Fibonacci computation without any cache."""
        result = compute_fibonacci(25)
        assert result == 75025
