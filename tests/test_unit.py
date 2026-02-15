"""
Unit tests for Fibonacci Queue Worker.

Tests individual components with real containers:
- Fibonacci computation with Redis caching
- S3 storage operations
- Database operations
- Cache isolation between tests
"""

import json

from worker import (
    compute_fibonacci,
    save_result_to_s3,
    save_job_to_db,
    get_job_from_db,
    ensure_bucket_exists,
)


class TestFibonacciComputation:
    """Test Fibonacci computation logic."""

    def test_base_cases(self):
        """Test Fibonacci base cases (0 and 1)."""
        assert compute_fibonacci(0) == 0
        assert compute_fibonacci(1) == 1

    def test_small_values(self):
        """Test small Fibonacci numbers without cache."""
        assert compute_fibonacci(2) == 1
        assert compute_fibonacci(3) == 2
        assert compute_fibonacci(4) == 3
        assert compute_fibonacci(5) == 5
        assert compute_fibonacci(10) == 55

    def test_with_redis_cache(self, redis_client):
        """Test Fibonacci computation with Redis caching."""
        # First computation
        result = compute_fibonacci(20, cache=redis_client)
        assert result == 6765

        # Verify cache was populated
        assert redis_client.get("fib:20") == "6765"
        assert redis_client.get("fib:19") == "4181"

    def test_cache_hit_speedup(self, redis_client):
        """Test that cached values are reused."""
        # Pre-populate cache
        redis_client.set("fib:10", "55")
        redis_client.set("fib:9", "34")

        # This should use cached values and extend from there
        result = compute_fibonacci(12, cache=redis_client)
        assert result == 144

    def test_progress_callback(self, redis_client):
        """Test progress callback is called for large computations."""
        progress_reports = []

        def callback(current, target):
            progress_reports.append((current, target))

        # n=100 should trigger progress updates
        result = compute_fibonacci(100, cache=redis_client, progress_callback=callback)

        # Verify result
        assert result == 354224848179261915075

        # Verify progress was reported
        assert len(progress_reports) > 0
        # All reports should have target=100
        assert all(target == 100 for _, target in progress_reports)

    def test_large_fibonacci(self, redis_client):
        """Test computation of larger Fibonacci numbers."""
        result = compute_fibonacci(50, cache=redis_client)
        assert result == 12586269025


class TestS3Storage:
    """Test S3 storage operations."""

    def test_save_and_retrieve_result(self, s3_client, s3_bucket):
        """Test saving and retrieving a result from S3."""
        job_id = "test-job-123"
        n = 10
        result = 55

        # Save result
        s3_uri = save_result_to_s3(s3_client, s3_bucket, job_id, n, result)

        # Verify URI format
        assert s3_uri == f"s3://{s3_bucket}/results/{job_id}.json"

        # Retrieve and verify content
        response = s3_client.get_object(Bucket=s3_bucket, Key=f"results/{job_id}.json")
        data = json.loads(response["Body"].read().decode())

        assert data["job_id"] == job_id
        assert data["n"] == n
        assert data["result"] == result
        assert "computed_at" in data

    def test_ensure_bucket_exists_creates_bucket(self, s3_client):
        """Test bucket creation when it doesn't exist."""
        bucket_name = "new-test-bucket"

        # Bucket shouldn't exist yet
        buckets = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
        if bucket_name in buckets:
            # Clean up from previous test run
            s3_client.delete_bucket(Bucket=bucket_name)

        # Create bucket
        ensure_bucket_exists(s3_client, bucket_name)

        # Verify it exists
        s3_client.head_bucket(Bucket=bucket_name)

        # Cleanup
        s3_client.delete_bucket(Bucket=bucket_name)

    def test_ensure_bucket_exists_idempotent(self, s3_client, s3_bucket):
        """Test that ensure_bucket_exists is idempotent."""
        # Should not raise even if bucket exists
        ensure_bucket_exists(s3_client, s3_bucket)
        ensure_bucket_exists(s3_client, s3_bucket)


class TestDatabaseOperations:
    """Test database operations."""

    def test_save_and_retrieve_job(self, db_connection):
        """Test saving and retrieving a job from Postgres."""
        job_id = "test-job-456"
        n = 15
        result = 610
        s3_uri = "s3://test-bucket/results/test-job-456.json"

        # Save job
        save_job_to_db(db_connection, job_id, n, result, s3_uri)

        # Retrieve job
        job = get_job_from_db(db_connection, job_id)

        assert job is not None
        assert job["id"] == job_id
        assert job["n"] == n
        assert job["result"] == result
        assert job["s3_uri"] == s3_uri
        assert job["completed_at"] is not None
        assert job["created_at"] is not None

    def test_get_nonexistent_job(self, db_connection):
        """Test retrieving a job that doesn't exist."""
        job = get_job_from_db(db_connection, "nonexistent-id")
        assert job is None

    def test_multiple_jobs(self, db_connection):
        """Test saving and retrieving multiple jobs."""
        jobs_data = [
            ("job-1", 5, 5, "s3://bucket/job-1.json"),
            ("job-2", 10, 55, "s3://bucket/job-2.json"),
            ("job-3", 15, 610, "s3://bucket/job-3.json"),
        ]

        for job_id, n, result, s3_uri in jobs_data:
            save_job_to_db(db_connection, job_id, n, result, s3_uri)

        # Verify all jobs
        for job_id, n, expected_result, s3_uri in jobs_data:
            job = get_job_from_db(db_connection, job_id)
            assert job is not None
            assert job["n"] == n
            assert job["result"] == expected_result


class TestCacheIsolation:
    """Test that cache is properly isolated between tests."""

    def test_cache_isolation_first(self, redis_client):
        """First test: set a cache value."""
        redis_client.set("isolation-test", "first")
        assert redis_client.get("isolation-test") == "first"

    def test_cache_isolation_second(self, redis_client):
        """Second test: verify previous value is cleared."""
        # This should be None because the fixture flushes between tests
        assert redis_client.get("isolation-test") is None

        # Set our own value
        redis_client.set("isolation-test", "second")
        assert redis_client.get("isolation-test") == "second"

    def test_cache_isolation_third(self, redis_client):
        """Third test: verify second test's value is also cleared."""
        assert redis_client.get("isolation-test") is None
