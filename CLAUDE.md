# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A queue worker demonstrating testcontainers with Python. The worker subscribes to a NATS message queue, computes Fibonacci numbers with Redis memoization, stores results in S3/LocalStack, and persists job metadata to PostgreSQL.

## Commands

```bash
# Install dependencies
uv sync --all-extras

# Run all tests
uv run pytest tests/ -v

# Run unit tests only
uv run pytest tests/test_unit.py -v

# Run integration tests only
uv run pytest tests/test_integration.py -v

# Run a single test
uv run pytest tests/test_unit.py::TestFibonacciComputation::test_computes_correct_values -v

# Local development with Docker Compose
docker compose up -d
docker compose logs -f worker
docker compose down

# Format Python code
uvx ruff format

# Lint Python code
uvx ruff check
```

## Architecture

**Data Flow:**
```
NATS Queue → Worker → Redis Cache → S3 (results JSON) → PostgreSQL (job metadata) → NATS Notifications
```

**Key Files:**
- `src/worker.py` - Async queue worker with Fibonacci computation, S3 storage, Postgres persistence, and NATS pub/sub
- `tests/conftest.py` - Testcontainers fixtures (session-scoped containers, function-scoped clients with cleanup)
- `tests/test_unit.py` - Unit tests for individual components with real containers
- `tests/test_integration.py` - End-to-end job flow tests

**Testcontainers Pattern:**
- Session-scoped containers (PostgreSQL, Redis, NATS, LocalStack) start once per test run
- Function-scoped clients provide fresh connections with automatic cleanup between tests
- Dynamic port mapping prevents conflicts; Ryuk sidecar ensures cleanup even after crashes

**Technologies:** Python 3.11+, uv package manager, pytest-asyncio, nats-py, redis, psycopg, boto3, testcontainers
