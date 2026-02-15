# Testcontainers Demo

## Testcontainers

Testcontainers is an open-source library that provides lightweight, throwaway Docker containers for integration testing. Instead of mocks or in-memory substitutes, you test against real services—databases, message brokers, browsers—with automatic lifecycle management. Rated **Adopt** on the [Thoughtworks Technology Radar](https://www.thoughtworks.com/radar/languages-and-frameworks/testcontainers), it's recognized as an industry best practice for reliable test environments.

### Key Benefits

| Benefit | Description |
|---------|-------------|
| **On-demand isolation** | No pre-provisioned infrastructure needed. Each pipeline gets isolated containers—no test data pollution |
| **Local = CI parity** | Run integration tests from your IDE just like unit tests. Same behaviour locally and in CI |
| **Reliable startup** | Built-in wait strategies (log, HTTP, port, health check) ensure containers are ready before tests run |
| **Automatic cleanup** | Ryuk sidecar removes all resources after tests—even after crashes or SIGKILL |
| **Dynamic port mapping** | Container ports map to random available host ports for reliable connections |

### Architecture

Testcontainers uses a layered design:

- **`DockerContainer`** (GenericContainer): Start any Docker image, expose ports, configure wait strategies, access metadata
- **Specialized modules** (e.g., `PostgresContainer`): Pre-configured containers with helper methods like `get_connection_url()`

This balances flexibility (any Docker image) with convenience (zero-config for common services).

### Installation

```bash
pip install testcontainers[postgres]
```

Database drivers are not bundled—install them separately (e.g., `psycopg`, `sqlalchemy`).

**Requirements:** Python 3.8+ and a Docker-compatible runtime (Docker Desktop, Docker Engine, or Testcontainers Cloud).

### Popular Python Modules

| Category | Modules |
|----------|---------|
| **Databases** | PostgreSQL, MySQL, MongoDB, Redis, Elasticsearch |
| **Message Brokers** | Kafka, RabbitMQ, NATS |
| **Cloud** | LocalStack (AWS), MinIO |
| **Web/Testing** | Selenium, Nginx |

See the [full module list](https://testcontainers.com/modules/?language=python) for 40+ available services.

### Examples

#### Database Integration Test

```python
from testcontainers.postgres import PostgresContainer
import psycopg

with PostgresContainer("postgres:16-alpine") as postgres:
    with psycopg.connect(postgres.get_connection_url(driver=None)) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100))")
            cur.execute("INSERT INTO users (name) VALUES ('Alice')")
            cur.execute("SELECT * FROM users")
            assert len(cur.fetchall()) == 1
```

#### Pytest Fixture with Lifecycle Management

```python
import pytest
import os
from testcontainers.postgres import PostgresContainer

postgres = PostgresContainer("postgres:16-alpine")

@pytest.fixture(scope="module", autouse=True)
def setup(request):
    postgres.start()
    os.environ["DB_CONN"] = postgres.get_connection_url(driver=None)
    request.addfinalizer(postgres.stop)

def test_create_user():
    # Test runs against real PostgreSQL
    pass
```

#### Generic Container for Any Service

```python
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

with DockerContainer("redis:7-alpine").with_exposed_ports(6379) as redis:
    wait_for_logs(redis, "Ready to accept connections")
    host, port = redis.get_container_host_ip(), redis.get_exposed_port(6379)
```

### Advanced Configuration

#### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` | Docker socket path | `/var/run/docker.sock` |
| `TESTCONTAINERS_HOST_OVERRIDE` | Container IP override | Auto-detected |
| `TESTCONTAINERS_RYUK_DISABLED` | Disable cleanup container | `false` |
| `TESTCONTAINERS_RYUK_PRIVILEGED` | Run Ryuk privileged | `false` |
| `RYUK_CONTAINER_IMAGE` | Custom Ryuk image | `testcontainers/ryuk:0.8.1` |
| `DOCKER_AUTH_CONFIG` | Private registry credentials (JSON) | — |

#### Private Registry Authentication

```bash
# AWS ECR
export DOCKER_AUTH_CONFIG='{"credHelpers": {"<account>.dkr.ecr.<region>.amazonaws.com": "ecr-login"}}'

# Inline credentials
export DOCKER_AUTH_CONFIG='{"auths": {"registry.example.com": {"auth": "<base64>"}}}'
```

#### Connection URLs

Database containers provide `get_connection_url()` with optional driver specification:

```python
postgres.get_connection_url()              # postgresql+psycopg2://...
postgres.get_connection_url(driver=None)   # postgresql://... (for psycopg v3)
```

### Resources

- [Official Site](https://testcontainers.com/)
- [Python Documentation](https://testcontainers-python.readthedocs.io/)
- [Getting Started Guide](https://testcontainers.com/guides/getting-started-with-testcontainers-for-python/)
- [Available Modules](https://testcontainers.com/modules/?language=python)
- [Thoughtworks Technology Radar](https://www.thoughtworks.com/radar/languages-and-frameworks/testcontainers)
