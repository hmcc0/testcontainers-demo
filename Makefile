.PHONY: demo-unit demo-integration demo-all pre-pull test

# Run unit tests (short output for live demo)
demo-unit:
	uv run pytest tests/test_unit.py -v --tb=short

# Run a single integration test (best for live demo)
demo-integration:
	uv run pytest tests/test_integration.py::TestCompleteJobFlow::test_full_job_flow -v

# Run all tests
demo-all:
	uv run pytest tests/ -v --tb=short

# Run full test suite (same as CI)
test:
	uv run pytest tests/ -v

# Pre-pull container images so the demo starts fast
pre-pull:
	docker pull postgres:16-alpine
	docker pull redis:7-alpine
	docker pull nats:2.10-alpine
	docker pull localstack/localstack:3
	docker pull testcontainers/ryuk:0.8.1
