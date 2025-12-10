.DEFAULT_GOAL := help

PYTEST := pytest
SOURCE_DIR := src/goldfish

.PHONY: help install-hooks lint test test-unit test-integration test-e2e test-deluxe ci clean

help:
	@echo "Available targets:"
	@echo "  install-hooks    Install pre-commit hooks"
	@echo "  lint             Run ruff and mypy"
	@echo "  test             Run fast unit tests (pre-commit)"
	@echo "  test-unit        Run all unit tests with coverage"
	@echo "  test-integration Run integration tests"
	@echo "  test-e2e         Run E2E tests (excludes deluxe GCE tests)"
	@echo "  test-deluxe      Run deluxe GCE tests (~30 min, requires cloud)"
	@echo "  ci               Full CI suite"
	@echo "  clean            Remove caches"

install-hooks:
	pre-commit install
	pre-commit install --hook-type pre-push

lint:
	pre-commit run ruff --all-files
	pre-commit run ruff-format --all-files
	pre-commit run mypy --all-files

test:
	$(PYTEST) tests/unit -q --tb=short -m "not slow"

test-unit:
	$(PYTEST) tests/unit -v --cov=$(SOURCE_DIR) --cov-report=xml --cov-report=term

test-integration:
	$(PYTEST) tests/integration -v --tb=short

test-e2e:
	$(PYTEST) tests/e2e -v --tb=short --ignore=tests/e2e/deluxe

test-deluxe:
	$(PYTEST) tests/e2e/deluxe -v --tb=short -m "deluxe_gce"

ci: lint test-unit test-integration
	@echo "CI checks passed!"

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache .coverage coverage.xml .mypy_cache htmlcov .ruff_cache
