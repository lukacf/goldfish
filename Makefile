.DEFAULT_GOAL := help

PYTEST := pytest
SOURCE_DIR := src/goldfish
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m

.PHONY: help install-hooks lint lint-imports audit test test-unit test-integration test-e2e test-deluxe ci ci-smoke release-preflight clean

help:
	@echo "$(GREEN)Goldfish Development$(NC)"
	@echo ""
	@echo "$(YELLOW)Setup$(NC)"
	@echo "  install-hooks    Install pre-commit hooks (commit + push stages)"
	@echo ""
	@echo "$(YELLOW)Quality$(NC)"
	@echo "  lint             Run ruff + mypy via pre-commit"
	@echo "  lint-imports     Run import-linter boundary checks"
	@echo "  audit            Run pip-audit for dependency vulnerabilities"
	@echo ""
	@echo "$(YELLOW)Testing$(NC)"
	@echo "  test             Fast unit + contract tests (pre-commit)"
	@echo "  test-unit        Full unit tests with coverage"
	@echo "  test-integration Integration tests (~3 min)"
	@echo "  test-e2e         E2E tests (excludes deluxe GCE tests)"
	@echo "  test-deluxe      Deluxe GCE tests (~30 min, requires cloud)"
	@echo ""
	@echo "$(YELLOW)CI$(NC)"
	@echo "  ci               Full CI suite (lint + unit + integration)"
	@echo "  ci-smoke         Fast CI check (lint + fast unit tests only)"
	@echo ""
	@echo "$(YELLOW)Release$(NC)"
	@echo "  release-preflight  Pre-release validation checklist"
	@echo "  verify-version     Check version consistency across files"
	@echo ""
	@echo "$(YELLOW)Cleanup$(NC)"
	@echo "  clean            Remove caches and build artifacts"

install-hooks:
	pre-commit install
	pre-commit install --hook-type pre-push

lint:
	pre-commit run ruff --all-files
	pre-commit run ruff-format --all-files
	pre-commit run mypy --all-files

lint-imports:
	lint-imports

audit:
	@echo "$(GREEN)Running dependency security audit...$(NC)"
	pip-audit --strict --desc --skip-editable

test:
	$(PYTEST) tests/unit tests/contracts -q --tb=short -m "not slow"

test-unit:
	$(PYTEST) tests/unit -v --cov=$(SOURCE_DIR) --cov-report=xml --cov-report=term

test-integration:
	$(PYTEST) tests/integration -v --tb=short

test-e2e:
	$(PYTEST) tests/e2e -v --tb=short --ignore=tests/e2e/deluxe

test-deluxe:
	$(PYTEST) tests/e2e/deluxe -v --tb=short -m "deluxe_gce"

ci: lint lint-imports audit test-unit test-integration
	@echo "$(GREEN)CI checks passed!$(NC)"

ci-smoke: lint test
	@echo "$(GREEN)Smoke CI passed!$(NC)"

verify-version:
	@echo "$(GREEN)Checking version consistency...$(NC)"
	@PYPROJECT_VER=$$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])"); \
	INIT_VER=$$(python -c "from goldfish import __version__; print(__version__)"); \
	echo "  pyproject.toml: $$PYPROJECT_VER"; \
	echo "  __init__.py:    $$INIT_VER"; \
	if [ "$$PYPROJECT_VER" != "$$INIT_VER" ]; then \
		echo "$(RED)Version mismatch!$(NC)"; exit 1; \
	fi; \
	echo "$(GREEN)Versions match: $$PYPROJECT_VER$(NC)"

release-preflight: verify-version lint lint-imports audit test-unit test-integration
	@echo ""
	@echo "$(GREEN)========================================$(NC)"
	@echo "$(GREEN)  Release preflight passed!$(NC)"
	@echo "$(GREEN)========================================$(NC)"
	@echo ""
	@CURRENT=$$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])"); \
	echo "Current version: $$CURRENT"; \
	echo ""; \
	echo "$(YELLOW)Next steps:$(NC)"; \
	echo "  1. Update CHANGELOG.md with release notes"; \
	echo "  2. git add -A && git commit -m 'chore: prepare release v$$CURRENT'"; \
	echo "  3. git tag v$$CURRENT && git push origin main --tags"

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache .coverage coverage.xml .mypy_cache htmlcov .ruff_cache dist build *.egg-info
