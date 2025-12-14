# Makefile for ETL File Sync project
# Provides convenient commands for testing and development

.PHONY: help test test-priority test-e2e test-integration test-unit test-all coverage clean

# Default target
help:
	@echo "ETL File Sync - Test Commands"
	@echo ""
	@echo "Priority-Based Testing:"
	@echo "  make test-priority    Run tests with priority-based strategy"
	@echo "  make test-priority-v  Run priority tests with verbose output"
	@echo "  make test-priority-all Run all tests regardless of priority results"
	@echo ""
	@echo "Individual Test Levels:"
	@echo "  make test-e2e         Run E2E tests only"
	@echo "  make test-integration Run integration tests only"
	@echo "  make test-unit        Run unit tests only"
	@echo "  make test-all         Run all tests (ordered by priority)"
	@echo ""
	@echo "Other Commands:"
	@echo "  make coverage         Run all tests with coverage report"
	@echo "  make clean            Clean generated files"
	@echo ""

# Priority-based test execution (recommended)
test-priority:
	@./scripts/run_tests.sh

test-priority-v:
	@./scripts/run_tests.sh --verbose

test-priority-coverage:
	@./scripts/run_tests.sh --coverage

test-priority-all:
	@./scripts/run_tests.sh --force-all

# Individual test levels
test-e2e:
	pytest -m e2e -v

test-integration:
	pytest -m integration -v

test-unit:
	pytest -m unit -v

# Run all tests (ordered by priority via conftest.py)
test-all:
	pytest -v

test:
	pytest -v

# Coverage report
coverage:
	pytest --cov=src/etl --cov-report=term-missing --cov-report=html:reports/coverage

# Clean generated files
clean:
	rm -rf reports/*.xml
	rm -rf reports/coverage
	rm -rf .pytest_cache
	rm -rf tests/__pycache__
	rm -rf src/**/__pycache__
	rm -f .coverage
