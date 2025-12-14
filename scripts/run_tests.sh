#!/bin/bash
# scripts/run_tests.sh
# Priority-based test execution script
#
# Priority Order:
# 1. E2E tests (highest) - If pass, lower priority tests are skipped
# 2. Integration tests - Run only if E2E fails
# 3. Unit tests (lowest) - Run only if higher priority tests fail

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
REPORTS_DIR="${PROJECT_ROOT}/reports"

# Ensure reports directory exists
mkdir -p "$REPORTS_DIR"

# Parse command line arguments
VERBOSE=""
COVERAGE=""
FORCE_ALL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        --coverage)
            COVERAGE="--cov=src/etl --cov-report=term-missing --cov-report=html:${REPORTS_DIR}/coverage"
            shift
            ;;
        --force-all)
            FORCE_ALL="true"
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Priority-based test execution strategy."
            echo "E2E tests run first. If they pass, lower priority tests are skipped."
            echo "If E2E tests fail, lower priority tests run to diagnose root cause."
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Enable verbose output"
            echo "  --coverage       Generate coverage report"
            echo "  --force-all      Run all tests regardless of priority results"
            echo "  -h, --help       Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Function to print colored status
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to run tests with a specific marker
run_tests() {
    local marker=$1
    local description=$2
    local report_file="${REPORTS_DIR}/${marker}_results.xml"

    print_status "$BLUE" "=========================================="
    print_status "$BLUE" "Running ${description}..."
    print_status "$BLUE" "=========================================="

    cd "$PROJECT_ROOT"

    # Activate virtual environment if exists
    if [ -f ".venv/bin/activate" ]; then
        source .venv/bin/activate
    fi

    if pytest -m "$marker" $VERBOSE $COVERAGE \
        --junit-xml="$report_file" \
        --tb=short 2>&1; then
        return 0
    else
        return 1
    fi
}

# Main execution
main() {
    local e2e_result=0
    local integration_result=0
    local unit_result=0
    local final_result=0

    print_status "$YELLOW" "============================================"
    print_status "$YELLOW" "Priority-Based Test Execution"
    print_status "$YELLOW" "============================================"
    print_status "$YELLOW" "Priority 1: E2E Tests (highest)"
    print_status "$YELLOW" "Priority 2: Integration Tests"
    print_status "$YELLOW" "Priority 3: Unit Tests (lowest)"
    print_status "$YELLOW" "============================================"
    echo ""

    # Priority 1: E2E Tests
    print_status "$BLUE" "[Priority 1] E2E Tests"
    if run_tests "e2e" "E2E Tests"; then
        print_status "$GREEN" "[PASS] E2E tests passed!"
        echo ""

        if [ -z "$FORCE_ALL" ]; then
            print_status "$GREEN" "============================================"
            print_status "$GREEN" "SUCCESS: E2E tests passed."
            print_status "$GREEN" "Lower priority tests skipped (system validated)."
            print_status "$GREEN" "============================================"
            echo ""
            print_status "$YELLOW" "Tip: Use --force-all to run all tests regardless of priority results."
            exit 0
        else
            print_status "$YELLOW" "[INFO] --force-all specified, continuing with lower priority tests..."
            echo ""
        fi
    else
        e2e_result=1
        final_result=1
        print_status "$RED" "[FAIL] E2E tests failed!"
        print_status "$YELLOW" "[INFO] Running lower priority tests to diagnose root cause..."
        echo ""
    fi

    # Priority 2: Integration Tests
    print_status "$BLUE" "[Priority 2] Integration Tests"
    if run_tests "integration" "Integration Tests"; then
        print_status "$GREEN" "[PASS] Integration tests passed!"
    else
        integration_result=1
        final_result=1
        print_status "$RED" "[FAIL] Integration tests failed!"
    fi
    echo ""

    # Priority 3: Unit Tests
    print_status "$BLUE" "[Priority 3] Unit Tests"
    if run_tests "unit" "Unit Tests"; then
        print_status "$GREEN" "[PASS] Unit tests passed!"
    else
        unit_result=1
        final_result=1
        print_status "$RED" "[FAIL] Unit tests failed!"
    fi
    echo ""

    # Summary
    print_status "$YELLOW" "============================================"
    print_status "$YELLOW" "Test Execution Summary"
    print_status "$YELLOW" "============================================"

    if [ $e2e_result -eq 0 ]; then
        print_status "$GREEN" "E2E Tests:         PASSED"
    else
        print_status "$RED" "E2E Tests:         FAILED"
    fi

    if [ $integration_result -eq 0 ]; then
        print_status "$GREEN" "Integration Tests: PASSED"
    else
        print_status "$RED" "Integration Tests: FAILED"
    fi

    if [ $unit_result -eq 0 ]; then
        print_status "$GREEN" "Unit Tests:        PASSED"
    else
        print_status "$RED" "Unit Tests:        FAILED"
    fi

    print_status "$YELLOW" "============================================"

    # Root cause analysis hint
    if [ $final_result -ne 0 ]; then
        echo ""
        print_status "$YELLOW" "Root Cause Analysis:"

        if [ $unit_result -ne 0 ]; then
            print_status "$RED" "  -> Unit tests failed: Check core logic and models"
        fi

        if [ $integration_result -ne 0 ] && [ $unit_result -eq 0 ]; then
            print_status "$RED" "  -> Integration tests failed but unit tests passed:"
            print_status "$RED" "     Check FTP connectivity and server configurations"
        fi

        if [ $e2e_result -ne 0 ] && [ $integration_result -eq 0 ] && [ $unit_result -eq 0 ]; then
            print_status "$RED" "  -> E2E tests failed but lower level tests passed:"
            print_status "$RED" "     Check Kafka connectivity, consumer logic, or full pipeline integration"
        fi
    fi

    exit $final_result
}

main
