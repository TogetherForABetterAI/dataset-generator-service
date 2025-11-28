#!/bin/bash
# Unified test runner script for dataset-generator-service
# Usage: ./test.sh [units|integration|e2e]

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to show usage
usage() {
    echo ""
    echo "Usage: $0 [units|integration|e2e]"
    echo ""
    echo "  units        - Run unit tests (fast, no external services)"
    echo "  integration  - Run integration tests (with RabbitMQ & PostgreSQL)"
    echo "  e2e          - Run end-to-end tests (full infrastructure)"
    echo ""
    echo "Examples:"
    echo "  $0 units"
    echo "  $0 integration"
    echo "  $0 e2e"
    echo ""
    exit 1
}

# Function to print colored header
print_header() {
    echo ""
    echo -e "${BLUE}===================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}===================================${NC}"
    echo ""
}

# Function to print success message
print_success() {
    echo ""
    echo -e "${GREEN}===================================${NC}"
    echo -e "${GREEN}  ✓ $1${NC}"
    echo -e "${GREEN}===================================${NC}"
    echo ""
}

# Function to print error message
print_error() {
    echo ""
    echo -e "${RED}===================================${NC}"
    echo -e "${RED}  ✗ $1${NC}"
    echo -e "${RED}===================================${NC}"
    echo ""
}

# Check argument
if [ $# -eq 0 ]; then
    usage
fi

case "$1" in
    units)
        print_header "Running UNIT Tests"
        
        echo "Executing tests in tests/units/..."
        pytest tests/units/ -v
        
        TEST_EXIT_CODE=$?
        
        if [ $TEST_EXIT_CODE -eq 0 ]; then
            print_success "Unit Tests PASSED"
        else
            print_error "Unit Tests FAILED"
        fi
        
        exit $TEST_EXIT_CODE
        ;;
    
    integration)
        print_header "Running INTEGRATION Tests"
        
        echo "1. Starting infrastructure (RabbitMQ, PostgreSQL)..."
        docker-compose -f docker-compose.test.yml up -d rabbitmq-e2e postgres-e2e
        
        echo ""
        echo "2. Waiting for services to be healthy (max 30s)..."
        TIMEOUT=30
        ELAPSED=0
        while [ $ELAPSED -lt $TIMEOUT ]; do
            RABBITMQ_HEALTH=$(docker inspect rabbitmq-e2e --format='{{.State.Health.Status}}' 2>/dev/null || echo "starting")
            POSTGRES_HEALTH=$(docker inspect postgres-e2e --format='{{.State.Health.Status}}' 2>/dev/null || echo "starting")
            
            if [ "$RABBITMQ_HEALTH" = "healthy" ] && [ "$POSTGRES_HEALTH" = "healthy" ]; then
                echo -e "${GREEN}✓ Services are ready!${NC}"
                break
            fi
            
            echo "  RabbitMQ: $RABBITMQ_HEALTH | PostgreSQL: $POSTGRES_HEALTH"
            sleep 2
            ELAPSED=$((ELAPSED + 2))
        done
        
        if [ $ELAPSED -ge $TIMEOUT ]; then
            print_error "Services did not become healthy in time"
            docker-compose -f docker-compose.test.yml logs
            docker-compose -f docker-compose.test.yml down -v
            exit 1
        fi
        
        echo ""
        echo "3. Executing tests in tests/integration/..."
        pytest tests/integration/ -v
        
        TEST_EXIT_CODE=$?
        
        echo ""
        echo "4. Cleaning up infrastructure..."
        docker-compose -f docker-compose.test.yml down -v
        
        if [ $TEST_EXIT_CODE -eq 0 ]; then
            print_success "Integration Tests PASSED"
        else
            print_error "Integration Tests FAILED"
        fi
        
        exit $TEST_EXIT_CODE
        ;;
    
    e2e)
        print_header "Running E2E Tests"
        
        echo "1. Starting full infrastructure (RabbitMQ, PostgreSQL, Service)..."
        docker-compose -f docker-compose.test.yml up --build -d
        
        echo ""
        echo "2. Waiting for services to be healthy (max 60s)..."
        TIMEOUT=60
        ELAPSED=0
        while [ $ELAPSED -lt $TIMEOUT ]; do
            RABBITMQ_HEALTH=$(docker inspect rabbitmq-e2e --format='{{.State.Health.Status}}' 2>/dev/null || echo "starting")
            POSTGRES_HEALTH=$(docker inspect postgres-e2e --format='{{.State.Health.Status}}' 2>/dev/null || echo "starting")
            SERVICE_STATUS=$(docker inspect dataset-service-e2e --format='{{.State.Status}}' 2>/dev/null || echo "starting")
            
            if [ "$RABBITMQ_HEALTH" = "healthy" ] && [ "$POSTGRES_HEALTH" = "healthy" ] && [ "$SERVICE_STATUS" = "running" ]; then
                echo -e "${GREEN}✓ All services are ready!${NC}"
                break
            fi
            
            echo "  RabbitMQ: $RABBITMQ_HEALTH | PostgreSQL: $POSTGRES_HEALTH | Service: $SERVICE_STATUS"
            sleep 2
            ELAPSED=$((ELAPSED + 2))
        done
        
        if [ $ELAPSED -ge $TIMEOUT ]; then
            print_error "Services did not become healthy in time"
            echo ""
            echo "Service logs:"
            docker-compose -f docker-compose.test.yml logs
            docker-compose -f docker-compose.test.yml down -v
            exit 1
        fi
        
        echo ""
        echo "3. Executing tests in tests/e2e/..."
        pytest tests/e2e/ -v -s
        
        TEST_EXIT_CODE=$?
        
        echo ""
        echo "4. Cleaning up infrastructure..."
        docker-compose -f docker-compose.test.yml down -v
        
        if [ $TEST_EXIT_CODE -eq 0 ]; then
            print_success "E2E Tests PASSED"
        else
            print_error "E2E Tests FAILED"
        fi
        
        exit $TEST_EXIT_CODE
        ;;
    
    *)
        echo -e "${RED}Error: Unknown test type '$1'${NC}"
        usage
        ;;
esac
