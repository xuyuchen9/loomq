# LoomQ Makefile
# Version: 0.6.0

.PHONY: help build test clean run docker-build docker-run docker-compose-up docker-compose-down

# Variables
JAR_FILE := target/loomq-0.6.0.jar
DOCKER_IMAGE := loomq:0.6.0
DOCKER_COMPOSE := docker-compose

# Default target
help:
	@echo "LoomQ Build System v0.6.0"
	@echo ""
	@echo "Available targets:"
	@echo "  build                - Build the project (mvn clean package)"
	@echo "  build-fast           - Build without tests"
	@echo "  test                 - Run all tests"
	@echo "  test-fast            - Run fast tests only"
	@echo "  test-integration     - Run integration tests"
	@echo "  test-benchmark       - Run benchmark tests"
	@echo "  clean                - Clean build artifacts"
	@echo "  run                  - Build and run locally"
	@echo "  run-jar              - Run the existing JAR"
	@echo "  docker-build         - Build Docker image"
	@echo "  docker-run           - Run Docker container (single node)"
	@echo "  docker-run-cluster   - Run Docker compose cluster"
	@echo "  docker-compose-up    - Start with docker-compose"
	@echo "  docker-compose-down  - Stop docker-compose"
	@echo "  docker-compose-logs  - View docker-compose logs"
	@echo "  format               - Format code (if configured)"
	@echo "  check                - Run checks (tests + lint)"
	@echo "  release              - Prepare release"

# Build targets
build:
	mvn clean package

build-fast:
	mvn clean package -DskipTests

# Test targets
test:
	mvn test

test-fast:
	mvn test -Dtest.excludedGroups=benchmark,slow,integration

test-integration:
	mvn test -Pintegration-tests

test-benchmark:
	mvn test -Pfull-tests -Dtest.groups=benchmark

# Clean
clean:
	mvn clean
	rm -rf logs/
	rm -rf data/

# Run targets
run: build-fast run-jar

run-jar:
	@if [ ! -f $(JAR_FILE) ]; then \
		echo "JAR file not found. Running 'make build-fast' first..."; \
		$(MAKE) build-fast; \
	fi
	./scripts/start.sh

# Docker targets
docker-build: build-fast
	docker build -t $(DOCKER_IMAGE) .

docker-run: docker-build
	docker run -d \
		--name loomq-single \
		-p 8080:8080 \
		-v loomq-data:/app/data/wal \
		-e JVM_XMS=2g \
		-e JVM_XMX=2g \
		$(DOCKER_IMAGE)

docker-run-cluster: docker-build
	$(DOCKER_COMPOSE) --profile cluster up -d

docker-compose-up: docker-build
	$(DOCKER_COMPOSE) up -d

docker-compose-down:
	$(DOCKER_COMPOSE) down --volumes

docker-compose-logs:
	$(DOCKER_COMPOSE) logs -f

# Utilities
format:
	@echo "Code formatting not configured. Add spotless plugin to pom.xml."

check: test
	@echo "All checks passed!"

release: clean build
	@echo "Release build complete: $(JAR_FILE)"
	@echo "Docker image: $(DOCKER_IMAGE)"
