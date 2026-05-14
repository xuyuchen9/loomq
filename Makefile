# LoomQ Makefile
# Version: 0.9.0

.PHONY: help build test clean run docker-build docker-run docker-compose-up docker-compose-down format check-format

# Variables
JAR_FILE := loomq-server/target/loomq-server-0.9.0.jar
DOCKER_IMAGE := loomq:0.9.0
DOCKER_COMPOSE := docker-compose
SPOTLESS_PLUGIN := com.diffplug.spotless:spotless-maven-plugin:3.0.0

# Default target
help:
	@echo "LoomQ Build System v0.9.0"
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
	@echo "  docker-compose-up    - Start the single-node docker-compose stack"
	@echo "  docker-compose-down  - Stop docker-compose"
	@echo "  docker-compose-logs  - View docker-compose logs"
	@echo "  format               - Apply Spotless formatting"
	@echo "  check-format         - Verify Spotless formatting"
	@echo "  check                - Run formatting checks + tests"
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
		-p 7928:7928 \
		-v loomq-data:/app/data/wal \
		-e JVM_XMS=2g \
		-e JVM_XMX=2g \
		$(DOCKER_IMAGE)

docker-compose-up: docker-build
	$(DOCKER_COMPOSE) up -d

docker-compose-down:
	$(DOCKER_COMPOSE) down --volumes

docker-compose-logs:
	$(DOCKER_COMPOSE) logs -f

format:
	mvn -B -ntp $(SPOTLESS_PLUGIN):apply

check-format:
	mvn -B -ntp $(SPOTLESS_PLUGIN):check

check: check-format test
	@echo "All checks passed!"

release: check-format build
	@echo "Release build complete: $(JAR_FILE)"
	@echo "Docker image: $(DOCKER_IMAGE)"
