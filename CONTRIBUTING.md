# Contributing to LoomQ

Thanks for your interest in contributing. LoomQ is a durable time kernel for distributed systems, built on Java 25 Virtual Threads.

## Development Environment

- **JDK 25+** (no `--enable-preview` required)
- **Maven 3.9+**
- Git

```bash
git clone https://github.com/loomq/loomq.git
cd loomq
mvn clean package -DskipTests
```

## Build & Test

```bash
# Fast build (skip tests)
mvn clean package -DskipTests

# Default tests (excludes benchmark/slow/integration)
mvn test

# Full test suite
mvn test -Pfull-tests

# Single test
mvn test -Dtest=ClassName
mvn test -Dtest=ClassName#methodName
```

Tests are JUnit 5 with Maven Surefire profiles. Integration and benchmark tests are tagged with JUnit 5 `@Tag` annotations.

## Coding Conventions

- **Language**: Code comments and commit messages may be in Chinese or English. Documentation is primarily Chinese (中文).
- **Naming**: Use "Intent" for the public model. Avoid legacy terminology.
- **Core vs Server**: `loomq-core` has zero HTTP/JSON dependencies. All transport logic belongs in `loomq-server`.
- **Virtual Threads**: Use `Executors.newVirtualThreadPerTaskExecutor()` for new concurrency. No traditional thread pools.
- **SPI boundaries**: Extend `DeliveryHandler`, `CallbackHandler`, or `RedeliveryDecider` rather than modifying scheduler internals.

## Pull Request Process

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes with tests
4. Run `mvn test` to verify no regressions
5. Submit a PR against `main`
6. CI will run the balanced test suite automatically

## Reporting Issues

- **Bugs**: Include JDK version, OS, reproduction steps, and relevant logs
- **Features**: Describe the use case and expected behavior
- Use [GitHub Issues](https://github.com/loomq/loomq/issues)

## Project Structure

```
loomq/
├── loomq-core/       # Embeddable kernel (zero HTTP/JSON deps)
├── loomq-server/     # Standalone Netty HTTP server
├── loomq-bom/        # Bill of Materials
├── benchmark/        # Performance benchmark suite
├── docs/             # Technical documentation
└── scripts/          # Startup and helper scripts
```

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
