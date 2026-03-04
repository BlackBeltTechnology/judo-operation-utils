# Contributing to JUDO

## Development Environment Requirements

Make sure your development environment complies with the requirements in the parent project's [CONTRIBUTING guide](https://github.com/BlackBeltTechnology/judo-community/blob/develop/CONTRIBUTING.adoc). Key requirements:

- **Java 21** JDK
- **Maven 3.9.4+** (or use the included `./mvnw` wrapper)

## Code Structure

This is a standard single-module Maven project:

| Directory | Contents |
|---|---|
| `src/main/java/` | Production code — `AbstractGeneratedScript`, `FunctionRunner`, `Kleene` |
| `src/test/java/` | Tests (JUnit 5 + Mockito + Hamcrest) |

## Submission Guidelines

### Submitting an Issue

Before creating a new issue, search the [issue tracker](https://github.com/BlackBeltTechnology/judo-operation-utils/issues) — your problem may already be reported or resolved.

To help us reproduce and fix bugs efficiently, please include:

- Output of `java -version` and `mvn -version`
- `pom.xml` or `.flattened-pom.xml` (when applicable)
- **A minimal reproduction case** — the most important piece. We need to isolate the problem before we can fix it.

File new issues via the [issue form](https://github.com/BlackBeltTechnology/judo-operation-utils/issues/new/choose).

### Submitting a Pull Request

This project follows [GitHub's standard forking model](https://guides.github.com/activities/forking/). Fork the project and submit pull requests from your fork.

## Commands

### Run Tests

```bash
./mvnw clean test
```

### Run Full Build

```bash
./mvnw clean install
```
