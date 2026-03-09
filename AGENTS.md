# JUDO Operation Utils - Project Documentation

## Project Overview


**Repository:** BlackBeltTechnology/judo-operation-utils
**License:** Eclipse Public License 2.0 (EPL-2.0)
**Java Version:** 21
**Build System:** Maven 3.9.4 with Maven Wrapper (`mvnw`), OSGi bundle packaging via `maven-bundle-plugin`

1. Provides `AbstractGeneratedScript` — the abstract base class that all JUDO-generated operation scripts extend, implementing `Function<Payload, Payload>`
2. Manages a `Container` system for tracking mapped (persisted) and unmapped (transient) transfer objects with automatic refresh, identity management, and mutable/immutable semantics
3. Supplies `FunctionRunner` — collection and query utilities (filter, sort, head/tail, aggregate, exists/forAll) used by generated scripts for JQL expression evaluation
4. Implements `Kleene` — three-valued (null-safe) boolean logic (and, or, xor, implies, not) for handling nullable boolean expressions in the JUDO expression language

## Code Instructions

1. First think through the problem, read the codebase for relevant files.
2. Before you make any major changes, check in with me and I will verify the plan.
3. Please every step of the way just give me a high level explanation of what changes you made.
4. Make every task and code change you do as simple as possible. We want to avoid making any massive or complex changes. Every change should impact as little code as possible. Everything is about simplicity.
5. Maintain a documentation file that describes how the architecture of the app works inside and out.
6. Never speculate about code you have not opened. If the user references a specific file, you MUST read the file before answering. Make sure to investigate and read relevant files BEFORE answering questions about the codebase. Never make any claims about code before investigating unless you are certain of the correct answer - give grounded and hallucination-free answers.
7. For implementation use TDD (Test-Driven Development): write or update tests first to define the expected behaviour, verify they fail, then write the minimal implementation to make them pass.
8. Use DRY (Don't Repeat Yourself): extract reusable logic into separate classes, utilities, or components. If the same pattern appears in multiple places, refactor it into a shared helper.

## Directory Structure

```
judo-operation-utils/
├── pom.xml                  # Single-module Maven build (OSGi bundle)
├── mvnw / mvnw.cmd          # Maven wrapper scripts
├── src/
│   ├── main/java/hu/blackbelt/judo/operation/utils/
│   │   ├── AbstractGeneratedScript.java   # Core base class for generated scripts
│   │   ├── FunctionRunner.java            # Collection/query/string utilities
│   │   └── Kleene.java                    # Three-valued boolean logic
│   └── test/java/hu/blackbelt/judo/operation/utils/
│       └── .githolder                     # Placeholder (test dir exists but empty)
├── .github/                 # Issue templates, CI flow documentation
├── .vscode/settings.json    # VS Code Java settings
├── .zed/settings.json       # Zed editor settings
├── README.md                # Project overview with architecture diagrams
├── CONTRIBUTING.md           # Contribution guide
└── LICENSE.txt              # EPL-2.0 license
```

## Core Modules

This is a single-module project (no Maven submodules). All source is in one package:

| Component | Type | Purpose |
|---|---|---|
| `AbstractGeneratedScript` | Abstract class | Base class for generated operation scripts. Manages Container lifecycle, provides CRUD operations (create, update, delete, navigate), query execution (primitive and complex, static and instance-based), and coercion. Implements `Function<Payload, Payload>`. |
| `Container` | Inner class of `AbstractGeneratedScript` | Wraps `EClass` + `Payload` pairs with dual identity (`__identifier` for mapped, `__unmappedid` for transient). Handles refresh from DAO, payload merging, immutable copies, and deletion tracking. |
| `FunctionRunner` | Class | Provides collection operations (`filter`, `sort`, `head`, `tail`, `heads`, `tails`, `count`, `empty`, `any`, `exists`, `forAll`, `contains`), aggregations (`min`, `max`, `sumInteger`, `sumDecimal`, `avg`, `avgDate`, `avgTimestamp`, `avgTime`), string operations (`substring`, `matches`, `like`, `replace`), actor/principal lookup, and environment variable resolution. |
| `Kleene` | Utility class | Static methods for three-valued logic: `and`, `or`, `xor`, `implies`, `not`. All methods correctly propagate `null` per Kleene's strong logic of indeterminacy. |
| `Holder<T>` | Inner class of `AbstractGeneratedScript` | Generic value wrapper used for output parameters and lambda iteration contexts. |
| `SortOrderBy<T>` | Inner class of `AbstractGeneratedScript` | Pairs a sort-key generator function with a descending flag for multi-field sorting. |

## Technology Stack

### Core Technologies
- **Eclipse EMF (Ecore)** — Metamodel framework (`EClass`, `EReference`, `EAttribute`, `EEnum`, `EDataType`)
- **judo-meta-asm** (`AsmModel`, `AsmUtils`) — JUDO's ASM metamodel layer built on EMF
- **judo-dao-api** (`DAO`, `Payload`, `IdentifierProvider`) — Data access abstraction for persistence operations
- **judo-dispatcher-api** (`Dispatcher`, `JudoPrincipal`, `VariableResolver`) — Operation dispatch and security context
- **Gson 2.9.1** — JSON processing
- **SLF4J 2.0.16** + Logback — Logging
- **Lombok 1.18.34** — Boilerplate reduction
- **OSGi** (Core 6.0.0, Compendium 6.0.0) — Module system for runtime deployment

### Build & Quality
- **Maven 3.9.4** with Maven Wrapper
- **maven-bundle-plugin 5.1.8** — OSGi bundle packaging
- **flatten-maven-plugin 1.3.0** — CI-friendly `${revision}` versioning
- **JUnit 5.9.1** — Unit testing
- **Mockito 4.8.0** — Mocking framework
- **Hamcrest 2.2** — Assertion matchers
- **JaCoCo 0.8.12** — Code coverage
- **SonarQube** (via sonar-maven-plugin 3.9.1) — Static analysis

## Build Commands

> **Note:** Use `./mvnw` (Maven Wrapper) — no global Maven installation required.

```bash
# Full build with tests
./mvnw clean install

# Build without tests
./mvnw clean install -DskipTests

# Run all tests
./mvnw test

# Run a single test class
./mvnw test -Dtest=ClassName

# Run a single test method
./mvnw test -Dtest=ClassName#methodName

# Generate code coverage report
./mvnw verify
# Report at: target/site/jacoco/index.html
```

### Maven Profiles

| Profile | Purpose |
|---|---|
| `sign-artifacts` | Sign artifacts using `sign-maven-plugin` for release publishing |
| `release-dummy` | Deploy to a local `/tmp/` directory for testing the release process |
| `release-judong` | Deploy snapshots to `nexus.judo.technology` (JUDO Nexus) |
| `release-central` | Deploy to Maven Central via Sonatype OSSRH with auto-release |
| `generate-github-asciidoc-diagrams` | Generate PNG diagrams from AsciiDoc sources using PlantUML |
| `update-source-code-license` | Update EPL-2.0 license headers in all source files |

## Key Configuration Files

| File | Purpose |
|---|---|
| `pom.xml` | Maven build config — dependencies, plugins, profiles, OSGi bundle instructions |
| `logback-test.xml` | Logback configuration for test execution |
| `.github/workflows/` | GitHub Actions CI/CD workflows (build, release, merge) |
| `LICENSE.txt` | Eclipse Public License 2.0 full text |

## Development Environment

**Required:**
- Java 21 JDK
- Maven 3.9.4+ (or use `./mvnw`)

**Optional:**
- IDE with Lombok support (IntelliJ IDEA, VS Code with Lombok extension, Eclipse with Lombok agent)
- SonarQube instance for local static analysis

## Git Workflow

- **Main Branch:** `develop`
- **Versioning:** `${revision}` property (currently `1.1.3-SNAPSHOT`), flattened at build time
- **Branch naming:** `feature/JNG-xxx_description`, `bugfix/JNG-xxx_description`, `release/X.Y.Z`
- **Commit rule:** Every commit must reference a JIRA ticket (`JNG-xxx`)
- **CI:** GitHub Actions — see [CIFLOW.md](.github/CIFLOW.md) for the full workflow

## Important Notes

1. This library is **not used directly** — it is a dependency of generated operation scripts produced by the JUDO code generator. Changes here affect all generated scripts at runtime.
2. The `Container` inner class uses **reference identity for unmapped objects** and **UUID-based identity for mapped objects**. The `equals`/`hashCode` contract differs based on whether `__identifier` is present.
3. Container `refresh()` is lazy — it only hits the DAO when `lastRefresh <= lastWrite`, avoiding unnecessary database roundtrips.
4. The `containerPayloadGet/Put/Remove/GetAs` methods on `Container` are the primary API used by generated scripts (marked with `// used by generated script` comments). They gate DAO access based on whether the attribute/reference is mapped.
5. `FunctionRunner.sort()` always appends an ID-based tiebreaker comparator to ensure deterministic ordering.
6. `Kleene` implements **strong Kleene logic** — `true OR null = true`, `false AND null = false`, but `null AND null = null`.

## Related Documentation

- [README.md](README.md) — Project overview with architecture and sequence diagrams
- [CONTRIBUTING.md](CONTRIBUTING.md) — How to submit issues and PRs
- [.github/CIFLOW.md](.github/CIFLOW.md) — CI/CD workflow documentation with flow diagrams
- [judo-community](https://github.com/BlackBeltTechnology/judo-community) — Parent aggregator project
- [judo-runtime-core](https://github.com/BlackBeltTechnology/judo-runtime-core) — Runtime that consumes this library
