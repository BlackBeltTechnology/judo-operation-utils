# judo-operation-utils — module agent doctrine

## Module purpose

`judo-operation-utils` is the runtime substrate that JUDO-generated *operation
scripts* execute on: an "operation" here is a model-declared custom behaviour
whose JQL body the JUDO code generator compiles into a Java class extending
`AbstractGeneratedScript`, and this module ships the superclass those generated
classes bind to — the `Payload`-in/`Payload`-out dispatch entry point
(`apply(Payload)`), the `Container` object graph that gives generated code a
mutable, identity-tracked, lazily DAO-refreshed view of mapped and unmapped
transfer objects, the `FunctionRunner` that evaluates JQL collection, aggregate
and string functions over those containers, and `Kleene`, which implements the
strong three-valued boolean semantics JQL requires for nullable expressions.

Callers at runtime are the dispatcher in `judo-runtime-core` (it constructs the
generated script, injects `DAO`, `Dispatcher`, `IdentifierProvider`, `AsmModel`
and `VariableResolver` through the setters, then invokes `apply`) and the
generated script bodies themselves. No hand-written application code calls into
this module, which is exactly why its signatures are frozen: generated code is
only re-emitted at model build time, so a source-incompatible change here breaks
every already-generated script until its owning project regenerates.

Artifact: `hu.blackbelt.judo:judo-operation-utils`, packaging `bundle` (OSGi via
`maven-bundle-plugin`), version `${revision}` (default `1.1.3-SNAPSHOT`), Java 21,
EPL-2.0. It sits on Eclipse EMF/Ecore, `judo-meta-asm`, `judo-dao-api` and
`judo-dispatcher-api`.

## Reactor map

`pom.xml` declares **no `<modules>`** — `judo-operation-utils` is a single-module
(non-aggregator) reactor. Nothing to descend into.

<modules>
  (none — single-module OSGi bundle reactor)
</modules>

What the POM contributes instead of submodules:

| POM contribution | Effect |
|---|---|
| `maven-bundle-plugin` (`bundle` packaging) | Emits the OSGi manifest so the artifact deploys into the JUDO runtime container. |
| `flatten-maven-plugin` | Resolves `${revision}` into the deployed POM (CI-friendly versioning); `.flattened-pom.xml` is git-ignored. |
| `lombok-maven-plugin` + Lombok `provided` dep | Delombok/annotation processing for the generated-script base classes. |
| `jacoco-maven-plugin` + `sonar-maven-plugin` | Coverage at `target/site/jacoco/index.html`, feeding SonarQube analysis. |
| Dependency block | Pins the runtime contract surface: `hu.blackbelt.judo.meta.asm.model`, `judo-dispatcher-api`, `judo-dao-api`, EMF Ecore/Common, Gson, SLF4J/Logback, OSGi Core 6.0.0 + Compendium. |
| Test scope | JUnit 5.9.1, Mockito 4.8.0, Hamcrest 2.2 (`logback-test.xml` configures test logging). |

## Build commands

Use the bundled Maven wrapper (`./mvnw`), never a repo-root `mvn` — no global
Maven installation is required.

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

### Maven profiles

| Profile | Purpose |
|---|---|
| `sign-artifacts` | Sign artifacts using `sign-maven-plugin` for release publishing |
| `release-dummy` | Deploy to a local `/tmp/` directory for testing the release process |
| `release-judong` | Deploy snapshots to `nexus.judo.technology` (JUDO Nexus) |
| `release-central` | Deploy to Maven Central via Sonatype OSSRH with auto-release |
| `generate-github-asciidoc-diagrams` | Generate PNG diagrams from AsciiDoc sources using PlantUML |
| `update-source-code-license` | Update EPL-2.0 license headers in all source files |

## Development environment

**Required:** Java 21 JDK; Maven 3.9.4+ (or use `./mvnw`).

**Optional:** IDE with Lombok support (IntelliJ IDEA, VS Code with the Lombok
extension, Eclipse with the Lombok agent); a SonarQube instance for local static
analysis.

## Git workflow

- **Main branch:** `develop`
- **Versioning:** `${revision}` property (currently `1.1.3-SNAPSHOT`), flattened at build time
- **Branch naming:** `feature/JNG-xxx_description`, `bugfix/JNG-xxx_description`, `release/X.Y.Z`
- **Commit rule:** every commit must reference a JIRA ticket (`JNG-xxx`)
- **CI:** GitHub Actions — see [.github/CIFLOW.md](.github/CIFLOW.md) for the full workflow

## Runtime contracts (do not break silently)

1. This library is **not used directly** — it is a dependency of generated
   operation scripts produced by the JUDO code generator. Changes here affect all
   generated scripts at runtime.
2. `Container` uses **reference identity for unmapped objects** and **UUID-based
   identity for mapped objects**. The `equals`/`hashCode` contract differs based on
   whether `__identifier` is present.
3. `Container.refresh()` is lazy — it only hits the DAO when
   `lastRefresh <= lastWrite`, avoiding unnecessary database roundtrips.
4. The `containerPayloadGet/Put/Remove/GetAs` methods on `Container` are the
   primary API used by generated scripts (marked with `// used by generated script`
   comments). They gate DAO access based on whether the attribute/reference is
   mapped.
5. `FunctionRunner.sort()` always appends an ID-based tiebreaker comparator to
   ensure deterministic ordering.
6. `Kleene` implements **strong Kleene logic** — `true OR null = true`,
   `false AND null = false`, but `null AND null = null`.

Per-file detail for the production sources (including the `AbstractGeneratedScript`
sidecar) lives in
`src/main/java/hu/blackbelt/judo/operation/utils/AGENTS.md` — read it before
editing any source file here.

## Working rules

1. First think through the problem, read the codebase for relevant files.
2. Before you make any major changes, check in with me and I will verify the plan.
3. Please every step of the way just give me a high level explanation of what changes you made.
4. Make every task and code change you do as simple as possible. We want to avoid making any massive or complex changes. Every change should impact as little code as possible. Everything is about simplicity.
5. Maintain a documentation file that describes how the architecture of the app works inside and out.
6. Never speculate about code you have not opened. If the user references a specific file, you MUST read the file before answering. Make sure to investigate and read relevant files BEFORE answering questions about the codebase. Never make any claims about code before investigating unless you are certain of the correct answer - give grounded and hallucination-free answers.
7. For implementation use TDD (Test-Driven Development): write or update tests first to define the expected behaviour, verify they fail, then write the minimal implementation to make them pass.
8. Use DRY (Don't Repeat Yourself): extract reusable logic into separate classes, utilities, or components. If the same pattern appears in multiple places, refactor it into a shared helper.

## Related documentation

- [README.md](README.md) — project overview with architecture and sequence diagrams
- [CONTRIBUTING.md](CONTRIBUTING.md) — how to submit issues and PRs
- [.github/CIFLOW.md](.github/CIFLOW.md) — CI/CD workflow documentation with flow diagrams
- [judo-community](https://github.com/BlackBeltTechnology/judo-community) — parent aggregator project
- [judo-runtime-core](https://github.com/BlackBeltTechnology/judo-runtime-core) — runtime that consumes this library

<!-- dox-doctrine -->
## Documentation Update Protocol (WRITE discipline)

Per-directory `AGENTS.md` files form a tree. Each directory `AGENTS.md` is the
per-file record for the files in that directory. This module-root `AGENTS.md`
holds doctrine + architecture pointers only — never a per-file index.

**Keep the root lean.** This file loads into every agent turn — every byte costs
tokens on every turn. A verbose root file buries the rules the model must follow
(signal dilution) and measurably degrades adherence; a lean file keeps doctrine
salient. Default assumption: your update does NOT belong in the root — route it
by the table below.

**Route every doc update by kind:**

| Kind of update | Goes in |
|---|---|
| New file in a directory, or its per-file detail / change history | Nearest directory `AGENTS.md`. Add a `` | `<basename>` | <purpose> | `` row, path-alphabetical. |
| Data flow, protocol, architecture rationale | `docs/architecture.md` or a `docs/<topic>.md` |
| End-user / developer setup | `README.md` |
| Cross-cutting rule every agent needs every turn (rare) | this module-root `AGENTS.md` |

**Read before editing (chain walk).** Before editing a file, read the nearest
`AGENTS.md` chain root→leaf so you know the file's recorded purpose, contracts,
and change history. Do not edit blind.

**Update after editing (closeout pass).** After changing a file, update its row
in the nearest directory `AGENTS.md`: find the file's row, update its purpose in
place; if absent, add it in path-alphabetical order. New directory → scaffold
its `AGENTS.md`. One row per file. The purpose carries a one-line summary, key
exported symbols, contracts/invariants, and `See change: <id>` history.

**Row style (caveman).** Short declarative fragments. Drop articles. Subject →
verb → object, present tense. One fact per row. Prefer concrete tokens (paths,
symbols, env vars) over prose. Keep identifiers verbatim.

**Size rule — split an over-large directory `AGENTS.md` file-based.** pi
auto-injects a directory `AGENTS.md` on every turn when cwd sits at/below it, so
an over-large directory `AGENTS.md` is not supported. Split it file-based: a row
exceeding the length threshold promotes to a per-file `<File>.AGENTS.md`
sidecar carrying that file's full detail (including every `See change:`). The
sidecar is pull-only — its name is not `AGENTS.md`, so pi never auto-injects it
— yet it stays search-indexed (`agents` doc_type). The directory `AGENTS.md`
keeps a one-line summary plus a `→ see `<File>.AGENTS.md`` pointer. Rows within
the threshold stay verbatim (lossless).

## Finding docs (READ discipline)

`kb_*` tools are faster and cheaper than raw search — they return a one-line
purpose + key exports per file, not raw bytes. **This fires on the ACTION, not
the intent** — before you `grep`/`rg` for a symbol, `cat`/read a file to learn
what it does, or chase an import, the kb call goes first. It fires **even
mid-task when you already know the file**; knowing the file does not exempt you.
When your reflex is the left column, run the right column instead:

| You're about to… | Do this FIRST instead |
|---|---|
| `grep -rn "SymbolName" src/` — find where a fn / type / const lives | `kb_search --doc-type agents "SymbolName"` — tree indexes key exports per file |
| `grep -rn "feature\|topic" src/` — how does X work / where's X handled | `kb_search "feature topic"` |
| `cat` / read a file just to learn its purpose before editing | `kb agents <path>` — one-line purpose + exports + change history |
| chase imports / callers across files | `kb_neighbors <path\|heading>` |
| read one doc section in full | `kb_get <path> <section>` |

**Fall-through (explicit):** if the kb call returns nothing relevant, `rg` /
source read is allowed — then add the missing directory `AGENTS.md` row per the
WRITE discipline. kb does NOT replace grep; it goes first.
