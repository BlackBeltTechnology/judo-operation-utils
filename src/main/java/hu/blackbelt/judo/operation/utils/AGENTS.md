# `src/main/java/hu/blackbelt/judo/operation/utils`

Whole production surface of `judo-operation-utils`. Everything here is called from
JUDO-generated operation scripts, never by hand-written application code — a signature change
here breaks generated code that is only re-generated at model build time.

| File | Purpose |
|---|---|
| `AbstractGeneratedScript.java` | Superclass of every generated operation script; owns the `Container` identity/refresh model, the `containerPayload*` API, and query/navigation calls. → see `AbstractGeneratedScript.java.AGENTS.md` |
| `FunctionRunner.java` | Evaluates JQL collection, aggregate and string functions over `Container` sets. Exports `filter`, `exists`, `forAll`, `sort`, `head`/`tail`/`heads`/`tails`, `min`/`max`/`sumInteger`/`avg`, `substring`, `like`, `getPrincipal`, `getVariable`. `sort` appends a `Container::getId` tiebreaker for deterministic order; `substring` is 1-based; `getVariable` throws `IllegalArgumentException` for a non-`EDataType`. |
| `Kleene.java` | Strong three-valued boolean logic with `null` as unknown. Exports static `and`, `or`, `xor`, `implies`, `not` over boxed `Boolean`. `or` yields TRUE when one side is TRUE even if the other is null, `and` yields FALSE likewise, `xor` is null whenever either side is null. Callers must keep results boxed; unboxing a null result throws `NullPointerException`. |

## Cross-file contracts

- `FunctionRunner` holds a back-reference to its `AbstractGeneratedScript` and reaches into
  its `asmUtils`, `dispatcher`, `principal`, and `variableResolver` fields directly. It is
  therefore useless standalone and must be constructed after the script's setters have run.
- `FunctionRunner.filter` coerces a null predicate result to `false` via
  `Objects.requireNonNullElse`, so `Kleene` unknowns degrade to exclusion rather than
  propagating out of a filter.
- `filter` collects to a `Set`, which means it inherits `Container.equals`/`hashCode` — two
  distinct unmapped containers survive filtering, two containers sharing an `__identifier`
  collapse to one.
