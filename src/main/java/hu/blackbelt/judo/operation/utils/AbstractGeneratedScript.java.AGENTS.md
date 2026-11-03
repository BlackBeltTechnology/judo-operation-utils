# `AbstractGeneratedScript.java`

Superclass every JUDO-generated operation script extends. Implements
`Function<Payload, Payload>`; `apply(Payload)` reads `Dispatcher.PRINCIPAL_KEY` into
`principal`, delegates to the abstract `doApply(Payload, Holder<Payload>)`, then shapes the
result: an unset output holder with `outputLowerBound != 0` throws
`RuntimeException("Script output is not set (missing return statement).")`, otherwise it
defaults to `Payload.empty()` for `outputUpperBound == 1` and an empty `LinkedHashSet` for a
collection output. A null `outputName` makes `apply` return `null`.

## Wiring

Setter-injected before first `apply`: `setDao(DAO)`, `setDispatcher(Dispatcher)`,
`setIdProvider(IdentifierProvider)`, `setAsmModel(AsmModel)` (also builds `asmUtils`),
`setVariableResolver(VariableResolver)`. Nothing is null-checked, so a script applied before
injection fails with `NullPointerException` inside the first DAO or ASM call.

## `Container` — object identity and DAO coupling

Inner class pairing an `EClass` with a `Payload`. Two identity schemes coexist:
`getMappedId()` reads `__identifier` (persisted objects), `getId()` falls back to
`__mutable_identifier`, and unmapped objects live under `__unmappedid`. `equals`/`hashCode`
switch on that: with a mapped id they compare and hash by UUID, without one they fall back to
`Object` reference identity. Mixing the two in one `Set` is therefore order-sensitive — a
container that acquires an `__identifier` after insertion is no longer findable in that set.

`refresh()` is lazy: it re-reads from the DAO only while `lastRefresh <= lastWrite`, and any
mutating path calls `write()` to bump `lastWrite`. A DAO row that has disappeared sets
`deleted = true` and merges `Payload.empty()`. `updatePayload(Payload)` merges new keys and
then *removes* every mapped attribute and reference absent from the incoming payload, so a
partial payload silently clears mapped fields. It throws
`IllegalStateException("Tried to refresh immutable container")` on an immutable container.

`getPayload()` returns `Payload.empty()` for a deleted container and otherwise refreshes
first — so it can hit the database.

## API used by generated scripts

`containerPayloadGet`, `containerPayloadPut`, `containerPayloadRemove`, and
`containerPayloadGetAs` (all marked `// used by generated script`) gate on mapped-ness: for a
mapped transfer-object attribute or reference they route through `getPayload()` (refresh, then
`write()` on mutation), otherwise they touch the in-memory payload directly. All four are
no-ops or return `null` once `deleted` is set — they never throw on a deleted container.

Container lifecycle helpers: `createContainer`, `createImmutableContainer`,
`createMutableContainer`, `createImmutableContainerList`, `createMutableContainerList`,
`spawnContainer`, `spawnContainers`, `deleteContainer`, `deleteElement`, `findContainers`.
Live containers are interned in `containers` (`Map<UUID, Map<String, Set<Container>>>`) and
`unmappeds` (`Map<UUID, Container>`) under the `lockContainers` monitor, so the same persisted
row yields the same `Container` instance within one script run.

## Query and navigation surface

`primitiveQueryCall` (subject-bound and static-by-FQ-name overloads) returns a single coerced
scalar and throws on `MULTIPLE_RESULTS_FOR_SINGLE_PRIMITIVE_QUERY_FORMAT`;
`complexQueryCall` (same two shapes) returns `Collection<Container>`. Static parameterized
queries on mapped transfer objects are rejected via `STATIC_QUERY_ON_MAPPED_TRANSFEROBJECTS`.
`containersFromNavigation`, `containersForAll`, `getStaticData`, `getStaticDataContainer`,
`assignNewEmbeddedCollection`, `updateAttribute`, `isContainment`, `resolveClassifier`,
`getEnumOrdinal`, `getClass`, `getScale`, and the two `coerce` overloads round out what
generated code calls.

## Nested helpers

- `Holder<T>` — mutable single-slot box with a public `value` field and `getValue()`. Carries
  script output out of `doApply` and carries the current element into lambda bodies.
- `SortOrderBy<T extends Comparable>` — public `generator` (`Function<Holder<Container>, T>`)
  plus `descending` flag; `FunctionRunner` builds comparators from arrays of these.

## Protocol constants

`TO_TYPE` (`__toType`) and `NULL_PARAMETER_FORMAT` / `EMPTY_STRING_PARAMETER_FORMAT` are
`public`; the payload keys `__identifier`, `__unmappedid`, `__to_identifier`, `__entityType`,
`__mutable_identifier` and the error-message formats are `protected`. Renaming any of them
breaks already-generated scripts at runtime, not at compile time.
