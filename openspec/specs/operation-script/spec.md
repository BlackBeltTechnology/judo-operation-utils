# operation-script Specification

## Purpose

Provides the `AbstractGeneratedScript` base class and its inner `Container` class, forming the runtime foundation for all JUDO-generated operation scripts. Manages transfer object lifecycle (create, refresh, update, delete), identity tracking for mapped and unmapped entities, query execution, and navigation across the ASM metamodel.

## Architecture

`AbstractGeneratedScript` implements `Function<Payload, Payload>`. It holds references to `DAO`, `Dispatcher`, `AsmModel`, `IdentifierProvider`, and `VariableResolver`, all injected by the runtime before `apply()` is called.

The `Container` inner class wraps an `EClass` and `Payload` pair. Containers are tracked in two maps:
- `containers: Map<UUID, Map<String, Set<Container>>>` — for mapped (persisted) objects, keyed by `__identifier`
- `unmappeds: Map<UUID, Container>` — for transient objects, keyed by `__unmappedid`

Subclasses implement the abstract `doApply(Payload, Holder<Payload>)` method containing the generated business logic.

## Requirements

### Requirement: Script execution lifecycle

The `apply(Payload)` method SHALL extract the `JudoPrincipal` from the exchange, delegate to `doApply`, and return the output payload. If `outputName` is set but no output was produced and `outputLowerBound != 0`, it SHALL throw a `RuntimeException`.

#### Scenario: Successful operation with output
- **GIVEN** a generated script with `outputName = "result"` and `outputLowerBound = 1`
- **WHEN** `apply(exchange)` is called and `doApply` sets `outputHolder.value`
- **THEN** the output payload is returned

#### Scenario: Missing required output
- **GIVEN** a generated script with `outputName = "result"` and `outputLowerBound = 1`
- **WHEN** `apply(exchange)` is called and `doApply` does NOT set `outputHolder.value`
- **THEN** a `RuntimeException` is thrown with message "Script output is not set (missing return statement)."

#### Scenario: Optional output not set (single-valued)
- **GIVEN** a generated script with `outputName = "result"`, `outputLowerBound = 0`, and `outputUpperBound = 1`
- **WHEN** `apply(exchange)` is called and `doApply` does NOT set `outputHolder.value`
- **THEN** a payload mapping `outputName` to `Payload.empty()` is returned

### Requirement: Container creation for mapped transfer objects

`createContainer(EClass, Payload)` SHALL return an existing container if one with the same `__identifier` and class name is already tracked. Otherwise it SHALL create a new container, assign a `__to_identifier`, and register it.

#### Scenario: First creation of a mapped container
- **GIVEN** a payload with `__identifier = UUID-1` and an EClass `Person`
- **WHEN** `createContainer(Person, payload)` is called for the first time
- **THEN** a new `Container` is created, registered in the `containers` map under `UUID-1`, and returned

#### Scenario: Returning existing mapped container
- **GIVEN** a container already exists for `__identifier = UUID-1` with class `Person`
- **WHEN** `createContainer(Person, payload)` is called with the same `__identifier`
- **THEN** the existing container's payload is updated and the existing container is returned

### Requirement: Container creation for unmapped transfer objects

When a payload lacks `__identifier`, `createContainer` SHALL generate a `__unmappedid` UUID, populate static features from the DAO, and recursively create containers for nested payloads.

#### Scenario: Creating an unmapped container
- **GIVEN** a payload without `__identifier`
- **WHEN** `createContainer(SomeClass, payload)` is called
- **THEN** a `__unmappedid` is generated, `dao.getStaticFeatures(clazz)` is merged, and the container is stored in `unmappeds`

### Requirement: Immutable container semantics

`createImmutableContainer` SHALL move `__identifier` to `__mutable_identifier` and set `immutable = true`. Calling `updatePayload` on an immutable container SHALL throw `IllegalStateException`.

#### Scenario: Creating an immutable copy
- **GIVEN** a mapped container with `__identifier = UUID-1`
- **WHEN** `createImmutableContainer(container)` is called
- **THEN** the new container has `__mutable_identifier = UUID-1`, no `__identifier`, and `immutable = true`

#### Scenario: Attempting to update an immutable container
- **GIVEN** an immutable container
- **WHEN** `updatePayload(newPayload)` is called
- **THEN** `IllegalStateException` with message "Tried to refresh immutable container" is thrown

### Requirement: Container refresh from DAO

`Container.refresh()` SHALL re-fetch the payload from the DAO when `lastRefresh <= lastWrite` for mapped containers. For unmapped containers, it SHALL recursively refresh nested container payloads and remove stale references.

#### Scenario: Refreshing a mapped container after a write
- **GIVEN** a mapped container whose `lastRefresh < lastWrite`
- **WHEN** `refresh()` is called
- **THEN** `dao.getByIdentifier(clazz, id)` is called and the payload is updated

#### Scenario: Skipping refresh when already current
- **GIVEN** a mapped container whose `lastRefresh > lastWrite`
- **WHEN** `refresh()` is called
- **THEN** no DAO call is made

### Requirement: Attribute update with DAO persistence

`updateAttribute(Container, attributeName, value)` SHALL update the attribute in the DAO if the container's class and attribute are both mapped. For unmapped attributes on mapped classes, only the in-memory payload is updated.

#### Scenario: Updating a mapped attribute
- **GIVEN** a mapped container with a mapped attribute `name`
- **WHEN** `updateAttribute(container, "name", "John")` is called
- **THEN** `dao.getByIdentifier` fetches the current state, the attribute is set, `dao.update` persists it, and `write()` is called

### Requirement: Element deletion

`deleteElement(Container)` SHALL call `dao.delete` for mapped containers and remove the container from tracking maps.

#### Scenario: Deleting a mapped element
- **GIVEN** a mapped container with `__identifier = UUID-1`
- **WHEN** `deleteElement(container)` is called
- **THEN** `dao.delete(clazz, UUID-1)` is invoked and the container is removed from `containers`

### Requirement: Navigation to related containers

`containersFromNavigation(Container, referenceName)` SHALL follow an `EReference` to retrieve related payloads. For mapped references, it delegates to `dao.getNavigationResultAt`. For unmapped references, it reads from the in-memory payload.

#### Scenario: Navigating a mapped reference
- **GIVEN** a mapped container with a mapped reference `orders`
- **WHEN** `containersFromNavigation(container, "orders")` is called
- **THEN** `dao.getNavigationResultAt(id, reference)` returns the related payloads, each wrapped in a new Container

### Requirement: Primitive query execution

`primitiveQueryCall` SHALL execute a DAO search with the given parameters and return a single typed result. If multiple results are found, it SHALL throw `IllegalStateException`.

#### Scenario: Successful primitive query
- **GIVEN** a mapped container and a query `totalAmount` that returns one BigDecimal
- **WHEN** `primitiveQueryCall(BigDecimal.class, container, "totalAmount", inputType, inputPayload)` is called
- **THEN** the single BigDecimal result is returned

#### Scenario: Multiple results for primitive query
- **WHEN** a primitive query returns more than one distinct value
- **THEN** `IllegalStateException` is thrown with the `MULTIPLE_RESULTS_FOR_SINGLE_PRIMITIVE_QUERY_FORMAT` message

### Requirement: Complex query execution

`complexQueryCall` SHALL execute a navigation query and return a collection of containers of the specified return type.

#### Scenario: Instance-scoped complex query
- **GIVEN** a mapped container and a reference query `relatedItems`
- **WHEN** `complexQueryCall(container, returnTypeFqName, "relatedItems", inputType, inputPayload)` is called
- **THEN** `dao.searchNavigationResultAt` is called and the results are wrapped in containers

### Requirement: Type coercion

`coerce(sourceValue, targetClass)` SHALL delegate to `dispatcher.coerce` for type conversion between compatible JUDO types.

#### Scenario: Coercing a value
- **GIVEN** a source value of type `Integer` and target class `BigDecimal`
- **WHEN** `coerce(42, BigDecimal.class)` is called
- **THEN** the dispatcher converts and returns the appropriate BigDecimal
