# function-runner Specification

## Purpose

Provides the `FunctionRunner` class — a collection of utility methods for collection operations, aggregations, string manipulation, sorting, actor/principal lookup, and environment variable resolution. Used by generated operation scripts to evaluate JQL expressions at runtime.

## Architecture

`FunctionRunner` holds a reference to its parent `AbstractGeneratedScript` instance and operates on `Container` objects. Collection operations work via `Holder<Container>` wrappers that allow lambda-based iteration. Sorting uses `SortOrderBy<T>` descriptors with multi-field chained comparators and a deterministic ID-based tiebreaker.

Key method categories:
- **Collection queries:** `any`, `count`, `empty`, `contains`, `filter`, `exists`, `forAll`
- **Aggregations:** `min`, `max`, `sumInteger`, `sumDecimal`, `avg`, `avgDate`, `avgTimestamp`, `avgTime`
- **Ordering:** `sort`, `head`, `tail`, `heads`, `tails`
- **String operations:** `substring`, `matches`, `like`, `replace`
- **Security:** `getPrincipal`, `isCurrentActor`
- **Environment:** `getVariable`

## Requirements

### Requirement: Collection element selection

`any(Collection)` SHALL return the first element of a collection, or `null` if the collection is null or empty.

#### Scenario: Non-empty collection
- **GIVEN** a collection with 3 containers
- **WHEN** `any(collection)` is called
- **THEN** the first element (iterator order) is returned

#### Scenario: Empty collection
- **GIVEN** an empty collection
- **WHEN** `any(collection)` is called
- **THEN** `null` is returned

### Requirement: Collection counting

`count(Collection)` SHALL return the size of a collection as `BigInteger`, or `BigInteger.ZERO` for null/empty collections.

#### Scenario: Counting elements
- **GIVEN** a collection with 5 containers
- **WHEN** `count(collection)` is called
- **THEN** `BigInteger.valueOf(5)` is returned

### Requirement: Collection emptiness check

`empty(Collection)` SHALL return `true` if the collection is null or empty, `false` otherwise.

#### Scenario: Null collection
- **WHEN** `empty(null)` is called
- **THEN** `true` is returned

### Requirement: Collection filtering

`filter(Collection, Function)` SHALL return a new collection containing only elements where the condition function returns `true`. Elements where the condition returns `null` SHALL be excluded.

#### Scenario: Filtering with mixed results
- **GIVEN** a collection of 3 containers and a condition that returns `true` for 2 of them and `null` for 1
- **WHEN** `filter(collection, condition)` is called
- **THEN** a set of 2 containers is returned

### Requirement: Existential quantifier

`exists(Collection, Function)` SHALL return `true` if at least one element satisfies the condition.

#### Scenario: At least one match
- **GIVEN** a collection where one element satisfies the condition
- **WHEN** `exists(collection, condition)` is called
- **THEN** `true` is returned

### Requirement: Universal quantifier

`forAll(Collection, Function)` SHALL return `true` only if all elements satisfy the condition.

#### Scenario: All match
- **GIVEN** a collection where all elements satisfy the condition
- **WHEN** `forAll(collection, condition)` is called
- **THEN** `true` is returned

#### Scenario: Not all match
- **GIVEN** a collection where one element does not satisfy the condition
- **WHEN** `forAll(collection, condition)` is called
- **THEN** `false` is returned

### Requirement: Aggregation operations

`min`, `max`, `sumInteger`, `sumDecimal`, and `avg` SHALL compute the respective aggregate over a collection using a generator function that extracts the value from each container.

#### Scenario: Sum of integers
- **GIVEN** containers with integer values [10, 20, 30]
- **WHEN** `sumInteger(collection, generator)` is called
- **THEN** `BigInteger.valueOf(60)` is returned

#### Scenario: Average of decimals
- **GIVEN** containers with decimal values [10.0, 20.0, 30.0]
- **WHEN** `avg(collection, generator)` is called
- **THEN** `BigDecimal.valueOf(20.0)` is returned

#### Scenario: Average with empty collection
- **GIVEN** an empty collection
- **WHEN** `avg(collection, generator)` is called
- **THEN** `BigDecimal.ZERO` is returned

### Requirement: Temporal aggregations

`avgDate`, `avgTimestamp`, and `avgTime` SHALL compute temporal averages by converting to epoch millis (or nano-of-day for time), averaging, and converting back.

#### Scenario: Average of dates
- **GIVEN** containers with dates [2024-01-01, 2024-01-03]
- **WHEN** `avgDate(collection, generator)` is called
- **THEN** approximately `2024-01-02` is returned

### Requirement: Deterministic sorting

`sort(Collection, SortOrderBy...)` SHALL sort containers using the provided sort order descriptors. When no sort orders are given, it SHALL sort by container ID. A tiebreaker comparator on container ID SHALL always be appended to ensure deterministic ordering.

#### Scenario: Multi-field sort with tiebreaker
- **GIVEN** containers with identical primary sort keys but different IDs
- **WHEN** `sort(collection, orderBy)` is called
- **THEN** containers with equal primary keys are ordered by their UUID

### Requirement: Head and tail selection

`head(Collection, SortOrderBy...)` SHALL return the first element after sorting. `tail(Collection, SortOrderBy...)` SHALL return the last element. Both SHALL return `null` for null/empty collections.

#### Scenario: Head of sorted collection
- **GIVEN** a collection sorted by age ascending
- **WHEN** `head(collection, ascByAge)` is called
- **THEN** the container with the smallest age is returned

### Requirement: Heads and tails (all minimums/maximums)

`heads(Collection, SortOrderBy...)` SHALL return all containers that share the minimum sort key. `tails` SHALL return all that share the maximum.

#### Scenario: Multiple containers with same minimum
- **GIVEN** 3 containers where 2 have age=20 (minimum) and 1 has age=30
- **WHEN** `heads(collection, ascByAge)` is called
- **THEN** a collection of 2 containers is returned

### Requirement: Substring extraction

`substring(String, BigInteger position, BigInteger length)` SHALL extract a substring using 1-based positioning. If the end index exceeds the string length, it SHALL return from position to end.

#### Scenario: Substring within bounds
- **GIVEN** string "Hello World"
- **WHEN** `substring("Hello World", 1, 5)` is called
- **THEN** "Hello" is returned

#### Scenario: Null string
- **WHEN** `substring(null, 1, 5)` is called
- **THEN** `null` is returned

### Requirement: Pattern matching

`matches(String, String)` SHALL return whether the text matches the regex pattern. `like(String, String, boolean)` SHALL convert SQL LIKE patterns (`%` → `.*?`, `_` → `.`) to regex, with optional case-insensitive matching. Both SHALL return `null` if either argument is null.

#### Scenario: SQL LIKE pattern matching
- **GIVEN** text "Hello World"
- **WHEN** `like("Hello World", "Hello%", false)` is called
- **THEN** `true` is returned

#### Scenario: Case-insensitive LIKE
- **WHEN** `like("HELLO", "hello", true)` is called
- **THEN** `true` is returned

### Requirement: String replacement

`replace(String, String, String)` SHALL replace all literal occurrences of the pattern in the text. It SHALL return `null` if the text is null.

#### Scenario: Replacing text
- **WHEN** `replace("foo bar foo", "foo", "baz")` is called
- **THEN** "baz bar baz" is returned

### Requirement: Principal lookup

`getPrincipal(EClass actorType)` SHALL return a container for the current actor's principal by invoking the `GET_PRINCIPAL` operation on the actor type. It SHALL return `null` if the actor type does not match the current principal's client.

#### Scenario: Matching actor type
- **GIVEN** a principal with client `demo.actors.Admin` and an actorType EClass with FQName `demo.actors.Admin`
- **WHEN** `getPrincipal(actorType)` is called
- **THEN** the `GET_PRINCIPAL` operation is dispatched and the result is returned as a Container

### Requirement: Environment variable resolution

`getVariable(typeNamespace, typeName, categoryName, variableName)` SHALL resolve the classifier, determine its data type, and delegate to `variableResolver.resolve` with the appropriate Java class. For enumeration types, the resolved string SHALL be converted to the enum literal's ordinal value.

#### Scenario: Resolving a string variable
- **GIVEN** a variable of type String
- **WHEN** `getVariable("config", "Settings", "app", "title")` is called
- **THEN** `variableResolver.resolve(String.class, "app", "title")` is invoked

### Requirement: Type assignability check

`isAssignable(Container, EClass)` SHALL return `true` if the container's class equals the target class, or if both are mapped transfer objects and the container's underlying entity type equals or extends the target's mapped entity type.

#### Scenario: Direct type match
- **GIVEN** a container of class `Person` and target class `Person`
- **WHEN** `isAssignable(container, Person)` is called
- **THEN** `true` is returned

#### Scenario: Subtype match via entity hierarchy
- **GIVEN** a container whose entity type `Employee` extends `Person`, and target class maps to `Person`
- **WHEN** `isAssignable(container, targetClass)` is called
- **THEN** `true` is returned
