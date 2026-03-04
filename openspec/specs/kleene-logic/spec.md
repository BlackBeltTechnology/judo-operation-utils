# kleene-logic Specification

## Purpose

Provides the `Kleene` utility class implementing three-valued (strong Kleene) logic for boolean operations that must correctly handle `null` values. Used by generated operation scripts when evaluating boolean expressions where operands may be undefined.

## Architecture

`Kleene` is a stateless utility class with five static methods: `and`, `or`, `xor`, `implies`, and `not`. All methods accept `Boolean` (nullable) parameters and return `Boolean` (nullable). The implementation follows strong Kleene logic where `null` represents an unknown truth value.

Truth tables:

| A | B | and | or | xor | implies |
|---|---|-----|-----|-----|---------|
| T | T | T | T | F | T |
| T | F | F | T | T | F |
| T | null | null | T | null | null |
| F | T | F | T | T | T |
| F | F | F | F | F | T |
| F | null | F | null | null | T |
| null | T | null | T | null | T |
| null | F | F | null | null | null |
| null | null | null | null | null | null |

| A | not |
|---|-----|
| T | F |
| F | T |
| null | null |

## Requirements

### Requirement: Kleene OR

`Kleene.or(Boolean, Boolean)` SHALL return `true` if either operand is `true`, `null` if either operand is `null` and neither is `true`, and `false` only if both operands are `false`.

#### Scenario: True dominates null
- **WHEN** `Kleene.or(true, null)` is called
- **THEN** `true` is returned

#### Scenario: Both false
- **WHEN** `Kleene.or(false, false)` is called
- **THEN** `false` is returned

#### Scenario: Both null
- **WHEN** `Kleene.or(null, null)` is called
- **THEN** `null` is returned

### Requirement: Kleene AND

`Kleene.and(Boolean, Boolean)` SHALL return `false` if either operand is `false`, `null` if either operand is `null` and neither is `false`, and `true` only if both operands are `true`.

#### Scenario: False dominates null
- **WHEN** `Kleene.and(false, null)` is called
- **THEN** `false` is returned

#### Scenario: Both true
- **WHEN** `Kleene.and(true, true)` is called
- **THEN** `true` is returned

#### Scenario: True and null
- **WHEN** `Kleene.and(true, null)` is called
- **THEN** `null` is returned

### Requirement: Kleene XOR

`Kleene.xor(Boolean, Boolean)` SHALL return `null` if either operand is `null`. If both are non-null, it SHALL return `true` if they differ and `false` if they are equal.

#### Scenario: Different values
- **WHEN** `Kleene.xor(true, false)` is called
- **THEN** `true` is returned

#### Scenario: Same values
- **WHEN** `Kleene.xor(true, true)` is called
- **THEN** `false` is returned

#### Scenario: Null propagation
- **WHEN** `Kleene.xor(true, null)` is called
- **THEN** `null` is returned

### Requirement: Kleene IMPLIES

`Kleene.implies(Boolean, Boolean)` SHALL return `false` only when the left operand is `true` and the right is `false`. It SHALL return `true` when the left is `false` or the right is `true`. Otherwise it SHALL return `null`.

#### Scenario: True implies false
- **WHEN** `Kleene.implies(true, false)` is called
- **THEN** `false` is returned

#### Scenario: False implies anything
- **WHEN** `Kleene.implies(false, null)` is called
- **THEN** `true` is returned

#### Scenario: Anything implies true
- **WHEN** `Kleene.implies(null, true)` is called
- **THEN** `true` is returned

### Requirement: Kleene NOT

`Kleene.not(Boolean)` SHALL return the logical negation of the operand, or `null` if the operand is `null`.

#### Scenario: Negating true
- **WHEN** `Kleene.not(true)` is called
- **THEN** `false` is returned

#### Scenario: Negating null
- **WHEN** `Kleene.not(null)` is called
- **THEN** `null` is returned
