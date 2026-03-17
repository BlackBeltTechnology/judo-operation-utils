# judo-operation-utils

[![Build](https://github.com/BlackBeltTechnology/judo-operation-utils/actions/workflows/build.yml/badge.svg?branch=develop)](https://github.com/BlackBeltTechnology/judo-operation-utils/actions/workflows/build.yml)

## Introduction

`judo-operation-utils` provides runtime utility classes that generated JUDO operation scripts depend on. When a JUDO model defines custom operations (business logic), those operations are compiled into Java classes that extend `AbstractGeneratedScript`. This library supplies that base class along with collection/query helpers (`FunctionRunner`) and three-valued logic (`Kleene`).

It is consumed by the [judo-runtime-core](https://github.com/BlackBeltTechnology/judo-runtime-core) Dispatcher, which instantiates generated scripts, injects the DAO layer and metamodel, and invokes them as `Function<Payload, Payload>`.

## How It Works

The library sits between the JUDO metamodel layer and the persistence layer, giving generated scripts a high-level API to manipulate transfer objects without writing raw DAO calls.

```mermaid
sequenceDiagram
    participant Dispatcher
    participant Script as Generated Script<br/>(extends AbstractGeneratedScript)
    participant Container as Container
    participant DAO
    participant AsmModel as ASM Metamodel

    Dispatcher->>Script: apply(Payload)
    Script->>AsmModel: resolve EClass / EReference
    Script->>DAO: getAllOf / getByIdentifier / search
    DAO-->>Script: List<Payload>
    Script->>Container: createContainer(EClass, Payload)
    Script->>Container: containerPayloadGet/Put
    Container->>DAO: update / delete (if mapped)
    Script-->>Dispatcher: output Payload
```

## Architecture

```mermaid
classDiagram
    class AbstractGeneratedScript {
        <<abstract>>
        +apply(Payload) Payload
        #doApply(Payload, Holder~Payload~)*
        #createContainer(EClass, Payload) Container
        #updateAttribute(Container, String, Object)
        #deleteElement(Container)
        #containersForAll(String, String) Collection
        #containersFromNavigation(Container, String) Set
        #primitiveQueryCall(Class, Container, String, String, Payload)
        #complexQueryCall(Container, String, String, String, Payload)
    }

    class Container {
        +payload : Payload
        +clazz : EClass
        +deleted : boolean
        +immutable : boolean
        +getPayload() Payload
        +refresh() Container
        +containerPayloadGet(String) Object
        +containerPayloadPut(String, Object)
    }

    class FunctionRunner {
        +filter(Collection, Function) Collection
        +sort(Collection, SortOrderBy...) List
        +head(Collection, SortOrderBy...) Container
        +count(Collection) BigInteger
        +sumInteger / sumDecimal / avg(...)
        +exists / forAll(Collection, Function) Boolean
        +getPrincipal(EClass) Container
        +getVariable(String, String, String, String) Object
    }

    class Kleene {
        +and(Boolean, Boolean) Boolean$
        +or(Boolean, Boolean) Boolean$
        +xor(Boolean, Boolean) Boolean$
        +implies(Boolean, Boolean) Boolean$
        +not(Boolean) Boolean$
    }

    AbstractGeneratedScript *-- Container : inner class
    AbstractGeneratedScript --> FunctionRunner : functionRunner
    AbstractGeneratedScript ..|> Function~Payload Payload~ : implements
```

### Container Identity Model

Containers use a dual-identity scheme to track both persisted and transient objects:

| Payload Key | Meaning |
|---|---|
| `__identifier` | UUID of a **mapped** (persisted) transfer object |
| `__unmappedid` | UUID of a **transient** (unmapped) object |
| `__mutable_identifier` | Stored on **immutable copies** — holds the original mapped UUID so the object can be made mutable again |
| `__toType` | Fully qualified transfer object type name |
| `__entityType` | Underlying entity type (used for type-assignability checks) |

## Key Dependencies

```mermaid
graph LR
    subgraph JUDO Ecosystem
        DAO[judo-dao-api<br/>DAO, Payload, IdentifierProvider]
        DISP[judo-dispatcher-api<br/>Dispatcher, JudoPrincipal, VariableResolver]
        ASM[judo-meta-asm<br/>AsmModel, AsmUtils]
    end
    subgraph Eclipse
        EMF[EMF Ecore<br/>EClass, EReference, EAttribute]
    end
    subgraph This Library
        AGS[AbstractGeneratedScript]
        FR[FunctionRunner]
        KL[Kleene]
    end
    AGS --> DAO
    AGS --> DISP
    AGS --> ASM
    ASM --> EMF
    FR --> AGS
```

## Build

```bash
# Full build
./mvnw clean install

# Build without tests
./mvnw clean install -DskipTests

# Run tests
./mvnw test

# Run a single test
./mvnw test -Dtest=ClassName#methodName
```

## Context

This project is a building block of the [judo-community](https://github.com/BlackBeltTechnology/judo-community) aggregator project. Check the corresponding documentation to understand how this module fits into the ecosystem.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on submitting issues and pull requests.

## License

This project is licensed under the [Eclipse Public License - v 2.0](https://www.eclipse.org/legal/epl-2.0/).
