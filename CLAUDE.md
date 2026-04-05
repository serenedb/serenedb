# SereneDB — Claude Guidelines

SereneDB is a distributed real-time search analytics database written in C++20, unifying Elasticsearch-style search with ClickHouse-style analytics behind a Postgres-compatible interface. Core dependencies: Velox (query execution), RocksDB (storage), IResearch (full-text search), Abseil (utilities).


## Naming Conventions

| Symbol | Style | Example |
|---|---|---|
| Classes / Structs | `PascalCase` | `TransactionId`, `IoContext` |
| Methods / Functions | `PascalCase` | `IsSet()`, `ServerId()` |
| Member variables | `_snake_case` (leading `_`) | `_name`, `_iocontext` |
| Boolean variables/functions | `is`/`has` prefix + follow symbol's style | `is_coordinator`/`HasIndex()` |
| Constants (compile-time) | `k` prefix with `PascalCase` | `kDefaultOptionsFilter` |

Avoid creating functions that looks like constructor calls. E.g. instead of Type() prefer MakeType()

## Code Style

Formatting is enforced by `.clang-format` (Google-based, 80-col limit, 2-space indent). pre-commit hooks will reject non-conforming diffs.

- Use `#pragma once` — never `#ifndef` include guards
- Headers use `.hpp`, template implementations use `.tpp`, sources use `.cpp`; `fwd.h` files are exceptions.
- No `using namespace` in headers — always use fully qualified names
- Prefer `constexpr` over `const` in sources and `inline constexpr` in headers; use `constinit` to avoid static init order issues
- Use designated initializers for POD-like types: `{.foo = 1, .bar = 2}`
- Keep templates and inline functions minimal (binary size matters)

## Include Ordering

Enforced by `.clang-format` — do not reorder manually.

1. PostgreSQL system headers (e.g., `postgres.h`) — angle brackets, highest priority
2. C standard library headers (`<stdio.h>`, `<string.h>`)
3. C++ standard library headers (`<vector>`, `<string>`)
4. Third-party headers.
5. Local headers (`"server/..."`, `"util/..."`)

## Containers & Types

- Dynamic arrays: `std::vector<T>`
- Strings: `std::string` and `std::string_view` (no raw `char*` buffers)
- Fixed-size arrays: `std::array<T, N>`
- Concurrency primitives: `absl::Mutex`, `absl::CondVar` (not `std::mutex`)
- Async callbacks: `absl::AnyInvocable` (not `std::function`)
- No raw owning pointers — use smart pointers or value semantics

### Hash containers — prefer `sdb::containers` over absl directly

| Instead of | Use |
|---|---|
| `absl::flat_hash_map` | `sdb::containers::FlatHashMap` |
| `absl::flat_hash_set` | `sdb::containers::FlatHashSet` |
| `absl::node_hash_map` | `sdb::containers::NodeHashMap` |
| `absl::node_hash_set` | `sdb::containers::NodeHashSet` |
| `boost::container::small_vector` | `sdb::containers::SmallVector` |

Note: `FlatHashSet`/`FlatHashMap` enforce a 32-byte size limit on `T` (use the `Node*` variants for larger types). Only fall back to absl directly if the `sdb::containers` variant fails to compile — and warn the user explicitly when that happens.

## General Principles

- Prefer value semantics and stack allocation over heap where reasonable
- Do not add error handling for scenarios that cannot occur; trust internal invariants
- Do not add speculative abstractions — implement exactly what is needed
- Do not add comments unless the logic is non-obvious; the code should be self-documenting.
