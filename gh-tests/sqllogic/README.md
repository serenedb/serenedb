# SQL Logic Test Runner

This repository contains a modern Rust-based SQL logic test runner for SereneDB, replacing the previous Python implementation.

## Prerequisites

- For Docker execution:
  - Docker
  - Docker Compose

- For native execution:
  - Rust toolchain
  - Running SereneDB instance

## Quick Start with Docker

```bash
./tests/sqllogic/run_in_docker.sh
```

This will:
1. Start SereneDB single-node container
2. Execute all tests in an isolated environment
3. Collect logs and clean up containers

## Native Execution (for debugging/development)

```bash
./tests/sqllogic/run.sh [OPTIONS]
```

Common options:
- `--single-port 7777`: Test against SereneDB on this port
- `--test 'path/to/*.test'`: Run specific test files
- `--debug`: Enable debug mode (disables timeout)
- `--jobs N`: Set parallelism level (default: CPU cores)
- `--override`: Rewrite test file (Note: doesn't create empty database)

Example:
```bash
./tests/sqllogic/run.sh --single-port 7777 --test 'tests/sqllogic/sdb/pg/simple_any/setop.test' --debug true
```

## Customizing Docker Execution

To modify test parameters in Docker:

1. Edit `tests/sqllogic/docker-compose.yml`
2. Change the command in the `tests` service:
```yaml
command: /sqllogic/_execute_tests_in_docker.sh --your-parameters-here
```

## Key Features

- Supports both simple and extended PostgreSQL protocols
- Parallel test execution
- JUnit XML report generation

## Troubleshooting

1. Check service logs:
   - `serenedb-single.log`

2. For Docker issues:
   - Verify ports are available
   - Check Docker resource limits

3. For native execution:
   - Ensure services are running
   - Verify Rust toolchain is installed (`cargo --version`)

## Directory organization
* `any/any/` – tests that **every SQL database** must pass (absent for now)
* `any/pg/` – tests that **every PostgreSQL-compatible backend** must pass, it should have symlink to `any/any/` when the last one appears.
* `sdb/pg/` – tests that **only SereneDB PostgreSQL-compatible backend** must pass. It has symlink to `any/pg/` suites with suffix `_any`.
* `pg/` – tests that only reference PostgreSQL backend must pass, but **SereneDB PostgreSQL-compatible backend strives to pass**. It has symlinks to `any/pg/` suites with suffix `_any`.

This structure is applied for each protocol.
```
any
  any/ (1)
  protocolX/ (2) --> (1)
  protocolY/ (3) --> (2)
  ...
sdb
  protocolX/ --> (2)
  protocolY/ --> (3)
  ...
protocolX/ --> (2)
protocolY/ --> (3)
...
```
where `protocolX` is PostgreSQL and `protocolY`, for example, **might be** MySql, DuckDB etc.

Expected output for `any/pg` should be overriden by reference PostgreSQL implementation (as for any other future protocol).

Let's see typical usages.

1) Run tests with only SereneDB PostgreSQL-compatible backend: just use `tests/sqllogic/sdb/pg/*/*.test` path pattern.
2) Run tests with both SereneDB PostgreSQL-compatible and reference PostreSQL: just use `tests/sqllogic/any/pg/*/*.test` path pattern.
3) You're adding new features and want to run SereneDB PostgreSQL-compatible on PostgreSQL tests that prevoiusly weren't passed: just use `tests/sqllogic/any/pg/new_feature/*.test` path pattern.
