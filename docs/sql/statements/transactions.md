---
title: Transaction Management
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB supports [ACID database transactions](https://en.wikipedia.org/wiki/Database_transaction).
Transactions provide isolation, i.e., changes made by a transaction are not visible from concurrent transactions until it is committed.
A transaction can also be aborted, which discards any changes it made so far.

## Statements

SereneDB provides the following statements for transaction management.

### Starting a Transaction

To start a transaction, run:

<SqlLogicTest id="sql/statements/transactions/example_001" />

### Committing a Transaction

You can commit a transaction to make it visible to other transactions and to write it to persistent storage (if using SereneDB in persistent mode).
To commit a transaction, run:

<SqlLogicTest id="sql/statements/transactions/example_002" />

If you are not in an active transaction, `COMMIT` does nothing and warns `there is no transaction in progress`, as in PostgreSQL.

### Rolling Back a Transaction

You can abort a transaction.
This operation, also known as rolling back, will discard any changes the transaction made to the database.
To abort a transaction, run:

<SqlLogicTest id="sql/statements/transactions/example_003" />

You can also use the abort command, which has an identical behavior:

<SqlLogicTest id="sql/statements/transactions/example_004" />

If you are not in an active transaction, `ROLLBACK` and `ABORT` do nothing and warn `there is no transaction in progress`.

A statement that fails inside a transaction aborts the whole transaction: every later statement answers `current transaction is aborted, commands ignored until end of transaction block` until `ROLLBACK` (a `COMMIT` then rolls back as well). There are no savepoints, so no part of the transaction can be kept.

## Multi-Statement Transactions

When multiple SQL statements are submitted together (e.g., separated by semicolons), they are executed within a single implicit transaction. If any statement fails, all preceding statements in the batch are rolled back. This also applies to `PRAGMA` commands that decompose into multiple internal operations, such as `COPY FROM DATABASE`.

## Isolation Level

SereneDB's concurrency model guarantees snapshot isolation. Transactions that violate this isolation level are aborted.

Two of [PostgreSQL's transaction isolation levels](https://www.postgresql.org/docs/current/transaction-iso.html) exist:

-   `REPEATABLE READ`, the default: the transaction reads one snapshot from its first statement to its end.
-   `READ COMMITTED`: each statement sees the data committed before it started, until the transaction writes. From its first `INSERT`, `UPDATE` or `DELETE` on, the transaction keeps one snapshot to its end, so its own uncommitted rows stay consistent.

Pick one with `BEGIN ISOLATION LEVEL READ COMMITTED`. `SERIALIZABLE` is refused with `transaction isolation level "serializable" is not supported`.

## Example

We illustrate the use of transactions through a simple example.

<SqlLogicTest id="sql/statements/transactions/example_005" />

The first transaction (inserting “Ada”) was committed but the second (deleting “Ada” and inserting “Bruce”) was aborted.
Therefore, the resulting table will only contain `<'Ada', 52>`.
