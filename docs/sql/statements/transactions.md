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

The whole batch is parsed before its first statement runs, so a syntax error anywhere in it runs none of them. Each statement is then analyzed only after the previous ones ran: `CREATE TABLE t (v INTEGER); ALTER TABLE t ADD COLUMN id SERIAL;` adds the serial column to the table the first statement created.

## Transactions Across Databases

A transaction can write to several databases of the same server, including DDL in each of them and cluster-wide objects such as roles and databases (`CREATE ROLE`, `CREATE DATABASE`, `GRANT`). The commit is atomic across all of them: after a crash, either every database has the transaction's changes or none has.

A database attached with `ATTACH` keeps its own file and commits on its own, so a transaction that writes to it cannot write to any other database.

## Isolation Level

SereneDB's concurrency model guarantees snapshot isolation. Transactions that violate this isolation level are aborted.

Two of [PostgreSQL's transaction isolation levels](https://www.postgresql.org/docs/current/transaction-iso.html) exist:

-   `REPEATABLE READ`, the default: the transaction reads one snapshot, taken by its first statement that reads or writes a table, to its end.
-   `READ COMMITTED`: each statement sees the data committed before it started, and the transaction's own changes. From the transaction's first DDL statement on, it keeps one snapshot to its end.

Pick one with `BEGIN ISOLATION LEVEL READ COMMITTED`. `SERIALIZABLE` is refused with `transaction isolation level "serializable" is not supported`.

Two transactions that update or delete the same row conflict: the first to commit wins, and the other fails with `could not serialize access due to concurrent update`, at the latest at its `COMMIT`. Under `READ COMMITTED`, a row conflicts when another transaction committed a change to it after the statement that updated or deleted it started, even if a later statement of the transaction saw that change; PostgreSQL would wait for the other writer and re-check the row instead.

An `INSERT ... ON CONFLICT`, an `INSERT OR REPLACE` or a `MERGE` that inserts a key another transaction committed after the statement's snapshot fails the same way, at the statement or at its `COMMIT`, so a retry sees the row and takes the conflict path. A plain `INSERT` of a key another transaction committed fails with the duplicate key error, which a retry would hit again.

`ALTER TABLE` and `DROP TABLE` wait for the commits that are already writing the table to finish, then change it. A transaction that wrote to the table but commits only after the change fails at `COMMIT` with a serialization failure; PostgreSQL would have made the `ALTER TABLE` wait for that transaction instead.

A transaction that creates, replaces or drops an index of a table, or alters or drops the table, holds the table until it commits or rolls back. Such a change from another transaction meanwhile fails at once with `table "<name>" is being changed by another transaction`; retry it once the first transaction ends (see [Concurrent changes](create_index/inverted.md#concurrent-changes)). `COMMENT ON COLUMN` does not hold the table.

## Example

We illustrate the use of transactions through a simple example.

<SqlLogicTest id="sql/statements/transactions/example_005" />

The first transaction (inserting “Ada”) was committed but the second (deleting “Ada” and inserting “Bruce”) was aborted.
Therefore, the resulting table will only contain `<'Ada', 52>`.
