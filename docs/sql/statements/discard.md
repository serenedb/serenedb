---
title: DISCARD
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `DISCARD` statement releases state the current session has built up, so a connection can be handed to the next client as if it were new. Connection poolers run `DISCARD ALL` between clients.

## Targets

| Statement | Effect |
|---|---|
| `DISCARD ALL` | Everything below, plus `DEALLOCATE ALL` and `RESET ALL`: every prepared statement of the session is dropped, and every setting the session changed returns to its default, the session authorization and the role included. |
| `DISCARD TEMP` | Drops the temporary tables, views, sequences, functions and types of the session. `DISCARD TEMPORARY` is the same statement. |
| `DISCARD SEQUENCES` | Forgets the session's sequence state: `currval` fails for every sequence until the next `nextval` on it. |
| `DISCARD PLANS` | Accepted and has no effect: prepared statements stay prepared. |

`DISCARD ALL` cannot run inside a transaction block. A `DISCARD TEMP` inside a transaction is undone by `ROLLBACK`.

## Examples

Drop the temporary objects of the session:

<SqlLogicTest id="sql/statements/discard/example_001" />

Reset the session: the setting returns to its default and the prepared statement is gone:

<SqlLogicTest id="sql/statements/discard/example_002" />

`DISCARD ALL` is rejected inside a transaction block:

<SqlLogicTest id="sql/statements/discard/example_003" />
