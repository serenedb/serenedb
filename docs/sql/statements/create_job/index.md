---
title: CREATE JOB
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

A **job** is a catalog object that runs one SQL statement on a schedule, inside the server. It lives in a schema next to tables and views, is persisted with the database, and keeps running across restarts until it is suspended or dropped.

## Examples

Roll up yesterday's events every day at 02:00 UTC, and run it once right away:

<SqlLogicTest id="sql/statements/create_job/index/example_001" />

## Syntax

```sql
CREATE [ OR REPLACE ] JOB [ IF NOT EXISTS ] name
    { EVERY interval [ OFFSET interval ] [ CONCURRENT ] | AFTER interval }
    [ SUSPENDED ]
    AS statement

ALTER JOB [ IF EXISTS ] name SUSPEND
ALTER JOB [ IF EXISTS ] name RESUME
ALTER JOB [ IF EXISTS ] name SET SCHEDULE { EVERY interval [ OFFSET interval ] [ CONCURRENT ] | AFTER interval }
ALTER JOB [ IF EXISTS ] name RENAME TO new_name
ALTER JOB name OWNER TO role

EXECUTE JOB name

DROP JOB [ IF EXISTS ] name [ CASCADE | RESTRICT ]
```

`interval` is an interval literal (`INTERVAL '15 minutes'`, `INTERVAL (2 * 3) MINUTE`), the short form `<number> <unit>` (`30 SECONDS`, `1 HOUR`, `1 MONTH`), or any constant expression in parentheses (`(INTERVAL '1 hour' * 2)`); it is evaluated once, when the statement runs. The body is any single statement except a transaction statement; it cannot use parameters. It is parsed when the job is created, stored as normalized SQL text (the `body` column of `duckdb_jobs()`), and bound again on every run, so it always sees the current definition of the objects it references.

`EXECUTE JOB name` is shorthand for `CALL execute_job('name')`.

## Schedules

The two schedule kinds follow ClickHouse's refreshable materialized views.

- **`EVERY interval [OFFSET offset]`** runs on a fixed calendar grid in UTC, the same grid `time_bucket` uses: `EVERY 1 HOUR` runs at the top of every hour, `EVERY 15 MINUTES` at :00, :15, :30 and :45, `EVERY 1 DAY` at midnight, `EVERY 1 WEEK` on Monday midnight, `EVERY 1 MONTH` on the first of the month. `OFFSET` shifts the grid: `EVERY 1 DAY OFFSET 2 HOURS` runs at 02:00. The offset must be shorter than the interval, and a month-based interval cannot be combined with days or time.
- **`AFTER interval`** runs `interval` after the previous run *finished*, so runs never overlap and a slow run pushes the next one back. The first run happens `interval` after the job is created or the server starts.

<SqlLogicTest id="sql/statements/create_job/index/example_002" />

A job never runs twice at the same time unless it is `CONCURRENT`. If an `EVERY` run is still going when the next grid point arrives, that tick is skipped and the job runs again at the first grid point after the run finishes. Ticks that fall while the server is down are not replayed.

`CONCURRENT` lets the runs of an `EVERY` job overlap: every grid point starts a run, even while earlier runs are still going, and `EXECUTE JOB` runs alongside them. Use it for bodies that are independent of each other; every run takes a session and a background thread of its own, so a body slower than its interval keeps several of them busy. `AFTER` jobs cannot be `CONCURRENT`, since each run is counted from the end of the previous one. `ALTER JOB ... SET SCHEDULE` turns it on or off with the rest of the schedule.

<SqlLogicTest id="sql/statements/create_job/index/example_008" />

Scheduled runs execute on the server's background thread pool (`--background_threads`), the pool that also runs index maintenance, so a long job does not hold a client session.

`SUSPENDED` creates the job paused.

## Changing a job

`ALTER JOB` suspends and resumes a job, replaces its schedule, or renames it. A suspended job keeps its definition and history but is not scheduled; a run already in progress finishes normally. `CREATE OR REPLACE JOB` replaces the whole definition, including the body.

<SqlLogicTest id="sql/statements/create_job/index/example_003" />

## Running a job on demand

`EXECUTE JOB` runs the body once, synchronously, and reports the body's error if it fails. The body runs in the job's own session and transaction, exactly as a scheduled run would, so its effects commit independently of the calling transaction. It works for suspended jobs too and does not change the schedule of an `EVERY` job; for an `AFTER` job the next run is counted from the end of this one. It fails if the job is already running, unless the job is `CONCURRENT`.

A body can run another job with `EXECUTE JOB`: that job runs in a session of its own while the calling run waits for it. Such calls nest at most 16 levels deep: a `CONCURRENT` job that executes itself, directly or through other jobs, fails with `EXECUTE JOB is nested more than 16 levels deep`, and every run in the chain is recorded as failed.

## Monitoring

`duckdb_jobs()` lists the jobs visible to the session with their schedule, their `body` and full `CREATE JOB` statement (`sql`), and their run state: `running`, `next_run`, `last_start`, `last_finish`, `last_status` (`success` or `failed`), `last_error`, `run_count` and `failure_count`. `duckdb_job_runs()` returns the most recent runs of all jobs (up to [`sdb_job_history_size`](../../../configuration/overview.md#logging-metrics-and-profiling), 1024 by default), with `trigger` set to `schedule` or `manual`. Run state is kept in memory and starts over when the server restarts.

<SqlLogicTest id="sql/statements/create_job/index/example_004" />

## Security

A job runs **as its owner**, the role that created it, in a session of its own: the body is checked against the owner's privileges, exactly as if the owner had typed it. Creating a job needs the `CREATE` privilege on the schema. Only the owner (or a member of the owning role, or a superuser) can alter, drop or execute the job; `ALTER JOB ... OWNER TO` hands it to another role, which from then on is the identity the job runs as.

<SqlLogicTest id="sql/statements/create_job/index/example_005" />

The job session starts with the job's schema first on the search path and reads **global** settings; session settings of the role that created the job do not apply.

## Jobs in other databases

A job can live in any SereneDB database, not only the current one; wherever it lives it runs as its owner, with its own schema first on the search path:

<SqlLogicTest id="sql/statements/create_job/index/example_006" />

Jobs exist only in SereneDB databases, like text search dictionaries and inverted indexes: `CREATE TEMPORARY JOB`, and jobs in `memory` or in an attached DuckDB file, are rejected with `Jobs are not supported by this catalog`.

## Dropping a job

`DROP JOB` removes the job. A run that is in progress is interrupted.

<SqlLogicTest id="sql/statements/create_job/index/example_007" />

## Jobs created by the server

The `reindex_interval` option of an inverted index over a view is implemented as a job named after the index; see [Automatic refresh](../../indexes/inverted/views.md#automatic-refresh).
