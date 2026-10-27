---
title: CREATE JOB
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

A **job** is a catalog object that runs SQL statements on a schedule, inside the server. It lives in a schema next to tables and views, is persisted with the database, and keeps running across restarts until it is suspended or dropped.

## Examples

Roll up yesterday's events every day at 02:00 UTC, and run it once right away:

<SqlLogicTest id="sql/statements/create_job/index/example_001" />

## Syntax

```sql
CREATE [ OR REPLACE ] JOB [ IF NOT EXISTS ] name
    { EVERY interval [ OFFSET interval ] [ RANDOMIZE FOR interval ] [ CONCURRENT ]
    | AFTER interval [ RANDOMIZE FOR interval ] }
    [ SUSPENDED ]
    AS { statement | 'statements' }

ALTER JOB [ IF EXISTS ] name SUSPEND
ALTER JOB [ IF EXISTS ] name RESUME
ALTER JOB [ IF EXISTS ] name SET SCHEDULE
    { EVERY interval [ OFFSET interval ] [ RANDOMIZE FOR interval ] [ CONCURRENT ]
    | AFTER interval [ RANDOMIZE FOR interval ] }
ALTER JOB [ IF EXISTS ] name RENAME TO new_name
ALTER JOB name OWNER TO role

EXECUTE JOB name

DROP JOB [ IF EXISTS ] name [ CASCADE | RESTRICT ]
```

`interval` is an interval literal (`INTERVAL '15 minutes'`, `INTERVAL (2 * 3) MINUTE`), the short form `<number> <unit>`, repeatable (`30 SECONDS`, `1 HOUR`, `1 MONTH`, `1 WEEK 2 DAYS`, `5 DAYS 2 HOURS`), or any constant expression in parentheses (`(INTERVAL '1 hour' * 2)`); it is evaluated once, when the statement runs. The body is one statement, or a string constant, usually dollar-quoted (`AS $$ ... $$`), that holds any number of statements. It is parsed when the job is created, so a syntax error fails `CREATE JOB`, and it cannot use parameters. One statement is stored as normalized SQL text, the statements of a string as written: the `body` column of `sdb_jobs()` shows them. A transaction statement cannot be the whole body, and neither can `EXPLAIN` of a transaction statement or of a statement that runs as several statements. A statement that runs as several statements, such as `TRUNCATE` of several tables or with `RESTART IDENTITY` or `CASCADE`, `ALTER TABLE ... ADD COLUMN` with `NOT NULL`, `UNIQUE`, `PRIMARY KEY` or a default that is not a constant, or `DROP ROLE` with several roles, has to be in a string: unquoted, `CREATE JOB` fails with `CREATE JOB body cannot be a statement that runs as several statements, unless it is quoted: AS $$ ... $$`. A `PIVOT` that takes its values from the data has to be in a string too, or list its values with `IN (...)`: unquoted, it fails `CREATE JOB`, as in a view.

A run executes the body the way a client's query of several statements runs: the statements run in order, each commits on its own unless the body groups them with `BEGIN` and `COMMIT`, and the first one that fails ends the run, leaving the statements before it committed. The body is bound again on every run, so it always sees the current definition of the objects it references, and a missing table, column or privilege fails the run rather than `CREATE JOB`. When a statement of the body drops its own job, the run ends there and counts as a success: the statements after it do not run. Dropping or replacing the job from another session cancels the statement that is running, and the run fails without the statements after it. A job can that way run once and remove itself:

<SqlLogicTest id="sql/statements/create_job/index/example_009" />

`EXECUTE JOB name` is shorthand for `CALL execute_job('name')`.

## Schedules

- **`EVERY interval [OFFSET offset]`** runs on a fixed calendar grid in UTC. An interval of days, weeks or time counts from Monday 1969-12-29 at midnight and a month-based interval from January 1970: `EVERY 1 HOUR` runs at the top of every hour, `EVERY 15 MINUTES` at :00, :15, :30 and :45, `EVERY 1 DAY` at midnight, `EVERY 1 WEEK` on Monday midnight, `EVERY 1 MONTH` on the first of the month. `OFFSET` shifts the grid: `EVERY 1 DAY OFFSET 2 HOURS` runs at 02:00. The offset must be shorter than the interval, and a month-based interval cannot be combined with days or time.
- **`AFTER interval`** runs `interval` after the previous run *finished*, so runs never overlap and a slow run pushes the next one back. The first run happens `interval` after the job is created or the server starts. `AFTER` takes no `OFFSET`.

```sql
EVERY 1 DAY                                -- every day at midnight
EVERY 1 MONTH                              -- on the 1st of every month, at midnight
EVERY 1 MONTH OFFSET 5 DAYS 2 HOURS        -- on the 6th of every month, at 02:00
EVERY 2 WEEKS OFFSET 5 DAYS 15 HOURS 10 MINUTES  -- every other Saturday, at 15:10
EVERY 30 MINUTES                           -- at 00:00, 00:30, 01:00, 01:30, ...
AFTER 30 MINUTES                           -- 30 minutes after the previous run finished
EVERY 1 WEEK 2 DAYS                        -- every 9 days: when the day number since 1969-12-29 is divisible by 9
EVERY 5 MONTHS                             -- when the month number since 1970-01 is divisible by 5
EVERY 1 DAY OFFSET 2 HOURS RANDOMIZE FOR 1 HOUR  -- every day at a random time between 01:30 and 02:30
```

`RANDOMIZE FOR spread` moves each run by a pseudo-random amount within `spread`, centered on the scheduled time, so that many jobs on the same grid do not all start at once. The amount is fixed per job and per scheduled time, so `next_run` stays the same however often it is computed, and a manual `EXECUTE JOB` does not move it. The spread must be shorter than the interval.

<SqlLogicTest id="sql/statements/create_job/index/example_002" />

A job never runs twice at the same time unless it is `CONCURRENT`. If an `EVERY` run is still going when the next grid point arrives, that tick is skipped and the job runs again at the first grid point after the run finishes. Ticks that fall while the server is down are not replayed.

`CONCURRENT` lets the runs of an `EVERY` job overlap: every grid point starts a run, even while earlier runs are still going, and `EXECUTE JOB` runs alongside them. Use it for bodies that are independent of each other; every run takes a session and a background thread of its own, so a body slower than its interval keeps several of them busy. `AFTER` jobs cannot be `CONCURRENT`, since each run is counted from the end of the previous one. `ALTER JOB ... SET SCHEDULE` turns it on or off with the rest of the schedule.

<SqlLogicTest id="sql/statements/create_job/index/example_008" />

Scheduled runs execute in the background, like index maintenance, so a long job does not hold a client session.

`SUSPENDED` creates the job paused.

## Changing a job

`ALTER JOB` suspends and resumes a job, replaces its schedule, or renames it. A suspended job keeps its definition and history but is not scheduled; a run already in progress finishes normally. `CREATE OR REPLACE JOB` replaces the whole definition, including the body, and starts the job afresh: a run in progress is interrupted, and `run_count`, `failure_count` and the `last_*` columns of `sdb_jobs()` start over, where `ALTER JOB` keeps them.

<SqlLogicTest id="sql/statements/create_job/index/example_003" />

## Running a job on demand

`EXECUTE JOB` runs the body once, synchronously, and reports the body's error if it fails. The body runs in the job's own session and transaction, exactly as a scheduled run would, so its effects commit independently of the calling transaction. It works for suspended jobs too and does not change the schedule of an `EVERY` job; for an `AFTER` job the next run is counted from the end of this one. It fails if the job is already running, unless the job is `CONCURRENT`.

A body can run another job with `EXECUTE JOB`: that job runs in a session of its own while the calling run waits for it. A chain of such calls holds at most [`sdb_job_max_depth`](../../../configuration/overview.md#query-execution-and-the-optimizer) runs, 16 by default: a `CONCURRENT` job that executes itself, directly or through other jobs, fails with `Max job depth limit of 16 exceeded`, and every run in the chain is recorded as failed.

## Monitoring

`sdb_jobs()` lists the jobs visible to the session with their schedule (`schedule`, `schedule_kind`, `schedule_interval`, `schedule_offset`, `schedule_randomize`), their `body` and full `CREATE JOB` statement (`sql`), and their run state: `running`, `next_run`, `last_start`, `last_finish`, `last_status` (`success` or `failed`), `last_error`, `run_count` and `failure_count`. `sdb_job_runs()` returns the most recent runs, up to [`sdb_job_history_size`](../../../configuration/overview.md#logging-metrics-and-profiling) of them for all jobs together, 1024 by default, with `trigger` set to `schedule`, or to `manual` for `EXECUTE JOB`, also when another job's body runs it. [Metadata Functions](../../functions/metadata.md#sdb_jobs) lists the columns of both. Run state is kept in memory only: a restart empties `sdb_job_runs()`, resets `running`, the `last_*` columns, `run_count` and `failure_count`, and computes `next_run` again from the schedule. Notices and warnings that a run raises, scheduled or started with `EXECUTE JOB`, go to the server log as warnings prefixed with the job's name.

<SqlLogicTest id="sql/statements/create_job/index/example_004" />

## Security

A job runs **as its owner**, the role that created it, in a session of its own: the body is checked against the owner's privileges, exactly as if the owner had typed it. Creating a job needs the `CREATE` privilege on the schema. Only the owner (or a member of the owning role, or a superuser) can alter, drop or execute the job; `ALTER JOB ... OWNER TO` hands it to another role, which from then on is the identity the job runs as.

<SqlLogicTest id="sql/statements/create_job/index/example_005" />

The job session's search path holds the job's schema alone, without `"$user"` and `public`, and the session reads **global** settings; session settings of the role that created the job do not apply. Built-in functions and the system catalogs stay reachable, but a job outside `public` must qualify the tables of other schemas, `public` included. A body that names a table it cannot find still creates the job, and fails when it runs:

<SqlLogicTest id="sql/statements/create_job/index/example_010" />

A search path holds only top-level schemas, so a job in a nested schema (`CREATE JOB outer.inner.job ...`) runs with the database's default search path (`"$user", public`), as a new session in that database would, instead of its own schema. Qualify the names in the body of such a job (`INSERT INTO outer.inner.events ...`).

## Jobs in other databases

A job can live in any SereneDB database, not only the current one; wherever it lives it runs as its owner, in its own database, with its own schema as the search path:

<SqlLogicTest id="sql/statements/create_job/index/example_006" />

Jobs exist only in SereneDB databases, like text search dictionaries and inverted indexes: `CREATE TEMPORARY JOB`, and jobs in `memory` or in an attached DuckDB file, are rejected with `Jobs are not supported by this catalog`.

## Dropping a job

`DROP JOB` removes the job. A run that is in progress is interrupted.

<SqlLogicTest id="sql/statements/create_job/index/example_007" />

## Jobs created by the server

The `reindex_interval` option of an inverted index over a view is implemented as a job named after the index; see [Automatic refresh](../../indexes/inverted/views.md#automatic-refresh).
