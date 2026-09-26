# Demo 7 -- Support tickets triaged as they arrive with `ai_system1`

Keeps a support queue triaged while tickets are written to it. [`ai_system1`](../../docs/sql/functions/ai.md#ai_system1) asks a Jev decision model closed questions and returns probabilities instead of free text: yes or no, one label from a list, or a level on an ordered scale.

The `tickets` table has a `triage` column that is [generated](../../docs/sql/functions/ai.md#ai_system1_on_write) by `ai_system1`:
- every `INSERT` asks three questions about each new ticket and stores the answers with the row;
- an `UPDATE` asks again only when a ticket's text changes;
- queries read the stored answers and send no requests.

The same SQL runs against a local [Kev](https://github.com/jaredpalmer/kev) server, which serves open Jev models, against the hosted [TypeSafe](https://typesafe.ai/) API, or against Jev on [OpenRouter](https://openrouter.ai/typesafe/jev-1.13). Only the secret changes.

## What it shows

- **Answers are created on insert.** The `triage` column is `GENERATED ALWAYS AS (ai_system1(body, questions := {...})) STORED`.
  - The `INSERT` in S1 and S3 sends one request per ticket, and `RETURNING` shows the fresh answers.
  - Those answers are the refund probability (a `noul` question), the team (`choice`) and the urgency on an ordered scale (`score`).
- **Reads are free.** S2 and S7 order and group the queue by the stored answers without sending a request.
- **In-place updates.**
  - In S4 a customer rewrites a ticket, and the `UPDATE` re-triages just that row.
  - In S5, closing tickets changes no column that `triage` reads, so it sends no requests.
- **A new field, filled in place.** S6 adds a `churn_risk` column later and fills it with an `UPDATE`. The `plan = 'enterprise'` filter runs first, so only enterprise tickets reach the model.
- **`NULL` stays `NULL`.** Ticket 13 has no body; its `triage` is `NULL` and costs no request.
- **Three providers, one script.** `bootstrap.sql` creates three secrets, and the `jev_secret` variable picks one:
  - `kev` for the local server;
  - `typesafe` for the hosted TypeSafe API;
  - `openrouter` for Jev on OpenRouter.

## Run

### 1. Start Kev

The test fixture in this repository builds a CPU image of Kev with the `kev-0.8b` model baked in. Build and start it from the repository root:

```bash
docker build -t serenedb-kev tests/sqllogic/fixtures/kev
docker run -d --name kev -e OMP_NUM_THREADS=8 -p 127.0.0.1:8009:8009 serenedb-kev
curl -s http://127.0.0.1:8009/v1/models
```

`OMP_NUM_THREADS=8` limits PyTorch to 8 threads. By default it uses one thread per core, which is far too many for a model this small: on a 128-core server a one-ticket request took 6.5 seconds with the default and 0.9 seconds with 8 threads.

The image is large (about 5.6 GB) because it holds the model and PyTorch. To run Kev without Docker, or with a bigger model on a GPU, follow [Kev's instructions](https://github.com/jaredpalmer/kev#run-it-locally). `bootstrap.sql` expects it at `http://localhost:8009`.

### 2. Create the table and run the demo

```bash
psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql
psql -h <host> -p <port> -U postgres -d postgres -f demo.sql
```

`bootstrap.sql` creates the secrets and an empty `tickets` table; `demo.sql` writes the tickets and queries them. The demo sends 22 requests and runs for about 30 seconds with Kev-0.8B on a CPU. It is tuned for that setup:

- `SET sdb_ai_max_concurrent_requests = 1` sends one request at a time. A CPU server answers requests one after another anyway, and it replies to requests that arrive together only once all of them are done. With the default of 16 in flight, a slow server can run past `sdb_ai_request_timeout`.
- The `triage` column asks its three questions in one request. Kev reads the ticket once for all three, which took half the time of three separate requests.
- In S6, `batch_size := 1` asks about each ticket in its own request. By default `ai_system1` packs up to 32 rows into one request, and the small model then confuses the tickets with each other. Packing 16 tickets into one request was only 1.5 times faster than 16 separate requests.

With a GPU server or the hosted API, raise `sdb_ai_max_concurrent_requests` and drop `batch_size := 1`.

### Use the hosted TypeSafe API instead

Put your TypeSafe API key into `CREATE SECRET typesafe` in `bootstrap.sql`, then pass that secret name to both scripts:

```bash
psql -h <host> -p <port> -U postgres -d postgres -v jev_secret=typesafe -f bootstrap.sql
psql -h <host> -p <port> -U postgres -d postgres -v jev_secret=typesafe -f demo.sql
```

### Use Jev through OpenRouter instead

OpenRouter serves Jev through its Decisions API, not its chat endpoint. That API takes the same requests as TypeSafe's, so the `openrouter` secret in `bootstrap.sql` only changes the URL and the model name:

```sql
CREATE SECRET openrouter (
    TYPE typesafe,
    base_url 'https://openrouter.ai/api',
    path '/alpha/decisions',
    api_key 'OPENROUTER_API_KEY',
    model '~typesafe/jev-latest'
  );
```

Put your OpenRouter API key into it, then pass `-v jev_secret=openrouter` to both scripts. OpenRouter marks the Decisions API as alpha, so its path may change.

### Kev on another machine

If Kev runs on another host, change `base_url` in `CREATE SECRET kev`. If Kev was started with `KEV_API_KEY`, add the same key as `api_key`. An `http://` URL to a host other than localhost is refused unless you run `SET sdb_ai_allow_insecure_endpoint = true` first, because the text is sent unencrypted.

## Why the secret is chosen in `bootstrap.sql`

A generated column's expression is bound when the table is created, and again by every `INSERT` or `UPDATE` that computes it. `bootstrap.sql` therefore writes the secret into the expression with `secret_name := :'jev_secret'`. Every session can then write tickets without setting `sdb_ai_system1_default_secret` first. To switch providers, run `bootstrap.sql` again with another `jev_secret`; it recreates the table.

`demo.sql` also sets `sdb_ai_system1_default_secret` from `jev_secret`, for the `churn_risk` backfill in S6, which calls `ai_system1` directly.

## Why `churn_risk` is a plain column

`ALTER TABLE ... ADD COLUMN` can't add a generated column yet. S6 therefore adds a plain `DOUBLE` column and fills it with `UPDATE ... SET churn_risk = ai_system1(...)`. Unlike `triage`, it isn't refreshed when a ticket's text changes, so run the `UPDATE` again for the rows that need it.
