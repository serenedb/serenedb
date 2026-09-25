# Demo 7 -- Support-ticket triage with `prompt_jev`

Triages a small support queue with [`prompt_jev`](../../docs/sql/functions/ai.md). Jev models answer closed questions with probabilities instead of free text: yes or no, one label from a list, or a level on an ordered scale. The same SQL runs against a local [Kev](https://github.com/jaredpalmer/kev) server, which serves open Jev models, against the hosted [TypeSafe](https://typesafe.ai/) API, or against Jev on [OpenRouter](https://openrouter.ai/typesafe/jev-1.13). Only the secret changes.

## What it shows

- **Three question types over a table column.** `noul` returns the probability of *yes* as a `DOUBLE` (Q1, Q4). `choice` returns the chosen label with its probabilities and a confidence (Q2). `score` returns the expected level on a scale with the probability of each level (Q3).
- **Cheap filters run first.** In Q3 and Q4 the `plan = 'enterprise'` condition is checked before any request is sent, so tickets on other plans never reach the model, even when `prompt_jev` itself sits in the `WHERE` clause (Q4).
- **Ask once, query many times.** Q5 asks three questions per ticket in one request with `questions := {...}` and stores the answers in `ticket_triage`. Q6 to Q8 are plain SQL over that table and send no requests.
- **`NULL` stays `NULL`.** Ticket 13 has no body; it gets `NULL` answers and costs no request.
- **Three providers, one script.** `bootstrap.sql` creates a `kev` secret for the local server, a `typesafe` secret for the hosted TypeSafe API and an `openrouter` secret for Jev on OpenRouter. `demo.sql` picks one through `sdb_ai_jev_default_secret`.

## Run

### 1. Start Kev

The test fixture in this repository builds a CPU image of Kev with the `kev-0.8b` model baked in. Build and start it from the repository root:

```bash
docker build -t serenedb-kev tests/sqllogic/fixtures/kev
docker run -d --name kev -e OMP_NUM_THREADS=8 -p 127.0.0.1:8009:8009 serenedb-kev
curl -s http://127.0.0.1:8009/v1/models
```

`OMP_NUM_THREADS=8` limits PyTorch to 8 threads. By default it uses one thread per core, which is far too many for a model this small: on a 128-core server a one-ticket request took 6.5 seconds with the default and 0.9 seconds with 8 threads.

The image is large (about 5.6 GB) because it holds the model and PyTorch. To run Kev without Docker, or with a bigger model on a GPU, follow [Kev's instructions](https://github.com/jaredpalmer/kev#run-it-locally); `bootstrap.sql` expects it at `http://localhost:8009`.

### 2. Load the tickets and run the demo

```bash
psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql
psql -h <host> -p <port> -U postgres -d postgres -f demo.sql
```

The demo is tuned for Kev-0.8B on a CPU:

- `SET sdb_ai_max_concurrent_requests = 1` sends one request at a time. A CPU server answers requests one after another anyway, and it replies to requests that arrive together only once all of them are done, so with the default of 16 a slow server can run past `sdb_ai_request_timeout`.
- `batch_size := 1` asks about each ticket in its own request. By default `prompt_jev` packs up to 32 rows into one request, and the small model then confuses the tickets with each other. Packing 16 tickets into one request was only 1.5 times faster than 16 separate requests.
- Q5 asks three questions per request. Kev reads the ticket once for all three, so this took half the time of three separate queries.

The demo sends about 60 requests. With 8 threads on a 128-core ARM server it runs for about a minute. With a GPU server or the hosted API, raise `sdb_ai_max_concurrent_requests` and drop `batch_size := 1`.

### Use the hosted TypeSafe API instead

Put your TypeSafe API key into `CREATE SECRET typesafe` in `bootstrap.sql`, run it again, then pick that secret when you run the demo:

```bash
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

Put your OpenRouter API key into it, run `bootstrap.sql` again, then pick that secret:

```bash
psql -h <host> -p <port> -U postgres -d postgres -v jev_secret=openrouter -f demo.sql
```

OpenRouter marks the Decisions API as alpha, so its path may change.

### Kev on another machine

If Kev runs on another host, change `base_url` in `CREATE SECRET kev`. If Kev was started with `KEV_API_KEY`, add the same key as `api_key`. An `http://` URL to a host other than localhost is refused unless you run `SET sdb_ai_allow_insecure_endpoint = true` first, because the text is sent unencrypted.
