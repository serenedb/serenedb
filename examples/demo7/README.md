# Demo 7 -- Support tickets triaged as they arrive with `ai_system_one`

Keeps a support queue triaged while tickets are written to it. [`ai_system_one`](../../docs/sql/functions/ai.md#ai_system_one) asks a Jev decision model closed questions and returns probabilities instead of free text: yes or no, one label from a list, or a level on an ordered scale.

The `tickets` table has a `triage` column that is [generated](../../docs/sql/functions/ai.md#ai_system_one_on_write) by `ai_system_one`:
- every `INSERT` asks three questions about each new ticket and stores the answers with the row;
- an `UPDATE` asks again only when a ticket's text changes;
- queries read the stored answers and send no requests.

The demo asks Jev on [OpenRouter](https://openrouter.ai/typesafe/jev-1.13), so all you need is an OpenRouter API key.

## What it shows

- **Answers are created on insert.** The `triage` column is `GENERATED ALWAYS AS (ai_system_one(body, questions := {...})) STORED`.
  - The `INSERT` in S1 and S3 sends one request per ticket, and `RETURNING` shows the fresh answers.
  - Those answers are the refund probability (a `noul` question), the team (`choice`) and the urgency on an ordered scale (`score`).
- **Reads are free.** S2 and S7 order and group the queue by the stored answers without sending a request.
- **In-place updates.**
  - In S4 a customer rewrites a ticket, and the `UPDATE` re-triages just that row.
  - In S5, closing tickets changes no column that `triage` reads, so it sends no requests.
- **A new field, filled in place.** S6 adds a `churn_risk` column later and fills it with an `UPDATE`. The `plan = 'enterprise'` filter runs first, so only enterprise tickets reach the model.
- **`NULL` stays `NULL`.** Ticket 13 has no body; its `triage` is `NULL` and costs no request.

## Run

```bash
psql -h <host> -p <port> -U postgres -d postgres -v openrouter_api_key="$OPENROUTER_API_KEY" -f bootstrap.sql
psql -h <host> -p <port> -U postgres -d postgres -f demo.sql
```

`bootstrap.sql` creates the `openrouter` secret from the key you pass and an empty `tickets` table; `demo.sql` writes the tickets and queries them. The key goes in on the command line, so it never has to be written into the script.

- The `triage` column asks its three questions in one request, so the model reads each ticket once for all three.
- In S6, `ai_system_one` packs the enterprise tickets into one request.

## The OpenRouter secret

OpenRouter serves Jev through its Decisions API, not its chat endpoint. That API takes the same requests as the TypeSafe System One API, so the secret has `TYPE typesafe` and only changes the URL and the model name:

```sql
CREATE SECRET openrouter (
    TYPE typesafe,
    base_url 'https://openrouter.ai/api',
    path '/alpha/decisions',
    api_key :'openrouter_api_key',
    model '~typesafe/jev-latest',
);
```

OpenRouter marks the Decisions API as alpha, so its path may change.

## Why the secret is named in the table

A generated column's expression is bound when the table is created, and again by every `INSERT` or `UPDATE` that computes it. `bootstrap.sql` therefore writes the secret into the expression with `secret_name := 'openrouter'`. Every session can then write tickets without setting `sdb_ai_system_one_default_secret` first.

`demo.sql` also sets `sdb_ai_system_one_default_secret = 'openrouter'`, for the `churn_risk` backfill in S6, which calls `ai_system_one` directly.

## Why `churn_risk` is a plain column

`ALTER TABLE ... ADD COLUMN` can't add a generated column yet. S6 therefore adds a plain `DOUBLE` column and fills it with `UPDATE ... SET churn_risk = ai_system_one(...)`. Unlike `triage`, it isn't refreshed when a ticket's text changes, so run the `UPDATE` again for the rows that need it.
