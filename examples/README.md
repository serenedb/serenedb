# SereneDB Examples

End-to-end demos that show what SereneDB does, against real data, in scripts you can run in under a minute.

## Demos

| | What it shows | Dataset |
|---|---|---|
| [demo0](demo0/) | Zero ETL search over **remote** Parquet files | IMDb reviews on Hugging Face (100k rows, 3 shards) |
| [demo1](demo1/) | Same demo over **local** Parquet -- same SQL, two orders of magnitude faster | IMDb reviews staged in `/tmp` |
| [demo2](demo2/) | Same demo over a **native table** -- production shape, durable, sub-millisecond queries | IMDb reviews ingested into a SereneDB table |
| [demo3](demo3/) | **Elastic-grade FTS tour** -- phrase + slop, boolean, prefix / wildcard / regex / fuzzy, ngram, four scoring models, byte-range highlights, plus a complex composite query and a `##`-chained mixed-part phrase | IMDb reviews on Hugging Face (~100k reviews, two analyzers in one index) |

## Running a demo

```bash
psql -h <host> -p <port> -U serenedb -d postgres -f <demo>/demo.sql
```

>
> **One-time bootstrap.** Some demos ship a `bootstrap.sql` that stages the data locally (downloads parquet shards, or ingests them into a native table). Run it once before `demo.sql`:
>
> ```bash
> psql -h <host> -p <port> -U postgres -d postgres -f <demo>/bootstrap.sql
> ```
>

If you don't have a SereneDB server running yet, check the [downloads page](https://serenedb.com/download).
