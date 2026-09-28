---
title: Python
sidebar_position: 1
split: page
---

# Python

SereneDB works with [psycopg](https://www.psycopg.org/), Python's most popular PostgreSQL adapter. Both psycopg 3 and psycopg2 are supported.

## Install

```sh
pip install "psycopg[binary]"
```

## Connect

```python
import psycopg

conn = psycopg.connect("host=localhost port=7890 dbname=postgres user=postgres")
```

## Create a table and insert data

```python
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY,
            title TEXT,
            views INTEGER
        )
    """)
    cur.execute(
        "INSERT INTO articles VALUES (%s, %s, %s)",
        (1, "Introduction to Vector Search", 4200)
    )
    conn.commit()
```

## Query

```python
with conn.cursor() as cur:
    cur.execute("SELECT title, views FROM articles ORDER BY views DESC")
    for row in cur:
        print(row)
```

## Using psycopg2

psycopg2 is also supported:

```sh
pip install psycopg2-binary
```

```python
import psycopg2

conn = psycopg2.connect("host=localhost port=7890 dbname=postgres user=postgres")

with conn.cursor() as cur:
    cur.execute(
        "INSERT INTO articles VALUES (%s, %s, %s)",
        (1, "Introduction to Vector Search", 4200)
    )
    conn.commit()
```

## Cleanup

```python
conn.close()
```

## Differences from PostgreSQL

- A `conn.transaction()` block inside another one fails, because the inner
  block needs a savepoint and SereneDB has no `SAVEPOINT`. Keep one
  transaction per unit of work.
- Named cursors (`conn.cursor(name=...)`) fail, because `DECLARE ... CURSOR`
  is not supported. Read large results with `cur.stream(query)` in psycopg 3,
  or page through them with `LIMIT` and a key.
- `LISTEN` and `NOTIFY` are not supported.
- psycopg sends a Python list of floats as `DOUBLE[]`. Cast it to the column
  type when you compare it with a vector column:
  `ORDER BY embedding <-> %s::FLOAT[768]`.

## LangChain

For LangChain applications, SereneDB ships a vector store integration that builds on psycopg 3: see [langchain-serenedb](./langchain-serenedb/index.md).
