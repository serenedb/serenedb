---
title: Python
sidebar_position: 1
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

conn = psycopg.connect("host=localhost port=7890 dbname=_system")
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

conn = psycopg2.connect("host=localhost port=7890 dbname=_system")

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
