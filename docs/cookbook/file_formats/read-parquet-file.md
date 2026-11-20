---
title: How to Read a Parquet File with SQL
sidebar_label: Read a Parquet File
sidebar_position: 1
description: Read a Parquet file with plain SQL from psql or any Postgres driver. Check its schema, query it on disk, S3 or HTTPS, and search its text with BM25.
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

A Parquet file is binary and columnar, and it stores its own schema and per-column statistics, so a text editor shows nothing useful: you read it with a program that understands the format. With SereneDB that program is a single `SELECT` from `psql` or any Postgres driver. SereneDB reads the file where it is, on local disk, in S3 or over HTTPS, without an import step.

Tested with SereneDB 26.11.2 on 2026-11-20.

## Read a Parquet file in one query

SereneDB reads a Parquet file when you put its path in the `FROM` clause. The steps below start a server, create a small file, and read it back.

### Start SereneDB with your files mounted

SereneDB reads files on the server side, so a path in your SQL is a path inside the container. Mount the folder that holds your Parquet files and connect with `psql`:

```bash
mkdir -p data && chmod a+rwx data

docker run -d --name serenedb -p 7890:7890 \
  -e SERENEDB_HOST_AUTH_METHOD=trust \
  -v "$PWD/data":/data \
  serenedb/serenedb

psql -h localhost -p 7890 -U postgres
```

A file at `./data/events.parquet` on your machine is then `/data/events.parquet` in SQL. Inside the container SereneDB runs as an unprivileged user: it needs read access to your files, and write access to the folder only for the examples below that write files with `COPY … TO`. To read only, skip the `chmod` and mount the folder with `:ro`. See [Docker](../../installation/docker.md) and [psql](../../clients/psql.md) for the other options.

### Make a sample file

The examples on this page read a small file of support tickets. To follow along without a file of your own, have SereneDB write it into the mounted folder:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/make_sample" />

`COPY … (FORMAT parquet)` is how SereneDB writes Parquet; [Parquet Export](parquet_export.md) covers its options.

### Read the rows

A path that ends in `.parquet` can stand in for a table name:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/read_rows" />

The `read_parquet` function does the same and also reads files with any other extension. Filters, ordering and joins work as they do on a table:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/read_filtered" />

## Look inside the file: columns, types, row groups and statistics

SereneDB shows a Parquet file's schema with `DESCRIBE`, a per-column profile with `SUMMARIZE`, and the file's internal layout with `parquet_metadata`. Together they answer what a Parquet viewer would show you.

Column names and types:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/describe" />

Minimum, maximum and the share of NULLs for every column ([`SUMMARIZE`](../meta/summarize.md) also reports averages, quantiles and distinct counts):

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/summarize" />

Row groups, physical types, compression and the min/max statistics stored in the file:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/metadata" />

Those statistics are why a filtered query on a large Parquet file can be fast. SereneDB reads only the columns a query uses, and it can skip row groups whose min/max range cannot match the filter ([Partial Reading](../../data_import_and_export/parquet/overview.md#partial-reading)). [Parquet Metadata](../../data_import_and_export/parquet/metadata.md) lists every metadata function.

## Read many Parquet files as one table

`read_parquet` accepts a glob pattern or a list of paths and returns their rows as one result. To try it, split the sample file into one folder per product:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/write_partitioned" />

A glob reads every file in the tree. With `hive_partitioning = true`, the `product=…` folder names become a `product` column:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/read_glob_hive" />

`filename = true` adds the path each row came from:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/read_glob_filename" />

When files have different columns, `union_by_name = true` lines them up by name and fills the missing ones with NULL. Here a second file has a `team` column and no `priority`:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/union_by_name" />

The options most often used together with a glob:

| Option | What it does |
|---|---|
| `filename` | Adds a column with the path of the file each row came from. |
| `hive_partitioning` | Turns `key=value` folder names into columns. It is detected automatically when the paths follow that layout. |
| `union_by_name` | Matches columns across files by name instead of by position. |
| `schema` | Sets column names and types explicitly instead of reading them from the files. |

The [Parquet reference](../../data_import_and_export/parquet/overview.md#parameters) lists all parameters, and [Multiple Files](../../data_import_and_export/multiple_files/overview.md) and [Hive Partitioning](../../data_import_and_export/partitioning/hive_partitioning.md) cover globs and partitions in depth.

## Read Parquet from S3 or over HTTPS

SereneDB reads `s3://` and `https://` paths with the same SQL as local files, including globs, filters and the metadata functions above.

For a private bucket, create a secret with your keys and region. `SCOPE` ties the secret to one bucket, so secrets for different buckets do not get in each other's way:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/s3_secret" />

A public bucket needs only its region. This reads one file of Ookla's open data set on the AWS Registry of Open Data:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/s3_public" />

An HTTPS URL needs no setup. This shows the columns of a public data set on Hugging Face:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/https" />

See [S3 Parquet Import](../network_cloud_storage/s3_import.md), [HTTP Parquet Import](../network_cloud_storage/http_import.md) and [AWS Credentials](../../configuration/aws_credentials.md) for endpoints, other S3-compatible stores and the ways to pass credentials.

## Read it from Python or any Postgres client

SereneDB speaks the PostgreSQL wire protocol, so any Postgres driver can read a Parquet file: send the same SQL you would type in `psql`. With [psycopg](../../clients/python.md):

```python
import psycopg

with psycopg.connect("host=localhost port=7890 user=postgres") as conn:
    rows = conn.execute(
        "SELECT id, priority, body FROM read_parquet('/data/tickets.parquet') "
        "WHERE product = %s ORDER BY id",
        ("search",),
    ).fetchall()

for row in rows:
    print(row)
```

```
(1, 'high', 'Search results are empty after the index rebuild')
(3, 'medium', 'Slow search when the query has many words')
(5, 'low', 'Typo on the search settings page')
```

Write the file path into the SQL text, as above, and pass values such as `product` as query parameters. The same query works from JDBC, Go, Node.js and BI tools such as [DBeaver](../../clients/dbeaver.md). If your code already reads Parquet with pandas or PyArrow, it keeps working on the same files.

## Keep it in place or load it

SereneDB can keep querying the file where it is, or copy its rows into a table. A view keeps the data in the file; a table stores a copy in SereneDB.

### Query it as a view

A view gives the file a name. The view stores only the query, so every query on it reads the file as it is at that moment:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/view" />

### Load it into a table

`CREATE TABLE … AS` copies rows into a new table, and `INSERT INTO … SELECT` appends more:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/load_table" />

[Parquet Import](parquet_import.md) shows how to load into an existing table and adjust the schema on the way in. [File Formats](../performance/file_formats.md) explains when loading pays off.

### Convert it to CSV

`COPY … TO` writes the result of any query to CSV. Spreadsheet tools such as Excel open the CSV file directly:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/to_csv" />

## Search the text in a Parquet file with BM25

SereneDB can build an inverted index over a view of Parquet files and rank matches with BM25, without loading the rows into a table. This uses the `tickets` view created above.

Define how text is split into terms:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/dictionary" />

Index the view. `body` gets full-text processing with the dictionary; `id` and `product` are indexed as exact values:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/index" />

Query the index by name and order by relevance. Stemming lets "searches" match "search", and the ticket that also contains "slow" ranks first:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/bm25" />

The index holds a snapshot of the files. After files are added, changed or removed behind the view, refresh it in one pass:

<SqlLogicTest id="cookbook/file_formats/read-parquet-file/reindex" />

[Indexing External Data](../search/indexing-external-data.md) and the [external data reference](../../sql/indexes/inverted/external-data.md) cover phrase queries, filters, automatic refresh and indexes over S3.

## FAQ

### Is a Parquet file human-readable?

No. Parquet stores data in binary, compressed column chunks with a footer that describes the schema and statistics. To look at the rows, read the file with a tool that understands the format, such as a `SELECT` in SereneDB, or convert it to CSV.

### How do I open a Parquet file in Excel?

Convert the file to CSV first, then open the CSV in Excel. In SereneDB that is one statement: `COPY (SELECT * FROM 'file.parquet') TO 'file.csv' (HEADER)`, as shown in [Convert it to CSV](#convert-it-to-csv).

### Can I convert a Parquet file to CSV?

Yes. `COPY (SELECT … FROM 'file.parquet') TO 'file.csv' (HEADER)` writes any query result to CSV, so you can choose the columns and filter the rows during the conversion. [CSV Export](csv_export.md) lists the CSV options.

### Is Parquet faster to read than CSV?

For queries that use some of the columns or filter on a column, usually yes. A Parquet reader fetches only the columns a query needs and can skip row groups by their min/max statistics, while a CSV file has to be parsed in full. [File Formats](../performance/file_formats.md) has more detail.

### Do I need Spark to read a Parquet file?

No. A single SQL query in SereneDB reads a Parquet file on disk, in S3 or over HTTPS, from `psql` or any Postgres driver.

## Related pages

- [Querying Parquet Files](query_parquet.md): the shortest recipe
- [Parquet Import](parquet_import.md): load Parquet into tables
- [Parquet Export](parquet_export.md): write query results as Parquet
- [Parquet reference](../../data_import_and_export/parquet/overview.md): every `read_parquet` and `COPY` option
- [Parquet Metadata](../../data_import_and_export/parquet/metadata.md)
- [Multiple Files](../../data_import_and_export/multiple_files/overview.md)
- [S3 Parquet Import](../network_cloud_storage/s3_import.md)
- [Indexing External Data](../search/indexing-external-data.md)
