---
title: R
sidebar_position: 7
---

# R

SereneDB works with [RPostgres](https://rpostgres.r-dbi.org/), an R interface to PostgreSQL using the DBI specification.

## Install

```r
install.packages("RPostgres")
```

## Connect

```r
library(DBI)

con <- dbConnect(RPostgres::Postgres(),
    host = "localhost",
    port = 7890
)
```

## Create a table and insert data

```r
dbExecute(con, "
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY,
        title TEXT,
        views INTEGER
    )
")

dbExecute(con, "INSERT INTO articles VALUES ($1, $2, $3)",
    params = list(1L, "Introduction to Vector Search", 4200L))
```

## Query

Queries return a `data.frame`:

```r
df <- dbGetQuery(con, "SELECT title, views FROM articles ORDER BY views DESC")
print(df)
```

## Bulk loading

For large datasets, use `dbWriteTable` with `copy = TRUE`:

```r
movies <- read.csv("movies.csv")
dbWriteTable(con, "movies", movies, copy = TRUE, overwrite = TRUE)
```

## Cleanup

```r
dbDisconnect(con)
```
