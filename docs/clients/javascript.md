---
title: JavaScript
sidebar_position: 3
---

# JavaScript

SereneDB works with [pg](https://node-postgres.com/) (node-postgres), the standard PostgreSQL client for Node.js.

## Install

```sh
npm install pg
```

## Connect

```javascript
const { Client } = require('pg');

const client = new Client({
    host: 'localhost',
    port: 7890,
});
await client.connect();
```

## Create a table and insert data

```javascript
await client.query(`
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY,
        title TEXT,
        views INTEGER
    )
`);

await client.query(
    'INSERT INTO articles VALUES ($1, $2, $3)',
    [1, 'Introduction to Vector Search', 4200]
);
```

## Query

```javascript
const res = await client.query(
    'SELECT title, views FROM articles ORDER BY views DESC'
);
for (const row of res.rows) {
    console.log(`${row.title} — ${row.views} views`);
}
```

## Cleanup

```javascript
await client.end();
```
