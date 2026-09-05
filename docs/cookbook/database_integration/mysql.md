---
draft: true
layout: docu
redirect_from:
- /docs/guides/database_integration/mysql
- /docs/guides/import/query_mysql
- /docs/preview/guides/database_integration/mysql
- /docs/stable/guides/database_integration/mysql
title: MySQL Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Usage

You can attach to a MySQL database using the following command:

<SqlLogicTest id="cookbook/database_integration/mysql/example_003" />

The string used by `ATTACH` is a PostgreSQL-style connection string (_not_ a MySQL connection string!). It is a list of connection arguments provided in `{key}={value}` format. Below is a list of valid arguments. Any options not provided are replaced by their default values.

|  Setting   |   Default    |
|------------|--------------|
| `database` | `NULL`       |
| `host`     | `localhost`  |
| `password` |              |
| `port`     | `0`          |
| `socket`   | `NULL`       |
| `user`     | current user |

You can directly read and write the MySQL database:

<SqlLogicTest id="cookbook/database_integration/mysql/read_write_table/example_004" />
