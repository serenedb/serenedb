---
title: DBeaver
sidebar_position: 11
---

# DBeaver

[DBeaver Community](https://dbeaver.io/) is a free universal database tool. It connects to SereneDB using the PostgreSQL driver.

## Setup

1. Download and install [DBeaver Community](https://dbeaver.io/download/)
2. Click **Database > New Database Connection**
3. Select **PostgreSQL**
4. Enter the connection details:

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `7890` |
| Username | `postgres` |
| Password | leave empty for a local server; otherwise the [role password](../security/client_authentication.md) |

5. Click **Test Connection** to verify, then **Finish**

## Features

- Browse schemas, tables and columns in the Database Navigator
- View and edit table data in the Data tab
- Write and execute SQL queries in the SQL Editor with auto-complete
- Visualize table relationships with ER diagrams
