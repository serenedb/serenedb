---
title: DataGrip
sidebar_position: 12
---

# DataGrip

[DataGrip](https://www.jetbrains.com/datagrip/) is a JetBrains IDE for databases. It connects to SereneDB using the PostgreSQL driver.

## Setup

1. Download and install [DataGrip](https://www.jetbrains.com/datagrip/download/)
2. Click the **+** icon or press **Cmd/Ctrl+N** to add a new Data Source
3. Select **PostgreSQL**
4. Enter the connection details:

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `7890` |
| User | `postgres` |
| Password | leave empty for a local server; otherwise use **User & Password** with the [role password](../security/client_authentication.md) |

5. Click **Test Connection** to verify, then **OK**

6. In the Schemas tab, check **All schemas** to see the full database structure

## Features

- Explore tables and schemas in the Database Explorer
- View and modify table data directly
- Write and run SQL in the Database Console with auto-complete and refactoring
