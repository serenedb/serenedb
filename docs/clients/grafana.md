---
title: Grafana
sidebar_position: 13
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Grafana

[Grafana](https://grafana.com/) can visualize data stored in SereneDB using the built-in PostgreSQL data source.

## Setup

1. Install and start Grafana following the [official guide](https://grafana.com/docs/grafana/latest/setup-grafana/installation/)
2. Navigate to **Connections > Data sources > Add data source**
3. Select **PostgreSQL**
4. Enter the connection details:

| Field | Value |
|---|---|
| Host | `localhost:7890` |
| TLS/SSL Mode | disable |

5. Click **Save & test**

## Create a dashboard

1. Create a new dashboard and add a panel
2. Select your SereneDB data source
3. Write a raw SQL query — for example:

<SqlLogicTest id="clients/grafana/example_001" />

4. Choose a visualization type (Time series, Bar chart, Table, etc.)

:::note
The interactive query builder is currently not supported. Use raw SQL queries.
:::
