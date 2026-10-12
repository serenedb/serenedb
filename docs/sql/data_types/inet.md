---
title: Inet
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

| Name   | Aliases | Description                                         |
| :----- | :------ | :-------------------------------------------------- |
| `INET` |         | IPv4 or IPv6 host address with an optional netmask |

An `INET` value holds an address and a netmask length. Without a netmask the value is a single host: `/32` for IPv4, `/128` for IPv6. With a netmask it can describe a network, and it may keep host bits beyond the netmask, as in `192.168.1.5/24`.

<SqlLogicTest id="sql/data_types/inet/example_001" />

## Ordering

`INET` values sort the way PostgreSQL sorts them: IPv4 before IPv6, then by the network part (the address with the bits beyond the netmask cleared), then by netmask length, then by the full address. So a value sorts next to the network it belongs to, and a wider network sorts before a narrower one with the same network part.

The same order applies to `<`, `<=`, `>`, `>=`, `BETWEEN`, `ORDER BY`, `min`, `max`, `greatest`, `least`, window functions and `ANY`/`ALL` subqueries. Equality compares the address and the netmask length, so `10.0.0.1/8` and `10.0.0.2/8` are different values.

<SqlLogicTest id="sql/data_types/inet/example_002" />

## Containment

`a <<= b` is true when `a` lies within the network `b` or equals it, and `a >>= b` is true when `a` contains `b` or equals it. An IPv4 value is never contained in an IPv6 network, nor the other way around.

<SqlLogicTest id="sql/data_types/inet/example_003" />

The PostgreSQL operators `<<`, `>>`, `&&`, `~`, `&` and `|` are not supported.

## Indexing

An [inverted index](../indexes/inverted/index.md) over an `INET` column answers equality, `IN`, comparisons, `BETWEEN`, `<<=` and `>>=`. Index the column without a text search dictionary.

<SqlLogicTest id="sql/data_types/inet/example_005" />

`>>=` finds the stored networks that contain an address, which suits tables of networks such as routing or geolocation tables.

<SqlLogicTest id="sql/data_types/inet/example_006" />

## Functions

`host`, `network`, `broadcast`, `netmask` and `family` take an `INET` value. Adding an integer to an `INET` value, or subtracting one, moves the address.

<SqlLogicTest id="sql/data_types/inet/example_004" />
