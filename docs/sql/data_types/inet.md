---
title: Inet
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

| Name   | Aliases | Description                                         |
| :----- | :------ | :-------------------------------------------------- |
| `INET` |         | IPv4 or IPv6 host address with an optional netmask |
| `CIDR` |         | IPv4 or IPv6 network address                        |

An `INET` value holds an address and a netmask length. Without a netmask the value is a single host: `/32` for IPv4, `/128` for IPv6. With a netmask it can describe a network, and it may keep host bits beyond the netmask, as in `192.168.1.5/24`.

<SqlLogicTest id="sql/data_types/inet/example_001" />

## Ordering

`INET` values sort the way PostgreSQL sorts them: IPv4 before IPv6, then by the network part (the address with the bits beyond the netmask cleared), then by netmask length, then by the full address. So a value sorts next to the network it belongs to, and a wider network sorts before a narrower one with the same network part.

The same order applies to `<`, `<=`, `>`, `>=`, `BETWEEN`, `ORDER BY`, `min`, `max`, `arg_min`, `arg_max`, `list_min`, `list_max`, `greatest`, `least`, window functions and `ANY`/`ALL` subqueries, and to arrays and rows that contain `INET` values. Equality compares the address and the netmask length, so `10.0.0.1/8` and `10.0.0.2/8` are different values.

<SqlLogicTest id="sql/data_types/inet/example_002" />

## Containment

`a <<= b` is true when `a` lies within the network `b` or equals it, and `a >>= b` is true when `a` contains `b` or equals it. An IPv4 value is never contained in an IPv6 network, nor the other way around.

<SqlLogicTest id="sql/data_types/inet/example_003" />

The PostgreSQL operators `<<`, `>>`, `&&`, `~`, `&` and `|` are not supported.

## Indexing

An [inverted index](../indexes/inverted/index.md) over an `INET` column answers equality, `IN`, comparisons, `BETWEEN`, `<<=` and `>>=`. Index the column without a text search dictionary. An expression that returns `INET`, such as `network(ip)`, can be indexed and searched the same way.

<SqlLogicTest id="sql/data_types/inet/example_005" />

`>>=` finds the stored networks that contain an address, which suits tables of networks such as routing or geolocation tables.

<SqlLogicTest id="sql/data_types/inet/example_006" />

## Conversion

A string converts to `INET` when it is a valid address, as in PostgreSQL. A `STRUCT`, row, `JSON` or `VARIANT` value converts when its fields `ip_type` (`1` for IPv4, `2` for IPv6), `address` and `mask` form a valid address. If one of these fields is `NULL`, the result is `NULL`. Any other invalid value is an error; `TRY_CAST` returns `NULL` instead.

<SqlLogicTest id="sql/data_types/inet/example_007" />

## CIDR

A `CIDR` value is a network: an `INET` whose bits beyond the netmask are all zero. A string with host bits set, such as `'192.168.1.5/24'`, is an error, and a `CIDR` value always shows its netmask, even `/32` and `/128`. Converting an `INET` to `CIDR` clears the host bits, `network` returns a `CIDR`, and a `CIDR` value is used as an `INET` wherever one is expected. Mixing the two, as in `COALESCE` or `UNION`, gives `INET`. `CIDR` columns sort, compare and are indexed the same way as `INET` columns.

<SqlLogicTest id="sql/data_types/inet/example_008" />

Unlike PostgreSQL, abbreviated input such as `'10'` for `10.0.0.0/8` is not accepted, and `min` and `max` of a `CIDR` column return a `CIDR` value rather than an `INET` one.

## Functions

`host`, `network`, `broadcast`, `netmask` and `family` take an `INET` value. Adding an integer to an `INET` value, or subtracting one, moves the address.

<SqlLogicTest id="sql/data_types/inet/example_004" />
