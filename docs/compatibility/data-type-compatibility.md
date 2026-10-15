---
split: headings
---

# Data Type Compatibility

`pg_type` lists every built-in PostgreSQL 18 type under PostgreSQL's name and OID, so clients that look types up by name or OID find all of them. This page says which of them hold values in SereneDB.

- 🟢 Supported: values behave as in PostgreSQL
- 🟡 Supported, with the differences described in Details
- 🔴 Not supported: the type is in `pg_type` and its name resolves (`'point'::regtype`, `format_type(600, NULL)`), but no value, cast or column can use it

The array type of each type, `<type>[]`, has the same support as the type, and the column shorthands `smallserial`, `serial` and `bigserial` work as in PostgreSQL. As in PostgreSQL, a call of a type's name with one argument, such as `int4('42')` or `text(123)`, is a cast to that type when no function of that name takes the argument. Pseudo-types such as `anyelement`, `record` and `trigger` are listed for function signatures and, as in PostgreSQL, hold no values. Besides these types, SereneDB has types of its own, such as [`VARIANT`](../sql/data_types/variant.md), [`MAP`](../sql/data_types/map.md) and [`UNION`](../sql/data_types/union.md).

## Data Types

| Type                         | Support State | Details |
|------------------------------|---------------|---------|
| aclitem                      | 🟢            |         |
| bit                          | 🟡            | A length is not enforced: `'1011'::bit(3)` stays `1011`. A bit string holds at least one bit: `B''` reads as `0`, and a `substring` without bits is an error. |
| bool                         | 🟢            | `boolean` |
| box                          | 🔴            |         |
| bpchar                       | 🟡            | `character(n)`: trailing blanks count in comparisons and in `length()`, so `'a '::char(2) = 'a'::char(2)` is false. |
| bytea                        | 🟢            |         |
| char                         | 🟡            | Values above 127 compare by their text form, such as `\303`, so `'é'::"char" > 'z'::"char"` is false. |
| cid                          | 🟢            |         |
| cidr                         | 🟢            |         |
| circle                       | 🔴            |         |
| date                         | 🟡            | A date before year 1 is read in the form `'0001-12-31 (BC)'` instead of `'0001-12-31 BC'`, and a cast to `text` gives that form. |
| datemultirange               | 🔴            |         |
| daterange                    | 🔴            |         |
| float4                       | 🟡            | `real`: a value out of range becomes `Infinity` or `0` instead of an error, so `'1e40'::float4` is `Infinity`. |
| float8                       | 🟡            | `double precision`: a value out of range becomes `Infinity` or `0` instead of an error, so `'1e400'::float8` and `1e308::float8 * 10` are `Infinity`. Hexadecimal input such as `'0x10'::float8` is not accepted. |
| gtsvector                    | 🔴            |         |
| inet                         | 🟢            |         |
| int2                         | 🟢            | `smallint` |
| int2vector                   | 🟢            |         |
| int4                         | 🟢            | `integer` |
| int4multirange               | 🔴            |         |
| int4range                    | 🔴            |         |
| int8                         | 🟢            | `bigint` |
| int8multirange               | 🔴            |         |
| int8range                    | 🔴            |         |
| interval                     | 🟡            | Field restrictions are ignored: `'1 year 2 mons'::interval year` keeps the months. |
| json                         | 🟢            |         |
| jsonb                        | 🔴            | Use `json`. A parameter a client sends as `jsonb` is read as `json`. |
| jsonpath                     | 🔴            |         |
| line                         | 🔴            |         |
| lseg                         | 🔴            |         |
| macaddr                      | 🟢            |         |
| macaddr8                     | 🟢            |         |
| money                        | 🟡            | Values read and print in the `C` locale's format, such as `$1,234.56`: there is no `lc_monetary` setting. A number compared or combined with `money`, as in `m = 5` or `COALESCE(m, 0)`, is read as an amount, where PostgreSQL raises an error. |
| name                         | 🟢            |         |
| numeric                      | 🟡            | Without a precision, `numeric` is `numeric(18,3)` and rounds to three fractional digits. The precision is at most 38, and `NaN` is not supported. |
| nummultirange                | 🔴            |         |
| numrange                     | 🔴            |         |
| oid                          | 🟢            |         |
| oidvector                    | 🟢            |         |
| path                         | 🔴            |         |
| pg_brin_bloom_summary        | 🔴            |         |
| pg_brin_minmax_multi_summary | 🔴            |         |
| pg_dependencies              | 🟡            | Text input is accepted, which PostgreSQL refuses. There are no extended statistics, so no catalog holds a value. |
| pg_lsn                       | 🟡            | Adding or subtracting a `numeric` gives a `numeric`; an integer gives a `pg_lsn`. |
| pg_mcv_list                  | 🟡            | Text input is accepted, which PostgreSQL refuses. There are no extended statistics, so no catalog holds a value. |
| pg_ndistinct                 | 🟡            | Text input is accepted, which PostgreSQL refuses. There are no extended statistics, so no catalog holds a value. |
| pg_node_tree                 | 🟡            | Text input is accepted, which PostgreSQL refuses. |
| pg_snapshot                  | 🔴            |         |
| point                        | 🔴            |         |
| polygon                      | 🔴            |         |
| refcursor                    | 🟡            | Values compare and sort like text, which PostgreSQL refuses. |
| regclass                     | 🟢            |         |
| regcollation                 | 🟢            |         |
| regconfig                    | 🟡            | Only numeric input and output: there is no text search configuration catalog. |
| regdictionary                | 🟡            | Names SereneDB's text search dictionaries; PostgreSQL's built-in `simple` does not exist. |
| regnamespace                 | 🟢            |         |
| regoper                      | 🟡            | Only numeric input and output: there is no operator catalog. |
| regoperator                  | 🟡            | Only numeric input and output: there is no operator catalog. |
| regproc                      | 🟢            |         |
| regprocedure                 | 🟢            |         |
| regrole                      | 🟢            |         |
| regtype                      | 🟢            |         |
| text                         | 🟢            |         |
| tid                          | 🟡            | Tables have no `ctid` column. |
| time                         | 🟡            | A precision such as `time(2)` is not accepted. |
| timestamp                    | 🟡            | A precision keeps milliseconds or microseconds: `timestamp(2)` keeps three fractional digits. |
| timestamptz                  | 🟡            | A precision such as `timestamptz(2)` is not accepted. |
| timetz                       | 🟢            |         |
| tsmultirange                 | 🔴            |         |
| tsquery                      | 🟡            | SereneDB's own [`TSQUERY`](../sql/data_types/tsquery.md), a query for inverted-index search, not PostgreSQL's. |
| tsrange                      | 🔴            |         |
| tstzmultirange               | 🔴            |         |
| tstzrange                    | 🔴            |         |
| tsvector                     | 🔴            |         |
| txid_snapshot                | 🔴            |         |
| uuid                         | 🟢            |         |
| varbit                       | 🟡            | A length such as `varbit(3)` is not accepted. A bit string holds at least one bit, as for `bit`. |
| varchar                      | 🟡            | A length is not enforced: `'abc'::varchar(2)` stays `abc`. |
| xid                          | 🟢            |         |
| xid8                         | 🟢            |         |
| xml                          | 🔴            |         |
