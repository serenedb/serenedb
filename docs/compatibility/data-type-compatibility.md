---
split: headings
---

# Data Type Compatibility

`pg_type` lists every built-in PostgreSQL 18 type under PostgreSQL's name and OID, so clients that look types up by name or OID find all of them. This page says which of them hold values in SereneDB.

- 🟢 Supported: values behave as in PostgreSQL
- 🟡 Supported, with the differences described in Details
- 🔴 Not supported: the type is in `pg_type` and its name resolves (`'point'::regtype`, `format_type(600, NULL)`), but no value, cast or column can use it

The array type of each type, `<type>[]`, has the same support as the type, and the column shorthands `smallserial`, `serial` and `bigserial` work as in PostgreSQL. An array type has a fixed number of dimensions, where a PostgreSQL array type takes any: `int4[]` holds one-dimensional arrays and `int4[][]` two-dimensional ones, so `'{{1,2},{3,4}}'::int4[]`, or a two-dimensional array a client sends for an `int4[]` parameter, is an error. As in PostgreSQL, a call of a type's name with one argument, such as `int4('42')` or `text(123)`, is a cast to that type when no function of that name takes the argument. Pseudo-types such as `anyelement`, `record` and `trigger` are listed for function signatures and, as in PostgreSQL, hold no values. Besides these types, SereneDB has types of its own, such as [`VARIANT`](../sql/data_types/variant.md), [`MAP`](../sql/data_types/map.md) and [`UNION`](../sql/data_types/union.md).

Values of `money`, `xml`, the geometric types and the range types cast to `json`, where PostgreSQL refuses the cast, and `to_json`, `json_build_object` and `COPY ... (FORMAT JSON)` write them in the form the cast gives: an amount as a number, such as `12.34`, an `xml` value as a string, a geometric value or a range as an object of its fields, such as `{"x":1.0,"y":2.0}` for `point '(1,2)'`, and a multirange as an array of such objects. PostgreSQL's `to_json` writes the text form of a `money`, geometric or range value instead, such as `"$12.34"` or `"(1,2)"`.

These types also cast to and from the types their values are stored in, where PostgreSQL refuses the cast, and functions of those types take them: `money` is a `numeric(19,2)`, `xml` a `text`, and a geometric value or a range a record of its fields. So `1.5::float8::money` is `$1.50`, `'1'::money::float8` is `1`, `abs('1'::money)` is `1`, `1::xml` is `1` and `'[1,5)'::int4range::int8range` is `[1,5)`. A record, row, list, `json` or `variant` value cast to a geometric, range or multirange type is checked as the type's text input is: `{'a': 0, 'b': 0, 'c': 1}::line` fails as `'{0,0,1}'::line` does, a range comes out canonical and a multirange sorted and merged.

The coordinates of the geometric types print as `float8` values do, so `point(1e15, 0.0001)` is `(1000000000000000,1e-04)` where PostgreSQL prints `(1e+15,0.0001)`.

## Data Types

| Type                         | Support State | Details |
|------------------------------|---------------|---------|
| aclitem                      | 🟢            |         |
| bit                          | 🟡            | A length is not enforced: `'1011'::bit(3)` stays `1011`. A bit string holds at least one bit: `B''` reads as `0`. |
| bool                         | 🟢            | `boolean` |
| box                          | 🟡            | The comparison operators such as `=` and `<` compare the corners one by one, where PostgreSQL compares areas, so `box '(1,1),(0,0)' = box '(2,2),(1,1)'` is false. `ORDER BY`, `DISTINCT` and `GROUP BY` use the same comparison, where PostgreSQL refuses them. A column of this type cannot be indexed. |
| bpchar                       | 🟡            | `character(n)`: trailing blanks count in comparisons and in `length()`, so `'a '::char(2) = 'a'::char(2)` is false. |
| bytea                        | 🟢            |         |
| char                         | 🟡            | Text input keeps the first byte or a `\ooo` escape, and the casts to and from `integer` take a byte's value, as in PostgreSQL. Values sort by their text, so the bytes `\200` to `\377` sort before letters, where PostgreSQL sorts by byte; a cast from another type, such as `65::bigint::"char"`, and `COPY ... (FORMAT csv)` keep the whole text, where PostgreSQL refuses the cast and keeps the first byte. |
| cid                          | 🟢            |         |
| cidr                         | 🟡            | A column of this type cannot be a `PRIMARY KEY` or `UNIQUE` column, and only an [inverted index](../sql/indexes/inverted/index.md) indexes it. `min` and `max` of a `cidr` column return `cidr`, where PostgreSQL returns `inet`. |
| circle                       | 🟡            | The comparison operators such as `=` and `<` compare the center and then the radius, where PostgreSQL compares areas, so `circle '<(0,0),1>' = circle '<(5,5),1>'` is false. `ORDER BY`, `DISTINCT` and `GROUP BY` use the same comparison, where PostgreSQL refuses them. A column of this type cannot be indexed. |
| date                         | 🟡            | A date before year 1 is read in the form `'0001-12-31 (BC)'` instead of `'0001-12-31 BC'`, and a cast to `text` gives that form. |
| datemultirange               | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| daterange                    | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| float4                       | 🟡            | `real`: a value out of range becomes `Infinity` or `0` instead of an error, so `'1e40'::float4` is `Infinity`. A whole number prints without an exponent, so `1000000::float4` is `1000000` where PostgreSQL prints `1e+06`. A cast to `text` spells infinity and NaN `inf` and `nan` where PostgreSQL writes `Infinity` and `NaN`. |
| float8                       | 🟡            | `double precision`: a value out of range becomes `Infinity` or `0` instead of an error, so `'1e400'::float8` and `1e308::float8 * 10` are `Infinity`. Hexadecimal input such as `'0x10'::float8` is not accepted. A whole number prints without an exponent, so `1e15::float8` is `1000000000000000` where PostgreSQL prints `1e+15`; other values print in their shortest form, so `0.0001::float8` is `1e-04`. A cast to `text` spells infinity and NaN `inf` and `nan` where PostgreSQL writes `Infinity` and `NaN`. |
| gtsvector                    | 🔴            |         |
| inet                         | 🟡            | A column of this type cannot be a `PRIMARY KEY` or `UNIQUE` column, and only an [inverted index](../sql/indexes/inverted/index.md) indexes it. `concat`, `format` and `array_to_string` print an address with its netmask, as a cast to `text` does (`10.0.0.1/32`), where PostgreSQL prints `10.0.0.1`. |
| int2                         | 🟢            | `smallint` |
| int2vector                   | 🟢            |         |
| int4                         | 🟢            | `integer` |
| int4multirange               | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| int4range                    | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| int8                         | 🟢            | `bigint` |
| int8multirange               | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| int8range                    | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| interval                     | 🟡            | Field restrictions are ignored: `'1 year 2 mons'::interval year` keeps the months. |
| json                         | 🟢            |         |
| jsonb                        | 🔴            | Use `json`. A parameter a client sends as `jsonb` is read as `json`. |
| jsonpath                     | 🔴            |         |
| line                         | 🟡            | `=` compares the coefficients, where PostgreSQL tests for the same line, so `line '{1,-1,0}' = line '{2,-2,0}'` is false. The other comparison operators, `ORDER BY`, `DISTINCT` and `GROUP BY` compare the coefficients one by one, where PostgreSQL refuses them. A column of this type cannot be indexed. |
| lseg                         | 🟡            | The comparison operators `<`, `<=`, `>` and `>=` compare the end points one by one, where PostgreSQL compares lengths. `ORDER BY`, `DISTINCT` and `GROUP BY` use the same comparison, where PostgreSQL refuses them. A column of this type cannot be indexed. |
| macaddr                      | 🟢            |         |
| macaddr8                     | 🟢            |         |
| money                        | 🟡            | Values read and print in the `C` locale's format, such as `$1,234.56`: there is no `lc_monetary` setting. A number compared or combined with `money`, as in `m = 5` or `COALESCE(m, 0)`, is read as an amount, where PostgreSQL raises an error. Parquet files hold an amount as a decimal number of dollars, such as `12.34`. |
| name                         | 🟡            | A value is not cut to 63 bytes: `length(repeat('x', 70)::name)` is `70`. |
| numeric                      | 🟡            | Without a precision, `numeric` is `numeric(18,3)` and rounds to three fractional digits. The precision is at most 38, and `NaN` is not supported. |
| nummultirange                | 🟡            | Bounds are `numeric` values, so without a precision they round to three fractional digits, as for `numrange`. A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| numrange                     | 🟡            | Bounds are `numeric` values, so without a precision they round to three fractional digits: `numrange(1.25, 2.5)` is `[1.250,2.500)`. A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| oid                          | 🟡            | Text input reads as a `bigint`: `'010'::oid` is `10`, where PostgreSQL reads octal and gives `8`, `'-1'::oid` stays `-1`, where PostgreSQL gives `4294967295`, and a value above `4294967295` is accepted. A number given for a `reg*` type, such as `'023'::regtype`, is decimal as well. |
| oidvector                    | 🟡            | Elements read as `oid` text does. |
| path                         | 🟡            | The comparison operators such as `=` and `<` compare whether the path is closed and then its points, where PostgreSQL compares the numbers of points, so `path '((0,0),(1,1))' = path '[(5,5),(6,6)]'` is false. `ORDER BY`, `DISTINCT` and `GROUP BY` use the same comparison, where PostgreSQL refuses them. A column of this type cannot be indexed. |
| pg_brin_bloom_summary        | 🔴            |         |
| pg_brin_minmax_multi_summary | 🔴            |         |
| pg_dependencies              | 🟡            | Text input is accepted, which PostgreSQL refuses. There are no extended statistics, so no catalog holds a value. |
| pg_lsn                       | 🟡            | Adding or subtracting a `numeric` gives a `numeric`; an integer gives a `pg_lsn`. |
| pg_mcv_list                  | 🟡            | Text input is accepted, which PostgreSQL refuses. There are no extended statistics, so no catalog holds a value. |
| pg_ndistinct                 | 🟡            | Text input is accepted, which PostgreSQL refuses. There are no extended statistics, so no catalog holds a value. |
| pg_node_tree                 | 🟡            | Text input is accepted, which PostgreSQL refuses. |
| pg_snapshot                  | 🔴            |         |
| point                        | 🟡            | The comparison operators such as `=` and `<`, `ORDER BY`, `DISTINCT` and `GROUP BY` compare `x` and then `y`, where PostgreSQL refuses them: `~=` tests for the same point. A column of this type cannot be indexed. |
| polygon                      | 🟡            | The comparison operators such as `=` and `<`, `ORDER BY`, `DISTINCT` and `GROUP BY` compare the points one by one, where PostgreSQL refuses them: `~=` tests for the same polygon. A column of this type cannot be indexed. |
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
| tsmultirange                 | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| tsquery                      | 🟡            | SereneDB's own [`TSQUERY`](../sql/data_types/tsquery.md), a query for inverted-index search, not PostgreSQL's. |
| tsrange                      | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| tstzmultirange               | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| tstzrange                    | 🟡            | A column of this type cannot be indexed, nor be a `PRIMARY KEY` or `UNIQUE` column. |
| tsvector                     | 🔴            |         |
| txid_snapshot                | 🔴            |         |
| uuid                         | 🟢            |         |
| varbit                       | 🟡            | A length such as `varbit(3)` is not accepted. A bit string holds at least one bit, as for `bit`. |
| varchar                      | 🟡            | A length is not enforced: `'abc'::varchar(2)` stays `abc`. |
| xid                          | 🟢            |         |
| xid8                         | 🟢            |         |
| xml                          | 🟡            | The SQL/XML expressions `XMLPARSE`, `XMLSERIALIZE`, `XMLELEMENT`, `XMLFOREST`, `XMLPI`, `XMLROOT`, `XMLEXISTS` and `XMLTABLE` are not supported, nor are `SET XML OPTION` (use `SET xmloption`) and the functions that map tables and queries to XML, such as `table_to_xml`. Values compare, sort and group as text, and text functions such as `length()` take them, where PostgreSQL raises an error. `COPY ... (FORMAT csv)` stores a value without checking that it is well-formed. |
