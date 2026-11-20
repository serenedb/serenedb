---
title: CREATE DOMAIN
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE DOMAIN` statement defines a domain: a type over a base type with an optional default, collation, `NOT NULL` and `CHECK` constraints. A domain can be used wherever a type can, and every value cast to it is checked against its constraints: on `INSERT` and `UPDATE`, in a cast with `::` or `CAST`, for a function argument and for each element of an array of the domain.

## Examples

Create a domain with a `CHECK` constraint and use it as a column type. A value that violates the constraint is rejected:

<SqlLogicTest id="sql/statements/create_domain/example_001" />

A domain default fills a column that has no default of its own, and `NOT NULL` rejects `NULL`:

<SqlLogicTest id="sql/statements/create_domain/example_002" />

A domain over another domain checks the constraints of both:

<SqlLogicTest id="sql/statements/create_domain/example_003" />

`TRY_CAST` returns `NULL` for a value that violates a domain constraint:

<SqlLogicTest id="sql/statements/create_domain/example_004" />

`ALTER DOMAIN` adds, renames and drops constraints and sets the default. A constraint added with `NOT VALID` checks new values only:

<SqlLogicTest id="sql/statements/create_domain/example_005" />

`DROP DOMAIN` fails while a column uses the domain; with `CASCADE` it drops those columns:

<SqlLogicTest id="sql/statements/create_domain/example_006" />

## Syntax

```sql
CREATE DOMAIN name [ AS ] data_type
    [ COLLATE collation ]
    [ DEFAULT expression ]
    [ [ CONSTRAINT constraint_name ] { NOT NULL | NULL | CHECK (expression) } ] ...

ALTER DOMAIN name { SET DEFAULT expression | DROP DEFAULT }
ALTER DOMAIN name DROP NOT NULL
ALTER DOMAIN name ADD [ CONSTRAINT constraint_name ] CHECK (expression) NOT VALID
ALTER DOMAIN name DROP CONSTRAINT [ IF EXISTS ] constraint_name [ RESTRICT | CASCADE ]
ALTER DOMAIN name RENAME CONSTRAINT constraint_name TO new_constraint_name
ALTER DOMAIN name RENAME TO new_name

DROP DOMAIN [ IF EXISTS ] name [ RESTRICT | CASCADE ]
```

A `CHECK` expression refers to the value being checked as `VALUE`. A constraint without a name is named `<domain>_check` or `<domain>_not_null`, with a number added when that name is taken. `COLLATE` is allowed only for a text base type.

A column of a domain type is described to clients by the domain's base type, and the domain appears in `pg_type` with `typtype` `d`.

## Limitations

- `ALTER DOMAIN ... ADD CONSTRAINT` without `NOT VALID`, `ALTER DOMAIN ... SET NOT NULL` and `ALTER DOMAIN ... VALIDATE CONSTRAINT` are not supported.
- `ALTER DOMAIN ... SET SCHEMA` and `ALTER DOMAIN ... OWNER TO` are not supported.
- `ALTER DOMAIN ... RENAME TO` fails while a column uses the domain.
- `COPY ... FROM` does not check domain constraints.
- Domain constraints are not listed in `pg_constraint` or `information_schema.domain_constraints`.
- A domain defined in another database cannot be used.
