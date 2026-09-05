# PostgreSQL Compatibility

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB implements the SQL dialect of PostgreSQL. While we are able to syntactically parse any PostgreSQL-compliant statement, not all underlying functionality is implemented yet.

This page gives a non-exhaustive overview of currently supported core SQL functionality, followed by the [behavioral differences](#behavioral-differences-from-postgresql) where SereneDB intentionally diverges from PostgreSQL. SereneDB strives for full PostgreSQL compatibility, and most features not currently supported will be added over time.

For PostgreSQL-specific functionality such as system table support, see the [System Table Compatibility](./system-table-compatibility.md) page.

## Data Definition

### Table Creation & Deletion
| Feature                 | Support State | Details                                                      |
|-------------------------|---------------|--------------------------------------------------------------|
| CREATE TABLE            | Yes           |   |
| DROP TABLE              | Yes           |     |
| Default Values          | Yes           |   |
| GENERATED               | Yes           |  |
| Check Constraints       | Yes           |   |
| Not-Null Constraints    | Yes           |   |
| Unique Constraints      | Yes           |   |
| Primary Keys            | Yes           |   |
| Foreign Keys            | Yes           | Enforced; over-eager on some `UPDATE`s |
| Named Constraints       | Yes           |  |
| Exclusion Constraints   | No            | See [issue](https://github.com/serenedb/serenedb/issues/205) |
| System Columns          | No            | See [issue](https://github.com/serenedb/serenedb/issues/202) |

### Table Modification (ALTER TABLE)
| Feature            | Support State | Details                                                      |
|--------------------|---------------|--------------------------------------------------------------|
| ADD COLUMN         | Yes           |  |
| DROP COLUMN        | Yes           |  |
| ADD CHECK          | No            | See [issue](https://github.com/serenedb/serenedb/issues/206) |
| ADD CONSTRAINT     | No            | See [issue](https://github.com/serenedb/serenedb/issues/206) |
| ADD FOREIGN KEY    | No            | See [issue](https://github.com/serenedb/serenedb/issues/206) |
| DROP CONSTRAINT    | No            | See [issue](https://github.com/serenedb/serenedb/issues/206) |
| ALTER COLUMN       | Partial       | `TYPE` is supported; `SET`/`DROP DEFAULT` and `SET NOT NULL` are not. See [issue](https://github.com/serenedb/serenedb/issues/206) |
| SET DEFAULT        | No            | See [issue](https://github.com/serenedb/serenedb/issues/206) |
| DROP DEFAULT       | No            | See [issue](https://github.com/serenedb/serenedb/issues/206) |
| COLUMN TYPE        | Yes           |  |
| RENAME COLUMN      | Yes           |  |
| RENAME TO          | Yes           |  |

### Privileges
| Feature               | Support State | Details                                                      |
|-----------------------|---------------|--------------------------------------------------------------|
| CREATE ROLE           | Yes           | See [Database Roles](../security/roles.md) |
| OWNER TO              | Yes           | Tables, views, sequences, schemas, types; not databases/functions yet |
| ALTER ROLE            | Yes           | Attributes, passwords, `VALID UNTIL`, `RENAME TO` |
| GRANT                 | Yes           | Privileges and role membership, incl. column grants; see [Privileges](../security/privileges.md) |
| REVOKE                | Yes           | Incl. `GRANT OPTION FOR` |
| SET ROLE              | Yes           | Enforced; membership required for non-superusers |
| INHERIT               | Yes           | `INHERIT`/`NOINHERIT` attributes enforced |
| ALTER DEFAULT PRIVILEGES | Partial    | Accepted and stored, not yet applied to new objects |
| Row Security Policies | No            | See [issue](https://github.com/serenedb/serenedb/issues/212) |

### Indexes
| Feature                 | Support State | Details                                                     |
|-------------------------|---------------|-------------------------------------------------------------|
| CREATE INDEX            | Yes           |  |
| GIN                     | No            |                                                             |
| BRIN                    | No            |                                                             |
| Multicolumn Indexes     | Yes           |  |
| Ordered Indexes         | Yes           |  |
| Unique Indexes          | Yes           |                                                    |
| Indexes on Expressions  | No            |                                                    |
| Partial Indexes         | Partial       | [Inverted indexes](../sql/statements/create_index/inverted.md#partial-indexes) only; plain indexes reject `WHERE` |

### Misc
| Feature                    | Support State | Details                                         |
|----------------------------|---------------|-------------------------------------------------|
| CREATE SCHEMA              | Yes           |    |
| DROP SCHEMA                | Yes           |      |
| search_path                | Yes           |              |
| Table Inheritance          | No            |                                                                |
| Table Partitioning         | No            |                                                                |
| Foreign Data Wrappers      | No            |                                                                |
| Views                      | Yes           |      |
| Databases                  | Yes           |  |
| Functions & Procedures     | Yes           |  |
| Custom Types               | Yes           |                                                 |
| Triggers                   | No            |                                                 |
| Prepared Statements        | Yes           |                                                 |
| Advisory Locks             | No            |                                                 |

## Data Manipulation
| Feature      | Support State | Details        |
|--------------|---------------|----------------|
| INSERT       | Yes           |    |
| UPDATE       | Yes           |    |
| DELETE       | Yes           |    |
| TRUNCATE     | Yes          |  |
| RETURNING    | Yes           |  |
| COPY FROM    | Yes           |      |
| COPY TO      | Yes           |      |
| ON CONFLICT  | Partial       |   |

## Queries
| Feature                    | Support State | Details        |
|----------------------------|---------------|----------------|
| Table & View References    | Yes           |                |
| Inner Joins                | Yes           |  |
| Outer Joins                | Yes           |  |
| Semijoins                  | Yes           |  |
| Antijoins                  | Yes           |                |
| Table Functions            | Yes           | |
| Lateral Subqueries         | Yes           |                |
| User-Specified Aliases     | Yes           |                |
| GROUP BY                   | Yes           |  |
| HAVING                     | Yes           |  |
| GROUPING SETS              | Yes           |                |
| CUBE                       | Yes           |                |
| ROLLUP                     | Yes           |                |
| WINDOW Functions           | Yes           |  |
| WITH                       | Yes           |  |
| WITH RECURSIVE             | Yes           |                |
| UNION                      | Yes           |                |
| UNION ALL                  | Yes           |                |
| INTERSECT                  | Yes           |                |
| INTERSECT ALL              | Yes           |                |
| EXCEPT                     | Yes           |                |
| EXCEPT ALL                 | Yes           |                |
| ORDER BY                   | Yes           |                |
| LIMIT                      | Yes           |                |
| OFFSET                     | Yes           |                |
| Table Generating Function  | Yes           |                |

## Data Types 

### Types
| Type                                              | Support State | Details                  |
|---------------------------------------------------|---------------|--------------------------|
| array                                             | Yes           |    |
| bigint                                            | Yes           |  |
| bigserial                                         | Yes           |                          |
| bit [ (n) ]                                       | Yes           |                          |
| bit varying [ (n) ]                               | Yes           |                          |
| boolean                                           | Yes           |  |
| box                                               | No            |                          |
| bytea                                             | Yes           | |
| character [ (n) ]                                 | Yes           | CHAR(n) not supported; use TEXT |
| character varying [ (n) ]                         | Yes           | VARCHAR works, VARCHAR(n) does not |
| cidr                                              | No            |                          |
| circle                                            | No            |                          |
| date                                              | Yes           |     |
| double precision                                  | Yes           |    |
| inet                                              | No            |                          |
| integer                                           | Yes           |    |
| interval [ fields ] [ (p) ]                       | Yes           |   |
| json                                              | Yes           |       |
| jsonb                                             | No            |                          |
| line                                              | No            |                          |
| lseg                                              | No            |                          |
| macaddr                                           | No            |                          |
| macaddr8                                          | No            |                          |
| money                                             | No            |                          |
| numeric [ (p, s) ]                                | Yes           |    |
| path                                              | No            |                          |
| pg_lsn                                            | No            |                          |
| pg_snapshot                                       | No            |                          |
| point                                             | No            |                          |
| polygon                                           | No            |                          |
| real                                              | Yes           |      |
| smallint                                          | Yes           |    |
| smallserial                                       | Yes           |                          |
| serial                                            | Yes           |                          |
| text                                              | Yes           |       |
| time [ (p) ] [ without time zone ]                | Partial       |                          |
| time [ (p) ] with time zone                       | Yes           |                          |
| timestamp [ (p) ] [ without time zone ]           | Yes           |  |
| timestamp [ (p) ] with time zone                  | Yes          |  |
| tsquery                                           | Yes           | [SereneDB `TSQUERY`](../sql/data_types/tsquery.md) (inverted index), not PG tsvector/tsquery |
| tsvector                                          | No            |                          |
| txid_snapshot                                     | No            |                          |
| uuid                                              | Yes           |       |
| xml                                               | No            |                          |

### Operators & Functions

#### Logical
| Feature | Support State | Details |
|---------|---------------|---------|
| AND     | Yes           |         |
| OR      | Yes           |         |
| NOT     | Yes           |         |

#### Comparison
| Feature         | Support State | Details |
|-----------------|---------------|---------|
| \<               | Yes           |         |
| >               | Yes           |         |
| \<=              | Yes           |         |
| >=              | Yes           |         |
| =               | Yes           |         |
| \<>              | Yes           |         |
| !=              | Yes           |         |
| BETWEEN         | Yes           |         |
| NOT BETWEEN     | Yes           |         |
| IS DISTINCT     | Yes           |         |
| IS NOT DISTINCT | Yes           |         |
| IS NULL         | Yes           |         |
| IS NOT NULL     | Yes           |         |
| IS TRUE         | Yes           |         |
| IS NOT TRUE     | Yes           |         |
| IS FALSE        | Yes           |         |
| IS NOT FALSE    | Yes           |         |
| IS UNKNOWN      | Yes           |         |
| IS NOT UNKNOWN  | Yes           |         |

#### Mathematical
| Feature          | Support State | Details                                                                 |
|------------------|---------------|-------------------------------------------------------------------------|
| +                | Yes           |                                                                         |
| -                | Yes           |                                                                         |
| *                | Yes           |                                                                         |
| /                | Yes           |                                                                         |
| %                | Yes           |                                                                         |
| ^                | Yes           |                                                                         |
| |/               | Yes            |                                                                         |
| ||/              | Yes            |                                                                         |
| @                | No            |                                                                         |
| &                | Yes           |                                                                         |
| |                | Yes            |                                                                         |
| #                | No            |                                                                         |
| ~                | Yes           |                                                                         |
| \<\<             | Yes           |                                                                         |
| >>               | Yes           |                                                                         |
| abs              | Yes           |                                                                         |
| cbrt             | Yes           |                                                                         |
| ceil             | Yes           |                                                                         |
| degrees          | Yes           |                                                                         |
| div              | Yes           |                                                                         |
| erf              | Yes           |                                                                         |
| erfc             | Yes           |                                                                         |
| exp              | Yes           |                                                                         |
| factorial        | Yes           |                                                                         |
| floor            | Yes           |                                                                         |
| gcd              | Yes           |                                                                         |
| lcm              | Yes           |                                                                         |
| ln               | Yes           |                                                                         |
| log              | Yes           |                                                                         |
| log10            | Yes           |                                                                         |
| min_scale        | No            |                                                                         |
| mod              | Yes           |                                                                         |
| pi               | Yes           |                                                                         |
| power            | Yes           |                                                                         |
| radians          | Yes           |                                                                         |
| round            | Yes           |                                                                         |
| scale            | No            |                                                                         |
| sign             | Yes           |                                                                         |
| sqrt             | Yes           |                                                                         |
| trim_scale       | No            |                                                                         |
| trunc            | Yes           |                                                                         |
| width_bucket     | Yes           |                                                                         |
| random           | Yes           |                                                                         |
| random_normal    | No            |                                                                         |
| setseed          | Yes           |                                                                         |
| acos             | Yes           |                                                                         |
| acosd            | Yes           |                                                                         |
| asin             | Yes           |                                                                         |
| asind            | Yes           |                                                                         |
| atan             | Yes           |                                                                         |
| atand            | Yes           |                                                                         |
| atan2            | Yes           |                                                                         |
| atan2d           | Yes           |                                                                         |
| cos              | Yes           |                                                                         |
| cosd             | Yes           |                                                                         |
| cot              | Yes           |                                                                         |
| cotd             | Yes           |                                                                         |
| sin              | Yes           |                                                                         |
| sind             | Yes           |                                                                         |
| tan              | Yes           |                                                                         |
| tand             | Yes           |                                                                         |
| sinh             | Yes           |                                                                         |
| cosh             | Yes           |                                                                         |
| tanh             | Yes           |                                                                         |
| asinh            | Yes           |                                                                         |
| acosh            | Yes           |                                                                         |
| atanh            | Yes           |                                                                         |

#### Text
| Feature             | Support State | Details                                                    |
|---------------------|---------------|------------------------------------------------------------|
| \|\|                | Yes           |                                                            |
| btrim               | No            |                                                            |
| bit_length          | Yes           |                                                            |
| char_length         | Yes           |                                                            |
| lower               | Yes           |                                                            |
| lpad                | Yes           |                                                            |
| ltrim               | Yes           |                                                            |
| normalize           | Yes           |                                                            |
| octet_length        | Yes           |                                                            |
| overlay             | Yes           |                                                            |
| position            | Yes           |                                                            |
| rpad                | Yes           |                                                            |
| rtrim               | Yes           |                                                            |
| substring           | Yes           | Currently not supporting regular expression arguments      |
| trim                | Yes           |                                                            |
| upper               | Yes           |                                                            |
| ^@                  | Yes           |                                                            |
| ascii               | Yes           |                                                            |
| chr                 | Yes           |                                                            |
| concat              | Yes           |                                                            |
| concat_ws           | Yes           |                                                            |
| format              | Yes           |                                                            |
| initcap             | Yes           |                                                            |
| left                | Yes           |                                                            |
| length              | Yes           |                                                            |
| md5                 | Yes           |                                                            |
| parse_ident         | No            |                                                            |
| quote_ident         | Yes           |                                                            |
| quote_literal       | Yes           |                                                            |
| quote_nullable      | Yes           |                                                            |
| repeat              | Yes           |                                                            |
| replace             | Yes           |                                                            |
| reverse             | Yes           |                                                            |
| right               | Yes           |                                                            |
| split_part          | Yes           |                                                            |
| starts_with         | Yes           |                                                            |
| string_to_array     | Yes           |                                                            |
| string_to_table     | No            |                                                            |
| strpos              | Yes           |                                                            |
| substr              | Yes           |                                                            |
| to_ascii            | No            |                                                            |
| to_hex              | Yes           |                                                            |
| translate           | Yes           |                                                            |
| unistr              | No            |                                                            |

#### Bytea
| Feature        | Support State | Details                   |
|----------------|---------------|---------------------------|
| \|\|           | Yes           |                           |
| bit_length     | Yes           |                           |
| btrim          | No            |                           |
| ltrim          | Yes           |                           |
| octet_length   | Yes           |                           |
| overlay        | Yes           |                           |
| position       | No            |                           |
| rtrim          | Yes           |                           |
| substring      | Yes           |                           |
| trim           | Yes           |                           |
| bit_count      | No            |                           |
| get_bit        | Yes           |                           |
| get_byte       | Yes           |                           |
| length         | Yes           |                           |
| md5            | Yes           |                           |
| set_bit        | Yes           |                           |
| set_byte       | Yes           |                           |
| sha224         | No            |                           |
| sha384         | No            |                           |
| sha512         | No            |                           |
| substr         | Yes           |                           |
| convert        | No            |                           |
| convert_from   | Yes           |                           |
| convert_to     | Yes           |                           |
| encode         | Yes           |                           |
| decode         | Yes           |                           |

#### Bit
| Feature       | Support State | Details |
|---------------|---------------|---------|
| \|\|          | Yes           |         |
| &             | Yes           |         |
| \|            | Yes           |         |
| #             | No            |         |
| ~             | Yes           |         |
| \<\<          | Yes           |         |
| >>            | Yes           |         |
| bit_count     | Yes           |         |
| bit_length    | Yes           |         |
| length        | Yes           |         |
| octet_length  | Yes           |         |
| overlay       | No            |         |
| position      | No            |         |
| substring     | No            |         |
| get_bit       | Yes           |         |
| set_bit       | Yes           |         |

#### Pattern Matching
| Feature                   | Support State | Details                                             |
|---------------------------|---------------|-----------------------------------------------------|
| LIKE                      | Yes           |                                                     |
| SIMILAR TO                | Yes           |                                                     |
| regexp_count              | Yes           |                                                     |
| regexp_instr              | Yes           |                                                     |
| regexp_like               | Yes           |                                                     |
| regexp_match              | Yes           |                                                     |
| regexp_matches            | Partial       |                                                     |
| regexp_replace            | Partial       | Currently not supporting replacing N’th match       |
| regexp_split_to_array     | Yes           |                                                     |
| regexp_split_to_table     | Yes           |                                                     |
| regexp_substr             | Yes           |                                                     |

#### Data Type Formatting
| Feature       | Support State | Details                |
|---------------|---------------|------------------------|
| to_char       | No            |                        |
| to_date       | No            |                        |
| to_number     | No            |                        |
| to_timestamp  | Partial       | Epoch (numeric) form only; text+format parsing unsupported |

#### Date/Time
| Feature                | Support State | Details                  |
|------------------------|---------------|--------------------------|
| +                      | Yes           |                          |
| -                      | Yes           |                          |
| *                      | Yes           |                          |
| /                      | Yes           |                          |
| age                    | Yes           |                          |
| clock_timestamp        | Yes           |                          |
| current_date           | Yes           |                          |
| current_time           | Yes           |                          |
| current_timestamp      | Yes           |                          |
| date_add               | Yes           | possible with +          |
| date_bin               | Yes           |                          |
| date_part              | Yes           |                          |
| date_subtract          | No            | possible with -          |
| date_trunc             | Yes           | Without timezone         |
| extract                | Yes           |                          |
| isfinite               | Yes           |                          |
| justify_days           | No            |                          |
| justify_hours          | No            |                          |
| justify_interval       | No            |                          |
| localtime              | Yes           |                          |
| localtimestamp         | Yes           |                          |
| make_date              | Yes           |                          |
| make_interval          | No            |                          |
| make_time              | Yes           |                          |
| make_timestamp         | Yes           |                          |
| make_timestamptz       | Yes           | With time zone name      |
| now                    | Yes           |                          |
| statement_timestamp    | No            |                          |
| timeofday              | Yes           |                          |
| transaction_timestamp  | Yes           |                          |
| to_timestamp           | Partial           | Epoch (numeric) form only; text+format parsing unsupported |
| OVERLAPS               | No            |                          |
| EXTRACT                | Yes           |                          |

#### Enum
| Feature      | Support State | Details |
|--------------|---------------|---------|
| enum_first   | Yes           |         |
| enum_last    | Yes           |         |
| enum_range   | Yes           |         |

#### Geometric

PostgreSQL's geometric types (`point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle`) and their operators/functions are not supported. SereneDB instead provides spatial data through a built-in `GEOMETRY` type and `ST_*` functions (`ST_Point`, `ST_Intersects`, `ST_Contains`, `ST_Distance`, …). See [Spatial Functions](../sql/functions/geometry.md) and the [GEOMETRY type](../sql/data_types/geometry.md).
| Feature        | Support State | Details |
|----------------|---------------|---------|
| +              | No            |         |
| -              | No            |         |
| *              | No            |         |
| /              | No            |         |
| @-@            | No            |         |
| @@             | No            |         |
| #              | No            |         |
| ##             | No            |         |
| \<->           | No            |         |
| @>             | No            |         |
| \<@            | No            |         |
| &&             | No            |         |
| \<\<           | No            |         |
| >>             | No            |         |
| &\<            | No            |         |
| &>             | No            |         |
| \<\<\|         No | No            |         |
| \|>>           | No            |         |
| &\<\|          No | No            |         |
| |&>            | No            |         |
| \<^            | No            |         |
| >^             | No            |         |
| ?#             | No            |         |
| ?-             | No            |         |
| ?|             | No            |         |
| ?-|            No | No            |         |
| ?||            | No            |         |
| ~=             | No            |         |
| area           | No            |         |
| center         | No            |         |
| diagonal       | No            |         |
| diameter       | No            |         |
| height         | No            |         |
| isclosed       | No            |         |
| isopen         | No            |         |
| length         | No            |         |
| npoints        | No            |         |
| pclose         | No            |         |
| popen          | No            |         |
| radius         | No            |         |
| slope          | No            |         |
| width          | No            |         |
| box            | No            |         |
| bounding_box   | No            |         |
| circle         | No            |         |
| line           | No            |         |
| lseg           | No            |         |
| path           | No            |         |
| point          | No            |         |
| polygon        | No            |         |

#### Network
| Feature            | Support State | Details |
|--------------------|---------------|---------|
| \<\<               | No            |         |
| \<\<=              | No            |         |
| >>                 | No            |         |
| \>\>=              | No            |         |
| &&                 | No            |         |
| ~                  | No            |         |
| &                  | No            |         |
| \|                 | No            |         |
| +                  | No            |         |
| -                  | No            |         |
| abbrev             | No            |         |
| broadcast          | No            |         |
| family             | No            |         |
| host               | No            |         |
| hostmask           | No            |         |
| inet_merge         | No            |         |
| inet_same_family   | No            |         |
| masklen            | No            |         |
| netmask            | No            |         |
| network            | No            |         |
| set_masklen        | No            |         |
| text               | No            |         |

#### Text Search
| Feature                  | Support State | Details |
|--------------------------|---------------|---------|
| @@                       | Yes           | [SereneDB full-text search](../cookbook/search/index.md) (inverted index), not PG tsvector/tsquery |
| \|\|                     | Yes           | [SereneDB full-text search](../cookbook/search/index.md) (inverted index), not PG tsvector/tsquery |
| &&                       | Yes           | [SereneDB full-text search](../cookbook/search/index.md) (inverted index), not PG tsvector/tsquery |
| !!                       | Yes           | [SereneDB full-text search](../cookbook/search/index.md) (inverted index), not PG tsvector/tsquery |
| \<->                     | No            |         |
| @>                       | No            |         |
| \<@                      | No            |         |
| array_to_tsvector        | No            |         |
| get_current_ts_config    | No            |         |
| length                   | No            |         |
| numnode                  | No            |         |
| plainto_tsquery          | Yes           | [SereneDB full-text search](../cookbook/search/index.md) (inverted index), not PG tsvector/tsquery |
| phraseto_tsquery         | Yes           | [SereneDB full-text search](../cookbook/search/index.md) (inverted index), not PG tsvector/tsquery |
| websearch_to_tsquery     | Yes           | [SereneDB full-text search](../cookbook/search/index.md) (inverted index), not PG tsvector/tsquery |
| querytree                | No            |         |
| setweight                | No            |         |
| strip                    | No            |         |
| to_tsquery               | Yes           | [SereneDB full-text search](../cookbook/search/index.md) (inverted index), not PG tsvector/tsquery |
| to_tsvector              | No            |         |
| json(b)_to_tsvector      | No            |         |
| ts_delete                | No            |         |
| ts_filter                | No            |         |
| ts_headline              | No            |         |
| ts_rank                  | No            |         |
| ts_rank_cd               | No            |         |
| ts_rewrite               | No            |         |
| tsquery_phrase           | Yes           | [SereneDB full-text search](../cookbook/search/index.md) (inverted index), not PG tsvector/tsquery |
| tsvector_to_array        | No            |         |
| unnest                   | No            |         |
| ts_debug                 | No            |         |
| ts_lexize                | Yes           | SereneDB extension: works with [CREATE TEXT SEARCH DICTIONARY](../sql/statements/create_text_search_dictionary/index.md) |
| ts_parse                 | No            |         |
| ts_token_type            | No            |         |
| ts_stat                  | No            |         |

#### UUID
| Feature                  | Support State | Details |
|--------------------------|---------------|---------|
| get_random_uuid          | Yes           |         |
| uuid_extract_timestamp   | Yes           |         |
| uuid_extract_version     | Yes           |         |

#### XML
| Feature               | Support State | Details |
|-----------------------|---------------|---------|
| xmltext               | No            |         |
| xmlcomment            | No            |         |
| xmlconcat             | No            |         |
| xmlelement            | No            |         |
| xmlforest             | No            |         |
| xmlpi                 | No            |         |
| xmlroot               | No            |         |
| xmlagg                | No            |         |
| `IS DOCUMENT`         | No            |         |
| `IS NOT DOCUMENT`     | No            |         |
| XMLEXISTS             | No            |         |
| xml_is_well_formed    | No            |         |
| xpath                 | No            |         |
| xpath_exists          | No            |         |
| XMLTABLE              | No            |         |

#### JSON

The PostgreSQL-named JSON functions below are largely unsupported. SereneDB instead provides JSON access through the [`JSON` column type](../data_import_and_export/json/json_type.md) and SereneDB-style functions — `json_extract`, `json_extract_string`, `json_keys`, `json_type`, `json_valid` and more (see [JSON Functions](../data_import_and_export/json/json_functions.md)). The PostgreSQL accessor operators `->`, `->>`, `#>` and `#>>` are supported.

Binary JSON (`jsonb`) is not supported, so every `jsonb_*` function and the `jsonb`-only operators are unavailable.

| Feature | Support State | Details |
|---|---|---|
| `->` | Yes |  |
| `->>` | Yes |  |
| `#>` | Yes |  |
| `#>>` | Yes |  |
| `@>` | No | Use `json_contains()` |
| `<@` | No | Use `json_contains()` |
| `?` | No | Use `json_exists()` |
| `?\|` | No | Use `json_exists()` |
| `?&` | No | Use `json_exists()` |
| `\|\|` | Yes |  |
| `-` | No |  |
| `#-` | No |  |
| `@?` | No | Use `json_exists()` |
| `@@` | No | No JSON-path predicate |
| to_json | Yes |  |
| to_jsonb | No | Use `to_json` |
| array_to_json | Yes |  |
| json_array | Yes |  |
| row_to_json | Yes |  |
| json_build_array | No | Use `json_array()` |
| jsonb_build_array | No |  |
| json_build_object | No | Use `json_object(key, value)` |
| jsonb_build_object | No |  |
| json_object | Partial | Requires even arg count; SereneDB `json_object(key, value)` |
| jsonb_object | No |  |
| `IS JSON` | No | Use `json_valid()` |
| json_array_elements | No | Use `json_each()` |
| jsonb_array_elements | No |  |
| json_array_elements_text | No | Use `json_extract_string()` |
| jsonb_array_elements_text | No |  |
| json_array_length | Yes |  |
| jsonb_array_length | No | Use `json_array_length` |
| json_each | Yes |  |
| jsonb_each | No |  |
| json_each_text | No | Use `json_each()` |
| jsonb_each_text | No |  |
| json_object_keys | No | Use `json_keys()` |
| jsonb_object_keys | No |  |
| json_populate_record | No | Use `json_transform()` |
| jsonb_populate_record | No |  |
| json_populate_recordset | No | Use `json_transform()` |
| jsonb_populate_recordset | No |  |
| json_to_record | No | Use `json_transform()` |
| jsonb_to_record | No |  |
| json_to_recordset | No | Use `json_transform()` |
| jsonb_to_recordset | No |  |
| jsonb_set | No |  |
| jsonb_set_lax | No |  |
| jsonb_insert | No |  |
| json_strip_nulls | Yes |  |
| jsonb_strip_nulls | No | Use `json_strip_nulls` |
| json_extract_path | Yes | Single-arg form needs a cast; multi-arg works |
| json_extract_path_text | Yes | Single-arg form needs a cast; multi-arg works |
| json_path | No | No `jsonpath` type |

#### Sequence Manipulation
| Feature  | Support State | Details |
|----------|---------------|---------|
| nextval  | Yes          |  |
| setval   | Yes          |  |
| currval  | Yes          |  |
| lastval  | No            |         |

#### Conditional
| Feature   | Support State | Details |
|-----------|---------------|---------|
| CASE      | Yes           |         |
| COALESCE  | Yes          |  |
| NULLIF    | Yes           |         |
| GREATEST  | Yes           |         |
| LEAST     | Yes           |         |

#### Array
| Feature            | Support State | Details                        |
|-------------------|---------------|--------------------------------|
| @>               | Yes           |                                |
| \<@               | Yes           |                                |
| &&               | Yes           |                                |
| \|\|               | Yes           | Not for multidimensional       |
| array_append       | Yes           |                                |
| array_cat          | Yes           |                                |
| array_dims         | Yes           |                                |
| array_fill         | No            |                                |
| array_lower        | Yes           |                                |
| array_ndims        | Yes           |                                |
| array_position     | Yes           |                                |
| array_positions    | Yes           |                                |
| array_prepend      | Yes           |                                |
| array_remove       | Yes           |                                |
| array_replace      | Yes           |                                |
| array_sample       | No            |                                |
| array_shuffle      | No            |                                |
| array_to_string    | Yes           |                                |
| array_upper        | Yes           |                                |
| array_cardinality  | Yes           | via cardinality()              |
| trim_array         | Yes           |                                |
| unnest             | Yes           | No multi-array expansion       |

#### Range
| Feature       | Support State | Details |
|---------------|---------------|---------|
| @>            | No            |         |
| \<@           | No            |         |
| &&            | No            |         |
| \<\<          | No            |         |
| >>            | No            |         |
| &\<           | No            |         |
| &>            | No            |         |
| -\| No        | No            |         |
| +             | No            |         |
| *             | No            |         |
| -             | No            |         |
| lower         | No            |         |
| upper         | No            |         |
| isempty       | No            |         |
| lower_inc     | No            |         |
| upper_inc     | No            |         |
| lower_inf     | No            |         |
| upper_inf     | No            |         |
| range_merge   | No            |         |
| multirange    | No            |         |
| unnest        | No            |         |

#### Aggregate Functions

##### Generic
| Feature                   | Support State | Details                       |
|----------------------------|---------------|-------------------------------|
| any_value                  | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| array_agg                  | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| avg                        | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| bit_and                    | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| bit_or                     | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| bit_xor                    | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| bool_and                   | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| bool_or                    | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| `count(*)`                 | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| `count("any")`             | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| json(b)_agg                | No            |                               |
| json(b)_objectagg          | No            |                               |
| json(b)_object_agg         | No            |                               |
| json_arrayagg              | No            |                               |
| max                        | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| min                        | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| range(_intersect)_agg      | No            |                               |
| string_agg                 | Yes           |                                  |
| sum                        | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| xmlagg                      | No            |                               |

##### Statistical
| Feature        | Support State | Details                          |
|----------------|---------------|----------------------------------|
| corr           | Yes           |                                  |
| covar_pop      | Yes           |                                  |
| covar_samp     | Yes           |                                  |
| regr_avgx      | Yes           |                                  |
| regr_avgy      | Yes           |                                  |
| regr_count     | Yes           |                                  |
| regr_intercept | Yes           |                                  |
| regr_r2        | Yes           |                                  |
| regr_slope     | Yes           |                                  |
| regr_sxx       | Yes           |                                  |
| regr_sxy       | Yes           |                                  |
| regr_syy       | Yes           |                                  |
| stddev         | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| stddev_pop     | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| stddev_samp    | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| variance       | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| var_pop        | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |
| var_samp       | Yes           | [Aggregate functions](../sql/functions/aggregates/index.md) |

##### Ordered-Set
| Feature            | Support State | Details                          |
|--------------------|---------------|----------------------------------|
| mode               | Yes           |                                  |
| percentile_cont    | Yes           |                                  |
| percentile_disc    | Yes           |                                  |

#### Window
| Feature        | Support State | Details |
|----------------|---------------|---------|
| row_number     | Yes           |         |
| rank           | Yes           |         |
| dense_rank     | Yes           |         |
| percent_rank   | Yes           |         |
| cume_dist      | Yes           |         |
| ntile          | Yes           |         |
| lag            | Yes           |         |
| lead           | Yes           |         |
| first_value    | Yes           |         |
| last_value     | Yes           |         |
| nth_value      | Yes           |         |

#### Subquery
| Feature    | Support State | Details |
|------------|---------------|---------|
| EXISTS     | Yes           |         |
| IN         | Yes           |         |
| NOT IN     | Yes           |         |
| ANY/SOME   | Yes           |         |
| ALL        | Yes           |         |

#### Array Comparison
| Feature     | Support State | Details |
|-------------|---------------|---------|
| EXISTS      | Yes           |         |
| IN          | Yes           |         |
| NOT IN      | Yes           |         |
| ANY/SOME    | Yes           |         |
| ALL         | Yes           |         |

#### Set Returning
| Feature             | Support State | Details                                                      |
|---------------------|---------------|--------------------------------------------------------------|
| generate_series     | Yes           |  |
| generate_subscript  | Yes           |  |

## Behavioral Differences from PostgreSQL

Even where a feature is supported, SereneDB intentionally diverges from PostgreSQL in a few places. SereneDB's SQL dialect closely follows PostgreSQL conventions; the exceptions are listed below.

### Floating-Point Arithmetic

SereneDB and PostgreSQL handle floating-point arithmetic differently for division by zero. SereneDB conforms to the [IEEE Standard for Floating-Point Arithmetic (IEEE 754)](https://en.wikipedia.org/wiki/IEEE_754) for both division by zero and operations involving infinity values. PostgreSQL returns an error for division by zero but aligns with IEEE 754 for handling infinity values. In SereneDB:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_001" />

On an attached PostgreSQL, the same division by zero raises an error instead (operations involving infinity follow IEEE 754 on both engines):

<SqlLogicTest id="compatibility/postgresql_comparisons_pgscan/example_006" />

### Integer Division Operator (`//`)

Integer division itself behaves exactly like PostgreSQL: when both operands are integers, `/` performs integer division, so both engines return `0` here:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_002" />

The divergence is that SereneDB additionally accepts the `//` operator as an explicit integer-division spelling, which PostgreSQL does not provide (on PostgreSQL `//` raises `operator does not exist`):

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_003" />

This returns `0`.

### `UNION` of Boolean and Integer Values

The following query fails in PostgreSQL but successfully completes in SereneDB:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_004" />

On an attached PostgreSQL, the same query raises an error:

<SqlLogicTest id="compatibility/postgresql_comparisons_pgscan/example_003" />

SereneDB instead performs an enforced cast and completes the query, returning the rows shown above.

### Implicit Casting on Equality Checks

SereneDB performs implicit casting on equality checks, e.g., converting strings to numeric and boolean values.
Therefore, there are several instances where PostgreSQL throws an error while SereneDB successfully computes the result. Each expression below is evaluated on both engines — the PostgreSQL column runs it on an attached PostgreSQL via `postgres_query`, the SereneDB column evaluates it locally:

<SqlLogicTest id="compatibility/postgresql_comparisons_pgscan/example_001" />

### Case Sensitivity for Quoted Identifiers

PostgreSQL is case-insensitive. The way PostgreSQL achieves case insensitivity is by lowercasing unquoted identifiers within SQL, whereas quoting preserves case, e.g., the following command creates a table named `mytable` but tries to query for `MyTaBLe` because quotes preserve the case.

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_005" />

PostgreSQL does not only treat quoted identifiers as case-sensitive; it treats all identifiers as case-sensitive, e.g., this also does not work:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_006" />

Therefore, case-insensitivity in PostgreSQL only works if you never use quoted identifiers with different cases.

For SereneDB, this behavior was problematic when interfacing with other tools (e.g., Parquet, Pandas) that are case-sensitive by default – since all identifiers would be lowercased all the time.
Therefore, SereneDB achieves case insensitivity by making identifiers fully case insensitive throughout the system but [_preserving their case_](./keywords_and_identifiers.md#rules-for-case-sensitivity).

In SereneDB, the scripts above complete successfully:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/preserved_case_tables/example_007" />

PostgreSQL's behavior of lowercasing identifiers is accessible using the [`preserve_identifier_case` option](../configuration/overview.md#local-configuration-options):

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_008" />

However, the case insensitive matching in the system for identifiers cannot be turned off.

### Using Double Equality Sign for Comparison

SereneDB supports both `=` and `==` for equality comparison, while PostgreSQL only supports `=`.

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_009" />

SereneDB returns `true`, while on an attached PostgreSQL the same query errors (`==` is not a PostgreSQL operator):

<SqlLogicTest id="compatibility/postgresql_comparisons_pgscan/example_004" />

Note that the use of `==` is not encouraged due to its limited portability.

### Vacuuming Tables

In PostgreSQL, the `VACUUM` statement garbage collects tables and analyzes tables.
In SereneDB, the [`VACUUM` statement](../sql/statements/vacuum/index.md) is only used to rebuild statistics.
To reclaim space, use the `CHECKPOINT` statement or compact the database by creating a fresh copy with the [`COPY FROM DATABASE` statement](../sql/statements/copy/index.md#copy-from-database--to).

### Strings

SereneDB escapes characters such as `'` when it serializes strings inside nested data structures — for example, casting a list that contains a quote to text:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_010" />

PostgreSQL's array-to-text conversion leaves the quote unescaped instead:

<SqlLogicTest id="compatibility/postgresql_comparisons_pgscan/example_002" />

### Functions

#### `regexp_extract` Function

Unlike PostgreSQL's `regexp_substr` function, SereneDB's `regexp_extract` returns empty strings instead of `NULL`s when there is no match:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_014" />

#### `to_date` Function

SereneDB does not support the [`to_date` PostgreSQL date formatting function](https://www.postgresql.org/docs/17/functions-formatting.html).
Instead, please use the [`strptime` function](../sql/functions/dateformat.md#strptime-examples):

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_015" />

#### `date_part` Function

Most parts extracted by the [`date_part` function](../sql/functions/datepart.md) are returned as integers. Since there are no infinite integer values in SereneDB, `NULL`s are returned for infinite timestamps:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_016" />

### Resolution of Type Names in the Schema

For [`CREATE TABLE` statements](../sql/statements/create_table/index.md), SereneDB attempts to resolve type names in the schema where a table is created. For example:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_011" />

On an attached PostgreSQL, the same `CREATE TABLE` errors because PostgreSQL does not resolve the unqualified type name in the table's schema:

<SqlLogicTest id="compatibility/postgresql_comparisons_pgscan/example_005" />

SereneDB runs the statement and creates the table successfully, confirmed by the following query:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/example_012" />

### Exploiting Functional Dependencies for `GROUP BY`

PostgreSQL can exploit functional dependencies, such as `i -> j` in the following query, and runs it. SereneDB instead raises the error shown below:

<SqlLogicTest id="sql/dialect/postgresql_compatibility/functional_dependencies/example_013" />

To work around this, add the other attributes or use the [`GROUP BY ALL` clause](../sql/query_syntax/groupby/index.md#group-by-all).
