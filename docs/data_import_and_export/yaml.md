---
title: YAML
split: headings
sidebar_position: 8
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

`read_yaml` reads YAML files into a table. It accepts a file path, a glob such as
`manifests/*.yaml`, or a list of paths. It comes from the community
[yaml extension](https://github.com/teaguesterling/duckdb_yaml), which is built in.

<SqlLogicTest id="data_import_and_export/yaml/read_documents" />

Each mapping document separated by `---` produces one row. Empty, comment-only
and null documents are skipped. Mapping keys become columns. A root sequence of
mappings produces one row per mapping; set `expand_root_sequence = false` to keep
it as one row. Scalar documents are returned in a single `json` column.
Files are opened through DuckDB's filesystem, including configured remote stores.

## Scalars and JSON compatibility

Plain scalars follow the boolean and number spellings of the
[YAML 1.2 JSON schema](https://yaml.org/spec/1.2.2/#102-json-schema).
Other plain scalars are strings, except the YAML core-schema null spellings:

| YAML value | JSON representation |
| --- | --- |
| `true`, `false` | Boolean |
| `no`, `yes`, `on`, `off`, `y`, `True`, `TRUE` | String |
| `1`, `-12`, `-0`, `1.10`, `1e3` | Number |
| `~`, `null`, `Null`, `NULL`, an empty value | Null |
| `1:20`, `012`, `0x10`, `.inf`, `.nan` | String |
| `"true"`, `"1.10"`, `!!str 123`, `""` | String |

Quoted scalars and explicit `!!str` tags remain SQL strings during inference,
including compose ports such as `"22:22"` and quoted dates. Explicit `columns`
can still request a different SQL type. Unquoted strings can infer temporal or
UUID types: `1:20` becomes `TIME`, ISO dates become `DATE`, and timestamps ending
in `Z` become `TIMESTAMPTZ`. Dates are detected with the same formats as `read_json`.
Reading the same JSON file with `read_yaml` preserves its quoted date strings as
`VARCHAR`, while `read_json` may infer `DATE`.

JSON's schema inference determines SQL types across sampled documents, including
nested structures, lists, missing fields, numeric widening and JSON fallback for
incompatible types. A mixture of mapping and scalar documents produces one JSON
`json` column. A mapping with 200 or more keys becomes a `MAP`, as in `read_json`.

Use `columns` to preserve numeric source text such as `version: 1.10`, or to read
large integers as `HUGEINT` or `DECIMAL` without a floating-point conversion:

<SqlLogicTest id="data_import_and_export/yaml/preserve_version" />

<SqlLogicTest id="data_import_and_export/yaml/scalars" />

`sample_size` (default 20480) and `maximum_sample_files` (default 32) control schema
sampling. A type mismatch after the sample raises an error by default. With
`ignore_errors = true`, a value that cannot be converted becomes `NULL`, as in
`read_json`; a document that cannot be parsed is skipped. Files that cannot be
opened are skipped as a whole.

## Anchors and merge keys

Anchors and aliases are expanded, including in extraction and conversion functions.
An unquoted `<<` merges a mapping or a sequence of mappings. Explicit keys override
merged keys regardless of their position; earlier mappings in a merge sequence
take precedence. Quoted `"<<"` is a normal key. Null and non-scalar mapping keys
are rejected. Keys that differ only in case across documents become separate
columns, named as in `read_json`.

Aliases are expanded. A cyclic alias, an expansion beyond one million node visits
or 64 MiB of scalar bytes, or nesting deeper than 1000 levels is an error. The
extension's `yaml_set_*` functions are not available. Reader errors identify the
file and source location.

## Other functions and limitations

The bundled extension also provides `read_yaml_objects`, `parse_yaml`,
`read_yaml_frontmatter`, `yaml_extract`, `yaml_to_json`, and `COPY TO ... (FORMAT yaml)`.
`columns` overrides types for detected columns; it does not restrict the result
to the listed columns, and names missing from the sample are omitted. `records`
selects a YAML path, rather than accepting the JSON reader's boolean option.
JSON's `maximum_depth`, `field_appearance_threshold`, `map_inference_threshold`,
`convert_strings_to_integers`, `dateformat`, and `timestampformat` options are not
exposed; inference uses the JSON defaults.

`read_yaml` reads each file into memory. It does not support `read_json`'s
`filename`, `hive_partitioning`, `union_by_name`, and `file_row_number` parameters.
