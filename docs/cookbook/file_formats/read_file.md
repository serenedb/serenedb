---
layout: docu
redirect_from:
- /docs/guides/file_formats/read_file
- /docs/guides/import/read_file
- /docs/preview/guides/file_formats/read_file
- /docs/stable/guides/file_formats/read_file
title: Directly Reading Files
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

SereneDB allows directly reading files via the [`read_text`](#read_text) and [`read_blob`](#read_blob) functions.
These functions accept a filename, a list of filenames, or a glob pattern. They output the content of each file as a `VARCHAR` or `BLOB`, respectively, along with metadata such as the file size and last modified time.

## `read_text`

The `read_text` table function reads from the selected source(s) to a `VARCHAR`. Each file results in a single row with the `content` field holding the entire content of the respective file.

<SqlLogicTest id="cookbook/file_formats/read_file/example_001" />

SereneDB first validates the file content as valid UTF-8. If `read_text` attempts to read a file with invalid UTF-8, SereneDB throws an error suggesting to use [`read_blob`](#read_blob) instead.

`read_text` also supports reading from pipes (e.g., `/dev/stdin`).

<DocCallout type="tip">

The maximum allowed file size for `read_text` is 3.9 GiB.

</DocCallout>

## `read_blob`

The `read_blob` table function reads from the selected source(s) to a `BLOB`:

<SqlLogicTest id="cookbook/file_formats/read_file/example_002" />

> The maximum allowed file size for `read_blob` is 3.9 GiB.

## Schema

The schemas of the tables returned by `read_text` and `read_blob` are identical:

<SqlLogicTest id="cookbook/file_formats/read_file/example_003" />

## Hive Partitioning

Data can be read from [Hive partitioned](../../data_import_and_export/partitioning/hive_partitioning.md) datasets.

<SqlLogicTest id="cookbook/file_formats/read_file/example_004" />


## Handling Missing Metadata

When the underlying filesystem cannot provide this data (e.g., HTTPFS may not always return a valid timestamp), the cell is set to `NULL` instead.

## Support for Projection Pushdown

These table functions also use projection pushdown to avoid computing properties unnecessarily. For example, you can glob a directory of large files to get file sizes in the `size` column. As long as you omit the `content` column, SereneDB won't read the file data.
