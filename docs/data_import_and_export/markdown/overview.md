---
title: Overview
sidebar_position: 1
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB reads Markdown files with three table functions:

| Function                        | Returns a row per                                                                  |
| :------------------------------ | :--------------------------------------------------------------------------------- |
| `read_markdown(files)`          | File, with its text and its frontmatter.                                           |
| `read_markdown_sections(files)` | Heading, with the section it starts.                                               |
| `read_markdown_blocks(files)`   | Block: heading, paragraph, code block, list, table, frontmatter and so on.        |

`files` is a path, a glob, a directory or a list of them, see [Reading Several Files](#reading-several-files). The [Markdown functions](../../data_import_and_export/markdown/markdown_functions.md) work on the text these functions return.

## Reading Documents

`read_markdown` returns a row per file:

| Column     | Type                    | Description                                                                                                                                                                                                              |
| :--------- | :---------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `content`  | `MARKDOWN`              | The text of the file.                                                                                                                                                                                                    |
| `metadata` | `MAP(VARCHAR, VARCHAR)` | The frontmatter, the block between two `---` lines at the start of the file: a key and a value per `key: value` line, or per `key = value` line between `+++` lines. Empty without frontmatter. Not returned with `extract_metadata = false`. |
| `stats`    | `STRUCT`                | Only with `include_stats = true`: the statistics [`md_stats`](../../data_import_and_export/markdown/markdown_functions.md#statistics) returns.                                                                           |

With this `post.md`:

```markdown
---
title: Hello
author: Ana
---

# Hello

First post.
```

<SqlLogicTest id="data_import_and_export/markdown/overview/example_001" />

A path that ends in `.md` or `.markdown` can be queried as a table, which `read_markdown` reads:

<SqlLogicTest id="data_import_and_export/markdown/overview/example_002" />

`MARKDOWN`, or `md`, is a text type: it converts to and from `VARCHAR`, and clients receive it as `text`. `content_as_varchar = true` returns `content` as `VARCHAR`.

## Reading Sections

`read_markdown_sections` returns a row per heading:

| Column         | Type       | Description                                                                                                                                                                                      |
| :------------- | :--------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `section_id`   | `VARCHAR`  | The heading text in lower case, with each run of characters other than ASCII letters, digits, `-` and `_` turned into one `-`, and no `-` at either end. An id that repeats an earlier one gets a numeric suffix, such as `intro-1`. |
| `section_path` | `VARCHAR`  | The ids of the enclosing sections and of the section, joined by `/`.                                                                                                                             |
| `level`        | `INTEGER`  | The heading level: `1` for `#`, `6` for `######`.                                                                                                                                                |
| `title`        | `VARCHAR`  | The heading text, without formatting.                                                                                                                                                            |
| `content`      | `MARKDOWN` | The Markdown under the heading, see `content_mode` below.                                                                                                                                        |
| `parent_id`    | `VARCHAR`  | The id of the enclosing section, the nearest one above with a lower level, or `NULL`.                                                                                                            |
| `start_line`   | `BIGINT`   | The line of the heading in the file, counted from 1, frontmatter included.                                                                                                                       |
| `end_line`     | `BIGINT`   | The last line of the section.                                                                                                                                                                    |
| `stats`        | `STRUCT`   | Only with `include_stats = true`: the statistics [`md_stats`](../../data_import_and_export/markdown/markdown_functions.md#statistics) returns for the section's `content`.                        |

With this `guide.md`:

```markdown
# Guide

Welcome.

## Install

Run the installer.

### Linux

Use the package.

## Usage

Run it.
```

<SqlLogicTest id="data_import_and_export/markdown/overview/example_003" />

A section's `content` is its Markdown as written in the file, from its first block to its last. `content` and `end_line` stop at the next heading by default (`content_mode = 'minimal'`). With `content_mode = 'full'` they reach the next heading of the same or a higher level, so a section includes its subsections. `content_mode = 'smart'` is `full`, except that content longer than `max_content_length` bytes keeps only the text before the first subsection, or its first `max_content_length` bytes when there is no such text, followed by a `... (see #id)` line per direct subsection.

`min_level` and `max_level` limit the heading levels returned, and `max_depth` limits their number, counted from `min_level`: `max_depth = 2` returns levels 1 and 2. A section's `parent_id` is then the nearest returned section above it with a lower level. A file with frontmatter adds a section with `section_id` `frontmatter`, `level` 0, the frontmatter text as `content` and `start_line` 1, unless `extract_metadata = false`, `min_level` is above 1 or `section_filter` names another section. `include_empty_sections = false` leaves out the sections whose `content` is empty, such as a heading followed directly by another with `content_mode = 'minimal'`.

### Selecting a Section

`section_filter` returns the section with that `section_id` and the sections inside it. A path ending in `#` and the id does the same:

<SqlLogicTest id="data_import_and_export/markdown/overview/example_004" />

## Reading Blocks

`read_markdown_blocks` returns a row per block, in the order of the file:

| Column          | Type                    | Description                                                                                                                                                                                         |
| :-------------- | :---------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kind`          | `VARCHAR`               | `block`, or `inline` for a part of a heading or paragraph with formatting: `text`, `bold`, `italic`, `code`, `link`, `image` and so on.                                                             |
| `element_type`  | `VARCHAR`               | `heading`, `paragraph`, `code`, `blockquote`, `list`, `list_item`, `table`, `hr`, `metadata` for the frontmatter, and so on.                                                                        |
| `content`       | `VARCHAR`               | The text of the block: the text of a heading, of a paragraph without formatting or of an inline part, the code of a code block, a table as JSON, the frontmatter as written. `NULL` for a block whose text is in the rows inside it, such as a list. |
| `level`         | `INTEGER`               | The nesting depth: `1` at the top of the document, one more inside each enclosing block.                                                                                                           |
| `encoding`      | `VARCHAR`               | `text`, `json` for a table, `yaml` or `toml` for the frontmatter.                                                                                                                                   |
| `attributes`    | `MAP(VARCHAR, VARCHAR)` | `heading_level` and `id` of a heading, `language` of a code block, `list_type` (`bullet` or `ordered`) of a list, `role` (`frontmatter`) of the frontmatter.                                         |
| `element_order` | `INTEGER`               | The position of the row in its file, counted from 0.                                                                                                                                                |

With this `blocks.md`:

````markdown
# Hello

This is a paragraph.

```sql
SELECT 1;
```
````

<SqlLogicTest id="data_import_and_export/markdown/overview/example_005" />

[`duck_blocks_to_md`](../../data_import_and_export/markdown/markdown_functions.md#blocks) turns blocks back into Markdown.

## Reading Several Files

Each function takes a path, a glob such as `docs/*.md` or `docs/**/*.md`, a directory, for its `.md` and `.markdown` files without its subdirectories, or a list of these. Every file must end in `.md` or `.markdown`. `filename = true`, or `include_filepath = true`, adds a `filename` column with the path of each row's file:

<SqlLogicTest id="data_import_and_export/markdown/overview/example_006" />

### Missing and Unreadable Files

A path that matches no file, a file that does not end in `.md` or `.markdown` and a file that cannot be read, such as one that is not valid UTF-8, are errors: `File or directory does not exist: missing.md`, `File is not a markdown file: notes.txt`, `Error reading Markdown file ...`. `ignore_errors = true` skips them with a warning instead:

<SqlLogicTest id="data_import_and_export/markdown/overview/example_007" />

A file larger than `maximum_file_size` is an error, with `ignore_errors` as well.

## Parameters

| Name                 | Description                                                                                                                                                                                                                                                                              | Type      | Default    |
| :------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------- | :--------- |
| `content_as_varchar` | Return `content` as `VARCHAR` instead of `MARKDOWN`. `read_markdown` and `read_markdown_sections` only.                                                                                                                                                                                  | `BOOL`    | `false`    |
| `content_mode`       | Where the `content` and `end_line` of a section stop: `minimal`, `full` or `smart`, see [Reading Sections](#reading-sections). `read_markdown_sections` only.                                                                                                                            | `VARCHAR` | `minimal`  |
| `extract_extensions` | Extractors that add a column each, separated by commas: `wikilinks` adds the `[[target]]` links of each row as [`md_extract_wikilinks`](../../data_import_and_export/markdown/markdown_functions.md#extracting-content) finds them, `tags` the `#tags` as `md_extract_tags` finds them, and `obsidian` both. | `VARCHAR` | `NULL`     |
| `extract_metadata`   | `read_markdown`: return the `metadata` column. `read_markdown_sections`: return the frontmatter as a section. `read_markdown_blocks`: return the frontmatter as a `metadata` block; without it `element_order` counts from the first block of the text.                                     | `BOOL`    | `true`     |
| `filename`           | Add a `filename` column with the path of each row's file. Alias: `include_filepath`.                                                                                                                                                                                                     | `BOOL`    | `false`    |
| `ignore_errors`      | Skip missing, non-Markdown and unreadable files with a warning.                                                                                                                                                                                                                         | `BOOL`    | `false`    |
| `include_content`    | Return the `content` of each section; with `false`, `content` is empty. `read_markdown_sections` only.                                                                                                                                                                                  | `BOOL`    | `true`     |
| `include_empty_sections` | Return the sections whose `content` is empty. `read_markdown_sections` only.                                                                                                                                                                                                         | `BOOL`    | `true`     |
| `include_stats`      | Return the `stats` column: of each file for `read_markdown`, of each section for `read_markdown_sections`.                                                                                                                                                                              | `BOOL`    | `false`    |
| `max_content_length` | The length in bytes from which `content_mode = 'smart'` shortens the content of a section. `0` means 2000. `read_markdown_sections` only.                                                                                                                                                | `UBIGINT` | `0`        |
| `max_depth`          | The number of heading levels returned, counted from `min_level`, from 1 to 6. `read_markdown_sections` only.                                                                                                                                                                             | `INTEGER` | `6`        |
| `max_level`          | The largest heading level returned, `6` for `######`. `read_markdown_sections` only.                                                                                                                                                                                                     | `INTEGER` | `6`        |
| `maximum_file_size`  | The largest file size in bytes; a larger file is an error. `0` means no limit.                                                                                                                                                                                                           | `UBIGINT` | `16777216` |
| `min_level`          | The smallest heading level returned, `1` for `#`. `read_markdown_sections` only.                                                                                                                                                                                                         | `INTEGER` | `1`        |
| `normalize_content`  | Turn `\r\n` and `\r` line breaks into `\n` before the file is parsed.                                                                                                                                                                                                                    | `BOOL`    | `true`     |
| `section_filter`     | Return only the section with this `section_id` and the sections inside it. `read_markdown_sections` only.                                                                                                                                                                                | `VARCHAR` | (empty)    |

`read_markdown_blocks` takes `extract_extensions`, `extract_metadata`, `filename`, `ignore_errors`, `maximum_file_size` and `normalize_content`.

## Writing Markdown

`COPY ... TO ... (FORMAT markdown)` writes the rows as a Markdown table by default. Number columns are aligned right, `BOOLEAN` columns centered and the others left; a `|` in a value is written as `\|` and a line break as `<br>`:

<SqlLogicTest id="data_import_and_export/markdown/overview/example_008" />

`markdown_mode = 'document'` writes a section per row: a heading of `level` `#` characters with the `title` text, then the `content`. A row with `level` 0 writes its `content` as frontmatter:

<SqlLogicTest id="data_import_and_export/markdown/overview/example_009" />

`markdown_mode = 'blocks'` writes rows in the columns `read_markdown_blocks` returns, so a file can be read as blocks, changed and written back:

<SqlLogicTest id="data_import_and_export/markdown/overview/example_010" />

| Name                  | Description                                                                                                      | Type      | Default        |
| :-------------------- | :--------------------------------------------------------------------------------------------------------------- | :-------- | :------------- |
| `markdown_mode`       | `table`, `document`, or `blocks` (also `duck_block`).                                                            | `VARCHAR` | `table`        |
| `header`              | Write the row of column names and the alignment row. `table` mode.                                               | `BOOL`    | `true`         |
| `escape_pipes`        | Put a backslash before each pipe character in a value. `table` mode.                                             | `BOOL`    | `true`         |
| `escape_newlines`     | Write a line break in a value as `<br>`. `table` mode.                                                           | `BOOL`    | `true`         |
| `null_value`          | The text written for `NULL`. `table` mode.                                                                       | `VARCHAR` | (empty)        |
| `level_column`        | The column with the heading level of a section, or the nesting depth of a block.                                 | `VARCHAR` | `level`        |
| `title_column`        | The column with the heading text of a section. `document` mode.                                                  | `VARCHAR` | `title`        |
| `content_column`      | The column with the text of a section or a block.                                                                | `VARCHAR` | `content`      |
| `frontmatter`         | Text written as frontmatter at the start of the file. `document` mode.                                           | `VARCHAR` | (empty)        |
| `blank_lines`         | The number of blank lines written after each section. `document` mode.                                           | `INTEGER` | `1`            |
| `kind_column`         | The column with the `kind` of a block. `blocks` mode.                                                            | `VARCHAR` | `kind`         |
| `element_type_column` | The column with the `element_type` of a block. `blocks` mode.                                                    | `VARCHAR` | `element_type` |
| `encoding_column`     | The column with the `encoding` of a block. `blocks` mode.                                                        | `VARCHAR` | `encoding`     |
| `attributes_column`   | The column with the `attributes` of a block. `blocks` mode.                                                      | `VARCHAR` | `attributes`   |
