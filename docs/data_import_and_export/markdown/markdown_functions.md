---
title: Markdown Functions
sidebar_position: 2
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

These functions take Markdown text: a `MARKDOWN` value, such as the `content` the [Markdown readers](../../data_import_and_export/markdown/overview.md) return, or any text. To all of them but `md_extract_metadata` and `md_extract_frontmatter`, the frontmatter at the start of a document is not part of its body.

## Converting

| Function               | Description                                                                                                 |
| :--------------------- | :---------------------------------------------------------------------------------------------------------- |
| `md_to_html(markdown)` | The document as HTML, with the tables, strikethrough, autolinks and task lists of GitHub Flavored Markdown. |
| `md_to_text(markdown)` | The document as plain text, without markup.                                                                 |
| `md_valid(markdown)`   | `true` for non-empty text, `false` for an empty string.                                                     |
| `value_to_md(value)`   | The value as `MARKDOWN`, in its text form.                                                                  |

<SqlLogicTest id="data_import_and_export/markdown/markdown_functions/example_001" />

## Statistics

`md_stats(markdown)` returns a `STRUCT` with the fields `word_count`, `char_count`, `line_count`, `heading_count`, `code_block_count`, `link_count` and `reading_time_minutes`. It counts headings, code blocks and links line by line, so it also counts a link in a code block or an image as a link. `md_stats(markdown, true)` counts them in the parsed document instead, as `md_extract_links` and `md_extract_code_blocks` find them.

<SqlLogicTest id="data_import_and_export/markdown/markdown_functions/example_002" />

## Extracting Content

Each of these functions returns a `LIST` with a `STRUCT` per element it finds. `line_number` counts the lines of the whole text, frontmatter included.

| Function                           | Elements and their fields                                                                                                                                       |
| :--------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `md_extract_code_blocks(markdown)` | Code blocks: `language`, `code`, `line_number`, `info_string`.                                                                                                  |
| `md_extract_links(markdown)`       | Links, inline and reference: `text`, `url`, `title`, `is_reference`, `line_number`.                                                                             |
| `md_extract_images(markdown)`      | Images: `alt_text`, `url`, `title`, `line_number`.                                                                                                              |
| `md_extract_table_rows(markdown)`  | Table cells: `table_index`, `row_type`, `row_index`, `column_index`, `cell_value`, `line_number`, `num_columns`, `num_rows`.                                    |
| `md_extract_tables_json(markdown)` | Tables: `table_index`, `line_number`, `num_columns`, `num_rows`, `headers`, `table_data`.                                                                       |
| `md_extract_wikilinks(markdown)`   | `[[target]]`, `[[target\|alias]]`, `[[target#heading]]`, `[[target^block]]` and `![[embed]]` links: `target`, `alias`, `anchor`, `is_embed`, `line_number`.      |
| `md_extract_tags(markdown)`        | `#tag` and `#nested/tag` outside of code: `tag`, `line_number`.                                                                                                 |

<SqlLogicTest id="data_import_and_export/markdown/markdown_functions/example_003" />

`md_extract_metadata(markdown)` returns the frontmatter as a `MAP(VARCHAR, VARCHAR)`: each line is split at its first `:`, or its first `=` between `+++` lines, and the spaces around the key and the value and a pair of double quotes around the value are removed. Nested maps, lists and values over several lines are not interpreted. `md_extract_frontmatter(markdown)` returns the frontmatter text as written, or `NULL` when there is none.

<SqlLogicTest id="data_import_and_export/markdown/markdown_functions/example_004" />

## Sections

| Function                                                                 | Description                                                                                                                             |
| :----------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------- |
| `md_extract_sections(markdown[, min_level, max_level[, content_mode]])` | The sections as a `LIST` of `STRUCT`s with the columns of [`read_markdown_sections`](../../data_import_and_export/markdown/overview.md#reading-sections). |
| `md_extract_section(markdown, section_id[, include_subsections])`       | The content of the section with that `section_id`, with its subsections when `include_subsections` is `true`. Empty when there is no such section. |
| `md_section_breadcrumb(markdown, section_id)`                           | The titles from the top section down to the section, joined by ` > `.                                                                   |

<SqlLogicTest id="data_import_and_export/markdown/markdown_functions/example_005" />

## Blocks

These functions take the rows of [`read_markdown_blocks`](../../data_import_and_export/markdown/overview.md#reading-blocks), each as a `STRUCT` of its columns.

| Function                          | Description                                                                         |
| :-------------------------------- | :---------------------------------------------------------------------------------- |
| `duck_blocks_to_md(blocks)`       | A list of blocks, in `element_order`, as a Markdown document.                       |
| `duck_block_to_md(block)`         | One block as Markdown.                                                              |
| `duck_blocks_to_sections(blocks)` | The sections of a list of blocks: `section_id`, `section_path`, `level`, `title`, `content`. |
