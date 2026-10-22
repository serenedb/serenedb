---
title: PDF Readers
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The PDF readers are available in SereneDB SQL and in `serened shell`, with no separate installation or `LOAD` statement. In the shell, file paths refer to the local machine.

## Read page text

`read_pdf(path, password := '', first_page := 1, last_page := ..., layout := 'auto')` returns one row per selected PDF page. The path can name a file or contain a glob pattern. Local paths are relative to the server's working directory and refer to files accessible to the server.

<SqlLogicTest id="pdf/read_pages" />

| Column | Type | Meaning |
| --- | --- | --- |
| `filename` | `VARCHAR` | Source path. |
| `page` | `BIGINT` | Page number, starting at 1. |
| `text` | `VARCHAR` | Extracted text, with a newline between lines. |
| `width` | `DOUBLE` | Page width in PDF points. |
| `height` | `DOUBLE` | Page height in PDF points. |
| `has_text_layer` | `BOOLEAN` | Whether the page yielded nonblank text. |

The default `layout := 'auto'` groups text into lines and detects reading columns from their positions. Columns read left to right, with lines top to bottom within each column; line-ending hyphens are removed when a word continues onto the next line. Complex layouts may need manual interpretation using `read_pdf_blocks`. Use `layout := 'raw'` for the document's content order, with a newline between text fragments. Extraction depends on the PDF's character mappings; documents with missing or incorrect mappings can produce incomplete or incorrect text.

## Read positioned text

`read_pdf_blocks` accepts `path`, `password`, `first_page`, and `last_page` and returns one row per nonblank text fragment in the document's content order. A fragment is not necessarily a paragraph, line, word, or table cell.

<SqlLogicTest id="pdf/read_blocks" />

| Column | Type | Meaning |
| --- | --- | --- |
| `filename` | `VARCHAR` | Source path. |
| `page` | `BIGINT` | Page number, starting at 1. |
| `block` | `BIGINT` | Fragment number within the page, starting at 1. |
| `text` | `VARCHAR` | Extracted fragment. |
| `x`, `y` | `DOUBLE` | Text origin in PDF points, adjusted for page rotation. |
| `width`, `height` | `DOUBLE` | Estimated fragment dimensions; `NULL` when unavailable. |

The coordinate system has its origin at the bottom left and increases upward along the y-axis. One PDF point is 1/72 inch. The text origin is on its baseline; it is not the lower-left corner of the estimated bounding box. Page boxes with nonzero origins retain their offsets. Dimensions are approximate and are not suitable for redaction or exact glyph hit testing.

## Passwords and page selection

Provide `password` for password-protected documents. A missing or incorrect password produces an error. Page bounds are inclusive and start at 1. `last_page` is clipped to the document's page count. An invalid range or a `first_page` beyond the document's page count produces an error. Page selection applies separately to every matching file.

<SqlLogicTest id="pdf/page_range" />

## Limits and errors

OCR and table extraction are not supported. `read_pdf` reports pages without extractable text with empty `text` and `has_text_layer = false`; `read_pdf_blocks` emits no fragments for those pages. A completed scan of a file whose selected pages have no extractable text raises an error. An empty text result does not establish that a page is scanned: it may be blank, contain outlines, or lack usable character mappings.

Files must be nonempty and seekable. Each PDF must fit in memory along with its parsed content. A malformed or unreadable PDF aborts the query. Use `ORDER BY filename, page` for page order, or `ORDER BY filename, page, block` for fragment order.

## Licenses

The PDF readers are distributed under Apache-2.0, with PoDoFo under MPL-2.0 and FreeType under the FreeType License. Binary packages include dependency licenses, attribution, and source links in the `pdf/` directory under the installed SereneDB documentation directory.
