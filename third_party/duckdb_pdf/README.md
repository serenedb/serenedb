# SereneDB PDF readers

SereneDB's PDF extension provides `read_pdf`, `read_pdf_blocks`, and `read_pdf_tables` under the
repository's [Apache-2.0 license](../../LICENSE).
PoDoFo is used under MPL-2.0, and FreeType under the FreeType License.
Their licenses, attribution, and source references accompany binary releases.

The SQL contract is documented in `docs/sql/functions/pdf.md` in the parent
repository. Text extraction supports local files, glob patterns, passwords,
page ranges, geometric reading order, and original content order. Ruled tables
support merged cells and missing outer borders. OCR, unruled tables, and document
editing are unsupported.
