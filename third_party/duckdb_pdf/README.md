# SereneDB PDF readers

SereneDB's Apache-2.0 PDF extension provides `read_pdf` and `read_pdf_blocks`.
PoDoFo is used under MPL-2.0, and FreeType under the FreeType License.
Their licenses, attribution, and source references accompany binary releases.

The SQL contract is documented in `docs/sql/functions/pdf.md` in the parent
repository. Text extraction supports local files, glob patterns, passwords,
page ranges, geometric reading order, and original content order. OCR and
document editing are unsupported.
