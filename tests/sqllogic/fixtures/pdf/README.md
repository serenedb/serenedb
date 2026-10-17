# PDF fixtures

`generate.py` supplies small structural fixtures for boundary and error tests. `rendered/` contains nine generated documents (26 pages) for `read_pdf_rendered.test`. Keep those PDFs in the repository so ordinary SQL test runs need only the server and test runner.

| PDF | Pages | Expected coverage |
| --- | ---: | --- |
| `business_report.pdf` | 4 | Fictional invoice totaling EUR 137.50, two columns, a blank page, and embedded-font accented text. |
| `compressed_report.pdf` | 4 | The same report with compressed objects and a cross-reference stream; identical extracted text. |
| `geometry.pdf` | 4 | Page rotations of 0, 90, 180, and 270 degrees; known text origins. |
| `offset_page.pdf` | 1 | Nonzero media-box origin and a smaller crop box; original text coordinates. |
| `forms.pdf` | 1 | Two occurrences of the same reusable text stamp at distinct positions. |
| `protected_aes-128.pdf` | 4 | AES-128 encryption; the same report text after authentication. |
| `protected_aes-256.pdf` | 4 | AES-256 encryption; user and owner passwords. |
| `scan.pdf` | 1 | A rasterized invoice with no text layer; extraction must report that OCR is unsupported. |
| `hybrid.pdf` | 3 | Text, a scanned invoice, and text; page scans retain the middle page, block scans skip it. |

The protected documents use `reader-secret` as the user password and `owner-secret` as the owner password. These are public test credentials. All invoice names, references, and amounts are fictional.

The embedded Bitstream Vera font is distributed with ReportLab; its notice is in [FONT-LICENSE.txt](FONT-LICENSE.txt).

## Regenerate and inspect

Run from the repository root. The packages below are fixture-generation tools, not server or SQL-test dependencies.

```sh
python3 -m venv /tmp/serenedb-pdf-fixtures-venv
/tmp/serenedb-pdf-fixtures-venv/bin/pip install -r tests/sqllogic/fixtures/pdf/requirements-rendered.txt
/tmp/serenedb-pdf-fixtures-venv/bin/python tests/sqllogic/fixtures/pdf/generate_rendered.py \
  tests/sqllogic/fixtures/pdf/rendered --previews /tmp/serenedb-pdf-previews
```

ReportLab supplies the layouts, pypdf supplies the encrypted and page-box variants, and PyMuPDF checks the expected text and page structure independently of SereneDB. Encryption uses fresh randomness, so regenerated encrypted files need not be byte-identical. Review the report previews and run the SQL tests after regeneration.

To validate existing files without replacing them:

```sh
/tmp/serenedb-pdf-fixtures-venv/bin/python tests/sqllogic/fixtures/pdf/generate_rendered.py \
  tests/sqllogic/fixtures/pdf/rendered --check-only
```

## Run the SQL tests

With the locally built server listening on port 7890 and the debug test runner already built:

```sh
CARGO_TARGET_DIR="$PWD/build_pdf/sqllogic" SDB_SKIP_SQLLOGIC_BUILD=1 \
  ./tests/sqllogic/run.sh --debug=true --single-port 7890 --jobs 1 \
  --test 'tests/sqllogic/sdb/pg/simple/read_pdf*.test' \
  --test 'tests/sqllogic/sdb/pg/site_docs/pdf.test'
```

Expect six `[OK]` results: three files in each PostgreSQL wire mode. The rendered-document suite covers both page and block extraction, geometry, passwords, blank and scanned pages, Unicode paths, globbing, and storing extracted text in a table.
