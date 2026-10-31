# PDF fixtures

PDFs are generated under the ignored `resources/tests/pdf/` directory. Do not commit them. The SQL test runner prepares them on demand; container runners prepare them on the host before starting containers, like the Iceberg fixtures.

`basic/` covers structural boundaries and errors. `rendered/` contains nine realistic documents (26 pages) for `read_pdf_rendered.test`:

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

Run from the repository root. Like Iceberg fixtures, PDF fixtures use the active Python environment when its packages are installed, or the current checkout's CI build image through Docker. Normal test runs generate fixtures automatically; to prepare them explicitly:

```sh
./scripts/ensure_pdf_fixture.sh
```

The Docker image is the one printed by `python3 scripts/ci/ci.py image`. If it has not been published, run `bash build_images.sh local` from `scripts/ci/` to build the checkout's images, as for Iceberg. CI prepares the images automatically. No PDF-specific environment variables are required. For a Docker-free setup, install `scripts/ci/pdf-fixture-requirements.txt` in your active Python environment.

Unchanged fixtures are reused; changing a generator or dependency manifest, or removing a generated PDF, triggers regeneration. Concurrent runners share a generation lock.

PDFium independently checks expected text and page structure before the fixtures are ready. Encryption uses fresh randomness, so regenerated encrypted files need not be byte-identical. To produce optional page previews, run `generate_rendered.py` with `--previews /tmp/serenedb-pdf-previews` and a separate output directory.

Fixture tools are test-only: ReportLab (BSD), pypdf (BSD), cryptography (Apache-2.0/BSD), pikepdf (MPL-2.0, with QPDF under Apache-2.0), and pypdfium2 (Apache-2.0/BSD, with PDFium and its bundled component licenses). They are not linked into SereneDB. Keep the license files included with their packages when distributing CI images.

With the fixture packages installed locally, validate existing files without replacing them:

```sh
python3 tests/sqllogic/fixtures/pdf/generate_rendered.py \
  resources/tests/pdf/rendered --check-only
```

## Build and test locally

Run from the repository root with the [build prerequisites](../../../../CONTRIBUTING.md) installed, plus Python 3.13 and Rust/Cargo for the SQL test runner. Initialize submodules and build the server and C++ tests:

```sh
git submodule update --init --jobs 4
cmake --preset lldb
cmake --build build --target serened serenedb-tests --parallel 4
```

Use the same Python test environment as the other SQL and driver tests (`tests/drivers/python/requirements.txt`). Docker supplies missing PDF fixture packages. The shared test runner generates the fixtures and starts and stops the required server:

```sh
scripts/ci/test.py --build-dir build --suite sqllogic --jobs 1 \
  --test 'sqllogic:*:sdb/pg/simple/read_pdf*.test' \
  --test 'sqllogic:*:sdb/pg/site_docs/sql/functions/pdf.test'
scripts/ci/test.py --build-dir build --suite gtest --jobs 1 \
  --test 'gtest:serenedb-tests:PdfLayoutTest.*'
scripts/ci/test.py --build-dir build --suite drivers --jobs 1 \
  --test 'drivers:python:test_shell_pdf'
```

For an already running local server listening on port 7890:

```sh
scripts/ci/test.py --suite sqllogic --server 127.0.0.1:7890 --jobs 1 \
  --test 'sqllogic:*:sdb/pg/simple/read_pdf*.test' \
  --test 'sqllogic:*:sdb/pg/site_docs/sql/functions/pdf.test'
```

Expect six passing units: three files in each PostgreSQL wire mode. The rendered-document suite covers both page and block extraction, geometry, passwords, blank and scanned pages, Unicode paths, globbing, and storing extracted text in a table.

Check generation caching, failure cleanup, and license installation with `python3 -m pytest scripts/ci/tests/test_pdf_fixtures.py scripts/ci/tests/test_pdf_notices.py`. These tests also run through the CI-script pre-commit hook.
