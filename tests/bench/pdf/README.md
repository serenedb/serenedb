# PDF reader evaluation

`corpus.json` pins three public US federal documents mirrored by pdfplumber: 17 pages, four ruled tables, and 13 pages without tables as negative controls. The rendered documents were inspected for table presence, and 25 cells were transcribed by hand. This small sample does not establish general PDF accuracy. Files are downloaded separately and are not redistributed in the repository.

Table precision and recall compare table counts per page. They do not measure bounding-box overlap. Cell checks require the annotated grid position and exact text after removing whitespace; they report a sample match count, not exhaustive cell precision. The NICS table has horizontal rules around groups of states, so each group is one multiline cell. The Senate document's body has no interior vertical rules, so its body is represented by merged cells. An extractor that infers extra rows or columns from whitespace can differ from this ground truth.

The 2026-11-10 component evaluation on macOS arm64, using the PoDoFo reader components compiled with Clang 23, found all four tables with no extra detections (precision 1.00, recall 1.00) and matched 25/25 sampled cells. Community `pdf` at `6535c81` in DuckDB 1.5.5 found four tables and eight extra detections (precision 0.33, recall 1.00), matching 6/25 sampled cells at the annotated grid positions. These results use the same documents but different extraction policies; they do not establish exhaustive cell accuracy. The candidate results were captured from the extraction components, not SereneDB SQL. Server validation, real non-Latin document evaluation, comparative speed, and installed dependency-size measurements remain outstanding.

Use a local server built with the `perf` preset, on an idle machine, and a separate Python environment with `duckdb==1.5.5` and `psycopg[binary]`. Install the community `pdf` extension into that environment beforehand. The default baseline is extension commit `6535c81`; a different version requires an explicit argument and must be recorded with the results. The GPL baseline remains an external evaluation tool and is not linked into SereneDB.

```sh
python scripts/perf/pdf_readers.py \
  --dsn 'host=127.0.0.1 port=5432 user=postgres dbname=postgres' \
  --corpus /tmp/pdf-corpus \
  --extension-directory /tmp/pdf-baseline-extensions \
  --out /tmp/pdf-comparison.json
```

Both readers use one thread, one warmup, and five measured scans per document. Times include fetching every result row; SereneDB includes PostgreSQL wire overhead. Downloads and extension loading are outside the measurements. Record the server commit, build flags, CPU/load, and installed dependency sizes alongside the JSON report. `--candidate-results` accepts captured rows for accuracy-only investigation; it cannot substantiate server correctness or performance.
