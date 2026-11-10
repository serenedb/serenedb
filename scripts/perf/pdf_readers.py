import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import platform
import statistics
import time
import urllib.request


ROOT = Path(__file__).resolve().parents[2]


def normalize(rows):
    tables = {}
    for page, table, row, cells in rows:
        tables.setdefault((page, table), []).append((row, cells))
    result = []
    page_tables = Counter()
    for (page, _), values in sorted(tables.items()):
        page_tables[page] += 1
        for index, (_, cells) in enumerate(sorted(values), 1):
            result.append([page, page_tables[page], index, cells])
    return result


def evaluate(documents, results):
    true_positive = false_positive = false_negative = matched_cells = total_cells = 0
    mismatches = []
    for document in documents:
        rows = normalize(results[document["name"]]["rows"])
        predicted = Counter(page for page, _ in {(row[0], row[1]) for row in rows})
        expected = {int(page): count for page, count in document["tables"].items()}
        for page in predicted.keys() | expected.keys():
            true_positive += min(predicted[page], expected.get(page, 0))
            false_positive += max(0, predicted[page] - expected.get(page, 0))
            false_negative += max(0, expected.get(page, 0) - predicted[page])
        cells = {(page, table, row, col): text or ""
                 for page, table, row, values in rows for col, text in enumerate(values, 1)}
        for page, table, row, col, expected_text in document["cells"]:
            total_cells += 1
            actual = cells.get((page, table, row, col))
            if actual is not None and "".join(actual.split()) == "".join(expected_text.split()):
                matched_cells += 1
            else:
                mismatches.append({"file": document["name"], "cell": [page, table, row, col],
                                   "expected": expected_text, "actual": actual})
    return {
        "table_true_positive": true_positive,
        "table_false_positive": false_positive,
        "table_false_negative": false_negative,
        "table_precision": true_positive / (true_positive + false_positive) if true_positive + false_positive else None,
        "table_recall": true_positive / (true_positive + false_negative) if true_positive + false_negative else None,
        "sampled_cells_matched": matched_cells,
        "sampled_cells_total": total_cells,
        "cell_mismatches": mismatches,
    }


def fetch_corpus(manifest, directory):
    directory.mkdir(parents=True, exist_ok=True)
    for document in manifest["documents"]:
        path = directory / document["name"]
        if not path.exists():
            with urllib.request.urlopen(manifest["source"] + document["name"], timeout=60) as response:
                data = response.read()
            if hashlib.sha256(data).hexdigest() != document["sha256"]:
                raise ValueError(f"Downloaded corpus hash mismatch: {path.name}")
            path.write_bytes(data)
        if hashlib.sha256(path.read_bytes()).hexdigest() != document["sha256"]:
            raise ValueError(f"Corpus hash mismatch: {path.name}")


def measure(execute, documents, directory, repetitions):
    results = {}
    for document in documents:
        path = str((directory / document["name"]).resolve())
        execute(path)
        timings = []
        for _ in range(repetitions):
            start = time.perf_counter()
            rows = execute(path)
            timings.append(time.perf_counter() - start)
        results[document["name"]] = {"rows": rows, "seconds": timings,
                                     "median_seconds": statistics.median(timings)}
    return results


def main():
    parser = argparse.ArgumentParser()
    candidate = parser.add_mutually_exclusive_group(required=True)
    candidate.add_argument("--dsn", help="Local SereneDB PostgreSQL connection string")
    candidate.add_argument("--candidate-results", type=Path, help="Previously captured rows; no timing comparison")
    parser.add_argument("--manifest", type=Path, default=ROOT / "tests/bench/pdf/corpus.json")
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--extension-directory", type=Path, required=True)
    parser.add_argument("--baseline-version", default="6535c81")
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    manifest = json.loads(args.manifest.read_text())
    fetch_corpus(manifest, args.corpus)
    import duckdb
    with duckdb.connect(config={"extension_directory": str(args.extension_directory)}) as baseline:
        baseline.execute("LOAD pdf")
        version = baseline.execute(
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='pdf'"
        ).fetchone()[0]
        if version != args.baseline_version:
            raise ValueError(f"Expected baseline {args.baseline_version}, got {version}")
        baseline.execute("SET threads=1")
        baseline_results = measure(
            lambda path: baseline.execute(
                "SELECT page, table_index, row_index, cells FROM read_pdf_tables(?, auto_ocr := false)", [path]
            ).fetchall(), manifest["documents"], args.corpus, args.repetitions,
        )
    if args.dsn:
        import psycopg
        with psycopg.connect(args.dsn, autocommit=True) as connection:
            connection.execute("SET statement_timeout = '60s'")
            connection.execute("SET threads=1")
            candidate_results = measure(
                lambda path: connection.execute(
                    "SELECT page, table_index, row_index, cells FROM read_pdf_tables(%s)", (path,)
                ).fetchall(), manifest["documents"], args.corpus, args.repetitions,
            )
    else:
        candidate_results = json.loads(args.candidate_results.read_text())
    report = {
        "platform": platform.platform(), "baseline_duckdb": duckdb.__version__, "baseline_pdf": version,
        "candidate_validation": "server SQL" if args.dsn else "supplied rows; not server validation",
        "normalization": "one-based table/row indexes; cell comparison ignores whitespace only",
        "table_metric": "per-page table counts; not bounding-box IoU or exhaustive cell precision",
        "candidate": {"evaluation": evaluate(manifest["documents"], candidate_results), "documents": candidate_results},
        "baseline": {"evaluation": evaluate(manifest["documents"], baseline_results), "documents": baseline_results},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: report[key]["evaluation"] for key in ("candidate", "baseline")}, indent=2))


if __name__ == "__main__":
    main()
