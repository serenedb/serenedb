import bz2
import json
import multiprocessing
import re
import sys

PTN = re.compile("[^a-zA-Z]+")


def transform(lines):
    out = []
    for line in lines:
        try:
            doc = json.loads(line)
        except ValueError:
            continue
        if doc.get("url", "") == "":
            continue
        out.append(json.dumps({"id": doc["url"], "text": PTN.sub(" ", doc["body"].lower())}))
    return "\n".join(out) + ("\n" if out else "")


def chunks(src, size=2000):
    batch = []
    for line in src:
        batch.append(line)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def main():
    src_path, dst_path = sys.argv[1], sys.argv[2]
    with bz2.open(src_path, "rt", encoding="utf-8") as src, open(dst_path, "w", encoding="utf-8") as dst, \
            multiprocessing.Pool(16) as pool:
        for text in pool.imap(transform, chunks(src), chunksize=4):
            dst.write(text)


if __name__ == "__main__":
    main()
