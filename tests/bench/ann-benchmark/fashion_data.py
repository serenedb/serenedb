"""
Downloads Fashion-MNIST and converts each image into a normalized float vector.

Each record:
{
  "id": 0,                                    # sample index
  "label": 9,                                 # class index (0-9)
  "class": "Ankle boot",                      # human-readable class name
  "vector": [0.0, 0.031, 0.514, ..., 0.0]   # 784 floats in [0.0, 1.0]
}
"""

import json
import struct
import gzip
import urllib.request
from pathlib import Path

# --- Config ---
OUTPUT_DIR   = Path("fashion_mnist_json")
MAX_SAMPLES  = None   # Set to e.g. 500 to cap output; None = all
ROUND_DIGITS = 4      # Decimal places for float values

FASHION_MNIST_FILES = {
    "train_images": "train-images-idx3-ubyte.gz",
    "train_labels": "train-labels-idx1-ubyte.gz",
    "test_images":  "t10k-images-idx3-ubyte.gz",
    "test_labels":  "t10k-labels-idx1-ubyte.gz",
}
BASE_URL = "https://github.com/zalandoresearch/fashion-mnist/raw/master/data/fashion/"

CLASS_NAMES = [
    "T-shirt/top",
    "Trouser",
    "Pullover",
    "Dress",
    "Coat",
    "Sandal",
    "Shirt",
    "Sneaker",
    "Bag",
    "Ankle boot",
]


def download(filename: str, dest: Path) -> Path:
    out = dest / filename
    if out.exists():
        print(f"  Skipping {filename} (already exists)")
        return out
    print(f"  Downloading {filename}...")
    urllib.request.urlretrieve(BASE_URL + filename, out)
    return out


def load_images(path: Path) -> list[list[float]]:
    with gzip.open(path, "rb") as f:
        magic, n, rows, cols = struct.unpack(">IIII", f.read(16))
        assert magic == 2051, "Not an MNIST image file"
        raw = f.read()
    size = rows * cols
    return [
        [round(raw[i * size + j] / 255.0, ROUND_DIGITS) for j in range(size)]
        for i in range(n)
    ]


def load_labels(path: Path) -> list[int]:
    with gzip.open(path, "rb") as f:
        magic, n = struct.unpack(">II", f.read(8))
        assert magic == 2049, "Not an MNIST label file"
        return list(f.read())


def main():
    raw_dir = OUTPUT_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print("Downloading Fashion-MNIST files...")
    paths = {k: download(v, raw_dir) for k, v in FASHION_MNIST_FILES.items()}

    for split in ("train", "test"):
        print(f"\nProcessing {split} split...")
        images = load_images(paths[f"{split}_images"])
        labels = load_labels(paths[f"{split}_labels"])

        pairs = list(zip(images, labels))
        if MAX_SAMPLES:
            pairs = pairs[:MAX_SAMPLES]

        records = [
            {"id": idx, "label": lbl, "class": CLASS_NAMES[lbl], "vector": vec}
            for idx, (vec, lbl) in enumerate(pairs)
        ]

        out_path = OUTPUT_DIR / f"fashion_mnist_{split}.json"
        with open(out_path, "w") as f:
            json.dump(records, f, indent=2)

        print(f"  Saved {len(records)} records → {out_path}")

    # Preview
    sample = records[0]
    nonzero = sum(1 for v in sample["vector"] if v > 0)
    print(f"\nExample record:")
    print(f"  id    : {sample['id']}")
    print(f"  label : {sample['label']}")
    print(f"  class : {sample['class']}")
    print(f"  vector: [{sample['vector'][0]}, {sample['vector'][1]}, ..., {sample['vector'][-1]}]")
    print(f"  dim   : {len(sample['vector'])}  |  non-zero: {nonzero}")


if __name__ == "__main__":
    main()
