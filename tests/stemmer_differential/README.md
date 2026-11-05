# Stemmer differential harness

Compares two implementations word by word:

- `reference_backend`: libstemmer's public `sb_stemmer_*` API as used by Serenedb;
- `candidate_backend`: direct calls to the generated UTF-8 stemmer selected at runtime.

`--candidate broken` deliberately corrupts every result and is useful for checking that the harness catches a difference.

## Corpus

Use the official `snowball-data` repository. Pinning the commit makes runs reproducible:

```sh
git clone https://github.com/snowballstem/snowball-data.git /tmp/snowball-data
git -C /tmp/snowball-data checkout a0ec0d0a2839ec885878868de20fcb63209d92b0
```

The harness reads each language's `voc.txt`, or `voc.txt.gz` when only the compressed file exists.

## Build and run

It can be built independently, without configuring the whole database:

```sh
cmake -S tests/stemmer_differential -B /tmp/stemmer-differential-build
cmake --build /tmp/stemmer-differential-build -j
```

Run one language, all languages, or a short smoke test:

```sh
/tmp/stemmer-differential-build/serenedb-stemmer-differential --data /tmp/snowball-data --algorithm czech
/tmp/stemmer-differential-build/serenedb-stemmer-differential --data /tmp/snowball-data --algorithm all
/tmp/stemmer-differential-build/serenedb-stemmer-differential --data /tmp/snowball-data --algorithm all --max-words 1000
```

Verify the negative path:

```sh
/tmp/stemmer-differential-build/serenedb-stemmer-differential --data /tmp/snowball-data --algorithm czech --candidate broken
```

The normal candidate exits with `0` only when every result matches. A difference exits with `1` and prints the language, corpus line, input, both outputs, and their byte representations.

From the main Serenedb build, build the same target by name:

```sh
cmake --build build --target serenedb-stemmer-differential
```
