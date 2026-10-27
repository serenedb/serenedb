# Installed C++ consumer

Build and install IResearch with `IRESEARCH_LIBRARY=ON` as described in [the library documentation](../../docs/iresearch-library.md). Use Clang 23 for this consumer, matching the library build.

```sh
CXX=clang++-23 cmake -S . -B build -G Ninja \
  -DCMAKE_PREFIX_PATH=/absolute/path/to/iresearch-install
cmake --build build
ctest --test-dir build --output-on-failure
```

The consumer links only `iresearch::iresearch`. It builds a memory index, releases the writer and queries its snapshot using Unicode text, phrase and prefix filters, BM25 scores with document norms, HNSW neighbors, a geographic distance filter and SQL tokenizer expressions. A separate shared library target checks that the installed archive can be embedded in a shared object.
