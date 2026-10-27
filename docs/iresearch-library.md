---
title: Embedding IResearch
split: headings
---

# Embedding IResearch

IResearch is a C++26 library for text, vector and geographic indexes. The installed CMake package provides `iresearch::iresearch`, the native C++ headers and a static library containing its dependencies. The archive is built with position independent code for use in executables and shared libraries. Consumers use the same indexing, analysis and query APIs as SereneDB.

## Build and install

Use Clang 23, CMake 3.30 or newer, Ninja, Python 3.9 or newer and Git. Initialize the required submodules or leave `SDB_UPDATE_SUBMODULES` enabled. Set `CC` and `CXX` to your Clang 23 binaries if their names differ from the commands below.

```sh
CC=clang-23 CXX=clang++-23 cmake -S . -B build_library -G Ninja \
  -DIRESEARCH_LIBRARY=ON -DSDB_LTO=OFF -DSDB_DEBUG_INFO=NONE
cmake --build build_library --target iresearch-library --parallel 2
cmake --install build_library --prefix "$PWD/iresearch-install" --component IResearch
```

`IRESEARCH_LIBRARY` defaults to `OFF`. It builds the complete IResearch library without the SereneDB server. Its DuckDB extensions are limited to ICU and `core_functions`, which supplies built-in functions for SQL tokenizers. It requires `SDB_STATIC=OFF` and cannot be combined with `IRESEARCH_BENCH`, which selects a restricted full-text feature set. With `SDB_GTEST=ON`, the build includes the IResearch unit tests without server tests.

Build the package as a top-level CMake project. Other projects consume the installation with `find_package` rather than adding the SereneDB source tree with `add_subdirectory`.

## Use from another project

```cmake
cmake_minimum_required(VERSION 3.30)
project(consumer LANGUAGES CXX)
find_package(IResearch CONFIG REQUIRED)
add_executable(consumer consumer.cpp)
target_link_libraries(consumer PRIVATE iresearch::iresearch)
```

Configure with `-DCMAKE_PREFIX_PATH=/absolute/path/to/iresearch-install`. The target supplies the headers, compile requirements and link dependencies. The installation can be moved to another directory before configuring the consumer.

Use the same Clang major version and target architecture for the library and its consumers. The application and its C++ dependencies must use the package's C++ standard library. Install each version into a clean prefix and keep its headers and archive together. The package does not promise C++ ABI or index-format compatibility between versions.

Build the example in `examples/iresearch-library` as a separate project to try text, BM25, HNSW and geographic queries against the installed package.

## Native API

Include headers by their installed paths, such as `<iresearch/index/index_writer.hpp>`, `<iresearch/index/directory_reader.hpp>` and `<iresearch/store/memory_directory.hpp>`.

Use `irs::IndexWriter` to add documents and commit batches, and `irs::DirectoryReader` to open committed indexes. A `MemoryDirectory` stores an index in memory; an `FSDirectory` stores it on disk. Readers represent snapshots, so an existing reader keeps its view when a writer commits more documents.

The native API supports multiple fields, configurable tokenizers and scorers, stored columns, phrase and Boolean queries, vector indexes and geographic filters. Applications choose their field identifiers and analyzer configuration. Query text must use the same analysis rules as the indexed field.

Initialize `irs::DuckDBEngine::Instance()` before creating indexes or readers. Keep the engine alive until all indexes, readers, directories and tokenizers have been destroyed. Call `CloseDatabases()` and `Shutdown()` only after those objects leave scope.

## Dependencies

The library requires DuckDB core, ICU text support and `core_functions` for SQL tokenizer expressions. Its complete feature set also requires FAISS, OpenBLAS, S2, FastText, Abseil, compression libraries and the other dependencies supplied by the package. A consumer does not locate or build those libraries separately.

The static package includes third-party headers and libraries. Preserve their license files when redistributing it. The operating system's C runtime and platform libraries remain external dependencies.
