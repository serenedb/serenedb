---
title: Embedding IResearch
split: headings
---

# Embedding IResearch

IResearch exposes a C interface for building and querying a full-text index
outside SereneDB. The installed public surface is `iresearch_ffi.h` and
`libiresearch-ffi`. The internal C++ headers and `iresearch-static` target are
implementation details and are not installed as a supported consumer API.

## Build and install

Use CMake 3.26 or newer, Ninja or another CMake generator, Python 3, Git, and a
Clang toolchain with C++26 support. Initialize the repository's submodules.
Library mode uses the repository's existing architecture and compiler settings;
it currently assumes SereneDB is the top-level CMake project. Consume the
installed package from other projects instead of adding the source tree with
`add_subdirectory`.

```sh
cmake -S . -B build_ffi -G Ninja \
  -DSDB_BUILD_IRESEARCH_ONLY=ON -DAUTO_UPDATE_MODULES=OFF \
  -DUSE_IPO=OFF -DUSE_DEBUG_INFO=NONE
cmake --build build_ffi --target iresearch-ffi -j 4
cmake --install build_ffi --prefix "$PWD/iresearch-install" --component IResearchFFI
```

Pass `CMAKE_C_COMPILER` and `CMAKE_CXX_COMPILER` explicitly when necessary. On
macOS, the Clang toolchain must also provide `llvm-ar` and `llvm-ranlib`. Homebrew
is one way to obtain the build tools; it is not intended as a runtime library
dependency. The build compiles its vendored libc++, libc++abi, and libunwind.
The operating system's C runtime and platform frameworks remain dependencies.
Windows has not been validated.

`SDB_BUILD_IRESEARCH_ONLY` defaults to `OFF`, preserving the full server build.
When enabled, it skips server targets, server installation, server tests, and
DuckDB's shell and tests. It uses the checked-in Lucene parser sources without
running grammar generators. `--component IResearchFFI` installs only the public
header, shared library and CMake package even when used with a full build.

## Dependencies and boundary

DuckDB core remains a required implementation dependency; library mode builds
zero DuckDB extensions, including no `core_functions`, Parquet or ICU extension.
IResearch uses DuckDB's generated Unicode text, property and collation code,
which the library compiles separately from the ICU extension. The HTML tokenizer's
entity lookup table is likewise compiled separately from the vendored `inet`
sources, without building the `inet` extension. No system ICU is needed.

The current IResearch target includes text, vector and geographic code. Its
dependencies therefore include FAISS with vendored OpenBLAS, S2, FastText,
Abseil, Boost headers, RE2, Roaring, SIMD libraries, compression libraries,
Snowball and YACLib. These are retained because the target still contains the
corresponding implementations. This mode does not yet offer independent
full-text, vector and geographic components. Connector SDKs, HTTP clients,
server documentation rendering and benchmark frameworks are excluded.

The C interface hides internal C++ types and exports only `irs_ffi_*` functions.
Consumers do not link DuckDB or the other implementation libraries separately.
The package intentionally has no advertised version compatibility guarantee;
the header and library should come from the same build.

## Consume the installed package

In a separate project:

```cmake
cmake_minimum_required(VERSION 3.26)
project(consumer LANGUAGES C)
find_package(IResearchFFI CONFIG REQUIRED)
add_executable(consumer consumer.c)
target_link_libraries(consumer PRIVATE iresearch::ffi)
```

Configure with `-DCMAKE_PREFIX_PATH=/absolute/path/to/iresearch-install`.
The exported target provides the installed include directory and library path.
See [the external consumer examples](https://github.com/serenedb/serenedb/tree/main/examples/iresearch-ffi) for
plain compiler commands and executable examples.

## API lifecycle

Index creation and reader opening initialize the process-wide engine implicitly.
Calling `irs_ffi_init` explicitly is optional. Create a directory-backed or
in-memory index, add text with caller-supplied row numbers, and commit before
opening readers. `irs_ffi_index_add` assigns sequential row numbers starting at
zero; `irs_ffi_index_add_row` accepts sparse, unordered caller row numbers.

Close every reader with `irs_ffi_reader_close` and every index with
`irs_ffi_index_close` before calling `irs_ffi_shutdown`. Shutdown is terminal:
the engine cannot be initialized again in that process. Handles are owned by
the caller and must not be used after closing. Input text, paths, queries and
prefilter buffers are borrowed for the duration of the call. The memory reader
copies the archive bytes, so the caller may release them after
`irs_ffi_reader_open_bytes` returns. A serialization write callback receives
borrowed data valid only until that callback returns; copy it to retain it.
Return zero from the callback to continue or nonzero to abort serialization.
If the writer reports a failed transaction commit, the index rejects further
additions, commits and serialization. Close that handle and rebuild the index.

Initialization, document addition, commit and serialization return zero on
success and `-1` on error. Handle creation and reader opening return a null
pointer on error. Document counts and searches return `-1` on error. Use
`irs_ffi_error` for the current thread's last error message; its returned string
is borrowed and may change on the next API call on that thread.

## Query semantics

The interface indexes one text field with a fixed lowercase Unicode text
tokenizer. Analyzer configuration and additional fields are not exposed.
Scored queries use default BM25 parameters `k1 = 1.2` and `b = 0.75`, with stored
document norms for length normalization. Scores should not be compared directly
with engines using a different analyzer or BM25 configuration.

`irs_ffi_search` accepts Lucene query syntax. The typed APIs accept match-all,
match-any, phrase, prefix and wildcard query shapes. Wildcard patterns use `*`
and `?`; SQL wildcard characters `%` and `_` are treated as literal characters.

A typed search returns the total eligible match count, including matches that
did not fit the output buffer. It writes at most
`min(total, limit, hits_len)` hits; a C API `limit` of zero means unlimited, so
the bound becomes `min(total, hits_len)`. This differs from the Paimon adapter:
an explicitly supplied Paimon limit of zero returns an empty result. The plain
Lucene query API writes at most `min(total, hits_len)` hits. A null output pointer
with `hits_len = 0` can be used to obtain only the count.

Pass NaN as `min_score` to leave the threshold unset. A set threshold is strict:
only scores greater than `min_score` qualify. This also applies to unscored
results, which require scoring internally when a threshold is set; the returned
scores are zero when `with_score` is false. Prefilter and score threshold checks
both affect the returned total.

The filtered API expects the exact CRoaring C++ `roaring::Roaring64Map` treemap
serialization produced by `write(buffer, true)`: a 64-bit map-entry count,
followed by each 32-bit high key and its portable serialized 32-bit Roaring
bitmap. It does not accept the CRoaring C `roaring64_bitmap_portable_serialize`
format. Use the same layout in a foreign-language producer and verify byte order
and compatibility. A null prefilter pointer with zero length means no prefilter.

Check analyzer behavior and index-format compatibility when upgrading the
library. See the external consumer examples for serialization, sparse row
numbers, typed queries and prefilters.
