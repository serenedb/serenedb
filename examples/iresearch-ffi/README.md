# Out-of-tree consumer

`consumer.c` builds independently with the installed `iresearch_ffi.h` and
`libiresearch-ffi`. It needs no include paths into the SereneDB source tree and
no C++ compiler.

Build and install the opt-in library configuration from the SereneDB root:

```bash
cmake -S . -B build_ffi -G Ninja \
  -DSDB_BUILD_IRESEARCH_ONLY=ON -DAUTO_UPDATE_MODULES=OFF \
  -DUSE_IPO=OFF -DUSE_DEBUG_INFO=NONE
cmake --build build_ffi --target iresearch-ffi -j 4
cmake --install build_ffi --prefix "$PWD/iresearch-install" --component IResearchFFI
```

Use a recent Clang compiler with C++26 support to build the library; set
`CMAKE_C_COMPILER` and `CMAKE_CXX_COMPILER` if Clang is not the default.
Initialize the required submodules first when automatic updates are disabled.
A C consumer does not need that C++ toolchain or any internal headers.

For a plain C compiler, copy `consumer.c` into a separate directory and compile
against the installation (replace `<prefix>` with its absolute path):

```bash
cc -std=c11 consumer.c -I<prefix>/include -L<prefix>/lib \
  -liresearch-ffi -Wl,-rpath,<prefix>/lib -o consumer
./consumer
```

For CMake, the installed package supplies all consumer include and link paths:

```cmake
cmake_minimum_required(VERSION 3.26)
project(consumer LANGUAGES C)
find_package(IResearchFFI CONFIG REQUIRED)
add_executable(consumer consumer.c)
target_link_libraries(consumer PRIVATE iresearch::ffi)
```

Configure this independent project with `-DCMAKE_PREFIX_PATH=<prefix>`, then
build and run it. The installation is relocatable; consumer CMake uses its
current prefix, not the SereneDB source or build tree. If you change the default
`CMAKE_INSTALL_LIBDIR`, adjust the plain compiler command accordingly.

See [the library documentation](../../docs/iresearch-library.md) for the public
API boundary, dependencies and limitations.

Expected output:

```
indexed documents: 4
  search         total=2  [row=2 score=0.339]  [row=3 score=0.320]
  lazy           total=2  [row=1 score=0.320]  [row=0 score=0.287]
  brown AND dog  total=2  [row=1 score=0.639]  [row=0 score=0.574]
  quick*         total=1  [row=0 score=1.000]
  nonexistent    total=0
```

`consumer_memory.c` is the same thing for a caller that owns its own storage:
the index is built in memory and handed over as one byte stream through a
callback, and the reader is opened from those bytes. Nothing touches the
filesystem. That is the shape a lakehouse format needs, where the index files
belong to the format and the only thing on offer is a sequential write and a
single read.

```
index never touched a file; archive handed over: 786 bytes
documents in the archive: 4
  search         total=2  [row=2 score=0.339]  [row=3 score=0.320]
  ...
```

`consumer_rows.c` numbers its documents 1000, 7, 999999, 42 and 5 -- sparse
and out of order, the way a pre-filtered batch arrives -- and gets those same
numbers back from every query. `consumer_search_types.c` walks the five query
shapes and checks the scored and unscored paths report the same totals.

`regression.c` exercises malformed archives, query semantics, output limits,
score thresholds and multiple segments using only the public C header.
`consumer_prefilter.c` additionally needs CRoaring's C headers and library to
construct the 32-bit bitmap chunks within a Roaring64Map treemap. That is an
example dependency; the installed FFI library and its public header do not
require a consumer to link CRoaring. A consumer can provide the treemap bytes
directly, as `regression.c` does.

The library exports its `irs_ffi_*` functions and nothing else, so a consumer
carrying its own zlib, OpenSSL or DuckDB does not collide with the ones linked
inside.
