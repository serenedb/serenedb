# jemalloc-cmake

Builds the `../jemalloc` submodule (serenedb/jemalloc, branch `v2026.07.21`,
based on upstream 5.3.1) with plain CMake instead of jemalloc's autotools
`./configure`, by compiling the sources against config headers generated once
per platform. Modelled on ClickHouse's `contrib/jemalloc-cmake`.

Unlike ClickHouse we use jemalloc's **default empty prefix** so it overrides
libc `malloc`/`free` directly (serenedb links it globally via
`link_libraries(jemalloc)`), and therefore do **not** define
`JEMALLOC_NO_RENAME`. Linux only.

## Layout

- `include/` — the arch-independent public headers (`jemalloc.h`,
  `jemalloc_defs.h`, `jemalloc_macros.h`, `jemalloc_protos.h`,
  `jemalloc_rename.h`, `jemalloc_typedefs.h`) plus the internal
  `jemalloc_preamble.h`. Shared by every arch because they all use the same
  (empty) prefix.
- `include_linux_<arch>/jemalloc/internal/jemalloc_internal_defs.h.in` — the one
  per-platform config header, kept as a template so the CMake build can inject
  `@JEMALLOC_CONFIG_MALLOC_CONF@`.

## Regenerating the headers (for a jemalloc version bump)

The common `include/` headers and the `x86_64` config header are generated
natively; the other Linux arches are cross-generated with the Debian/Ubuntu
cross toolchains (`gcc-<arch>-linux-gnu`).

```sh
cd third_party/jemalloc          # the submodule, at the target tag
./autogen.sh                     # produces ./configure (needs autoconf)

# x86_64 (native) — also the source of the common include/ headers:
./configure --with-version=<ver>-0-g0
# harvest include/jemalloc/*.h + include/jemalloc/internal/jemalloc_preamble.h
# into jemalloc-cmake/include/, and
# include/jemalloc/internal/jemalloc_internal_defs.h into
# jemalloc-cmake/include_linux_x86_64/.../jemalloc_internal_defs.h.in

# other Linux arches (cross), matching upstream/ClickHouse LG_PAGE:
#   aarch64 : aarch64-linux-gnu     --with-lg-page=16 --with-lg-hugepage=29
#   ppc64le : powerpc64le-linux-gnu --with-lg-page=16 --with-lg-hugepage=21
#   riscv64 : riscv64-linux-gnu     --with-lg-page=16 --with-lg-hugepage=29
#   s390x   : s390x-linux-gnu       --with-lg-page=12 --with-lg-hugepage=20
make distclean
./configure --host=<triple> CC=<triple>-gcc --with-lg-page=<N> \
    --with-lg-hugepage=<M> --with-version=<ver>-0-g0
# harvest only jemalloc_internal_defs.h into the arch's dir as the .h.in
```

Then re-apply these manual patches to every `jemalloc_internal_defs.h.in`
(the first is required for the build; the others follow ClickHouse):

1. Template the runtime config:
   `#define JEMALLOC_CONFIG_MALLOC_CONF ""` →
   `#define JEMALLOC_CONFIG_MALLOC_CONF "@JEMALLOC_CONFIG_MALLOC_CONF@"`
2. Drop the non-standard `JEMALLOC_HAVE_ATTR_FORMAT_GNU_PRINTF` (absent in 5.3.1).
3. Disable `JEMALLOC_HAVE_CLOCK_MONOTONIC_COARSE` — it can go backwards after
   `clock_adjtime(ADJ_FREQUENCY)`; plain `CLOCK_MONOTONIC` is fine here.
