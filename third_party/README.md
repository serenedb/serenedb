**Do not forget to update [LICENSES.md](LICENSES.md)!**

# Rules

**1. Fork upstream, keep patches on a version branch.** Fork into the serenedb
org and add it as a submodule. Our changes live on `vYYYY.MM.DD` branches on top
of an upstream commit, never on `main`, so what we changed stays visible and a
newer upstream can be rebased instead of hand-merged. The gitlink must be
reachable from such a branch and not older than the one on `main`;
`scripts/check_submodule_pointers.py` enforces it. `yaclib` is the exception --
it tracks its upstream remote and carries no serenedb branches.

**2. DuckDB extensions are separate submodules.** The `duckdb_*` directories sit
alongside `duckdb` rather than inside it, and are wired in through
`BUILD_EXTENSIONS` and `DUCKDB_EXTENSION_CONFIGS`. `duckdb_clickhouse` is the
reverse case: our own code, here only to sit with the extensions.

**3. Awkward upstream builds get a `<name>-cmake` directory.** When upstream's
build system is unusable for us, the submodule stays pristine and our CMakeLists
lives beside it. Nothing in the submodule is patched for build reasons, so an
update is a plain pointer bump.

**4. No submodule inside a submodule.** A recursive `git submodule update` must
produce the same tree as a non-recursive one, or the build depends on how the
checkout was made and CI stops matching a developer's tree. Remove any that a
fork introduces, on our version branch.

# Layout

59 submodules, all serenedb forks except `yaclib`.

7 wrappers: `aws-cmake`, `azure-cmake`, `jemalloc-cmake`, `libstemmer_c-cmake`,
`liburing-cmake`, `libxml2-cmake`, `openssl-cmake`. Three of them carry
generated per-arch config headers: `jemalloc-cmake`, `libxml2-cmake`,
`openssl-cmake`.

8 directories in-tree rather than submodules -- mostly historical, to be fixed:
`fastText`, `kaldi`, `llhttp`, `magic_enum`, `openfst`, `simdcomp`, `sse2neon`,
plus our own `duckdb_clickhouse`.

Anything non-obvious about a specific library is commented where it happens in
`CMakeLists.txt` -- the OpenSSL pre-seeding and its drift guard, the header-only
ICU target that avoids a `re2 -> ICU::uc -> duckdb_static -> re2` cycle, why
clickhouse-cpp is compiled by us instead of `add_subdirectory`, curl's
pre-answered `HAVE_*` probes.

# Updating

Bump the submodule pointer onto a newer version branch. Two need more.

## libstemmer_c

[Snowball](https://snowballstem.org/) stemming, carried as the generated C
distribution rather than the Snowball sources: the submodule holds the
unmodified output of `make dist_libstemmer_c`, so the build needs no Snowball
compiler, no `.sbl` sources and no Perl. Only the UTF-8 stemmers are compiled --
`make_stemmer_ptr` always passes a null charenc, which libstemmer reads as
UTF-8.

Run `scripts/update_libstemmer.sh`. It regenerates the distribution from
upstream, refreshes the source list in `libstemmer_c-cmake/CMakeLists.txt` and
commits in the submodule.

## jemalloc

Built with plain CMake instead of jemalloc's autotools `./configure`, against
config headers generated once per platform. We use jemalloc's default empty
prefix so it overrides libc `malloc`/`free` directly (serenedb links it globally
via `link_libraries(jemalloc)`), and therefore do not define
`JEMALLOC_NO_RENAME`. Linux only.

`jemalloc-cmake/include/` holds the arch-independent public headers, shared by
every arch because they all use the same empty prefix.
`include_linux_<arch>/jemalloc/internal/jemalloc_internal_defs.h.in` is the one
per-platform config header, kept as a template so the build can inject
`@JEMALLOC_CONFIG_MALLOC_CONF@`.

On a version bump, regenerate both. The common headers and `x86_64` are native;
other arches cross-generate with the Debian/Ubuntu `gcc-<arch>-linux-gnu`
toolchains.

```sh
cd third_party/jemalloc          # the submodule, at the target tag
./autogen.sh                     # produces ./configure (needs autoconf)

# x86_64 (native), and the source of the common include/ headers. Use clang:
# serenedb builds with clang and only clang's config is correct for the common
# headers -- gcc sets JEMALLOC_HAVE_ATTR_FORMAT_GNU_PRINTF, which clang rejects
# under -Wignored-attributes. The per-arch internal_defs are identical either
# way, so the cross arches below may use gcc.
./configure CC=clang CXX=clang++ --with-version=<ver>-0-g0
# harvest include/jemalloc/*.h + internal/jemalloc_preamble.h into
# jemalloc-cmake/include/, and internal/jemalloc_internal_defs.h into
# jemalloc-cmake/include_linux_x86_64/.../jemalloc_internal_defs.h.in

# other Linux arches, matching upstream LG_PAGE:
#   aarch64 : aarch64-linux-gnu     --with-lg-page=16 --with-lg-hugepage=29
#   ppc64le : powerpc64le-linux-gnu --with-lg-page=16 --with-lg-hugepage=21
#   riscv64 : riscv64-linux-gnu     --with-lg-page=16 --with-lg-hugepage=29
#   s390x   : s390x-linux-gnu       --with-lg-page=12 --with-lg-hugepage=20
make distclean
./configure --host=<triple> CC=<triple>-gcc --with-lg-page=<N> \
    --with-lg-hugepage=<M> --with-version=<ver>-0-g0
# harvest only jemalloc_internal_defs.h into the arch's dir as the .h.in
```

Then re-apply to every `jemalloc_internal_defs.h.in`:

1. `#define JEMALLOC_CONFIG_MALLOC_CONF ""` ->
   `#define JEMALLOC_CONFIG_MALLOC_CONF "@JEMALLOC_CONFIG_MALLOC_CONF@"`
   (required for the build)
2. Drop `JEMALLOC_HAVE_ATTR_FORMAT_GNU_PRINTF` -- non-standard, absent in 5.3.1.
3. Disable `JEMALLOC_HAVE_CLOCK_MONOTONIC_COARSE` -- it can go backwards after
   `clock_adjtime(ADJ_FREQUENCY)`; plain `CLOCK_MONOTONIC` is fine here.
