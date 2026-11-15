# jemalloc-cmake

The headers here come from jemalloc's own configure, run on the submodule's tree. Regenerate them after every jemalloc update; never port template changes by hand.

1. Export the tree and generate `configure`: `git -C third_party/jemalloc archive HEAD | tar -x -C <dir>`, then `autoconf` in `<dir>`.
2. Run `CC=<our clang> CXX=<our clang++> ./configure` in `<dir>` on an x86_64 Linux host.
3. Copy `include/jemalloc/internal/jemalloc_internal_defs.h` to `include_linux_x86_64/jemalloc/internal/jemalloc_internal_defs.h.in`, and the public headers and `include/jemalloc/internal/jemalloc_preamble.h` over their copies in `include/jemalloc/`.
4. Reapply these edits:
   - `JEMALLOC_CONFIG_MALLOC_CONF` is `"@JEMALLOC_CONFIG_MALLOC_CONF@"`, filled from `CMakeLists.txt`.
   - `LG_VADDR` stays 48 on x86_64, whatever paging the generating host uses.
   - `JEMALLOC_HAVE_CLOCK_MONOTONIC_COARSE` is undefined: that clock can go backwards after `clock_adjtime`.
   - `JEMALLOC_HAVE_PROCESS_MADVISE` is defined: configure only finds it where the system headers define `PIDFD_SELF`, and jemalloc falls back to `madvise` on kernels without it.
   - `JEMALLOC_HAVE_ATTR_FORMAT_GNU_PRINTF` is undefined: it is non-standard.
   - The version macros and the `smallocx_<gid>` names keep the nominal `5.3.1-0-g0`.
5. The other architectures' internal defs get the same changes; their platform values (`LG_PAGE`, `LG_HUGEPAGE`, `LG_VADDR`, `CPU_SPINWAIT`, the CPU features) stay.
