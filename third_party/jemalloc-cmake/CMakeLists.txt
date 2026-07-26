# Native CMake build of jemalloc (the ../jemalloc submodule), modelled on
# ClickHouse's contrib/jemalloc-cmake. Instead of running jemalloc's autotools
# ./configure at build time, we compile the sources against per-platform config
# headers generated once from our 5.3.1 source (see README.md for how they were
# regenerated). Linux only, and unlike ClickHouse we build with jemalloc's
# default empty prefix so it overrides libc malloc/free directly (serenedb links
# it globally) -- hence NO -DJEMALLOC_NO_RENAME.

project(jemalloc C)

set(LIBRARY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../jemalloc")

# jemalloc runtime config (malloc_conf), substituted into the config header via
# configure_file below. Overridable without a cache purge. Based on ClickHouse's
# tuned Linux server config, with duckdb's larger oversize threshold.
#   percpu_arena:percpu        cap arenas at the CPU count (vs the 4*CPU default)
#                              so idle threads don't each retain an arena's RSS.
#   oversize_threshold:64MiB   route large allocations to a dedicated arena,
#                              isolating big transient buffers/hashtables.
#   dirty_decay_ms:5000        purge dirty pages 5s after they go idle (default
#                              is 10s); muzzy_decay_ms:0 returns muzzy pages to
#                              the OS immediately, keeping RSS bounded.
#   background_thread:true     purge on jemalloc's own threads, off the free()
#                              path.
#   lg_extent_max_active_fit:8 reuse an extent for requests up to 2^8x smaller
#                              (default 2^6); helps hashtable-heavy queries.
# prof:* is intentionally omitted -- serenedb compiles prof only under
# USE_JEMALLOC_PROF, so arming it here on a non-prof build would be rejected.
set(JEMALLOC_CONFIG_MALLOC_CONF
    "percpu_arena:percpu,oversize_threshold:67108864,muzzy_decay_ms:0,dirty_decay_ms:5000,background_thread:true,lg_extent_max_active_fit:8")
set(JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE ""
    CACHE STRING "Override jemalloc's default malloc_conf string")
if(JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE)
    set(JEMALLOC_CONFIG_MALLOC_CONF "${JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE}")
endif()
message(STATUS "jemalloc malloc_conf: ${JEMALLOC_CONFIG_MALLOC_CONF}")

# The pinned submodule's src/*.c is exactly the set that goes into the static
# library (verified identical to ClickHouse's hand-maintained list), minus
# zone.c which is Darwin-only (we are Linux-only).
file(GLOB SRCS "${LIBRARY_DIR}/src/*.c")
list(REMOVE_ITEM SRCS "${LIBRARY_DIR}/src/zone.c")

add_library(jemalloc_native STATIC ${SRCS})

# Select the per-platform config header directory (Linux only).
if(NOT (CMAKE_SYSTEM_NAME STREQUAL "Linux"))
    message(FATAL_ERROR "serenedb jemalloc-cmake currently supports Linux only")
endif()
string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" _sdb_arch)
if(_sdb_arch MATCHES "x86_64|amd64")
    set(JEMALLOC_INCLUDE_PREFIX "include_linux_x86_64")
elseif(_sdb_arch MATCHES "aarch64|arm64")
    set(JEMALLOC_INCLUDE_PREFIX "include_linux_aarch64")
elseif(_sdb_arch MATCHES "ppc64le|powerpc64le")
    set(JEMALLOC_INCLUDE_PREFIX "include_linux_ppc64le")
elseif(_sdb_arch MATCHES "riscv64")
    set(JEMALLOC_INCLUDE_PREFIX "include_linux_riscv64")
elseif(_sdb_arch MATCHES "s390x")
    set(JEMALLOC_INCLUDE_PREFIX "include_linux_s390x")
else()
    message(FATAL_ERROR "jemalloc-cmake: unsupported arch '${CMAKE_SYSTEM_PROCESSOR}'")
endif()

# Substitute @JEMALLOC_CONFIG_MALLOC_CONF@ into the config header.
configure_file(
    "${JEMALLOC_INCLUDE_PREFIX}/jemalloc/internal/jemalloc_internal_defs.h.in"
    "${JEMALLOC_INCLUDE_PREFIX}/jemalloc/internal/jemalloc_internal_defs.h")

# Include order: our regenerated public headers first, then the submodule's own
# headers, then the configured per-platform internal defs.
target_include_directories(jemalloc_native SYSTEM PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_include_directories(jemalloc_native PRIVATE "${LIBRARY_DIR}/include")
target_include_directories(jemalloc_native SYSTEM PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/${JEMALLOC_INCLUDE_PREFIX}")

# We do not ship private_namespace.h, so disable the private namespace.
target_compile_definitions(jemalloc_native PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)
# for RTLD_NEXT / secure_getenv
target_compile_options(jemalloc_native PRIVATE -D_GNU_SOURCE)

# Optional heap profiling (serenedb's USE_JEMALLOC_PROF). Uses libgcc's
# _Unwind_Backtrace (no libunwind dependency), matching the historical
# --enable-prof --disable-prof-libunwind configuration.
if(USE_JEMALLOC_PROF)
    target_compile_definitions(jemalloc_native PRIVATE -DJEMALLOC_PROF=1 -DJEMALLOC_PROF_LIBGCC=1)
endif()
