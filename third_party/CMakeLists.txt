# ------------------------------------------------------------------------------
# External Projects used by SereneDB
# ------------------------------------------------------------------------------

foreach(lang C CXX)
    string(
        APPEND CMAKE_${lang}_FLAGS
        " -Wall -Wextra -Wthread-safety -Wno-deprecated -Wno-missing-field-initializers -Wno-sign-compare -Wno-unused -Wno-unused-parameter"
    )
endforeach()

sdb_update_module(sqllogictest-rs stringzilla)

################################################################################
## JEMALLOC
################################################################################
if(SDB_ALLOC STREQUAL "JE")
    # jemalloc source is the third_party/jemalloc submodule; jemalloc-cmake
    # compiles it natively (no autotools) into the `jemalloc_native` target.
    sdb_add_module(jemalloc DIR jemalloc-cmake NO_WARNINGS)
    # Expose it as an IMPORTED target: it is linked globally, so it ends up in
    # the link interface of vendored libs that export() their targets (croaring),
    # and export() tolerates IMPORTED but not in-tree targets.
    add_library(jemalloc STATIC IMPORTED GLOBAL)
    set_target_properties(
        jemalloc
        PROPERTIES
            IMPORTED_LOCATION
                "${CMAKE_CURRENT_BINARY_DIR}/jemalloc-cmake/libjemalloc_native.a"
            INTERFACE_INCLUDE_DIRECTORIES
                "${CMAKE_CURRENT_SOURCE_DIR}/jemalloc-cmake/include"
    )
    add_dependencies(jemalloc jemalloc_native)
    link_libraries(jemalloc)
endif()

###############################################################################
## simdutf (SIMD UTF-8 validation + base64)
################################################################################

set(SIMDUTF_TESTS OFF CACHE BOOL "" FORCE)
set(SIMDUTF_TOOLS OFF CACHE BOOL "" FORCE)
set(SIMDUTF_ICONV OFF CACHE BOOL "" FORCE)
set(SIMDUTF_CXX_STANDARD ${CMAKE_CXX_STANDARD} CACHE STRING "" FORCE)
sdb_add_module(simdutf)
if(SDB_SANITIZE MATCHES "^Memory")
    target_compile_definitions(simdutf PUBLIC SIMDUTF_IMPLEMENTATION_ICELAKE=0)
endif()

################################################################################
## fast_float
################################################################################

set(FASTFLOAT_INSTALL OFF CACHE BOOL "" FORCE)
sdb_add_module(fast_float)

###############################################################################
## simdjson
################################################################################

set(SIMDJSON_ENABLE_THREADS OFF CACHE BOOL "" FORCE)
set(SIMDJSON_CXX_STANDARD ${CMAKE_CXX_STANDARD} CACHE STRING "" FORCE)
sdb_add_module(simdjson)

################################################################################
## YACLib
################################################################################

set(YACLIB_FLAGS "CORO")
if(SDB_DEV)
    set(YACLIB_LOG "DEBUG")
else()
    set(YACLIB_LOG "")
endif()
sdb_add_module(yaclib)

################################################################################
## openssl
################################################################################

sdb_add_module(openssl DIR openssl-cmake)
set(OPENSSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIR} PARENT_SCOPE)
set(OPENSSL_LIBRARIES ${OPENSSL_LIBRARIES} PARENT_SCOPE)

# Repeating the trick done by NuRaft that helped our build survive folly.
# Pre-seed the cache variables that cmake's FindOpenSSL.cmake consumes so any
# nested find_package(OpenSSL) (folly, curl, ...) short-circuits onto our vendored
# OpenSSL instead of silently linking against the system libssl/libcrypto and
# producing an ABI-mismatched binary. OPENSSL_INCLUDE_DIR was already set above
# by openssl-cmake's PARENT_SCOPE export; we only need to promote it to CACHE.
# OPENSSL_LIBRARIES is exported as the target names OpenSSL::{Crypto,SSL}, but
# FindOpenSSL's find_library() short-circuit needs file paths, so spell those
# out here.
set(OPENSSL_INCLUDE_DIR "${OPENSSL_INCLUDE_DIR}" CACHE PATH "" FORCE)
set(OPENSSL_CRYPTO_LIBRARY
    "${CMAKE_BINARY_DIR}/third_party/openssl-cmake/libcrypto.a"
    CACHE FILEPATH
    ""
    FORCE
)
set(OPENSSL_SSL_LIBRARY
    "${CMAKE_BINARY_DIR}/third_party/openssl-cmake/libssl.a"
    CACHE FILEPATH
    ""
    FORCE
)

# Snapshot the pre-seeded values so we can verify at the end of this file that
# no later add_subdirectory() has silently re-pointed OpenSSL at a system copy.
set(_sdb_expected_OPENSSL_INCLUDE_DIR "${OPENSSL_INCLUDE_DIR}")
set(_sdb_expected_OPENSSL_CRYPTO_LIBRARY "${OPENSSL_CRYPTO_LIBRARY}")
set(_sdb_expected_OPENSSL_SSL_LIBRARY "${OPENSSL_SSL_LIBRARY}")

################################################################################
## ZLIB-NG
################################################################################

set(WITH_GZFILEOP OFF CACHE BOOL "" FORCE)
set(ZLIB_COMPAT ON CACHE BOOL "" FORCE)
set(ZLIB_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
set(ZLIBNG_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_GTEST OFF CACHE BOOL "" FORCE)
set(WITH_OPTIM ON CACHE BOOL "" FORCE)
set(WITH_REDUCED_MEM OFF CACHE BOOL "" FORCE)
set(WITH_NEW_STRATEGIES OFF CACHE BOOL "" FORCE)
set(WITH_NATIVE_INSTRUCTIONS OFF CACHE BOOL "" FORCE)
set(WITH_RUNTIME_CPU_DETECTION ON CACHE BOOL "" FORCE)
set(WITH_INFLATE_STRICT OFF CACHE BOOL "" FORCE)
set(WITH_INFLATE_ALLOW_INVALID_DIST OFF CACHE BOOL "" FORCE)
set(SKIP_INSTALL_ALL ON CACHE BOOL "" FORCE)
set(WITH_SANITIZER "OFF" CACHE STRING "" FORCE)

sdb_add_module(zlib-ng)

# For compatibility
add_library(ZLIB::ZLIB ALIAS zlib-ng)
set(ZLIB_FOUND ON CACHE BOOL "" FORCE)
set(ZLIB_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/zlib-ng" CACHE PATH "" FORCE)
set(ZLIB_LIBRARY zlib-ng CACHE STRING "" FORCE)

################################################################################
## BOOST
################################################################################

sdb_add_module(boost)
add_library(Boost::headers ALIAS boost_boost)

################################################################################
### text, collation, datetime and property headers of duckdb's icu extension
################################################################################

add_library(sdb_icu INTERFACE)
target_include_directories(
    sdb_icu
    INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/extension/icu/include
        ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/extension/icu/collation/include
        ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/extension/icu/datetime/include
        ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/extension/icu/properties/include
        ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/extension/icu/text/include
)

add_library(icu ALIAS sdb_icu)
add_library(ICU::uc ALIAS sdb_icu)

################################################################################
### abseil-cpp library
################################################################################

set(ABSL_PROPAGATE_CXX_STD OFF CACHE BOOL "" FORCE)
set(ABSL_USE_SYSTEM_INCLUDES ON CACHE BOOL "" FORCE)
sdb_add_module(abseil-cpp NO_WARNINGS)
target_link_libraries(absl_strings PUBLIC simdutf)

################################################################################
### zxc library -- lossless compression built for fast decode, an HTTP
### response content-coding
################################################################################

set(ZXC_BUILD_CLI OFF CACHE BOOL "" FORCE)
set(ZXC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZXC_INSTALL OFF CACHE BOOL "" FORCE)
set(ZXC_NATIVE_ARCH OFF CACHE BOOL "" FORCE)
set(ZXC_ENABLE_LTO OFF CACHE BOOL "" FORCE)
sdb_add_module(zxc)

################################################################################
### protozero library -- header-only protobuf wire format, used for OTLP
################################################################################

sdb_update_module(protozero)
add_library(protozero INTERFACE)
target_include_directories(
    protozero
    INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/protozero/include"
)

################################################################################
### geographiclib-c library -- geodesic computations, used by duckdb_spatial
################################################################################

sdb_update_module(geographiclib-c)
add_library(
    geographiclib
    STATIC
    "${CMAKE_CURRENT_SOURCE_DIR}/geographiclib-c/src/geodesic.c"
)
target_include_directories(
    geographiclib
    INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/geographiclib-c/src"
)

################################################################################
### re2 library
################################################################################

set(RE2_USE_ICU ON CACHE BOOL "" FORCE)
set(RE2_INSTALL OFF CACHE BOOL "" FORCE)
set(RE2_TEST ${SDB_GTEST} CACHE BOOL "" FORCE)
if(SDB_GTEST)
    file(WRITE ${CMAKE_FIND_PACKAGE_REDIRECTS_DIR}/gtest-config.cmake "")
    enable_testing()
endif()
sdb_add_module(re2 COVERAGE)
if(SDB_GTEST)
    get_property(re2_targets DIRECTORY re2 PROPERTY BUILDSYSTEM_TARGETS)
    list(FILTER re2_targets INCLUDE REGEX "_test$")
    foreach(target ${re2_targets})
        target_link_libraries(${target} PRIVATE icu_extension)
    endforeach()
    add_custom_target(re2-tests DEPENDS ${re2_targets})
endif()

################################################################################
### tcmalloc library
################################################################################

if(SDB_ALLOC STREQUAL "TC")
    sdb_add_module(tcmalloc)
endif()

################################################################################
### googletest library
################################################################################

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(GTEST_HAS_ABSL ON CACHE BOOL "" FORCE)
sdb_add_module(googletest NO_WARNINGS)

################################################################################
### benchmark library
################################################################################

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_CXX_STANDARD ${CMAKE_CXX_STANDARD} CACHE STRING "" FORCE)
sdb_add_module(benchmark)

################################################################################
### sse2neon library -- need this after abseil and highway
################################################################################

add_library(sse2neon INTERFACE)
if(ARCH_AARCH64)
    target_include_directories(
        sse2neon
        INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/sse2neon"
    )
endif()

################################################################################
### fastText library
################################################################################

# TODO(mbkkt) make it submodule
add_subdirectory(fastText)

################################################################################
### BLAS
################################################################################
add_library(sdb_blas INTERFACE)
add_library(sdb::blas ALIAS sdb_blas)

set(USE_THREAD 0 CACHE BOOL "" FORCE)
set(USE_OPENMP 0 CACHE BOOL "" FORCE)
set(USE_LOCKING 1 CACHE BOOL "" FORCE)
set(NO_WARMUP 1 CACHE BOOL "" FORCE)
set(NO_AFFINITY 1 CACHE BOOL "" FORCE)
set(NO_LAPACK 0 CACHE BOOL "" FORCE)
set(C_LAPACK 1 CACHE BOOL "" FORCE)
set(BUILD_SINGLE 1 CACHE BOOL "" FORCE)
set(NO_CBLAS 1 CACHE BOOL "" FORCE)
set(CMAKE_Fortran_COMPILER NOTFOUND CACHE FILEPATH "" FORCE)
if(SDB_SANITIZE MATCHES "^Memory")
    message(
        STATUS
        "OpenBLAS: MSan build, building the generic C kernels instead of the assembly"
    )
    set(DYNAMIC_ARCH OFF CACHE BOOL "" FORCE)
    set(TARGET GENERIC CACHE STRING "" FORCE)
else()
    set(DYNAMIC_ARCH ON CACHE BOOL "" FORCE)
    if(ARCH_AARCH64)
        set(TARGET ARMV8 CACHE STRING "" FORCE)
        set(DYNAMIC_LIST
            "NEOVERSEN1;NEOVERSEV1;NEOVERSEN2;ARMV8SVE"
            CACHE STRING
            ""
            FORCE
        )
    elseif(ARCH_AMD64)
        set(TARGET HASWELL CACHE STRING "" FORCE)
        set(DYNAMIC_LIST
            "HASWELL;ZEN;SKYLAKEX;COOPERLAKE;SAPPHIRERAPIDS"
            CACHE STRING
            ""
            FORCE
        )
    endif()
endif()
sdb_add_module(OpenBLAS NO_WARNINGS)
target_link_libraries(sdb_blas INTERFACE openblas)
if(ARCH_AMD64 AND DYNAMIC_ARCH)
    foreach(_core SKYLAKEX COOPERLAKE SAPPHIRERAPIDS)
        target_compile_options(kernel_${_core} PRIVATE $<$<CONFIG:Debug>:-O2>)
    endforeach()
endif()

################################################################################
### pcg, header-only, vendored by duckdb
################################################################################
add_library(pcg INTERFACE)
target_include_directories(
    pcg
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/pcg
)

################################################################################
### faiss library
################################################################################
set(FAISS_OPT_LEVEL "dd")
set(FAISS_ENABLE_GPU OFF CACHE BOOL "" FORCE)
set(FAISS_ENABLE_PYTHON OFF CACHE BOOL "" FORCE)
set(FAISS_ENABLE_EXTRAS OFF CACHE BOOL "" FORCE)
sdb_add_module(faiss)
set_property(TARGET faiss PROPERTY INTERFACE_COMPILE_OPTIONS "")
target_link_libraries(faiss PUBLIC pcg)

################################################################################
## snowball stemmer
################################################################################

sdb_add_module(libstemmer_c DIR libstemmer_c-cmake)

################################################################################
## LZ4
################################################################################

set(LZ4_BUILD_CLI OFF CACHE BOOL "" FORCE)
set(LZ4_POSITION_INDEPENDENT_LIB OFF CACHE BOOL "" FORCE)
sdb_add_module(lz4 DIR lz4/build/cmake)

################################################################################
## ZSTD
################################################################################

set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ZSTD_INSTALL OFF CACHE BOOL "" FORCE)
sdb_add_module(zstd DIR zstd/build/cmake)
add_library(zstd::zstd ALIAS libzstd_static)

################################################################################
## SNAPPY
################################################################################

set(SNAPPY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(SNAPPY_INSTALL OFF CACHE BOOL "" FORCE)
sdb_add_module(snappy)

################################################################################
## BROTLI
################################################################################

set(BROTLI_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(BROTLI_DISABLE_TESTS ON CACHE BOOL "" FORCE)
set(BROTLI_BUNDLED_MODE ON CACHE BOOL "" FORCE)
sdb_add_module(brotli)

################################################################################
## Google S2
################################################################################

set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(S2_USE_SYSTEM_INCLUDES ON CACHE BOOL "" FORCE)
sdb_add_module(s2geometry)

################################################################################
# llhttp
################################################################################

set(LLHTTP_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/llhttp/src/api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/llhttp/src/http.c
    ${CMAKE_CURRENT_SOURCE_DIR}/llhttp/src/llhttp.c
)

add_library(llhttp STATIC ${LLHTTP_SOURCES})

target_include_directories(
    llhttp
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/llhttp/include"
)

################################################################################
## CURL (required by DuckDB's httpfs extension for S3 I/O)
################################################################################

# Force static HTTP(S)-only build, using our vendored OpenSSL.
set(BUILD_CURL_EXE OFF CACHE BOOL "" FORCE)
set(CURL_DISABLE_INSTALL ON CACHE BOOL "" FORCE)
set(CURL_ENABLE_EXPORT_TARGET OFF CACHE BOOL "" FORCE)
set(HTTP_ONLY ON CACHE BOOL "" FORCE)
# nghttp2 was removed from third_party; force HTTP/2 off so curl does not pick
# up a system libnghttp2.
set(USE_NGHTTP2 OFF CACHE BOOL "" FORCE)
set(CURL_USE_OPENSSL ON CACHE BOOL "" FORCE)
set(CURL_USE_LIBPSL OFF CACHE BOOL "" FORCE)
set(CURL_ZSTD OFF CACHE STRING "" FORCE)
set(CURL_BROTLI OFF CACHE STRING "" FORCE)
set(CURL_USE_LIBSSH2 OFF CACHE BOOL "" FORCE)
set(CURL_USE_LIBSSH OFF CACHE BOOL "" FORCE)
set(USE_LIBIDN2 OFF CACHE BOOL "" FORCE)
set(ENABLE_UNIX_SOCKETS OFF CACHE BOOL "" FORCE)
set(PICKY_COMPILER OFF CACHE BOOL "" FORCE)
set(CURL_ENABLE_SSL ON CACHE BOOL "" FORCE)
# Pre-answer all of curl's check_symbol_exists() tests that link OpenSSL.
# Those tests pass OpenSSL::SSL / OpenSSL::Crypto via CMAKE_REQUIRED_LIBRARIES,
# but those are ALIAS targets from our vendored OpenSSL and don't propagate
# into a fresh try_compile subproject. We know the flavor and version of the
# OpenSSL we link, so pre-answer and skip the detection.
# Keep this list in sync with curl's CMakeLists.txt if curl is bumped.
set(HAVE_BORINGSSL 0 CACHE INTERNAL "")
set(HAVE_AWSLC 0 CACHE INTERNAL "")
set(HAVE_LIBRESSL 0 CACHE INTERNAL "")
set(HAVE_DES_ECB_ENCRYPT 1 CACHE INTERNAL "")
set(HAVE_SSL_SET0_WBIO 1 CACHE INTERNAL "")
set(HAVE_OPENSSL_SRP 0 CACHE INTERNAL "")
set(HAVE_SSL_SET_QUIC_TLS_CBS 0 CACHE INTERNAL "")
set(HAVE_SSL_SET_QUIC_USE_LEGACY_CODEPOINT 0 CACHE INTERNAL "")
set(HAVE_SSL_SET1_ECH_CONFIG_LIST 0 CACHE INTERNAL "")

sdb_add_module(curl)

# Expose curl to downstream find_package(CURL) consumers (duckdb/httpfs).
# FindCURL.cmake short-circuits when these variables are already set.
if(NOT TARGET CURL::libcurl)
    add_library(CURL::libcurl ALIAS libcurl_static)
endif()
set(CURL_FOUND TRUE CACHE INTERNAL "")
set(CURL_INCLUDE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/curl/include"
    CACHE INTERNAL
    ""
)
set(CURL_INCLUDE_DIRS
    "${CMAKE_CURRENT_SOURCE_DIR}/curl/include"
    CACHE INTERNAL
    ""
)
set(CURL_LIBRARIES CURL::libcurl CACHE INTERNAL "")
set(CURL_LIBRARY CURL::libcurl CACHE INTERNAL "")

################################################################################
## clickhouse-cpp (submodule; compiled into our own static lib; do NOT
## add_subdirectory its CMakeLists -- that bundles contrib/{absl,lz4,zstd}
## and collides with ours)
################################################################################

sdb_update_module(clickhouse-cpp)

file(
    GLOB_RECURSE CLICKHOUSE_CPP_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/clickhouse-cpp/clickhouse/*.cpp"
)
list(
    APPEND CLICKHOUSE_CPP_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/clickhouse-cpp/contrib/cityhash/cityhash/city.cc"
)

add_library(clickhouse_cpp STATIC ${CLICKHOUSE_CPP_SOURCES})

target_include_directories(
    clickhouse_cpp
    SYSTEM
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/clickhouse-cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/clickhouse-cpp/contrib/cityhash/cityhash"
)

target_compile_definitions(
    clickhouse_cpp
    PRIVATE WITH_OPENSSL=1 CH_MAP_BOOL_TO_UINT8=1
)

target_link_libraries(
    clickhouse_cpp
    PUBLIC lz4_static zstd::zstd absl::int128 OpenSSL::SSL OpenSSL::Crypto
)

###############################################################################
## ada (WHATWG URL parsing) -- comes after simdutf (ADA_USE_SIMDUTF can use it)
################################################################################

set(ADA_TESTING OFF CACHE BOOL "" FORCE)
set(ADA_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(ADA_TOOLS OFF CACHE BOOL "" FORCE)
set(ADA_USE_SIMDUTF ON CACHE BOOL "" FORCE)
sdb_add_module(ada)

###############################################################################
## croaring
################################################################################

set(ENABLE_ROARING_TESTS OFF CACHE BOOL "" FORCE)
set(ROARING_USE_CPM OFF CACHE BOOL "" FORCE)
sdb_add_module(croaring)

################################################################################
## fmt
################################################################################

sdb_add_module(fmt)

################################################################################
## jansson
################################################################################

set(JANSSON_EXAMPLES OFF CACHE BOOL "" FORCE)
set(JANSSON_BUILD_DOCS OFF CACHE BOOL "" FORCE)
set(JANSSON_WITHOUT_TESTS ON CACHE BOOL "" FORCE)
set(JANSSON_INSTALL OFF CACHE BOOL "" FORCE)
sdb_add_module(jansson)

################################################################################
## AVRO
################################################################################

set(ZLIB_FOUND ON CACHE BOOL "" FORCE)
set(SNAPPY_FOUND ON CACHE BOOL "" FORCE)
set(JANSSON_FOUND ON CACHE BOOL "" FORCE)
set(THREADSAFE ON CACHE BOOL "" FORCE)
sdb_add_module(avro DIR avro/lang/c NO_WARNINGS)

################################################################################
## libxml2
################################################################################

sdb_add_module(libxml2 DIR libxml2-cmake)

################################################################################
## Azure SDK (monolithic build)
################################################################################

sdb_add_module(azure-sdk-for-cpp DIR azure-cmake)

################################################################################
## cmark-gfm
################################################################################

set(CMARK_TESTS OFF CACHE BOOL "" FORCE)
set(CMARK_SHARED OFF CACHE BOOL "" FORCE)
set(CMARK_STATIC ON CACHE BOOL "" FORCE)
sdb_add_module(cmark-gfm)

target_include_directories(
    libcmark-gfm_static
    INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/cmark-gfm/src
        ${CMAKE_CURRENT_BINARY_DIR}/cmark-gfm/src
)
target_include_directories(
    libcmark-gfm-extensions_static
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/cmark-gfm/extensions
)

################################################################################
## duckdb
################################################################################

set(DUCKDB_OPTIMIZATION_PROFILE NONE CACHE STRING "" FORCE)
set(BUILD_SHELL FALSE CACHE BOOL "" FORCE)
set(BUILD_SHELL_STATIC_LIB TRUE CACHE BOOL "" FORCE)
option(
    SDB_BUILD_DUCKDB_UNITTESTS
    "Build DuckDB's unittest binary for vendored-extension test suites"
    ON
)
option(
    SDB_BUILD_DUCKDB_BENCHMARKS
    "Build DuckDB's benchmark_runner for its benchmark suites"
    ON
)
if(IRESEARCH_BENCH)
    set(BUILD_UNITTESTS FALSE CACHE BOOL "" FORCE)
    set(ENABLE_UNITTEST_CPP_TESTS FALSE CACHE BOOL "" FORCE)
    set(BUILD_BENCHMARKS FALSE CACHE BOOL "" FORCE)
else()
    set(BUILD_UNITTESTS ${SDB_BUILD_DUCKDB_UNITTESTS} CACHE BOOL "" FORCE)
    set(ENABLE_UNITTEST_CPP_TESTS
        ${SDB_BUILD_DUCKDB_UNITTESTS}
        CACHE BOOL
        ""
        FORCE
    )
    set(BUILD_BENCHMARKS ${SDB_BUILD_DUCKDB_BENCHMARKS} CACHE BOOL "" FORCE)
endif()
set(BUILD_COMPLETE_EXTENSION_SET FALSE CACHE BOOL "" FORCE)
set(BUILD_EXTENSIONS_ONLY FALSE CACHE BOOL "" FORCE)
# No dynamic extension load/install (we link a fixed BUILD_EXTENSIONS set).
set(ENABLE_EXTENSION_LOAD FALSE CACHE BOOL "" FORCE)
set(DUCKDB_CAPABILITIES "none" CACHE STRING "" FORCE)
set(EXTENSION_STATIC_BUILD TRUE CACHE BOOL "" FORCE)
set(ENABLE_SANITIZER FALSE CACHE BOOL "" FORCE)
set(ENABLE_UBSAN FALSE CACHE BOOL "" FORCE)
set(ENABLE_THREAD_SANITIZER FALSE CACHE BOOL "" FORCE)
set(EXPORT_DLL_SYMBOLS FALSE CACHE BOOL "" FORCE)
set(DUCKDB_SKIP_INSTALL TRUE CACHE BOOL "" FORCE)
set(LATEST_STORAGE FALSE CACHE BOOL "" FORCE)
# Suppress the per-extension "vcpkg.json present but VCPKG_BUILD not set" warning.
set(VCPKG_BUILD ON CACHE BOOL "" FORCE)
set(SPATIAL_USE_GEOS FALSE CACHE BOOL "" FORCE)
set(SPATIAL_USE_BOOST TRUE CACHE BOOL "" FORCE)
set(SPATIAL_USE_RTREE FALSE CACHE BOOL "" FORCE)
if(IRESEARCH_BENCH)
    set(BUILD_EXTENSIONS "icu" CACHE STRING "" FORCE)
    set(STATICALLY_LINK_EXTENSIONS "icu" CACHE STRING "" FORCE)
    set(DUCKDB_EXTENSION_CONFIGS "" CACHE STRING "" FORCE)
else()
    set(BUILD_EXTENSIONS
        "core_functions;parquet;tpch;tpcds;json;httpfs;avro;iceberg;postgres_scanner;autocomplete;icu;inet;azure;spatial"
        CACHE STRING
        ""
        FORCE
    )
    set(STATICALLY_LINK_EXTENSIONS
        "core_functions;parquet;tpch;tpcds;json;httpfs;avro;iceberg;postgres_scanner;autocomplete;icu;inet;azure;spatial;markdown;clickhouse_scanner"
        CACHE STRING
        ""
        FORCE
    )
    # Extensions whose build config lives NEXT TO the extension instead of as a
    # shim inside the duckdb fork (.github/config/extensions/): duckdb includes
    # these files after the BUILD_EXTENSIONS list is resolved.
    set(DUCKDB_EXTENSION_CONFIGS
        "${CMAKE_CURRENT_SOURCE_DIR}/duckdb_clickhouse/extension_config.cmake"
        "${CMAKE_CURRENT_SOURCE_DIR}/duckdb_markdown/extension_config.cmake"
        CACHE STRING
        ""
        FORCE
    )
endif()

sdb_update_module(
    duckdb_httpfs
    duckdb_azure
    duckdb_avro
    duckdb_iceberg
    duckdb_postgres
    postgres
    duckdb_inet
    duckdb_markdown
    duckdb_spatial
    database-connector
)

# duckdb's in-tree extensions resolve their includes from directory scope (not from duckdb_static's
# INTERFACE), so expose simdutf's header to the whole duckdb subtree here. The duckdb_static PUBLIC link
# to simdutf (added below) is what pulls the compiled simdutf object code into the final binary.
include_directories(SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/simdutf/include)

sdb_add_module(duckdb COVERAGE)
target_compile_options(duckdb_storage PRIVATE -Wno-thread-safety-analysis)
if(TARGET iceberg_planning_scan_plan)
    target_compile_options(
        iceberg_planning_scan_plan
        PRIVATE -Wno-thread-safety-analysis
    )
endif()
if(TARGET libpq)
    target_compile_options(libpq PRIVATE -w)
endif()

if(TARGET duckdb_shell)
    target_compile_definitions(
        duckdb_shell
        PRIVATE SERENEDB_SHELL_VERSION="${SERENEDB_VERSION}"
    )
endif()

target_include_directories(
    duckdb_static
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/src/include
        ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/utf8proc/include
)

# Link extension objects that aren't automatically included in duckdb_static
target_link_libraries(
    duckdb_static
    PUBLIC
        icu_extension
        # SIMD string-encoding routines (base64 + UTF-8/UTF-16/Latin-1). This
        # one PUBLIC edge propagates the simdutf include to duckdb core and to
        # every static extension (they each link duckdb_static).
        simdutf
)

if(NOT IRESEARCH_BENCH)
    target_link_libraries(
        iceberg_catalog_rest_storage_authorization
        PRIVATE OpenSSL::Crypto
    )

    target_link_libraries(
        duckdb_static
        PUBLIC
            core_functions_extension
            parquet_extension
            tpch_extension
            tpcds_extension
            json_extension
            httpfs_extension
            azure_extension
            avro_extension
            iceberg_extension
            postgres_scanner_extension
            autocomplete_extension
            inet_extension
            markdown_extension
            spatial_extension
    )
endif()

# !!! KEEP THIS LAST !!!
# Guard against a future add_subdirectory() in this file re-pointing OpenSSL at
# a system copy via find_package / CACHE FORCE / etc. The pre-seeded cache
# variables at the top of this file are what makes sure folly & curl link the
# vendored libssl/libcrypto; if any of them drift, the binary will silently go
# back to the ABI-mismatched state that crashes.
# if this starts breaking  - most likely someone in third_party is playing
# with FORCE setting variables. Then maybe dependency provider will help,
# but looks like overkill for now.
foreach(_var OPENSSL_INCLUDE_DIR OPENSSL_CRYPTO_LIBRARY OPENSSL_SSL_LIBRARY)
    if(NOT "${${_var}}" STREQUAL "${_sdb_expected_${_var}}")
        message(
            FATAL_ERROR
            "${_var} drifted from the vendored pre-seed.\n"
            "  expected: ${_sdb_expected_${_var}}\n"
            "  actual:   ${${_var}}\n"
            "Something added in third_party/ changed this cache variable. "
            "Re-point it at the vendored tree or rework the pre-seed at the "
            "top of third_party/CMakeLists.txt."
        )
    endif()
endforeach()
