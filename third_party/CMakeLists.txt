# ------------------------------------------------------------------------------
# External Projects used by SereneDB
# ------------------------------------------------------------------------------

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -Wno-unused -Wno-sign-compare -Wno-deprecated -Wno-suggest-override -Wno-suggest-destructor-override -Wno-missing-field-initializers"
)

include(ExternalProject)
include(UpdateModule)

sdb_update_module(${GIT_EXECUTABLE} "sqllogictest-rs" ${CMAKE_SOURCE_DIR}/third_party "README.md")

################################################################################
## JEMALLOC
################################################################################
if("${SDB_ALLOC}" MATCHES "JE")
    # TODO(mbkkt) make it submodule
    add_subdirectory(jemalloc)
    file(MAKE_DIRECTORY "${JEMALLOC_HOME}/include")
    add_library(jemalloc STATIC IMPORTED GLOBAL)
    set_target_properties(
        jemalloc
        PROPERTIES
            IMPORTED_LOCATION "${JEMALLOC_HOME}/lib/libjemalloc.a"
            INTERFACE_INCLUDE_DIRECTORIES "${JEMALLOC_HOME}/include"
    )
    add_dependencies(jemalloc jemalloc_build)
    link_libraries(jemalloc)
    set(FOLLY_USE_JEMALLOC ON)
else()
    set(FOLLY_USE_JEMALLOC OFF)
endif()

###############################################################################
## simdutf (SIMD UTF-8 validation + base64)
################################################################################

set(SIMDUTF_TESTS OFF CACHE BOOL "" FORCE)
set(SIMDUTF_TOOLS OFF CACHE BOOL "" FORCE)
set(SIMDUTF_ICONV OFF CACHE BOOL "" FORCE)
set(SIMDUTF_CXX_STANDARD ${CMAKE_CXX_STANDARD} CACHE STRING "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "simdutf" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(simdutf)

################################################################################
## fast_float
################################################################################

set(FASTFLOAT_INSTALL OFF CACHE BOOL "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "fast_float" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(fast_float)

###############################################################################
## simdjson
################################################################################

set(SIMDJSON_ENABLE_THREADS OFF CACHE BOOL "" FORCE)
set(SIMDJSON_CXX_STANDARD ${CMAKE_CXX_STANDARD} CACHE STRING "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "simdjson" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(simdjson)

################################################################################
## geographiclib-c
################################################################################

# TODO(mbkkt) make it submodule
add_subdirectory(geographiclib-c)
if(USE_DTRACE)
    set(SDT_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/sdt PARENT_SCOPE)
endif()

################################################################################
## YACLib
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "yaclib" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(YACLIB_FLAGS "CORO")
if(SDB_DEV)
    set(YACLIB_LOG "DEBUG")
else()
    set(YACLIB_LOG "")
endif()
add_subdirectory(yaclib)

################################################################################
## openssl
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "openssl" ${CMAKE_CURRENT_SOURCE_DIR} "README")
add_subdirectory(openssl-cmake)
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
## liburing
################################################################################

if(SDB_IOURING)
    sdb_update_module(${GIT_EXECUTABLE} "liburing" ${CMAKE_CURRENT_SOURCE_DIR} "README")
    add_subdirectory(liburing-cmake)
    set(uring_FOUND ON)
    set(uring_FOUND ON PARENT_SCOPE)
endif()

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

sdb_update_module(${GIT_EXECUTABLE} "zlib-ng" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(zlib-ng)

# Create ZLIB::ZLIB alias for velox compatibility
add_library(ZLIB::ZLIB ALIAS zlib)

################################################################################
## MINIZIP-NG
################################################################################

set(MZ_COMPAT ON CACHE BOOL "" FORCE)
set(MZ_ZLIB ON CACHE BOOL "" FORCE)
set(ZLIB_FOUND ON CACHE BOOL "" FORCE)
set(MZ_BZIP2 OFF CACHE BOOL "" FORCE)
set(MZ_LZMA OFF CACHE BOOL "" FORCE)
set(MZ_ZSTD OFF CACHE BOOL "" FORCE)
set(MZ_LIBCOMP OFF CACHE BOOL "" FORCE)
set(MZ_FETCH_LIBS OFF CACHE BOOL "" FORCE)
set(MZ_PKCRYPT OFF CACHE BOOL "" FORCE)
set(MZ_WZAES OFF CACHE BOOL "" FORCE)
set(MZ_OPENSSL OFF CACHE BOOL "" FORCE)
set(MZ_LIBBSD OFF CACHE BOOL "" FORCE)
set(MZ_ICONV OFF CACHE BOOL "" FORCE)

sdb_update_module(${GIT_EXECUTABLE} "minizip-ng" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(minizip-ng)

target_link_libraries(minizip PUBLIC zlib)

################################################################################
## BOOST
################################################################################

# TODO(mbkkt) make it submodule
sdb_update_module(${GIT_EXECUTABLE} "boost" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(boost)
set(BOOST_VERSION ${BOOST_VERSION} PARENT_SCOPE)
set(BOOST_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/boost")
# explicitly set since suppressing Boost search
# needed for iresearch and boost::text
set(Boost_INCLUDE_DIRS "${BOOST_INCLUDE_PATH}")
set(Boost_INCLUDE_DIRS "${Boost_INCLUDE_DIRS}" PARENT_SCOPE)

add_library(Boost::headers ALIAS boost_boost)

################################################################################
### icu library (provided by duckdb's bundled ICU 78.3)
################################################################################

# serenedb links a single ICU: the one vendored inside duckdb's icu extension
# (compiled into icu_extension -> duckdb_static). This target only carries the
# ICU 78 headers; the ICU symbols are resolved at final-executable link time
# from icu_extension (pulled in via duckdb_static). We deliberately do NOT add a
# link edge to duckdb_static here: duckdb links serenedb's shared `re2`, and re2
# links ICU::uc, so a link edge would create a dependency cycle
# (re2 -> ICU::uc -> duckdb_static -> re2). Header-only avoids that; every
# executable that uses ICU already links duckdb_static.
add_library(sdb_icu INTERFACE)
target_include_directories(
    sdb_icu
    SYSTEM
    INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/extension/icu/third_party/icu/common
        ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/extension/icu/third_party/icu/i18n
)

add_library(icu ALIAS sdb_icu)
add_library(icu-uc ALIAS sdb_icu)
add_library(ICU::uc ALIAS sdb_icu)

################################################################################
### abseil-cpp library
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "abseil-cpp" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(ABSL_PROPAGATE_CXX_STD OFF CACHE BOOL "" FORCE)
set(ABSL_USE_SYSTEM_INCLUDES ON CACHE BOOL "" FORCE)
add_subdirectory(abseil-cpp)
target_link_libraries(absl_strings PUBLIC simdutf)

################################################################################
### re2 library
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "re2" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(RE2_USE_ICU ON CACHE BOOL "" FORCE)
set(RE2_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(re2)

################################################################################
### tcmalloc library
################################################################################

if("${SDB_ALLOC}" MATCHES "TC")
    sdb_update_module(${GIT_EXECUTABLE} "tcmalloc" ${CMAKE_CURRENT_SOURCE_DIR} "LICENSE")
    add_subdirectory(tcmalloc)
endif()

################################################################################
### googletest library
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "googletest" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(GTEST_HAS_ABSL ON CACHE BOOL "" FORCE)
add_subdirectory(googletest)

################################################################################
### benchmark library
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "benchmark" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_CXX_STANDARD ${CMAKE_CXX_STANDARD} CACHE STRING "" FORCE)
add_subdirectory(benchmark)

################################################################################
### streamvbyte library
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "streamvbyte" ${CMAKE_CURRENT_SOURCE_DIR} "LICENSE")
add_subdirectory(streamvbyte)

################################################################################
### sse2neon library -- need this after abseil and highway, but before simdcomp
################################################################################

add_library(sse2neon INTERFACE)
if(ARCH_AARCH64)
    target_include_directories(
        sse2neon
        SYSTEM
        INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/sse2neon"
    )
endif()

################################################################################
### OpenFST + Kaldi library
################################################################################

# We build OpenFST extensions provided by Kaldi as a part of OpenFST
set(Kaldi_sources
    ${CMAKE_CURRENT_SOURCE_DIR}/kaldi/src/base/io-funcs.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/kaldi/src/base/kaldi-error.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/kaldi/src/base/kaldi-utils.cc
)

set(OpenFST_sources
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/compat.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/encode.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/flags.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/fst.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/fst-types.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/mapped-file.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/properties.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/symbol-table.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/symbol-table-ops.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/util.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/openfst/weight.cc
    ${Kaldi_sources}
)

add_library(iresearch-ofst OBJECT ${OpenFST_sources})

# disable warnings for 3rd-party libs for a cleaner build
if(MSVC)
    target_compile_options(
        iresearch-ofst
        # MSVC2015-2017 require "/bigobj" to compile debug build
        PRIVATE "$<$<CONFIG:Debug>:/bigobj>"
    )
endif()

target_include_directories(
    iresearch-ofst
    SYSTEM
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/openfst
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/kaldi/src
)

target_link_libraries(
    iresearch-ofst
    PUBLIC absl::flat_hash_set absl::flat_hash_map
)

################################################################################
### SIMDCOMP library
################################################################################

# TODO(mbkkt) make it submodule
add_subdirectory(simdcomp)

################################################################################
### frozen library
################################################################################

set(FROZEN_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/frozen/include)
set(FROZEN_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/frozen/include PARENT_SCOPE)

################################################################################
### Boost::Text library
################################################################################

set(CXX_STD ${CMAKE_CXX_STANDARD} CACHE STRING "" FORCE)
# TODO(mbkkt) make it submodule
add_subdirectory(text)
target_compile_definitions(
    text
    PUBLIC
        -DBOOST_STL_INTERFACES_DISABLE_CONCEPTS=1
        -DBOOST_STL_INTERFACES_DISABLE_DEDUCED_THIS=1
)

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

sdb_update_module(${GIT_EXECUTABLE} "OpenBLAS" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(USE_THREAD 0 CACHE BOOL "" FORCE)
set(USE_OPENMP 0 CACHE BOOL "" FORCE)
set(USE_LOCKING 1 CACHE BOOL "" FORCE)
set(NO_WARMUP 1 CACHE BOOL "" FORCE)
set(NO_AFFINITY 1 CACHE BOOL "" FORCE)
set(NO_LAPACK 1 CACHE BOOL "" FORCE)
set(BUILD_SINGLE 1 CACHE BOOL "" FORCE) # only sgemm_ is ever called; drops DOUBLE/COMPLEX/COMPLEX16
set(NO_CBLAS 1 CACHE BOOL "" FORCE) # only the Fortran-ABI sgemm_ is called, never cblas_sgemm
set(CMAKE_Fortran_COMPILER NOTFOUND CACHE FILEPATH "" FORCE)
set(DYNAMIC_ARCH OFF CACHE BOOL "" FORCE)
if(ARCH_AARCH64)
    set(TARGET ARMV8 CACHE STRING "" FORCE)
elseif(ARCH_AMD64)
    set(TARGET HASWELL CACHE STRING "" FORCE)
endif()
add_subdirectory(OpenBLAS)
target_link_libraries(sdb_blas INTERFACE openblas)

################################################################################
### faiss library
################################################################################
set(FAISS_LIB "")
set(FAISS_LIB faiss)
if(ARCH_AMD64)
    set(FAISS_OPT_LEVEL "avx2")
    set(FAISS_LIB faiss_avx2)
elseif(ARCH_AARCH64)
    # TODO(mbkkt) is it ok?
    set(FAISS_OPT_LEVEL "sve")
    set(FAISS_LIB faiss_sve)
endif()
set(FAISS_LIB "${FAISS_LIB}" CACHE INTERNAL "faiss target to link" FORCE)

sdb_update_module(${GIT_EXECUTABLE} "faiss" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(FAISS_ENABLE_GPU OFF CACHE BOOL "" FORCE)
set(FAISS_ENABLE_PYTHON OFF CACHE BOOL "" FORCE)
set(FAISS_ENABLE_EXTRAS OFF CACHE BOOL "" FORCE)
add_subdirectory(faiss)
target_compile_options(
    ${FAISS_LIB}
    PUBLIC
        -Wno-suggest-override
        -Wno-sign-compare
        -Wno-suggest-destructor-override
)

################################################################################
## snowball stemmer
################################################################################

if(NOT PERL_FOUND)
    set(PERL_FOUND TRUE) # suppress error for Perl not-found

    # MSVC will execute ADD_CUSTOM_COMMAND even though OUTPUT is already present
    if(MSVC)
        set(PERL_EXECUTABLE echo)
    endif()
endif()

set(SNOWBALL_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/snowball")
find_package(SNOWBALL REQUIRED)
add_library(stemmer::stemmer ALIAS stemmer-static)

################################################################################
## LZ4
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "lz4" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

set(LZ4_BUILD_CLI OFF CACHE BOOL "" FORCE)
set(LZ4_POSITION_INDEPENDENT_LIB OFF CACHE BOOL "" FORCE)
add_subdirectory(lz4/build/cmake)

get_target_property(LZ4_INCLUDE_DIR lz4_static INTERFACE_INCLUDE_DIRECTORIES)

# Create lz4::lz4 alias for velox compatibility
add_library(lz4::lz4 ALIAS lz4_static)

################################################################################
## ZSTD
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "zstd" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ZSTD_INSTALL OFF CACHE BOOL "" FORCE)

add_subdirectory(zstd/build/cmake)

# Create zstd::zstd alias for velox compatibility
add_library(zstd::zstd ALIAS libzstd_static)

################################################################################
## SNAPPY
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "snappy" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(SNAPPY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(SNAPPY_INSTALL OFF CACHE BOOL "" FORCE)

add_subdirectory(snappy)

################################################################################
## Google S2
################################################################################

set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(S2_USE_SYSTEM_INCLUDES ON CACHE BOOL "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "s2geometry" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(s2geometry)

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

sdb_update_module(${GIT_EXECUTABLE} "curl" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

# Force static HTTP(S)-only build, using our vendored OpenSSL.
set(BUILD_CURL_EXE OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(CURL_DISABLE_INSTALL ON CACHE BOOL "" FORCE)
set(CURL_ENABLE_EXPORT_TARGET OFF CACHE BOOL "" FORCE)
set(HTTP_ONLY ON CACHE BOOL "" FORCE)
# nghttp2 was removed from third_party; force HTTP/2 off so curl does not pick
# up a system libnghttp2.
set(USE_NGHTTP2 OFF CACHE BOOL "" FORCE)
set(CURL_USE_OPENSSL ON CACHE BOOL "" FORCE)
set(CURL_USE_LIBPSL OFF CACHE BOOL "" FORCE)
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

add_subdirectory(curl)

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
## IMMER
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "immer" ${CMAKE_CURRENT_SOURCE_DIR} "README.rst")

set(immer_BUILD_EXAMPLES OFF CACHE BOOL "don't build examples")
set(immer_BUILD_EXTRAS OFF CACHE BOOL "don't build extras")
set(immer_BUILD_TESTS OFF CACHE BOOL "don't build tests")
set(immer_BUILD_DOCS OFF CACHE BOOL "don't docs")
add_subdirectory(immer)

################################################################################
## LINK_DIRECTORIES
################################################################################

set(LINK_DIRECTORIES "${LINK_DIRECTORIES}" PARENT_SCOPE)

###############################################################################
## ada (WHATWG URL parsing) -- comes after simdutf (ADA_USE_SIMDUTF can use it)
################################################################################

set(ADA_TESTING OFF CACHE BOOL "" FORCE)
set(ADA_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(ADA_TOOLS OFF CACHE BOOL "" FORCE)
set(ADA_USE_SIMDUTF ON CACHE BOOL "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "ada" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(ada)

###############################################################################
## croaring
################################################################################

set(ENABLE_ROARING_TESTS OFF CACHE BOOL "" FORCE)
set(ROARING_USE_CPM OFF CACHE BOOL "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "croaring" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(croaring)

################################################################################
### tests only library
################################################################################

add_subdirectory(tests)

################################################################################
## fmt
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "fmt" ${CMAKE_CURRENT_SOURCE_DIR} "LICENSE")
add_subdirectory(fmt)

################################################################################
## jansson
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "jansson" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

set(JANSSON_EXAMPLES OFF CACHE BOOL "" FORCE)
set(JANSSON_BUILD_DOCS OFF CACHE BOOL "" FORCE)
set(JANSSON_WITHOUT_TESTS ON CACHE BOOL "" FORCE)
set(JANSSON_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(jansson)

################################################################################
## AVRO
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "avro" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

set(ZLIB_FOUND ON CACHE BOOL "" FORCE)
set(SNAPPY_FOUND ON CACHE BOOL "" FORCE)
set(JANSSON_FOUND ON CACHE BOOL "" FORCE)
add_subdirectory(avro/lang/c)

################################################################################
## AWS SDK (monolithic build)
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "aws-c-auth" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-c-cal" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-c-common" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-c-compression" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-c-event-stream" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-c-http" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-c-io" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-c-mqtt" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-c-s3" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-c-sdkutils" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-checksums" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-sdk-cpp" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "aws-crt-cpp" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(aws-cmake)

set(AWSSDK_FOUND ON CACHE BOOL "" FORCE)
set(AWSSDK_LIBRARIES _aws CACHE STRING "" FORCE)
set(AWSSDK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/aws-cmake" CACHE PATH "" FORCE)

################################################################################
## duckdb
################################################################################

set(BUILD_SHELL FALSE CACHE BOOL "" FORCE)
set(BUILD_SHELL_STATIC_LIB TRUE CACHE BOOL "" FORCE)
option(
    SDB_BUILD_DUCKDB_UNITTESTS
    "Build DuckDB's unittest binary for vendored-extension test suites"
    ON
)
set(BUILD_UNITTESTS ${SDB_BUILD_DUCKDB_UNITTESTS} CACHE BOOL "" FORCE)
set(ENABLE_UNITTEST_CPP_TESTS FALSE CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS FALSE CACHE BOOL "" FORCE)
set(BUILD_COMPLETE_EXTENSION_SET FALSE CACHE BOOL "" FORCE)
set(BUILD_EXTENSIONS_ONLY FALSE CACHE BOOL "" FORCE)
# Keep builtin extensions (core_functions, parquet, json, etc.)
set(ENABLE_EXTENSION_AUTOLOADING FALSE CACHE BOOL "" FORCE)
set(ENABLE_EXTENSION_AUTOINSTALL FALSE CACHE BOOL "" FORCE)
# No dynamic extension load/install (we link a fixed BUILD_EXTENSIONS set). Drops
# the load/install machinery and, via -DDUCKDB_DISABLE_EXTENSION_LOAD, lets the
# read-mostly registries take shared_ptr_ipp.hpp's lock-free plain_load() path.
set(DISABLE_EXTENSION_LOAD TRUE CACHE BOOL "" FORCE)
set(EXTENSION_STATIC_BUILD TRUE CACHE BOOL "" FORCE)
set(ENABLE_SANITIZER FALSE CACHE BOOL "" FORCE)
set(ENABLE_UBSAN FALSE CACHE BOOL "" FORCE)
set(ENABLE_THREAD_SANITIZER FALSE CACHE BOOL "" FORCE)
set(EXPORT_DLL_SYMBOLS FALSE CACHE BOOL "" FORCE)
set(DUCKDB_SKIP_INSTALL TRUE CACHE BOOL "" FORCE)
set(LATEST_STORAGE TRUE CACHE BOOL "" FORCE)
# Suppress the per-extension "vcpkg.json present but VCPKG_BUILD not set" warning.
set(VCPKG_BUILD ON CACHE BOOL "" FORCE)
set(BUILD_EXTENSIONS
    "core_functions;parquet;tpch;tpcds;json;httpfs;avro;iceberg;postgres_scanner;autocomplete;icu;inet"
    CACHE STRING
    ""
    FORCE
)

sdb_update_module(${GIT_EXECUTABLE} "duckdb" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "duckdb_httpfs" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "duckdb_avro" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "duckdb_iceberg" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "duckdb_postgres" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "duckdb_inet" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
sdb_update_module(${GIT_EXECUTABLE} "database-connector" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

# duckdb's in-tree extensions resolve their includes from directory scope (not from duckdb_static's
# INTERFACE), so expose simdutf's header to the whole duckdb subtree here. The duckdb_static PUBLIC link
# to simdutf (added below) is what pulls the compiled simdutf object code into the final binary.
include_directories(SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/simdutf/include)

add_subdirectory(duckdb)

target_compile_definitions(
    duckdb_shell_static
    PRIVATE SERENEDB_SHELL_VERSION="${SERENEDB_VERSION}"
)

target_include_directories(
    duckdb_static
    SYSTEM
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/src/include
)

# Link extension objects that aren't automatically included in duckdb_static
target_link_libraries(
    duckdb_static
    PUBLIC
        duckdb_generated_extension_loader
        core_functions_extension
        parquet_extension
        tpch_extension
        tpcds_extension
        json_extension
        httpfs_extension
        avro_extension
        iceberg_extension
        postgres_scanner_extension
        autocomplete_extension
        icu_extension
        inet_extension
        # SIMD string-encoding routines (base64 + UTF-8/UTF-16/Latin-1). This
        # one PUBLIC edge propagates the simdutf include to duckdb core and to
        # every static extension (they each link duckdb_static).
        simdutf
)

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
