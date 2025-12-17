# ------------------------------------------------------------------------------
# External Projects used by SereneDB
# ------------------------------------------------------------------------------

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -Wno-unused -Wno-sign-compare -Wno-deprecated -Wno-suggest-override -Wno-suggest-destructor-override -Wno-missing-field-initializers"
)

include(ExternalProject)
include(UpdateModule)

sdb_update_module(${GIT_EXECUTABLE} "sqllogictest-rs" ${CMAKE_SOURCE_DIR}/scripts "README.md")

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
## V8
################################################################################

if(USE_V8)
    option(USE_PRECOMPILED_V8 "use a precompiled V8" OFF)

    set(V8_VERSION "12.1.165" CACHE INTERNAL "${PROJECT_NAME}: Version" FORCE)
    set(V8_VERSION ${V8_VERSION} PARENT_SCOPE)
    set(V8_SUB_DIR "v8")
    add_subdirectory(v8-build)
    add_library(v8_interface INTERFACE)
    target_include_directories(v8_interface SYSTEM INTERFACE ${V8_INCLUDE_DIR})

    add_library(icu INTERFACE)
    target_link_libraries(icu INTERFACE ${ICU_LIBS})
    target_include_directories(icu SYSTEM INTERFACE ${ICU_INCLUDE_DIR})

    # For re2
    add_library(icu-uc ALIAS icu)
    add_library(ICU::uc ALIAS icu)
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
## JEMALLOC
################################################################################

if("${SDB_ALLOC}" MATCHES "JE")
    # TODO(mbkkt) make it submodule
    add_subdirectory(jemalloc)
    include_directories("${JEMALLOC_HOME}/include")
    add_library(jemalloc INTERFACE)
    add_dependencies(jemalloc jemalloc_build)
    target_include_directories(jemalloc INTERFACE "${JEMALLOC_HOME}/include")
    target_link_directories(jemalloc INTERFACE "${JEMALLOC_HOME}/lib")
    target_link_libraries(
        jemalloc
        INTERFACE "${JEMALLOC_HOME}/lib/libjemalloc.a"
    )
    link_libraries(jemalloc)
    set(JEMALLOC_HOME "${JEMALLOC_HOME}" PARENT_SCOPE)
    set(FOLLY_USE_JEMALLOC ON)
else()
    set(FOLLY_USE_JEMALLOC OFF)
endif()

################################################################################
## BOOST
################################################################################

# TODO(mbkkt) make it submodule
add_subdirectory(boost)
set(BOOST_VERSION ${BOOST_VERSION} PARENT_SCOPE)
set(BOOST_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/boost")
# explicitly set since suppressing Boost search
# needed for iresearch and boost::text
set(Boost_INCLUDE_DIRS "${BOOST_INCLUDE_PATH}")
set(Boost_INCLUDE_DIRS "${Boost_INCLUDE_DIRS}" PARENT_SCOPE)

add_library(Boost::regex ALIAS boost_boost)
add_library(Boost::headers ALIAS boost_boost)

################################################################################
### icu library
################################################################################

if(NOT TARGET ICU::uc)
    sdb_update_module(${GIT_EXECUTABLE} "icu" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
    add_subdirectory(icu)

    # For re2
    add_library(icu-uc ALIAS _icuuc)
    add_library(ICU::uc ALIAS _icuuc)
endif()

################################################################################
### abseil-cpp library
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "abseil-cpp" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(ABSL_PROPAGATE_CXX_STD OFF CACHE BOOL "" FORCE)
set(ABSL_USE_SYSTEM_INCLUDES ON CACHE BOOL "" FORCE)
add_subdirectory(abseil-cpp)

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

sdb_update_module(${GIT_EXECUTABLE} "faiss" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(FAISS_ENABLE_GPU OFF CACHE BOOL "" FORCE)
set(FAISS_ENABLE_PYTHON OFF CACHE BOOL "" FORCE)
add_subdirectory(faiss)

################################################################################
## some stuff for IResearch
################################################################################

if(NOT PERL_FOUND)
    set(PERL_FOUND TRUE) # suppress error for Perl not-found

    # MSVC will execute ADD_CUSTOM_COMMAND even though OUTPUT is already present
    if(MSVC)
        set(PERL_EXECUTABLE echo)
    endif()
endif()

sdb_update_module(${GIT_EXECUTABLE} "lz4" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
set(LZ4_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/lz4")
set(LZ4_INCLUDE_DIR "${LZ4_ROOT}/lib")
set(SNOWBALL_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/snowball")

find_package(LZ4 REQUIRED)
find_package(SNOWBALL REQUIRED)

add_library(stemmer::stemmer ALIAS stemmer-static)

################################################################################
## Google S2
################################################################################

set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(S2_USE_SYSTEM_INCLUDES ON CACHE BOOL "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "s2geometry" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(s2geometry)

################################################################################
## ROCKSDB
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "rocksdb" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

set(WITH_GFLAGS OFF)
add_subdirectory(rocksdb)
add_library(rocksdb_interface INTERFACE)
target_include_directories(
    rocksdb_interface
    SYSTEM
    INTERFACE ${PROJECT_SOURCE_DIR}/third_party/rocksdb/include
)
target_include_directories(
    rocksdb_interface
    SYSTEM
    INTERFACE ${PROJECT_SOURCE_DIR}/third_party/rocksdb
)
list(APPEND LINK_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}/rocksdb")

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
## NGHTTP2
################################################################################

add_subdirectory(nghttp2)

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
## DATE
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "date" ${CMAKE_CURRENT_SOURCE_DIR} "LICENSE.txt")
add_library(date_interface INTERFACE)
target_include_directories(
    date_interface
    SYSTEM
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/date/include
)

################################################################################
## Linenoise
################################################################################
if(USE_V8)
    add_library(
        linenoise-ng
        STATIC
        linenoise-ng/src/ConvertUTF.cpp
        linenoise-ng/src/linenoise.cpp
        linenoise-ng/src/wcwidth.cpp
    )

    target_include_directories(
        linenoise-ng
        SYSTEM
        PUBLIC ${PROJECT_SOURCE_DIR}/third_party/linenoise-ng/include
    )
endif()

################################################################################
## LINK_DIRECTORIES
################################################################################

set(LINK_DIRECTORIES "${LINK_DIRECTORIES}" PARENT_SCOPE)

################################################################################
## taojson
################################################################################

set(TAOCPP_JSON_BUILD_TESTS
    OFF
    CACHE BOOL
    "Build taocpp::json test programs"
    FORCE
)
set(TAOCPP_JSON_BUILD_EXAMPLES
    OFF
    CACHE BOOL
    "Build taocpp::json example programs"
    FORCE
)
add_subdirectory(taocpp-json)

################################################################################
## tzdata
################################################################################

list(
    APPEND
    TZ_DATA_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/africa"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/antarctica"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/asia"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/australasia"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/backward"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/backzone"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/calendars"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/etcetera"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/europe"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/NEWS"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/northamerica"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/southamerica"
    "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/version"
    # "${CMAKE_CURRENT_SOURCE_DIR}/tzdata/windowsZones.xml"
)

set(TZ_DATA_FILES ${TZ_DATA_FILES} PARENT_SCOPE)

add_custom_target(tzdata)

if(MSVC)
    add_custom_command(
        TARGET tzdata
        POST_BUILD
        COMMAND
            ${CMAKE_COMMAND} -E make_directory
            ${CMAKE_BINARY_DIR}/bin/$<CONFIG>/tzdata
        COMMAND
            ${CMAKE_COMMAND} -E make_directory
            ${CMAKE_BINARY_DIR}/tests/$<CONFIG>/tzdata
        COMMAND
            ${CMAKE_COMMAND} -E copy ${TZ_DATA_FILES}
            ${CMAKE_BINARY_DIR}/bin/$<CONFIG>/tzdata
        COMMAND
            ${CMAKE_COMMAND} -E copy ${TZ_DATA_FILES}
            ${CMAKE_BINARY_DIR}/tests/$<CONFIG>/tzdata
    )
else()
    add_custom_command(
        TARGET tzdata
        POST_BUILD
        COMMAND
            ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/bin/tzdata
        COMMAND
            ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/tests/tzdata
        COMMAND
            ${CMAKE_COMMAND} -E copy ${TZ_DATA_FILES}
            ${CMAKE_BINARY_DIR}/bin/tzdata
        COMMAND
            ${CMAKE_COMMAND} -E copy ${TZ_DATA_FILES}
            ${CMAKE_BINARY_DIR}/tests/tzdata
    )
endif()

################################################################################
## NuRaft
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "nuraft" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

set(BOOST_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/boost")
set(BOOST_LIBRARY_PATH On)
set(LIBBOOST_SYSTEM boost_boost CACHE STRING "" FORCE)

set(OPENSSL_INCLUDE_PATH ${OPENSSL_INCLUDE_DIR})
set(OPENSSL_LIBRARY_PATH ${PROJECT_BINARY_DIR}/bin)
add_subdirectory(nuraft)
unset(OPENSSL_INCLUDE_PATH)
unset(OPENSSL_LIBRARY_PATH)

################################################################################
## libpg_query
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "libpg_query" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

add_subdirectory(libpg_query)

###############################################################################
## simdjson
################################################################################

set(SIMDJSON_ENABLE_THREADS OFF CACHE BOOL "" FORCE)
set(SIMDJSON_CXX_STANDARD ${CMAKE_CXX_STANDARD} CACHE STRING "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "simdjson" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(simdjson)

################################################################################
### tests only library
################################################################################

add_subdirectory(tests)

################################################################################
## fast_float
################################################################################

set(FASTFLOAT_INSTALL OFF CACHE BOOL "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "fast_float" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(fast_float)

################################################################################
## xsimd
################################################################################

set(XSIMD_SKIP_INSTALL OFF CACHE BOOL "" FORCE)
sdb_update_module(${GIT_EXECUTABLE} "xsimd" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(xsimd)

################################################################################
## double-conversion
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "double-conversion" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(double-conversion)
add_library(double-conversion::double-conversion ALIAS double-conversion)

################################################################################
## fmt
################################################################################

if(SDB_USE_FMT)
    sdb_update_module(${GIT_EXECUTABLE} "fmt" ${CMAKE_CURRENT_SOURCE_DIR} "LICENSE")
    add_subdirectory(fmt)
endif()

################################################################################
## fake libraries (fmt, gflags, glog, folly)
################################################################################

add_subdirectory(fake)

################################################################################
## folly
################################################################################

set(FOLLY_HAVE_INT128_T ON)
set(FOLLY_HAVE_EXTRANDOM_SFMT19937 OFF)
set(FOLLY_HAVE_LIBGFLAGS ON)
set(FOLLY_HAVE_LIBGLOG ON)
# TODO(mbkkt) FOLLY_LIBRARY_SANITIZE_ADDRESS
set(FOLLY_USE_SYMBOLIZER OFF)
set(FOLLY_HAVE_LIBUNWIND OFF)
set(FOLLY_HAVE_ELF OFF)
set(FOLLY_HAVE_DWARF OFF)
sdb_update_module(${GIT_EXECUTABLE} "folly" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(folly)
target_include_directories(folly PUBLIC ${OPENSSL_INCLUDE_DIR})
target_link_libraries(folly PUBLIC ${OPENSSL_LIBRARIES})

################################################################################
## velox
################################################################################
if(SDB_GTEST)
    # Needed for our connector tests
    set(VELOX_BUILD_VECTOR_TEST_UTILS ON CACHE BOOL "" FORCE)
else()
    set(VELOX_BUILD_VECTOR_TEST_UTILS OFF CACHE BOOL "" FORCE)
endif()

set(VELOX_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(VELOX_MONO_LIBRARY ON CACHE BOOL "" FORCE)
set(VELOX_DEPENDENCY_SOURCE_DEFAULT "BUNDLED" CACHE STRING "" FORCE)
set(VELOX_DEPENDENCY_SOURCE "BUNDLED" CACHE STRING "" FORCE)

set(VELOX_ENABLE_EXEC ON CACHE BOOL "" FORCE)
set(VELOX_ENABLE_AGGREGATES ON CACHE BOOL "" FORCE)
set(VELOX_ENABLE_HIVE_CONNECTOR OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_TPCH_CONNECTOR OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_PRESTO_FUNCTIONS ON CACHE BOOL "" FORCE)
set(VELOX_ENABLE_SPARK_FUNCTIONS ON CACHE BOOL "" FORCE)
set(VELOX_ENABLE_EXPRESSION ON CACHE BOOL "" FORCE)
set(VELOX_ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_BENCHMARKS_BASIC OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_S3 OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_GCS OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_ABFS OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_HDFS OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_PARQUET OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_ARROW OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_GEO OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_REMOTE_FUNCTIONS OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_CCACHE OFF CACHE BOOL "" FORCE)
set(VELOX_ENABLE_COMPRESSION_LZ4 OFF CACHE BOOL "" FORCE)

set(VELOX_BUILD_RUNNER OFF CACHE BOOL "" FORCE)

# TODO(mbkkt) do we need really large(2e9+) hashtables?
set(VELOX_ENABLE_INT64_BUILD_PARTITION_BOUND ON CACHE BOOL "" FORCE)
set(VELOX_SIMDJSON_SKIPUTF8VALIDATION OFF CACHE BOOL "" FORCE)

sdb_update_module(${GIT_EXECUTABLE} "velox" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(velox)

################################################################################
## axiom
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "axiom" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")
add_subdirectory(axiom)

target_include_directories(
    velox
    SYSTEM
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/velox
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/axiom
)

################################################################################
## WAMR (SHOULD BE LAST!)
################################################################################

if(USE_WASM)
    sdb_update_module(${GIT_EXECUTABLE} "wasm-micro-runtime" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

    set(WAMR_BUILD_PLATFORM "linux")
    set(WAMR_BUILD_INTERP 1)
    set(WAMR_BUILD_FAST_INTERP 1)
    set(WAMR_BUILD_FAST_JIT 0) # TODO(mbkkt) doesn't work in cmake configure step
    set(WAMR_BUILD_JIT 0) # needs llvm
    set(WAMR_BUILD_AOT 0) # compile wasm to their aot format
    set(WAMR_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/wasm-micro-runtime)
    set(WAMR_ROOT_DIR ${WAMR_ROOT_DIR} PARENT_SCOPE)
    set(WAMR_DISABLE_HW_BOUND_CHECK 1)

    include(${WAMR_ROOT_DIR}/build-scripts/runtime_lib.cmake)
    add_library(vmlib ${WAMR_RUNTIME_LIB_SOURCE})
endif()
