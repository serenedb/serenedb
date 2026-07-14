add_compile_warnings_flags()

################################################################################
## LIB_SERENE
################################################################################

add_definitions("-DHAS_REMOTE_API=0")
add_definitions(
    "-DARCHITECTURE_OPTIMIZATIONS=\"${ARCHITECTURE_OPTIMIZATIONS}\""
)

add_library(
    serene_base
    STATIC
    basics/file_utils.cpp
    basics/metrics.cpp
    basics/number_of_cores.cpp
    basics/string_utils.cpp
    basics/static_strings.cpp
    basics/lifecycle.cpp
    basics/application-exit.cpp
    basics/debugging.cpp
    basics/files.cpp
    basics/lockfile.cpp
    basics/signals.cpp
    basics/async_utils.cpp
    basics/dtoa.cpp
    basics/bit_packing.cpp
    basics/file_utils_ext.cpp
    basics/network_utils.cpp
    basics/crash_handler.cpp
    basics/random/random_generator.cpp
    basics/random/uniform_character.cpp
    basics/message_buffer.cpp
    basics/message_sequence_view.cpp
    basics/duckdb_engine.cpp
    basics/log.cpp
    basics/log_flags.cpp
    basics/zstd_context.cpp
)

set(FUNCTION2_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/third_party/function2")
set(MAGIC_ENUM_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/third_party/magic_enum")

target_include_directories(
    serene_base
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR} # auto-generated headers, like "basics/build.h"
        ${FUNCTION2_INCLUDE_DIR}
        ${MAGIC_ENUM_INCLUDE_DIR}
)

# Rebuild generated error files so they are up-to-date when all
# dependent stuff is built
add_dependencies(serene_base exitcodefiles)

target_link_libraries(
    serene_base
    PUBLIC
        absl::flags
        absl::flags_parse
        absl::flat_hash_set
        absl::random_random
        absl::synchronization
        absl::crc32c
        absl::flat_hash_map
        absl::node_hash_set
        absl::node_hash_map
        absl::synchronization
        absl::status
        absl::statusor
        absl::strings
        faiss
        fmt::fmt
        yaclib
        zlib
        icu
        lz4_static
        zstd::zstd # basics/zstd_context.cpp (reusable zstd contexts)
        serene_build_id
        # The SDB_* logger hot path forwards directly to duckdb::Logger
        # virtuals; we need duckdb's headers (and the Logger vtable) at
        # link time. Pulled in PUBLICly so any target that uses
        # SDB_INFO / SDB_TRACE inline gets the duckdb headers it needs.
        duckdb_static
        ${SYSTEM_LIBRARIES}
    PRIVATE
        boost_boost
        absl::stacktrace
        absl::symbolize
        absl::failure_signal_handler
)

################################################################################
## iresearch
################################################################################

set(IRESEARCH_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/iresearch")
set(IRESEARCH_BUILD_DIR "${CMAKE_BINARY_DIR}/iresearch")

# copy pre-built files over to the build directory
# this is required to avoid triggering file generation via bison and perl during the build procedure
file(
    GLOB_RECURSE IRESEARCH_PREBUILT
    RELATIVE "${IRESEARCH_ROOT}.build"
    FOLLOW_SYMLINKS
    "${IRESEARCH_ROOT}.build/*"
)
foreach(ELEMENT ${IRESEARCH_PREBUILT})
    configure_file(
        "${IRESEARCH_ROOT}.build/${ELEMENT}"
        "${IRESEARCH_BUILD_DIR}/${ELEMENT}"
        COPYONLY
    )
endforeach()

add_subdirectory(iresearch "${IRESEARCH_BUILD_DIR}" EXCLUDE_FROM_ALL)

target_compile_definitions(iresearch-static PRIVATE -DDISABLE_EXECINFO)

################################################################################
## serene
################################################################################

add_library(serene STATIC app/app_server.cpp app/init.cpp)

target_link_libraries(serene PUBLIC s2 boost_boost serene_base simdjson)

# this is "."
target_include_directories(serene PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# some auto-generated headers live here, like "basics/build.h"
target_include_directories(serene PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

target_include_directories(serene SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})

add_dependencies(serene exitcodefiles)

add_subdirectory(geo)
add_subdirectory(build_id)
