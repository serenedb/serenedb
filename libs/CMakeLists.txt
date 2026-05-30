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
    basics/encoding_utils.cpp
    basics/number_utils.cpp
    basics/cleanup_functions.cpp
    basics/exceptions.cpp
    basics/file_result.cpp
    basics/file_utils.cpp
    basics/number_of_cores.cpp
    basics/page_size.cpp
    basics/physical_memory.cpp
    basics/read_write_lock.cpp
    basics/result.cpp
    basics/result_error.cpp
    basics/string_utils.cpp
    basics/thread.cpp
    basics/static_strings.cpp
    basics/application-exit.cpp
    basics/debugging.cpp
    basics/error.cpp
    basics/files.cpp
    basics/levenshtein.cpp
    basics/memory.cpp
    basics/process-utils.cpp
    basics/signals.cpp
    basics/system-functions.cpp
    basics/terminal-utils-posix.cpp
    basics/threads-posix.cpp
    basics/strings.cpp
    basics/zip.cpp
    basics/datetime.cpp
    basics/hashes.cpp
    basics/utf8_helper.cpp
    basics/cpu_usage_snapshot.cpp
    basics/file_descriptors.cpp
    basics/hybrid_logical_clock.cpp
    basics/socket-utils.cpp
    basics/terminal-utils.cpp
    basics/thread_utils.cpp
    basics/async_utils.cpp
    basics/dtoa.cpp
    basics/bit_packing.cpp
    basics/so_utils.cpp
    basics/file_utils_ext.cpp
    basics/process_utils.cpp
    basics/network_utils.cpp
    basics/memory.cpp
    basics/crash_handler.cpp
    basics/units_helper.cpp
    basics/random/random_generator.cpp
    basics/random/uniform_character.cpp
    basics/logger/logger.cpp
    basics/ssl/ssl_interface.cpp
    basics/ssl/ssl_helper.cpp
    basics/message_buffer.cpp
    basics/message_chunk.cpp
    basics/message_sequence_view.cpp
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
add_dependencies(serene_base errorfiles exitcodefiles)

target_link_libraries(
    serene_base
    PUBLIC
        absl::flat_hash_set
        absl::random_random
        absl::synchronization
        absl::crc32c
        absl::flat_hash_map
        absl::node_hash_set
        absl::node_hash_map
        absl::synchronization
        absl::strings
        faiss
        fmt::fmt
        yaclib
        zlib
        icu
        lz4_static
        minizip
        serene_build_id
        ${SYSTEM_LIBRARIES}
    PRIVATE date_interface boost_boost absl::stacktrace absl::symbolize
)

################################################################################
## VPack
################################################################################

add_subdirectory(vpack EXCLUDE_FROM_ALL)

target_link_libraries(vpack PUBLIC serene_base)

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

set(LIB_SERVER_POSIX
    endpoint/endpoint_unix_domain.cpp
    endpoint/endpoint_srv.cpp
)

set(LIB_SERVER_DATE ${CMAKE_SOURCE_DIR}/third_party/date/src/tz.cpp)

add_library(
    serene
    STATIC
    ${LIB_SERVER_POSIX}
    ${LIB_SERVER_DATE}
    app/app_feature.cpp
    app/app_server.cpp
    app/global_context.cpp
    app/logger_feature.cpp
    app/name_validator.cpp
    app/options/option.cpp
    app/options/program_options.cpp
    app/options/section.cpp
    app/options/translator.cpp
    endpoint/endpoint.cpp
    endpoint/endpoint_ip.cpp
    endpoint/endpoint_ip_v4.cpp
    endpoint/endpoint_ip_v6.cpp
    endpoint/endpoint_list.cpp
    rest/common_defines.cpp
    rest/general_request.cpp
    rest/general_response.cpp
    rest/http_request.cpp
    rest/http_response.cpp
    rest/version.cpp
)

target_link_libraries(
    serene
    PUBLIC s2 boost_boost vpack date_interface
)

# this is "."
target_include_directories(serene PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# some auto-generated headers live here, like "basics/build.h"
target_include_directories(serene PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

target_include_directories(serene SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})

target_link_libraries(serene PUBLIC rocksdb_interface)

add_dependencies(serene errorfiles exitcodefiles)

add_subdirectory(geo)
add_subdirectory(build_id)
