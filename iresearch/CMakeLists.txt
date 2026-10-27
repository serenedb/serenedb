find_package(FLEX)
find_package(BISON)
if(FLEX_FOUND AND BISON_FOUND AND NOT IRESEARCH_LIBRARY)
    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/parser/lucene_lexer.cpp
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND
            ${CMAKE_SOURCE_DIR}/scripts/generate_c.sh ${FLEX_EXECUTABLE}
            iresearch/parser/lucene_lexer.cpp iresearch/parser/lucene_lexer.l
        MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/parser/lucene_lexer.l
        VERBATIM
    )
    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/parser/lucene_parser.cpp
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND
            ${CMAKE_SOURCE_DIR}/scripts/generate_c.sh ${BISON_EXECUTABLE}
            iresearch/parser/lucene_parser.cpp iresearch/parser/lucene_parser.y
        MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/parser/lucene_parser.y
        VERBATIM
    )
endif()

add_library(iresearch STATIC parser/lucene_lexer.cpp parser/lucene_parser.cpp)

add_subdirectory(analysis)
add_subdirectory(formats)
add_subdirectory(index)
add_subdirectory(search)
add_subdirectory(store)
add_subdirectory(utils)

target_include_directories(
    iresearch
    PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR}
)
target_include_directories(
    iresearch
    SYSTEM
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/duckdb/third_party/fastpforlib
    PUBLIC
        ${CMAKE_SOURCE_DIR}/third_party/stringzilla/include
        ${CMAKE_SOURCE_DIR}/third_party/magic_enum
)

if(ARCH_AMD64)
    target_compile_definitions(
        iresearch
        PUBLIC SZ_USE_HASWELL=1 SZ_USE_SKYLAKE=1 SZ_USE_ICELAKE=1
    )
elseif(ARCH_AARCH64)
    target_compile_definitions(
        iresearch
        PUBLIC SZ_USE_NEON=1 SZ_USE_SVE=1 SZ_USE_SVE2=1
    )
endif()

target_link_libraries(
    iresearch
    PUBLIC
        icu
        absl::flat_hash_map
        absl::flat_hash_set
        absl::crc32c
        absl::flags
        absl::flags_parse
        absl::node_hash_map
        absl::node_hash_set
        absl::random_random
        absl::status
        absl::statusor
        absl::strings
        absl::synchronization
        fmt::fmt
        yaclib
        zlib
        zstd::zstd
        boost_boost
        simdjson
        sse2neon
        pcg
        re2
        roaring::roaring
        duckdb_static
    PRIVATE
        absl::failure_signal_handler
        absl::stacktrace
        absl::symbolize
        simdutf
)

if(IRESEARCH_BENCH)
    target_compile_definitions(
        iresearch
        PUBLIC IRESEARCH_NO_GEO IRESEARCH_NO_VECTOR
    )
    return()
endif()

target_include_directories(
    iresearch
    SYSTEM
    PUBLIC
        ${CMAKE_SOURCE_DIR}/third_party/duckdb/extension/core_functions/include
)
target_link_libraries(
    iresearch
    PUBLIC OpenSSL::Crypto OpenSSL::SSL fasttext-static faiss s2
    PRIVATE lz4_static stemmer-static
)

if(IRESEARCH_LIBRARY)
    target_compile_definitions(iresearch PRIVATE IRESEARCH_LIBRARY)
    target_sources(
        iresearch
        PRIVATE
            ${PROJECT_SOURCE_DIR}/third_party/duckdb_inet/src/inet_html_table.cpp
    )
    set_source_files_properties(
        ${PROJECT_SOURCE_DIR}/third_party/duckdb_inet/src/inet_html_table.cpp
        PROPERTIES COMPILE_OPTIONS -w
    )
    target_include_directories(
        iresearch
        PRIVATE ${PROJECT_SOURCE_DIR}/third_party/duckdb_inet/src
    )
endif()
