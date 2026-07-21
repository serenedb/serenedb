add_custom_target(serenedb-bench-micro)

function(add_bench FILE_NAME)
    set(NAME serenedb-bench-micro-${FILE_NAME})
    add_executable(${NAME} ${FILE_NAME}.cpp)
    target_link_libraries(${NAME} PRIVATE sereneserver benchmark)
    if(VELOX_ENABLE_FAISS)
        target_compile_definitions(${NAME} PRIVATE VELOX_ENABLE_FAISS)
    endif()
    add_dependencies(serenedb-bench-micro ${NAME})
endfunction()

add_bench(crc32c)
add_bench(function)
add_bench(hash_map)
add_bench(lower_bound)
add_bench(memory_file)
add_bench(segmentation_stream)
add_bench(split_by_non_alpha)
add_bench(determinize_star)
add_bench(term_enumeration)
add_bench(top_term_selector)
add_bench(random)
add_bench(io_buffers)
add_bench(fp_neg_zero_cmp)
add_bench(uuid_print)
add_bench(json_functions)
add_bench(config_var_lookup)
add_bench(norm_gather)
add_bench(path_hierarchy_tokenizer)
add_bench(regexp_vs_wildcard)
add_bench(filter_prepare)
add_bench(consecutive_run_length)
add_bench(parse_query)
add_bench(literal_lookup)
add_bench(ci_hash)
add_bench(ci_lower)
add_bench(ci_fold128)
add_bench(ci_compare)
add_bench(socket_write)
add_bench(duckdb_collector)
add_bench(columnstore_vs_duckdb)
add_bench(select_thresholds)
add_bench(simd_base64)
add_bench(simd_json_validate)
add_bench(simd_utf8_validate)
add_bench(frozen_vs_alternatives)

# SIMD string-encoding benches: link the candidate libraries directly.
target_link_libraries(serenedb-bench-micro-simd_base64 PRIVATE simdutf)
target_link_libraries(serenedb-bench-micro-simd_json_validate PRIVATE simdjson)
target_link_libraries(
    serenedb-bench-micro-simd_utf8_validate
    PRIVATE simdutf simdjson
)
# Utf8Proc baseline lives in duckdb's vendored utf8proc (not a target-interface
# include), so add its header dir explicitly for the utf8 bench.
target_include_directories(
    serenedb-bench-micro-simd_utf8_validate
    SYSTEM
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/duckdb/third_party/utf8proc/include
)
target_include_directories(
    serenedb-bench-micro-ci_hash
    SYSTEM
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/lz4/lib
)

# frozen_vs_alternatives measures the boost::text frozen::set use-cases against
# what we already vendor; needs frozen headers + the boost::text data tables.
target_link_libraries(serenedb-bench-micro-frozen_vs_alternatives PRIVATE text)
target_compile_definitions(
    serenedb-bench-micro-frozen_vs_alternatives
    PRIVATE BOOST_TEXT_DISABLE_CONCEPTS
)
target_include_directories(
    serenedb-bench-micro-frozen_vs_alternatives
    SYSTEM
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/frozen/include
)

# regexp_vs_wildcard splits UTF-8 via duckdb's vendored utf8proc + simdutf.
target_link_libraries(serenedb-bench-micro-regexp_vs_wildcard PRIVATE simdutf)
target_include_directories(
    serenedb-bench-micro-regexp_vs_wildcard
    SYSTEM
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/duckdb/third_party/utf8proc/include
)

target_include_directories(
    serenedb-bench-micro-literal_lookup
    SYSTEM
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/frozen/include
)
# frozen's compile-time perfect-hash construction at N=1000 exceeds the default
# constexpr step budget.
target_compile_options(
    serenedb-bench-micro-literal_lookup
    PRIVATE -fconstexpr-steps=200000000
)

# vector distances
add_executable(vector-distances-bench vector_distances.cpp)
target_compile_options(
    vector-distances-bench
    PRIVATE -fassociative-math -fno-signed-zeros
)

target_link_libraries(vector-distances-bench PRIVATE sereneserver benchmark)
if(VELOX_ENABLE_FAISS)
    target_compile_definitions(
        vector-distances-bench
        PRIVATE VELOX_ENABLE_FAISS
    )
endif()
add_dependencies(serenedb-bench-micro vector-distances-bench)
