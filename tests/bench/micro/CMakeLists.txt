add_executable(serenedb-bench-micro main.cpp)
target_link_libraries(
    serenedb-bench-micro
    PRIVATE
        sereneserver
        benchmark
        search-benchmark-common
        simdutf
        simdjson
        ${FAISS_LIB}
)
sdb_link_duckdb_extensions(serenedb-bench-micro PRIVATE)
target_include_directories(
    serenedb-bench-micro
    PRIVATE ${CMAKE_SOURCE_DIR}/tests/iresearch
)
target_include_directories(
    serenedb-bench-micro
    SYSTEM
    PRIVATE
        ${CMAKE_SOURCE_DIR}/third_party/duckdb/third_party/utf8proc/include
        ${CMAKE_SOURCE_DIR}/third_party/lz4/lib
)
add_custom_command(
    TARGET serenedb-bench-micro
    POST_BUILD
    COMMAND
        ${CMAKE_COMMAND} -E create_symlink serenedb-bench-micro
        $<TARGET_FILE_DIR:serenedb-bench-micro>/search-benchmark-game-build
    COMMAND
        ${CMAKE_COMMAND} -E create_symlink serenedb-bench-micro
        $<TARGET_FILE_DIR:serenedb-bench-micro>/search-benchmark-game-query
)

function(add_bench NAME)
    set(MODULE ${NAME})
    if(ARGC GREATER 1)
        set(MODULE ${ARGV1})
    endif()
    target_sources(serenedb-bench-micro PRIVATE ${NAME}.cpp)
    set_property(
        SOURCE ${NAME}.cpp TARGET_DIRECTORY serenedb-bench-micro
        APPEND
        PROPERTY COMPILE_DEFINITIONS "SDB_BENCH_MODULE=\"${MODULE}\""
    )
    set_property(
        SOURCE ${NAME}.cpp TARGET_DIRECTORY serenedb-bench-micro
        APPEND
        PROPERTY
            COMPILE_OPTIONS
                -include
                ${CMAKE_SOURCE_DIR}/tests/bench/micro/bench_module.h
    )
endfunction()

add_bench(bm25_byte_norm)
add_bench(ascii_probe)
add_bench(vector_access)
add_bench(crc32c)
add_bench(delimited_scan)
add_bench(document_mask)
add_bench(function)
add_bench(hash_map)
add_bench(insertion_order_map)
add_bench(tokenizer_pool)
add_bench(invert_kernels)
add_bench(lower_bound)
add_bench(memory_file)
add_bench(multi_delimited_tokenizer)
add_bench(posting_fill_seek)
add_bench(posting_block_codec)
add_bench(text_stream)
add_bench(split_by_non_alpha)
add_bench(term_enumeration)
add_bench(top_term_selector)
add_bench(random)
add_bench(io_buffers)
add_bench(fp_neg_zero_cmp)
add_bench(uuid_print)
add_bench(json_functions)
add_bench(config_var_lookup)
add_bench(setting_ref)
add_bench(norm_gather)
add_bench(position_groups)
add_bench(word_bloom)
add_bench(path_hierarchy_tokenizer)
add_bench(regexp_vs_wildcard)
add_bench(http_compression)
add_bench(sloppy_phrase_vs_disjunction)
add_bench(phrase_search)
add_bench(filter_prepare)
add_bench(consecutive_run_length)
add_bench(parse_query)
add_bench(literal_lookup)
add_bench(ci_hash)
add_bench(ci_compare)
add_bench(socket_write)
add_bench(duckdb_collector)
add_bench(columnstore_vs_duckdb)
add_bench(select_thresholds)
add_bench(clustering)
add_bench(simd_base64)
add_bench(simd_json_validate)
add_bench(simd_utf8_validate)
add_bench(virtual_column_map)
add_bench(tokenizer_fill)
add_bench(stemming)
add_bench(stringzilla_kernels)
add_bench(vector_distances)

if(ARCH_AMD64)
    add_bench(ci_lower)
    add_bench(ci_fold128)
endif()

set_property(
    SOURCE http_compression.cpp
    APPEND
    PROPERTY COMPILE_DEFINITIONS "SDB_DOCS_DIR=\"${CMAKE_SOURCE_DIR}/docs\""
)

# Same relaxed FP search/scorers/bm25.cpp is built with, otherwise the kernels here
# aren't the ones we ship.
set_property(
    SOURCE bm25_byte_norm.cpp
    APPEND
    PROPERTY
        COMPILE_OPTIONS
            -fapprox-func
            -fno-math-errno
            -fassociative-math
            -freciprocal-math
            -fno-signed-zeros
            -fno-trapping-math
            -fno-rounding-math
            -ffp-contract=fast
)

set_property(
    SOURCE vector_distances.cpp
    APPEND
    PROPERTY COMPILE_OPTIONS -fassociative-math -fno-signed-zeros
)
