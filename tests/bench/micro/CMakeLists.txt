add_executable(serenedb-bench-micro main.cpp)
target_link_libraries(
    serenedb-bench-micro
    PRIVATE
        sereneserver
        benchmark
        search-benchmark-common
        simdutf
        simdjson
        faiss
)
sdb_link_duckdb_extensions(serenedb-bench-micro)
target_include_directories(
    serenedb-bench-micro
    PRIVATE ${CMAKE_SOURCE_DIR}/tests/iresearch
)

function(add_bench name)
    target_sources(serenedb-bench-micro PRIVATE ${name}.cpp)
    set_property(
        SOURCE ${name}.cpp
        APPEND
        PROPERTY COMPILE_DEFINITIONS "SDB_BENCH_MODULE=\"${name}\""
    )
    set_property(
        SOURCE ${name}.cpp
        APPEND
        PROPERTY
            COMPILE_OPTIONS -include ${CMAKE_CURRENT_SOURCE_DIR}/bench_module.h
    )
endfunction()

add_bench(bm25_byte_norm)
add_bench(ascii_probe)
add_bench(vector_access)
add_bench(crc32c)
add_bench(delimited_scan)
add_bench(document_mask)
add_bench(docs_mask_kinds)
add_bench(function)
add_bench(hash_map)
add_bench(insertion_order_map)
add_bench(tokenizer_pool)
add_bench(invert_kernels)
add_bench(lower_bound)
add_bench(exclude_costs)
add_bench(memory_file)
add_bench(multi_delimited_tokenizer)
add_bench(posting_fill_seek)
add_bench(posting_block_codec)
add_bench(text_stream)
add_bench(split_by_non_alpha)
add_bench(term_enumeration)
add_bench(top_term_selector)
add_bench(random)
set_property(
    SOURCE random.cpp
    APPEND
    PROPERTY COMPILE_DEFINITIONS USE_PCG_RANDOM
)
add_bench(io_buffers)
add_bench(fp_neg_zero_cmp)
add_bench(hugeint)
add_bench(uuid_print)
add_bench(json_functions)
add_bench(config_var_lookup)
add_bench(setting_ref)
add_bench(norm_gather)
add_bench(norm_fetch)
add_bench(word_bloom)
add_bench(path_hierarchy_tokenizer)
add_bench(regexp_vs_wildcard)
add_bench(regexp_engines)
add_bench(packed_key_agg)
add_bench(http_codecs)
add_bench(http_compression)
set_property(
    SOURCE http_compression.cpp
    APPEND
    PROPERTY COMPILE_DEFINITIONS "SDB_DOCS_DIR=\"${CMAKE_SOURCE_DIR}/docs\""
)
add_bench(http_response)
add_bench(http_router)
add_bench(listen_spec)
add_bench(sloppy_phrase_vs_disjunction)
add_bench(phrase_search)
add_bench(phrase_recheck)
add_bench(row_gather_density)
add_bench(filter_prepare)
add_bench(consecutive_run_length)
add_bench(parse_query)
add_bench(literal_lookup)
add_bench(ci_hash)
add_bench(ci_compare)
add_bench(socket_write)
add_bench(duckdb_collector)
add_bench(columnstore_vs_duckdb)
add_bench(column_point_read)
add_bench(select_thresholds)
add_bench(clustering)
add_bench(curve_ranges)
add_bench(cartesian_ranges)
target_include_directories(
    serenedb-bench-micro
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/duckdb_spatial/src
)
add_bench(simd_base64)
add_bench(simd_json_validate)
add_bench(json_leaf_reader)
add_bench(simd_utf8_validate)
add_bench(virtual_column_map)
add_bench(tokenizer_fill)
add_bench(stemming)
add_bench(stringzilla_kernels)
add_bench(vector_distances)
add_bench(quant_quality)
add_bench(table_fetch)

if(ARCH_AMD64)
    add_bench(ci_lower)
    add_bench(ci_fold128)
    add_bench(rot_bench)
endif()
