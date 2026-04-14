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

add_bench(call_once)
add_bench(column_writer)
add_bench(crc32c)
add_bench(function)
add_bench(hash_map)
add_bench(lower_bound)
add_bench(memory_file)
add_bench(segmentation_stream)
# add_bench(simd_utils)
add_bench(top_term_collector)
add_bench(random)
add_bench(io_buffers)
add_bench(fp_neg_zero_cmp)
add_bench(uuid_print)
add_bench(json_functions)
add_bench(config_var_lookup)
add_bench(norm_gather)
add_bench(path_hierarchy_tokenizer)

# vector distances
add_executable(vector-distances-bench vector_distances.cpp)
target_compile_options(
    vector-distances-bench
    PRIVATE
        -fno-lto # no faiss inlined
        -fapprox-func
        -fno-math-errno
        -fassociative-math
        -freciprocal-math
        -fno-signed-zeros
        -fno-trapping-math
        -fno-rounding-math
        -ffp-contract=fast
)
target_link_options(vector-distances-bench PRIVATE -fno-lto)

target_link_libraries(vector-distances-bench PRIVATE sereneserver benchmark)
if(VELOX_ENABLE_FAISS)
    target_compile_definitions(
        vector-distances-bench
        PRIVATE VELOX_ENABLE_FAISS
    )
endif()
add_dependencies(serenedb-bench-micro vector-distances-bench)
