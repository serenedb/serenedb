add_custom_target(serenedb-bench-micro)

set(BENCH_RESOURCE_DIR ${CMAKE_SOURCE_DIR}/resources/tests/bench/micro/)

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/bench_config.hpp.in"
    "${CMAKE_CURRENT_BINARY_DIR}/bench_config.hpp"
)

function(add_bench FILE_NAME)
    set(NAME serenedb-bench-micro-${FILE_NAME})
    add_executable(${NAME} ${FILE_NAME}.cpp 
        ${CMAKE_CURRENT_BINARY_DIR}/bench_config.hpp
    )   
    target_link_libraries(${NAME} PRIVATE sereneserver benchmark)
    add_dependencies(serenedb-bench-micro ${NAME})

    target_include_directories(${NAME} PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
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
add_bench(fst_table)