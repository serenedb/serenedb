set(SDB_SDSL_SOURCE_DIR
    ""
    CACHE PATH
    "SDSL source checkout, including external/libdivsufsort"
)
if(NOT EXISTS "${SDB_SDSL_SOURCE_DIR}/include/sdsl/csa_wt.hpp")
    message(
        FATAL_ERROR
        "SDB_FM_INDEX requires -DSDB_SDSL_SOURCE_DIR=<sdsl-lite checkout>"
    )
endif()

set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(BUILD_SHARED_LIBS OFF)
set(BUILD_EXAMPLES OFF)
set(BUILD_DIVSUFSORT64 ON)
set(USE_OPENMP OFF)
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
add_subdirectory(
    "${SDB_SDSL_SOURCE_DIR}/external/libdivsufsort"
    divsufsort
    SYSTEM
)

add_library(
    sdb_sdsl
    STATIC
    "${SDB_SDSL_SOURCE_DIR}/lib/bits.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/coder_elias_delta.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/coder_elias_gamma.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/coder_fibonacci.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/config.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/construct_config.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/construct_isa.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/construct_sa.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/construct_sa_se.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/csa_alphabet_strategy.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/io.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/memory_management.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/ram_filebuf.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/ram_fs.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/rrr_vector_15.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/sd_vector.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/sfstream.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/uint128_t.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/uint256_t.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/util.cpp"
    "${SDB_SDSL_SOURCE_DIR}/lib/wt_helper.cpp"
)
target_include_directories(
    sdb_sdsl
    SYSTEM
    PUBLIC
        "${SDB_SDSL_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_BINARY_DIR}/divsufsort/include"
)
target_link_libraries(sdb_sdsl PUBLIC divsufsort divsufsort64)
target_compile_options(sdb_sdsl PRIVATE -w)
