cmake_minimum_required(VERSION 3.25)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(serenedb_stemmer_differential LANGUAGES C CXX)

    set(CMAKE_CXX_STANDARD 20)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)

    get_filename_component(SERENEDB_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
    set(LIBSTEMMER_DIR "${SERENEDB_SOURCE_DIR}/third_party/libstemmer_c")

    file(GLOB GENERATED_STEMMERS CONFIGURE_DEPENDS "${LIBSTEMMER_DIR}/src_c/stem_UTF_8_*.c")
    add_library(
        stemmer-static
        STATIC
        ${GENERATED_STEMMERS}
        "${LIBSTEMMER_DIR}/runtime/api.c"
        "${LIBSTEMMER_DIR}/runtime/snowball_runtime.c"
        "${LIBSTEMMER_DIR}/libstemmer/libstemmer_utf8.c"
    )
    target_include_directories(stemmer-static PUBLIC "${LIBSTEMMER_DIR}/include" PRIVATE "${LIBSTEMMER_DIR}")
else()
    set(SERENEDB_SOURCE_DIR "${CMAKE_SOURCE_DIR}")
    set(LIBSTEMMER_DIR "${SERENEDB_SOURCE_DIR}/third_party/libstemmer_c")
endif()

find_package(ZLIB REQUIRED)

add_executable(serenedb-stemmer-differential main.cpp)
target_include_directories(serenedb-stemmer-differential PRIVATE "${LIBSTEMMER_DIR}")
target_link_libraries(serenedb-stemmer-differential PRIVATE stemmer-static ZLIB::ZLIB)
