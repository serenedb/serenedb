# - Find Snowball (libstemmer.h, stemmer.lib libstemmer.a, libstemmer.so, and libstemmer.so.0d)
# This module defines
#  SNOWBALL_INCLUDE_DIR, directory containing headers
#  SNOWBALL_LIBRARY_DIR, directory containing Snowball libraries
#  SNOWBALL_SHARED_LIB, path to stemmer.so/stemmer.dll
#  SNOWBALL_STATIC_LIB, path to stemmer.lib
#  SNOWBALL_FOUND, whether Snowball has been found

if("${SNOWBALL_ROOT}" STREQUAL "")
    set(SNOWBALL_ROOT "$ENV{SNOWBALL_ROOT}")
    if(NOT "${SNOWBALL_ROOT}" STREQUAL "")
        string(REPLACE "\"" "" SNOWBALL_ROOT ${SNOWBALL_ROOT})
    endif()
endif()

if(NOT "${SNOWBALL_ROOT}" STREQUAL "")
    set(SNOWBALL_SEARCH_HEADER_PATHS
        ${SNOWBALL_ROOT}/include
        ${SNOWBALL_ROOT}/include/libstemmer
    )

    set(SNOWBALL_SEARCH_LIB_PATHS
        ${SNOWBALL_ROOT}/lib
        ${SNOWBALL_ROOT}/build
        ${SNOWBALL_ROOT}/Release
        ${SNOWBALL_ROOT}/build/Release
        ${SNOWBALL_ROOT}/Debug
        ${SNOWBALL_ROOT}/build/Debug
    )

    set(SNOWBALL_SEARCH_SRC_PATHS
        ${SNOWBALL_ROOT}
        ${SNOWBALL_ROOT}/libstemmer
        ${SNOWBALL_ROOT}/libstemmer/libstemmer
        ${SNOWBALL_ROOT}/src
        ${SNOWBALL_ROOT}/src/libstemmer
        ${SNOWBALL_ROOT}/src/libstemmer/libstemmer
    )
else()
    set(SNOWBALL_SEARCH_HEADER_PATHS
        "/usr/include"
        "/usr/include/libstemmer"
        "/usr/include/x86_64-linux-gnu"
        "/usr/include/x86_64-linux-gnu/libstemmer"
    )

    set(SNOWBALL_SEARCH_LIB_PATHS
        "/lib"
        "/lib/x86_64-linux-gnu"
        "/usr/lib"
        "/usr/lib/x86_64-linux-gnu"
    )

    set(SNOWBALL_SEARCH_SRC_PATHS
        "/usr/src"
        "/usr/src/libstemmer"
        "/usr/src/libstemmer/libstemmer"
    )
endif()

find_path(
    SNOWBALL_INCLUDE_DIR
    libstemmer.h
    PATHS ${SNOWBALL_SEARCH_HEADER_PATHS}
    NO_DEFAULT_PATH # make sure we don't accidentally pick up a different version
)

find_path(
    SNOWBALL_SRC_DIR_LIBSTEMMER
    libstemmer_c.in
    PATHS ${SNOWBALL_SEARCH_SRC_PATHS}
    NO_DEFAULT_PATH # make sure we don't accidentally pick up a different version
)

if(NOT SNOWBALL_INCLUDE_DIR OR NOT SNOWBALL_SRC_DIR_LIBSTEMMER)
    message(FATAL_ERROR "cannot build snowball from source")
endif()

set(SNOWBALL_FOUND TRUE)
get_filename_component(
    SNOWBALL_SRC_DIR_PARENT
    ${SNOWBALL_SRC_DIR_LIBSTEMMER}
    DIRECTORY
)
set(STEMMER_SOURCE_DIR ${SNOWBALL_SRC_DIR_PARENT})
add_subdirectory(
    ${CMAKE_SOURCE_DIR}/third_party/snowball
    EXCLUDE_FROM_ALL # do not build unused targets
)
set(SNOWBALL_LIBRARY_DIR ${SNOWBALL_SEARCH_LIB_PATHS})
set(SNOWBALL_STATIC_LIB stemmer-static)
target_include_directories(stemmer-static PUBLIC ${SNOWBALL_INCLUDE_DIR})
