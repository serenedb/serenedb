# - Find Snowball (libstemmer.h)
# This module defines
#  SNOWBALL_INCLUDE_DIR, directory containing headers
#  SNOWBALL_FOUND, whether Snowball has been found

if("${SNOWBALL_ROOT}" STREQUAL "")
    message(FATAL_ERROR "SNOWBALL_ROOT is not set")
endif()

set(SNOWBALL_SEARCH_HEADER_PATHS
    ${SNOWBALL_ROOT}/include
    ${SNOWBALL_ROOT}/include/libstemmer
)

set(SNOWBALL_SEARCH_SRC_PATHS
    ${SNOWBALL_ROOT}
    ${SNOWBALL_ROOT}/libstemmer
    ${SNOWBALL_ROOT}/libstemmer/libstemmer
    ${SNOWBALL_ROOT}/src
    ${SNOWBALL_ROOT}/src/libstemmer
    ${SNOWBALL_ROOT}/src/libstemmer/libstemmer
)

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
    ${SNOWBALL_ROOT}
    EXCLUDE_FROM_ALL # do not build unused targets
)
target_include_directories(stemmer-static PUBLIC ${SNOWBALL_INCLUDE_DIR})
