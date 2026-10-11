cmake_minimum_required(VERSION 3.22)
project(re2-tests C CXX)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(SDB_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..)
set(TP ${SDB_ROOT}/third_party)

list(APPEND CMAKE_MODULE_PATH ${SDB_ROOT}/cmake)
include(OptimizeForArchitecture)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${ARCHITECTURE_OPTIMIZATIONS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ARCHITECTURE_OPTIMIZATIONS}")

enable_testing()

set(SIMDUTF_TESTS OFF CACHE BOOL "" FORCE)
set(SIMDUTF_TOOLS OFF CACHE BOOL "" FORCE)
set(SIMDUTF_ICONV OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP}/simdutf simdutf EXCLUDE_FROM_ALL)

set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
set(ABSL_BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP}/abseil-cpp absl EXCLUDE_FROM_ALL)
target_link_libraries(absl_strings PUBLIC simdutf)

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP}/googletest googletest EXCLUDE_FROM_ALL)

set(RE2_TEST ON CACHE BOOL "" FORCE)
set(RE2_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(${TP}/re2 re2)
