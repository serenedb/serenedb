include_directories(include)

add_subdirectory(storage)

add_library(
  clickhouse_ext_library OBJECT
  clickhouse_extension.cpp
  clickhouse_connection.cpp
  clickhouse_types.cpp
  clickhouse_filter_pushdown.cpp
  clickhouse_scanner.cpp
  clickhouse_query.cpp
  clickhouse_secrets.cpp)

target_include_directories(clickhouse_ext_library PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../clickhouse-cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:clickhouse_ext_library>
    PARENT_SCOPE)
