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
    ${CMAKE_CURRENT_SOURCE_DIR}/../../clickhouse-cpp
    # Shared dbconnector filter-pushdown renderer (dbconnector::table_scan::
    # FilterPushdown). Headers only: the object code is compiled by the postgres
    # extension (postgres_ext_dbconnector), which is always statically linked
    # into serened alongside this one, so the symbols resolve without compiling
    # -- and re-compiling them here would duplicate-define them in the binary.
    ${CMAKE_CURRENT_SOURCE_DIR}/../../database-connector/src/include)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:clickhouse_ext_library>
    PARENT_SCOPE)
