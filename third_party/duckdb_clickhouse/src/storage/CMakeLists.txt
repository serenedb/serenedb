add_library(
  clickhouse_ext_storage OBJECT
  clickhouse_catalog.cpp
  clickhouse_schema_entry.cpp
  clickhouse_table_entry.cpp
  clickhouse_insert.cpp
  clickhouse_delete.cpp
  clickhouse_update.cpp
  clickhouse_merge_into.cpp
  clickhouse_transaction.cpp
  clickhouse_transaction_manager.cpp
  clickhouse_storage.cpp)

target_include_directories(clickhouse_ext_storage PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../clickhouse-cpp
    # dbconnector headers reached via clickhouse_scanner.hpp (BindData base);
    # object code comes from postgres_ext_dbconnector, same as in ../CMakeLists.
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../database-connector/src/include)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:clickhouse_ext_storage>
    PARENT_SCOPE)
