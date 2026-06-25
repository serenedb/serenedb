add_library(
  clickhouse_ext_storage OBJECT
  clickhouse_catalog.cpp
  clickhouse_schema_entry.cpp
  clickhouse_table_entry.cpp
  clickhouse_insert.cpp
  clickhouse_delete.cpp
  clickhouse_update.cpp
  clickhouse_transaction.cpp
  clickhouse_transaction_manager.cpp
  clickhouse_storage.cpp)

target_include_directories(clickhouse_ext_storage PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../clickhouse-cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:clickhouse_ext_storage>
    PARENT_SCOPE)
