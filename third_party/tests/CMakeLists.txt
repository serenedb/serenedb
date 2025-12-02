################################################################################
### CmdLine library
################################################################################

add_library(
  iresearch-cmdline
  INTERFACE
)

target_include_directories(
  iresearch-cmdline
  SYSTEM
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/cmdline
)

################################################################################
### RapidJSON library
################################################################################

sdb_update_module(${GIT_EXECUTABLE} "rapidjson" ${CMAKE_CURRENT_SOURCE_DIR} "README.md")

add_library(rapidjson INTERFACE)

target_include_directories(rapidjson SYSTEM
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/rapidjson/include
)

target_compile_definitions(rapidjson
  INTERFACE -DRAPIDJSON_HAS_STDSTRING
)

################################################################################
### utfcpp library
################################################################################

add_library(
  iresearch-utfcpp
  INTERFACE
)

target_include_directories(
  iresearch-utfcpp
  SYSTEM
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/utfcpp
)
