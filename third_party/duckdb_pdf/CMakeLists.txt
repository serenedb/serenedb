cmake_minimum_required(VERSION 3.28)
project(pdf)

include_directories(src/include)
build_static_extension(pdf src/pdf_extension.cpp)
build_loadable_extension(pdf "" src/pdf_extension.cpp)
target_link_libraries(pdf_extension podofo)
target_link_libraries(pdf_loadable_extension podofo)

install(
  TARGETS pdf_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
