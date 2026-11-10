set(TARGET_NAME pdf)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(EXTENSION_SOURCES src/pdf_extension.cpp src/podofo_reader.cpp
                      src/podofo_layout.cpp src/podofo_tables.cpp)
include_directories(src/include)
include_directories(../duckdb/third_party/utf8proc/include)
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} "" ${EXTENSION_SOURCES})
foreach(target ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
  target_link_libraries(${target} podofo duckdb_utf8proc)
endforeach()
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
