set(PODOFO_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../podofo")
include("${PODOFO_SOURCE_DIR}/PoDoFoVersion.cmake")

configure_file(
    "${PODOFO_SOURCE_DIR}/src/podofo/auxiliary/podofo_config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/podofo_config.h"
)
configure_file(
    "${PODOFO_SOURCE_DIR}/src/podofo/private/podofo_config_private.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/podofo_config_private.h"
)

file(
    GLOB_RECURSE PODOFO_SOURCES
    CONFIGURE_DEPENDS
    "${PODOFO_SOURCE_DIR}/src/podofo/main/*.cpp"
    "${PODOFO_SOURCE_DIR}/src/podofo/auxiliary/*.cpp"
    "${PODOFO_SOURCE_DIR}/src/podofo/optional/*.cpp"
    "${PODOFO_SOURCE_DIR}/src/podofo/staging/*.cpp"
    "${PODOFO_SOURCE_DIR}/src/podofo/private/*.cpp"
)
list(
    FILTER PODOFO_SOURCES
    EXCLUDE
    REGEX "/(PdfFontConfigWrapper|FontUtilsAFDKO|JpegCommon)\\.cpp$"
)
add_library(
    podofo STATIC
    ${PODOFO_SOURCES}
    "${PODOFO_SOURCE_DIR}/3rdparty/chromium/pdfium/core/fxcodec/fax/faxmodule.cpp"
    "${PODOFO_SOURCE_DIR}/3rdparty/chromium/pdfium/core/fxcodec/scanlinedecoder.cpp"
    "${PODOFO_SOURCE_DIR}/3rdparty/utf8proc/utf8proc.c"
)
target_compile_definitions(
    podofo
    PUBLIC PODOFO_STATIC
    PRIVATE PODOFO_BUILD HAS_UNCAUGHT_EXCEPTIONS=1 UTF8PROC_STATIC
)
target_include_directories(
    podofo
    SYSTEM
    PUBLIC
        "${PODOFO_SOURCE_DIR}/src"
        "${CMAKE_CURRENT_BINARY_DIR}/include"
    PRIVATE
        "${PODOFO_SOURCE_DIR}/3rdparty/chromium"
        "${PODOFO_SOURCE_DIR}/3rdparty/date"
        "${PODOFO_SOURCE_DIR}/3rdparty/tcbspan"
        "${PODOFO_SOURCE_DIR}/3rdparty/utf8cpp"
        "${PODOFO_SOURCE_DIR}/3rdparty/utf8proc"
)
target_link_libraries(podofo PRIVATE freetype _libxml2 OpenSSL::SSL ZLIB::ZLIB)
