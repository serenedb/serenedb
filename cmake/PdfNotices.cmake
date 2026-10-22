set(PDF_PODOFO_SOURCE "${PROJECT_SOURCE_DIR}/third_party/podofo")
set(PDF_FREETYPE_SOURCE "${PROJECT_SOURCE_DIR}/third_party/freetype")
foreach(dependency PODOFO FREETYPE)
    execute_process(
        COMMAND
            "${GIT_EXECUTABLE}" -C "${PDF_${dependency}_SOURCE}" rev-parse HEAD
        OUTPUT_VARIABLE PDF_${dependency}_REVISION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        COMMAND_ERROR_IS_FATAL ANY
    )
endforeach()

configure_file(
    "${PROJECT_SOURCE_DIR}/resources/licenses/pdf/NOTICE.md.in"
    "${PROJECT_BINARY_DIR}/pdf-notices/NOTICE.md"
    @ONLY
)
install(
    FILES "${PROJECT_BINARY_DIR}/pdf-notices/NOTICE.md"
    DESTINATION "${CMAKE_INSTALL_DOCDIR}/pdf"
)
install(
    FILES "${PDF_PODOFO_SOURCE}/COPYING.MPL" "${PDF_PODOFO_SOURCE}/NOTICE"
    DESTINATION "${CMAKE_INSTALL_DOCDIR}/pdf/podofo"
)
install(
    FILES "${PROJECT_SOURCE_DIR}/resources/licenses/pdf/THIRD_PARTY.txt"
    DESTINATION "${CMAKE_INSTALL_DOCDIR}/pdf/podofo"
)
foreach(component pdfium numerics)
    install(
        FILES "${PDF_PODOFO_SOURCE}/3rdparty/chromium/${component}/LICENSE"
        DESTINATION "${CMAKE_INSTALL_DOCDIR}/pdf/podofo/${component}"
    )
endforeach()
install(
    FILES
        "${PDF_FREETYPE_SOURCE}/LICENSE.TXT"
        "${PDF_FREETYPE_SOURCE}/docs/FTL.TXT"
    DESTINATION "${CMAKE_INSTALL_DOCDIR}/pdf/freetype"
)
