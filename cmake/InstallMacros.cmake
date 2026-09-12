################################################################################
## INSTALL
################################################################################

if(NOT CMAKE_INSTALL_SYSCONFDIR_SERENE)
    message(FATAL_ERROR "CMAKE_INSTALL_SYSCONFDIR_SERENE not set!")
endif()

# Global macros ----------------------------------------------------------------
# installs a config file -------------------------------------------------------
macro(install_config name path)
    if(OS_DARWIN)
        # var is redirected to ~ for the macos bundle
        set(LOCALSTATEDIR "@HOME@")
    else()
        set(LOCALSTATEDIR "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}")
    endif()

    configure_file(
        "${path}/${name}.conf.in"
        "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_SYSCONFDIR_SERENE}/${name}.conf"
        NEWLINE_STYLE UNIX
        @ONLY
    )

    install(
        FILES
            ${PROJECT_BINARY_DIR}/${CMAKE_INSTALL_SYSCONFDIR_SERENE}/${name}.conf
        DESTINATION ${CMAKE_INSTALL_SYSCONFDIR_SERENE}
    )
endmacro()

# installs a readme file converting EOL ----------------------------------------
macro(install_readme input output)
    install(
        CODE
            "configure_file(${PROJECT_SOURCE_DIR}/${input} \"${PROJECT_BINARY_DIR}/${output}\" NEWLINE_STYLE UNIX)"
    )
    install(
        FILES "${PROJECT_BINARY_DIR}/${output}"
        DESTINATION "${CMAKE_INSTALL_DOCDIR}"
    )
endmacro()
