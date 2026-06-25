################################################################################
## INSTALL
################################################################################

if(
    NOT CMAKE_INSTALL_SYSCONFDIR_SERENE
    OR NOT CMAKE_INSTALL_FULL_SYSCONFDIR_SERENE
    OR NOT CMAKE_INSTALL_DATAROOTDIR_SERENE
    OR NOT CMAKE_INSTALL_FULL_DATAROOTDIR_SERENE
)
    message(
        FATAL_ERROR,
        "CMAKE_INSTALL_DATAROOTDIR or CMAKE_INSTALL_SYSCONFDIR not set!"
    )
endif()

# Global macros ----------------------------------------------------------------
# installs a config file -------------------------------------------------------
macro(install_config name path)
    if(DARWIN AND NOT HOMEBREW)
        set(PKGDATADIR "@ROOTDIR@/${CMAKE_INSTALL_DATAROOTDIR_SERENE}")
        if(DARWIN)
            # var will be redirected to ~ for the macos bundle
            set(LOCALSTATEDIR "@HOME@${INC_CPACK_SERVER_STATE_DIR}")
        else()
            set(LOCALSTATEDIR "@ROOTDIR@${CMAKE_INSTALL_LOCALSTATEDIR}")
        endif()
        set(SBINDIR "@ROOTDIR@/${CMAKE_INSTALL_SBINDIR}")
        set(SYSCONFDIR "@ROOTDIR@/${CMAKE_INSTALL_SYSCONFDIR_SERENE}")
    else()
        set(PKGDATADIR "${CMAKE_INSTALL_FULL_DATAROOTDIR_SERENE}")
        set(LOCALSTATEDIR "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}")
    endif()

    if(ENABLE_UID_CFG)
        set(DEFINEUID "")
    else()
        set(DEFINEUID "# ")
    endif()

    set(CRLFSTYLE "UNIX")
    set(PROGRAM_SUFFIX "")

    configure_file(
        "${path}/${name}.conf.in"
        "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_SYSCONFDIR_SERENE}/${name}.conf"
        NEWLINE_STYLE ${CRLFSTYLE}
        @ONLY
    )

    install(
        FILES
            ${PROJECT_BINARY_DIR}/${CMAKE_INSTALL_SYSCONFDIR_SERENE}/${name}.conf
        DESTINATION ${CMAKE_INSTALL_SYSCONFDIR_SERENE}
    )

    set(INSTALL_CONFIGFILES_LIST
        "${INSTALL_CONFIGFILES_LIST};${CMAKE_INSTALL_SYSCONFDIR_SERENE}/${name}.conf"
        CACHE INTERNAL
        "INSTALL_CONFIGFILES_LIST"
    )
endmacro()

# installs a readme file converting EOL ----------------------------------------
macro(install_readme input output)
    set(where "${CMAKE_INSTALL_DOCDIR}")

    set(PKG_VERSION "")
    if(${USE_VERSION_IN_LICENSEDIR})
        set(PKG_VERSION "-${SERENEDB_VERSION}")
    endif()
    set(CRLFSTYLE "UNIX")

    install(
        CODE
            "configure_file(${PROJECT_SOURCE_DIR}/${input} \"${PROJECT_BINARY_DIR}/${output}\" NEWLINE_STYLE ${CRLFSTYLE})"
    )
    install(FILES "${PROJECT_BINARY_DIR}/${output}" DESTINATION "${where}")
endmacro()

# installs a link to an executable ---------------------------------------------
if(INSTALL_MACROS_NO_TARGET_INSTALL)
    macro(install_command_alias name where alias)
        add_custom_command(
            OUTPUT ${alias} POST_BUILD
            COMMAND
                ${CMAKE_COMMAND} -E create_symlink ${name}
                ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${alias}
        )
        install(
            PROGRAMS ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${alias}
            DESTINATION ${where}
        )
    endmacro()
else()
    macro(install_command_alias name where alias)
        add_custom_command(
            TARGET ${name}
            POST_BUILD
            COMMAND
                ${CMAKE_COMMAND} -E create_symlink ${name}
                ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${alias}
        )
        install(
            PROGRAMS ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${alias}
            DESTINATION ${where}
        )
    endmacro()
endif()

macro(to_native_path sourceVarName)
    string(REGEX REPLACE "//*" "/" "myVar" "${${sourceVarName}}")
    set("INC_${sourceVarName}" ${myVar})
endmacro()
