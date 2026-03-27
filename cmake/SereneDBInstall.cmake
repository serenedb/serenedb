include(GNUInstallDirs)
set(SERENEDB_SOURCE_DIR ${CMAKE_SOURCE_DIR})
set(CMAKE_INSTALL_SYSCONFDIR_SERENE
    "${CMAKE_INSTALL_SYSCONFDIR}/${CMAKE_PROJECT_NAME}"
)
set(CMAKE_INSTALL_FULL_SYSCONFDIR_SERENE
    "${CMAKE_INSTALL_FULL_SYSCONFDIR}/${CMAKE_PROJECT_NAME}"
)

set(CMAKE_INSTALL_DATAROOTDIR_SERENE
    "${CMAKE_INSTALL_DATAROOTDIR}/${CMAKE_PROJECT_NAME}"
)
set(CMAKE_INSTALL_FULL_DATAROOTDIR_SERENE
    "${CMAKE_INSTALL_FULL_DATAROOTDIR}/${CMAKE_PROJECT_NAME}"
)

if(DARWIN)
    set(ENABLE_UID_CFG false)
else()
    set(ENABLE_UID_CFG true)
endif()

# debug info directory:
if(${CMAKE_INSTALL_LIBDIR} STREQUAL "usr/lib64")
    # some systems have weird places for usr/lib:
    set(CMAKE_INSTALL_DEBINFO_DIR "usr/lib/debug/")
else()
    set(CMAKE_INSTALL_DEBINFO_DIR "${CMAKE_INSTALL_LIBDIR}/debug/")
endif()

set(CMAKE_INSTALL_SYSCONFDIR_SERENE
    "${CMAKE_INSTALL_SYSCONFDIR}/${CMAKE_PROJECT_NAME}"
)
set(CMAKE_INSTALL_FULL_SYSCONFDIR_SERENE
    "${CMAKE_INSTALL_FULL_SYSCONFDIR}/${CMAKE_PROJECT_NAME}"
)

# database directory
set(SERENEDB_DB_DIRECTORY "${PROJECT_BINARY_DIR}/var/lib/${CMAKE_PROJECT_NAME}")
file(MAKE_DIRECTORY ${SERENEDB_DB_DIRECTORY})

# logs
file(MAKE_DIRECTORY "${PROJECT_BINARY_DIR}/var/log/${CMAKE_PROJECT_NAME}")

set(INSTALL_ICU_DT_DEST "${CMAKE_INSTALL_DATAROOTDIR}/${CMAKE_PROJECT_NAME}")
set(INSTALL_TZDATA_DEST
    "${CMAKE_INSTALL_DATAROOTDIR}/${CMAKE_PROJECT_NAME}/tzdata"
)

set(CMAKE_TEST_DIRECTORY "tests")

include(InstallMacros)

# install ----------------------------------------------------------------------
if(UNIX)
    install(
        DIRECTORY ${SERENEDB_MAN_DIR}
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}
    )
endif()

install_readme(resources/licenses/GPL-3 GPL-3)
install_readme(resources/licenses/LGPL-3 LGPL-3)
install_readme(LICENSE LICENSE.txt)

################################################################################
### @brief install log directory
################################################################################

install(
    DIRECTORY ${PROJECT_BINARY_DIR}/var/log/serenedb
    DESTINATION ${CMAKE_INSTALL_LOCALSTATEDIR}/log
)

################################################################################
### @brief install database directory
################################################################################

install(
    DIRECTORY ${SERENEDB_DB_DIRECTORY}
    DESTINATION ${CMAKE_INSTALL_LOCALSTATEDIR}/lib
)

################################################################################
### @brief detect if we're on a systemd enabled system; if install unit file.
################################################################################

set(SYSTEMD_FOUND false)
set(IS_SYSTEMD_INSTALL 0)

if(UNIX)
    set(SERVICE_NAME "serenedb")

    # use pkgconfig for systemd detection
    find_package(PkgConfig QUIET)
    if(NOT PKG_CONFIG_FOUND)
        message(STATUS "pkg-config not found - skipping systemd detection")
    else()
        set(SYSTEMD_UNIT_DIR "")
        message(STATUS "detecting systemd")
        pkg_check_modules(SYSTEMD systemd)

        if(SYSTEMD_FOUND)
            message(STATUS "-- systemd found")

            # get systemd_unit_dir -- e.g /lib/systemd/system/
            # cmake to old: pkg_get_variable(SYSTEMD_UNIT_DIR systemd systemdsystemunitdir)
            execute_process(
                COMMAND
                    ${PKG_CONFIG_EXECUTABLE} systemd
                    --variable=systemdsystemunitdir
                OUTPUT_VARIABLE SYSTEMD_UNIT_DIR
                OUTPUT_STRIP_TRAILING_WHITESPACE
            )
            set(IS_SYSTEMD_INSTALL 1)

            # set prefix
            if(
                CMAKE_INSTALL_PREFIX
                AND NOT "${CMAKE_INSTALL_PREFIX}" STREQUAL "/"
            )
                set(SYSTEMD_UNIT_DIR
                    "${CMAKE_INSTALL_PREFIX}/${SYSTEMD_UNIT_DIR}/"
                )
            endif()

            # configure and install systemd service
            configure_file(
                ${SERENEDB_SOURCE_DIR}/packages/systemd/serenedb.service.in
                ${PROJECT_BINARY_DIR}/serenedb.service
                NEWLINE_STYLE UNIX
            )
            install(
                FILES ${PROJECT_BINARY_DIR}/serenedb.service
                DESTINATION ${SYSTEMD_UNIT_DIR}/
                RENAME ${SERVICE_NAME}.service
            )
        else()
            message(STATUS "-- systemd not found")
        endif(SYSTEMD_FOUND)
    endif(NOT PKG_CONFIG_FOUND)
endif(UNIX)
################################################################################
### @brief propagate the locations into our programms:
################################################################################

set(PATH_SEP "/")

to_native_path("PATH_SEP")
to_native_path("CMAKE_INSTALL_FULL_LOCALSTATEDIR")
to_native_path("CMAKE_INSTALL_FULL_SYSCONFDIR_SERENE")
to_native_path("PKGDATADIR")
to_native_path("CMAKE_INSTALL_DATAROOTDIR_SERENE")
to_native_path("CMAKE_INSTALL_SBINDIR")
to_native_path("CMAKE_INSTALL_BINDIR")
to_native_path("INSTALL_ICU_DT_DEST")
to_native_path("CMAKE_TEST_DIRECTORY")
to_native_path("INSTALL_TZDATA_DEST")

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/libs/basics/directories.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/libs/basics/directories.h"
    NEWLINE_STYLE UNIX
)

install(
    FILES "${CMAKE_SOURCE_DIR}/libs/basics/exitcodes.dat"
    DESTINATION "${INSTALL_ICU_DT_DEST}"
    RENAME exitcodes.dat
)

install(
    FILES "${CMAKE_SOURCE_DIR}/packages/serenedb-helper"
    DESTINATION "${INSTALL_ICU_DT_DEST}"
    RENAME serenedb-helper
)

install(
    FILES "${CMAKE_SOURCE_DIR}/packages/serenedb-update-db"
    DESTINATION "${INSTALL_ICU_DT_DEST}"
    RENAME serenedb-update-db
)

install(FILES ${TZ_DATA_FILES} DESTINATION "${INSTALL_TZDATA_DEST}")

if(THIRDPARTY_SBIN)
    install(
        FILES ${THIRDPARTY_SBIN}
        PERMISSIONS
            OWNER_READ
            OWNER_WRITE
            OWNER_EXECUTE
            GROUP_READ
            GROUP_EXECUTE
            WORLD_READ
            WORLD_EXECUTE
        DESTINATION "${CMAKE_INSTALL_SBINDIR}"
    )
endif()

if(THIRDPARTY_BIN)
    install(
        FILES ${THIRDPARTY_BIN}
        PERMISSIONS
            OWNER_READ
            OWNER_WRITE
            OWNER_EXECUTE
            GROUP_READ
            GROUP_EXECUTE
            WORLD_READ
            WORLD_EXECUTE
        DESTINATION "${CMAKE_INSTALL_BINDIR}"
    )
endif()
