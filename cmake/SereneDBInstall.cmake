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

if(OS_DARWIN)
    set(ENABLE_UID_CFG false)
else()
    set(ENABLE_UID_CFG true)
endif()

# debug info directory:
if(CMAKE_INSTALL_LIBDIR STREQUAL "lib64")
    # some systems have weird places for usr/lib:
    set(CMAKE_INSTALL_DEBINFO_DIR "usr/lib/debug/")
else()
    set(CMAKE_INSTALL_DEBINFO_DIR "${CMAKE_INSTALL_LIBDIR}/debug/")
endif()

# database directory
set(SERENEDB_DB_DIRECTORY "${PROJECT_BINARY_DIR}/var/lib/${CMAKE_PROJECT_NAME}")
file(MAKE_DIRECTORY ${SERENEDB_DB_DIRECTORY})

# logs
file(MAKE_DIRECTORY "${PROJECT_BINARY_DIR}/var/log/${CMAKE_PROJECT_NAME}")

include(InstallMacros)

# install ----------------------------------------------------------------------
install_readme(LICENSE LICENSE.txt)

# glibc license files (required for static linking, see glibc LICENSES file)
install(
    DIRECTORY ${CMAKE_SOURCE_DIR}/resources/licenses/glibc/
    DESTINATION ${CMAKE_INSTALL_DOCDIR}/glibc
)

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

# systemd service file is installed via debian packaging
# (packages/debian/source/common/serenedb.service)
