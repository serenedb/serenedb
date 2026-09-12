include(GNUInstallDirs)
set(CMAKE_INSTALL_SYSCONFDIR_SERENE
    "${CMAKE_INSTALL_SYSCONFDIR}/${CMAKE_PROJECT_NAME}"
)

# database directory
set(SERENEDB_DB_DIRECTORY "${PROJECT_BINARY_DIR}/var/lib/${CMAKE_PROJECT_NAME}")
file(MAKE_DIRECTORY ${SERENEDB_DB_DIRECTORY})

# logs
file(MAKE_DIRECTORY "${PROJECT_BINARY_DIR}/var/log/${CMAKE_PROJECT_NAME}")

include(InstallMacros)

# install ----------------------------------------------------------------------
install(
    FILES "${CMAKE_SOURCE_DIR}/LICENSE"
    DESTINATION "${CMAKE_INSTALL_DOCDIR}"
    RENAME LICENSE.txt
)

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
