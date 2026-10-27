if(CMAKE_VERSION VERSION_LESS "3.30")
    message(FATAL_ERROR "IRESEARCH_LIBRARY requires CMake 3.30 or newer")
endif()

if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    message(
        FATAL_ERROR
        "IRESEARCH_LIBRARY requires Clang and the vendored libc++"
    )
endif()

include(CMakePackageConfigHelpers)
if(Python3_VERSION VERSION_LESS "3.9")
    message(FATAL_ERROR "IRESEARCH_LIBRARY requires Python 3.9 or newer")
endif()
if(NOT CMAKE_CXX_COMPILER_AR)
    message(
        FATAL_ERROR
        "IRESEARCH_LIBRARY requires llvm-ar from the Clang toolchain"
    )
endif()
foreach(
    _iresearch_install_dir
    CMAKE_INSTALL_INCLUDEDIR
    CMAKE_INSTALL_LIBDIR
    CMAKE_INSTALL_DATADIR
)
    if(IS_ABSOLUTE "${${_iresearch_install_dir}}")
        message(
            FATAL_ERROR
            "IRESEARCH_LIBRARY requires a relative ${_iresearch_install_dir}"
        )
    endif()
endforeach()
set(_iresearch_package_dir "${CMAKE_BINARY_DIR}/iresearch-package")
set(_iresearch_package_stage "${_iresearch_package_dir}/$<CONFIG>")
set(_iresearch_package_script
    "${PROJECT_SOURCE_DIR}/cmake/iresearch_package.py"
)
set(_iresearch_archive
    "${_iresearch_package_stage}/${CMAKE_STATIC_LIBRARY_PREFIX}iresearch-bundle${CMAKE_STATIC_LIBRARY_SUFFIX}"
)
set(_iresearch_usage "${_iresearch_package_stage}/usage.txt")
set(_iresearch_link "${_iresearch_package_stage}/link.json")

file(
    CONFIGURE
    OUTPUT "${_iresearch_package_dir}/probe.cpp"
    CONTENT "int main() { return 0; }\n"
    @ONLY
)
add_executable(
    iresearch-library-link
    EXCLUDE_FROM_ALL
    "${_iresearch_package_dir}/probe.cpp"
)
target_link_libraries(iresearch-library-link PRIVATE iresearch)

separate_arguments(_iresearch_compile_options UNIX_COMMAND "${SDB_ARCH_FLAGS}")
list(
    APPEND _iresearch_compile_options
    -nostdinc++
    -fsized-deallocation
    ${SDB_SANITIZE_FLAGS}
)
set(_iresearch_link_options -nodefaultlibs -nostdlib++ ${SDB_SANITIZE_FLAGS})
if(SDB_LTO)
    list(APPEND _iresearch_link_options -flto=thin)
    if(NOT APPLE)
        list(APPEND _iresearch_link_options -fuse-ld=lld)
    endif()
endif()
file(
    GENERATE OUTPUT "${_iresearch_usage}"
    CONTENT
        "source=${PROJECT_SOURCE_DIR}\nbuild=${PROJECT_BINARY_DIR}\ninclude_destination=${CMAKE_INSTALL_INCLUDEDIR}\ninclude=$<JOIN:$<TARGET_PROPERTY:iresearch-library-link,INCLUDE_DIRECTORIES>,\ninclude=>\ndefine=$<JOIN:$<TARGET_PROPERTY:iresearch-library-link,COMPILE_DEFINITIONS>,\ndefine=>\ncompile_option=$<JOIN:${_iresearch_compile_options},\ncompile_option=>\nlink_option=$<JOIN:${_iresearch_link_options},\nlink_option=>\n"
    TARGET iresearch-library-link
)
set_target_properties(
    iresearch-library-link
    PROPERTIES
        CXX_LINKER_LAUNCHER
            "${Python3_EXECUTABLE};${_iresearch_package_script};capture;${_iresearch_link}"
        LINK_DEPENDS "${_iresearch_package_script};${_iresearch_usage}"
)
add_custom_command(
    TARGET iresearch-library-link
    POST_BUILD
    COMMAND
        "${Python3_EXECUTABLE}" "${_iresearch_package_script}" bundle --link
        "${_iresearch_link}" --usage "${_iresearch_usage}" --archive
        "${_iresearch_archive}" --archiver "${CMAKE_CXX_COMPILER_AR}"
    BYPRODUCTS
        "${_iresearch_archive}"
        "${_iresearch_package_stage}/IResearchUsage.cmake"
        "${_iresearch_link}"
    VERBATIM
)
add_custom_target(
    iresearch-library
    ALL
    DEPENDS iresearch-library-link "${_iresearch_archive}"
)
add_custom_target(
    iresearch-package-tests
    COMMAND
        "${CMAKE_COMMAND}" -E env "CXX=${CMAKE_CXX_COMPILER}"
        "AR=${CMAKE_CXX_COMPILER_AR}" "${Python3_EXECUTABLE}" -m unittest
        discover -s "${PROJECT_SOURCE_DIR}/cmake/tests" -v
    VERBATIM
)

set(IRESEARCH_LIBRARY_NAME
    "${CMAKE_STATIC_LIBRARY_PREFIX}iresearch${CMAKE_STATIC_LIBRARY_SUFFIX}"
)
string(
    REGEX MATCH "^[0-9]+"
    IRESEARCH_CLANG_MAJOR
    "${CMAKE_CXX_COMPILER_VERSION}"
)
configure_package_config_file(
    "${PROJECT_SOURCE_DIR}/cmake/IResearchConfig.cmake.in"
    "${_iresearch_package_dir}/IResearchConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/IResearch"
    PATH_VARS CMAKE_INSTALL_LIBDIR
)
install(
    CODE
        "execute_process(COMMAND \"${Python3_EXECUTABLE}\" \"${_iresearch_package_script}\" headers --usage \"${_iresearch_usage}\" --output \"${_iresearch_package_stage}\" COMMAND_ERROR_IS_FATAL ANY)"
    COMPONENT IResearch
)
install(
    FILES "${_iresearch_archive}"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RENAME "${IRESEARCH_LIBRARY_NAME}"
    COMPONENT IResearch
)
install(
    DIRECTORY "${_iresearch_package_stage}/include/"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    COMPONENT IResearch
)
install(
    DIRECTORY "${_iresearch_package_stage}/licenses/"
    DESTINATION "${CMAKE_INSTALL_DATADIR}/licenses/IResearch"
    COMPONENT IResearch
)
install(
    FILES
        "${_iresearch_package_dir}/IResearchConfig.cmake"
        "${_iresearch_package_stage}/IResearchUsage.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/IResearch"
    COMPONENT IResearch
)
