cmake_minimum_required(VERSION 3.26)
message(
    STATUS
    "CMake version: ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}"
)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE
        Release
        CACHE STRING
        "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel."
        FORCE
    )
endif()

if(
    NOT CMAKE_BUILD_TYPE MATCHES "(Debug)|(Release)|(RelWithDebInfo)|(MinSizeRel)"
)
    message(
        FATAL_ERROR
        "expecting CMAKE_BUILD_TYPE: Debug Release RelWithDebInfo MinSizeRel, got ${CMAKE_BUILD_TYPE}."
    )
endif()

# where to find CMAKE modules
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH})

# be verbose about flags used
option(VERBOSE "be verbose about flags used" OFF)

# treat warnings as errors on some platforms
option(USE_FAIL_ON_WARNINGS "treat warnings as errors" ON)

option(AUTO_UPDATE_MODULES "automatically update git modules" ON)

option(SDB_IOURING "allow to use iouring" ON)

option(SDB_CLUSTER "build with cluster support" OFF)

find_package(Git 1.5.3 REQUIRED)

# ------------------------------------------------------------------------------
# VERSION information
# ------------------------------------------------------------------------------

# stable release:   MAJOR.MINOR.PATCH
# hot fix:          MAJOR.MINOR.PATCH-FIXNUMBER
# unstable release: MAJOR.MINOR.PATCH-TYPE.NUMBER
# devel:            MAJOR.MINOR.0-devel
#
# These are mapped to the following variables:
#
# SERENEDB_VERSION_MAJOR = MAJOR
# SERENEDB_VERSION_MINOR = MINOR
# SERENEDB_VERSION_PATCH = PATCH
#
# for pre-releases, otherwise empty:
#
# SERENEDB_VERSION_PRELEASE_TYPE   = TYPE
# SERENEDB_VERSION_PRELEASE_NUMBER = NUMBER
#
set(SERENEDB_VERSION_MAJOR "1")
set(SERENEDB_VERSION_MINOR "0")

# when building the nightly SERENEDB_VERSION_PATCH will be set
if(NOT DEFINED SERENEDB_VERSION_PATCH)
    set(SERENEDB_VERSION_PATCH "0")
    set(SERENEDB_VERSION_RELEASE_TYPE "")
    set(SERENEDB_VERSION_RELEASE_NUMBER "")
else()
    unset(SERENEDB_VERSION_RELEASE_TYPE) # do not remove space
    unset(SERENEDB_VERSION_RELEASE_NUMBER) # do not remove space
endif()

# unset TYPE and NUMBER in case they are empty
if(DEFINED SERENEDB_VERSION_RELEASE_TYPE)
    if(SERENEDB_VERSION_RELEASE_TYPE STREQUAL "")
        unset(SERENEDB_VERSION_RELEASE_TYPE) # do not remove space
        unset(SERENEDB_VERSION_RELEASE_NUMBER) # do not remove space
    endif()
else()
    unset(SERENEDB_VERSION_RELEASE_NUMBER) # do not remove space
endif()

if(DEFINED SERENEDB_VERSION_RELEASE_NUMBER)
    if(SERENEDB_VERSION_RELEASE_NUMBER STREQUAL "")
        unset(SERENEDB_VERSION_RELEASE_NUMBER) # do not remove space
    endif()
endif()

# semantic version
set(SERENEDB_PLAIN_VERSION
    "${SERENEDB_VERSION_MAJOR}.${SERENEDB_VERSION_MINOR}.${SERENEDB_VERSION_PATCH}"
)

if(DEFINED SERENEDB_VERSION_RELEASE_TYPE)
    if(DEFINED SERENEDB_VERSION_RELEASE_NUMBER)
        set(SERENEDB_VERSION
            "${SERENEDB_PLAIN_VERSION}-${SERENEDB_VERSION_RELEASE_TYPE}.${SERENEDB_VERSION_RELEASE_NUMBER}"
        )
    else()
        set(SERENEDB_VERSION
            "${SERENEDB_PLAIN_VERSION}-${SERENEDB_VERSION_RELEASE_TYPE}"
        )
    endif()
else()
    set(SERENEDB_VERSION "${SERENEDB_PLAIN_VERSION}")
endif()
set(SERENEDB_JS_VERSION "js")

message(STATUS "SERENEDB PLAIN VERSION: ${SERENEDB_PLAIN_VERSION}")
message(STATUS "SERENEDB VERSION: ${SERENEDB_VERSION}")
message(STATUS "SERENEDB JS VERSION: ${SERENEDB_JS_VERSION}")

project(
    serenedb
    LANGUAGES CXX C ASM
    VERSION ${SERENEDB_VERSION_MAJOR}.${SERENEDB_VERSION_MINOR}
)

# Options

enable_testing()

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

# required for clang completion in editors - must be set after creating project
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# TODO(mbkkt) maybe disable in CI
set(CMAKE_COLOR_DIAGNOSTICS ON)

# disable not fatal logs and all vlogs in compile time
add_definitions(-DABSL_MAX_VLOG_VERBOSITY=-1)
add_definitions(-DABSL_MIN_LOG_LEVEL=3)

# enable V8/JavaScript in executables.
option(USE_V8 "build with V8/JavaScript support" OFF)
if(USE_V8)
    add_definitions("-DUSE_V8=1")
endif()

# enable dtrace
option(USE_DTRACE "enable dtrace probes" OFF)
if(USE_DTRACE)
    add_definitions("-DUSE_DTRACE=1")
endif()

if(USE_COVERAGE STREQUAL "SourceBased")
    # add_definitions("-DUSE_COVERAGE=1")
    add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
    add_link_options(-fprofile-instr-generate -fcoverage-mapping)
elseif(USE_COVERAGE STREQUAL "SourceBasedMCDC")
    add_definitions("-DUSE_COVERAGE=1")
    add_compile_options(
        -fprofile-instr-generate
        -fcoverage-mapping
        -fcoverage-mcdc
    )
    add_link_options(
        -fprofile-instr-generate
        -fcoverage-mapping
        -fcoverage-mcdc
    )
elseif(USE_COVERAGE STREQUAL "GCOV")
    add_definitions("-DUSE_COVERAGE=1")
    add_compile_options(--coverage)
    add_link_options(--coverage)
elseif(NOT "${USE_COVERAGE}" STREQUAL "")
    message(FATAL_ERROR "Invalid USE_COVERAGE value ${USE_COVERAGE}")
endif()

# for the packages
set(SERENEDB_PACKAGE_VENDOR "SereneDB")
set(SERENEDB_PACKAGE_CONTACT "SereneDB")
set(SERENEDB_URL_INFO_ABOUT "SereneDB")

set(CLEAN_AUTOGENERATED_FILES)
set(PACKAGES_LIST)
set(COPY_PACKAGES_LIST)
set(CLEAN_PACKAGES_LIST)
set(INSTALL_CONFIGFILES_LIST)

# ------------------------------------------------------------------------------
# update files containing VERSION information
# ------------------------------------------------------------------------------

string(TIMESTAMP SERENEDB_BUILD_DATE "%Y-%m-%d %H:%M:%S")

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/libs/basics/build.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/libs/basics/build.h"
    NEWLINE_STYLE UNIX
)

if(NOT DEFINED GENERATE_BUILD_DATE OR GENERATE_BUILD_DATE)
    set(GENERATE_BUILD_DATE
        ON
        CACHE INTERNAL
        "whether we should generate the build date"
    )
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/libs/basics/build-date.h.in"
        "${CMAKE_CURRENT_BINARY_DIR}/libs/basics/build-date.h"
        NEWLINE_STYLE UNIX
    )
else()
    set(GENERATE_BUILD_DATE
        OFF
        CACHE INTERNAL
        "whether we should generate the build date"
    )
endif()

################################################################################
## Find the git revision
################################################################################

function(
    determine_repository_version
    source_dir
    build_repository
    have_build_repository
)
    # Get commit hash
    execute_process(
        WORKING_DIRECTORY ${source_dir}
        COMMAND ${GIT_EXE} rev-parse --short HEAD
        OUTPUT_VARIABLE COMMIT_RAW
    )
    if(NOT COMMIT_RAW)
        message(
            FATAL_ERROR
            "Can't extract current commit with the command: 'git rev-parse --short HEAD'"
        )
    endif()

    string(STRIP ${COMMIT_RAW} COMMIT_SHORT)

    if(NOT DEFINED BUILD_REPO_INFO OR BUILD_REPO_INFO STREQUAL "default")
        execute_process(
            WORKING_DIRECTORY ${source_dir}
            COMMAND ${GIT_EXE} branch --show-current
            OUTPUT_VARIABLE BRANCH_NAME_RAW
        )
        if(NOT BRANCH_NAME_RAW)
            # For example, in docker we do 'checkout'. Hence, it is impossible to detect branch
            set(${build_repository} "${COMMIT_SHORT}" PARENT_SCOPE)
            set(${have_build_repository} "1" PARENT_SCOPE)
        else()
            string(STRIP ${BRANCH_NAME_RAW} BRANCH_NAME)
            set(${build_repository}
                "refs/${BRANCH_NAME} ${COMMIT_SHORT}"
                PARENT_SCOPE
            )
            set(${have_build_repository} "1" PARENT_SCOPE)
        endif()
    elseif(BUILD_REPO_INFO STREQUAL "release")
        if(
            "${SERENEDB_VERSION_RELEASE_NUMBER}" STREQUAL ""
            AND SERENEDB_VERSION_RELEASE_TYPE MATCHES "^[1-9][0-9]*$"
        )
            string(REPLACE "-" "." RELEASE_TAG ${SERENEDB_VERSION})
        else()
            set(RELEASE_TAG ${SERENEDB_VERSION})
        endif()
        set(RELEASE_TAG "v${RELEASE_TAG}")
        execute_process(
            WORKING_DIRECTORY ${source_dir}
            COMMAND ${GIT_EXE} describe --all --tags --match ${RELEASE_TAG}
            OUTPUT_VARIABLE TAG_RAW
        )
        if(NOT TAG_RAW)
            message(
                FATAL_ERROR
                "Can't extract tag using the command: 'git describe --all --tags --match v${SERENEDB_PLAIN_VERSION}"
            )
        else()
            string(STRIP ${TAG_RAW} TAG)
            set(${build_repository} "refs/${TAG} ${COMMIT_SHORT}" PARENT_SCOPE)
            set(${have_build_repository} "1" PARENT_SCOPE)
        endif()
    elseif(BUILD_REPO_INFO STREQUAL "nightly")
        set(${build_repository}
            "refs/head/${SERENEDB_VERSION_MAJOR}.${SERENEDB_VERSION_MINOR} ${COMMIT_SHORT}"
            PARENT_SCOPE
        )
        set(${have_build_repository} "1" PARENT_SCOPE)
    else()
        set(${build_repository}
            "GIT FAILED TO RETRIEVE THE VERSION - UNSUPPORTED BUILD MODE"
            PARENT_SCOPE
        )
        set(${have_build_repository} "1" PARENT_SCOPE)
    endif()
endfunction()

find_program(GIT_EXE git)
if(DEFINED GIT_EXE AND IS_DIRECTORY "${CMAKE_SOURCE_DIR}/.git")
    determine_repository_version(${CMAKE_SOURCE_DIR} SERENEDB_BUILD_REPOSITORY HAVE_SERENEDB_BUILD_REPOSITORY)
else()
    set(SERENEDB_BUILD_REPOSITORY "")
    set(HAVE_SERENEDB_BUILD_REPOSITORY "0")
endif()

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/libs/basics/build-repository.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/libs/basics/build-repository.h"
    NEWLINE_STYLE UNIX
)

if(VERBOSE)
    message(STATUS "SERENEDB_BUILD_REPOSITORY=\"${SERENEDB_BUILD_REPOSITORY}\"")
endif()

################################################################################
## EXTERNAL PROGRAMS
################################################################################

find_package(Python3 REQUIRED)
get_filename_component(PYTHON_EXECUTABLE "${Python3_EXECUTABLE}" REALPATH)

set($ENV{PYTHON_EXECUTABLE} ${PYTHON_EXECUTABLE})
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "from shutil import which"
    RESULT_VARIABLE EXIT_CODE
    OUTPUT_QUIET
)
if(NOT "${EXIT_CODE}" EQUAL "0")
    message(FATAL_ERROR "python shutil.which package is required! ")
endif()

################################################################################
## COMPILER FEATURES
################################################################################

if(
    NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang"
    OR CMAKE_CXX_COMPILER_VERSION VERSION_LESS "16.0"
)
    message(
        FATAL_ERROR
        "SereneDB requires clang 16.0 or newer, building with different compilers is unsupported"
    )
endif()

set(CMAKE_CXX_STANDARD 26 CACHE STRING "" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# need threads
find_package(Threads REQUIRED)

################################################################################
## TARGET ARCHITECTURE
################################################################################

include(OptimizeForArchitecture)

################################################################################
## MAINTAINER MODE
################################################################################

option(
    SDB_DEV
    "whether we want to have assertions and other development features"
    OFF
)

if(SDB_DEV)
    add_definitions("-DSDB_DEV=1")

    # TODO(mbkkt) add_definitions("-D_FORTIFY_SOURCE=3")
else()
    add_definitions(-DNDEBUG)
endif()

if(SDB_GTEST)
    add_definitions("-DSDB_GTEST=1")
endif()

################################################################################
## FAILURE TESTS
################################################################################

option(
    SDB_FAULT_INJECTION
    "whether we want to have failure tests compiled in"
    OFF
)

if(SDB_FAULT_INJECTION)
    add_definitions("-DSDB_FAULT_INJECTION=1")
endif()

################################################################################
## INTERPROCEDURAL OPTIMIZATION (LINK TIME OPTIMIZATION)
################################################################################

set(USE_IPO
    AUTO
    CACHE STRING
    "Use interprocedural optimization: ON, OFF or AUTO"
)
set_property(CACHE USE_IPO PROPERTY STRINGS AUTO ON OFF)

set(IPO_ENABLED OFF)

# Determine value if IPO_ENABLED from USE_IPO and CMAKE_BUILD_TYPE
if(USE_IPO STREQUAL "AUTO")
    if(
        NOT SDB_GTEST
        AND CMAKE_BUILD_TYPE MATCHES "(Release)|(RelWithDebInfo)|(MinSizeRel)"
    )
        set(IPO_ENABLED ON)
    endif()
elseif(USE_IPO)
    set(IPO_ENABLED ON)
endif()

message(STATUS "IPO_ENABLED: ${IPO_ENABLED}")

################################################################################
## ALLOC
################################################################################

# TODO(mbkkt) -fnew-alignment=8 and tcmalloc is nothrow -- ABSL_ALLOCATOR_NOTHROW
if("${SDB_ALLOC}" MATCHES "JE")
    add_definitions("-DSERENEDB_HAVE_JEMALLOC=1")
elseif("${SDB_ALLOC}" MATCHES "TC")
    add_definitions("-DSERENEDB_HAVE_TCMALLOC=1")
endif()

################################################################################
## FLAGS
################################################################################

include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)

# Tests whether an argument can be passed to the C compiler without diagnostics,
# and appends it to the first variable if possible.
# If called with multiple arguments, they are tested and possibly appended
# independently.
function(add_c_flags_if_supported var)
    foreach(flag ${ARGN})
        set(flag_var_name "C_COMPILER_SUPPORTS_${flag}")
        # Note that this compiles a test file, also using CMAKE_C_FLAGS, which
        # may thus affect the result.
        # Also note that check_c_compiler_flag *internally caches* the result for
        # per *var_name*, forcing us to use a unique variable for each call with the
        # same option.
        # An alternative (albeit more costly) would be to unset the variable each time.
        check_c_compiler_flag(${flag} "${flag_var_name}")
        set(is_supported ${${flag_var_name}})
        if(is_supported)
            set(${var} "${${var}} ${flag}")
        endif()
    endforeach()
    set(${var} "${${var}}" PARENT_SCOPE)
endfunction()

# Tests whether an argument can be passed to the C++ compiler without diagnostics,
# and appends it to the first variable if possible.
# If called with multiple arguments, they are tested and possibly appended
# independently.
function(add_cxx_flags_if_supported var)
    foreach(flag ${ARGN})
        set(flag_var_name "CXX_COMPILER_SUPPORTS_${flag}")
        # Note that this compiles a test file, also using CMAKE_CXX_FLAGS, which
        # may thus affect the result.
        # Also note that check_cxx_compiler_flag *internally caches* the result for
        # per *var_name*, forcing us to use a unique variable for each call with the
        # same option.
        # An alternative (albeit more costly) would be to unset the variable each time.
        check_cxx_compiler_flag(${flag} "${flag_var_name}")
        set(is_supported ${${flag_var_name}})
        if(is_supported)
            set(${var} "${${var}} ${flag}")
        endif()
    endforeach()
    set(${var} "${${var}}" PARENT_SCOPE)
endfunction()

function(add_compile_warnings_flags)
    if(USE_FAIL_ON_WARNINGS)
        add_compile_options(-Werror)
    endif()
endfunction()

# compiler options
set(EXTRA_FLAGS "-Wall -Wextra -Wthread-safety -Wdeprecated")
set(EXTRA_FLAGS "${EXTRA_FLAGS} -Wno-unused-parameter")
set(EXTRA_FLAGS "${EXTRA_FLAGS} ${ARCHITECTURE_OPTIMIZATIONS}")

set(EXTRA_C_FLAGS "")
# TODO(mbkkt) add -Wheader-hygiene -Wignored-base-class-qualifiers
set(EXTRA_CXX_FLAGS "-Wsuggest-override -Wsuggest-destructor-override")
set(EXTRA_CXX_FLAGS
    "${EXTRA_CXX_FLAGS} -Wno-missing-designated-field-initializers -Wno-deprecated-declarations -Wno-deprecated-copy-with-dtor"
)
set(EXTRA_CXX_FLAGS
    "${EXTRA_CXX_FLAGS} -fsized-deallocation -fstrict-vtable-pointers"
)
if(IPO_ENABLED)
    add_definitions("-DSERENEDB_USE_IPO=1")
    # TODO(mbkkt) Try to apply -fsplit-lto-unit, -flo=full -fbasic-block-sections=all
    # -fexperimental-relative-c++-abi-vtables, -fassume-unique-vtables -fvirtual-function-elimination
    # -fassume-nothrow-exception-dtor -fasync-exceptions -fasynchronous-unwind-tables
    set(EXTRA_FLAGS
        "${EXTRA_FLAGS} -flto=thin -faddrsig -ffunction-sections -fdata-sections"
    )
    # set(EXTRA_CXX_FLAGS "${EXTRA_CXX_FLAGS} -fforce-emit-vtables -fwhole-program-vtables")
    set(CMAKE_EXE_LINKER_FLAGS
        "${CMAKE_EXE_LINKER_FLAGS} -flto=thin -Wl,-icf=safe -Wl,--gc-sections"
    )
endif()

set(NONE_FRAME_INFO "-fomit-frame-pointer -momit-leaf-frame-pointer")
set(FULL_FRAME_INFO "-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")

# debug info flags
set(NONE_DEBUG_INFO "-g0")
# no-limit-debug-info to produce symbols for glibc/libc++/etc
set(BASE_DEBUG_INFO "-fno-limit-debug-info")
# no-standalone-debug to reduce size of debug info
# eliminate-unused-debug to remove some unused symbols and types (they're can be used in compile time)
# line-tables-only to remove everything except address => file:line mapping
# TODO(mbkkt) eliminate-unused-debug-symbols eliminate-unused-debug-types unused for some reason
set(LINES_DEBUG_INFO
    "${BASE_DEBUG_INFO} -fno-standalone-debug -gline-tables-only"
)
# column-info to add column in line info
# TODO(mbkkt) maybe gz=zstd
set(COLUMNS_DEBUG_INFO "${LINES_DEBUG_INFO} -gcolumn-info")
# TODO(mbkkt) maybe no-inline and no-optimize-sibling-calls
set(SANITIZE_DEBUG_INFO "${COLUMNS_DEBUG_INFO}")
set(COVERAGE_DEBUG_INFO "${COLUMNS_DEBUG_INFO}")
set(PROFILE_DEBUG_INFO "${COLUMNS_DEBUG_INFO} -fdebug-info-for-profiling")
# TODO(mbkkt) no-eliminate-unused-debug-symbols no-eliminate-unused-debug-types unused for some reason
set(FULL_DEBUG_INFO "${BASE_DEBUG_INFO} -g3 -fstandalone-debug -fdebug-macro")
set(LLDB_DEBUG_INFO "${FULL_DEBUG_INFO} -glldb")
set(GDB_DEBUG_INFO "${FULL_DEBUG_INFO} -ggdb")

if(USE_DEBUG_INFO STREQUAL "NONE")
    set(DEBUG_INFO_FLAGS "${NONE_DEBUG_INFO}")
elseif(USE_DEBUG_INFO STREQUAL "LINES")
    set(DEBUG_INFO_FLAGS "${LINES_DEBUG_INFO}")
elseif(USE_DEBUG_INFO STREQUAL "COLUMNS")
    set(DEBUG_INFO_FLAGS "${COLUMNS_DEBUG_INFO}")
elseif(USE_DEBUG_INFO STREQUAL "SANITIZE")
    set(DEBUG_INFO_FLAGS "${SANITIZE_DEBUG_INFO}")
elseif(USE_DEBUG_INFO STREQUAL "COVERAGE")
    set(DEBUG_INFO_FLAGS "${COVERAGE_DEBUG_INFO}")
elseif(USE_DEBUG_INFO STREQUAL "PROFILE")
    set(DEBUG_INFO_FLAGS "${PROFILE_DEBUG_INFO}")
elseif(USE_DEBUG_INFO STREQUAL "LLDB")
    set(DEBUG_INFO_FLAGS "${LLDB_DEBUG_INFO}")
elseif(USE_DEBUG_INFO STREQUAL "GDB")
    set(DEBUG_INFO_FLAGS "${GDB_DEBUG_INFO}")
else()
    set(DEBUG_INFO_FLAGS "${FULL_DEBUG_INFO}")
endif()

# c
# note: when building one of the build types, CMake will automatically combine
# the base flags from CMAKE_C_FLAGS with build type-specific flags in
# CMAKE_C_FLAGS_${CMAKE_BUILD_TYPE}.
# there is no need to repeat the base flags in the build-type specific flags!
set(CMAKE_C_FLAGS_DEBUG
    "-O0 ${FULL_FRAME_INFO} ${DEBUG_INFO_FLAGS}"
    CACHE INTERNAL
    "C debug flags"
)
set(CMAKE_C_FLAGS_MINSIZEREL
    "-Os ${NONE_FRAME_INFO} ${DEBUG_INFO_FLAGS}"
    CACHE INTERNAL
    "C minimal size flags"
)
set(CMAKE_C_FLAGS_RELEASE
    "-O3 ${NONE_FRAME_INFO} ${DEBUG_INFO_FLAGS}"
    CACHE INTERNAL
    "C release flags"
)
set(CMAKE_C_FLAGS_RELWITHDEBINFO
    "-O3 ${FULL_FRAME_INFO} ${DEBUG_INFO_FLAGS}"
    CACHE INTERNAL
    "C release with debug info flags"
)

# cxx
# note: when building one of the build types, CMake will automatically combine
# the base flags from CMAKE_CXX_FLAGS with build type-specific flags in
# CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE}.
# there is no need to repeat the base flags in the build-type specific flags!
set(CMAKE_CXX_FLAGS_DEBUG
    "-O0 ${FULL_FRAME_INFO} ${DEBUG_INFO_FLAGS}"
    CACHE INTERNAL
    "C++ debug flags"
)
set(CMAKE_CXX_FLAGS_MINSIZEREL
    "-Os ${NONE_FRAME_INFO} ${DEBUG_INFO_FLAGS}"
    CACHE INTERNAL
    "C++ minimal size flags"
)
set(CMAKE_CXX_FLAGS_RELEASE
    "-O3 ${NONE_FRAME_INFO} ${DEBUG_INFO_FLAGS}"
    CACHE INTERNAL
    "C++ release flags"
)
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "-O3 ${FULL_FRAME_INFO} ${DEBUG_INFO_FLAGS}"
    CACHE INTERNAL
    "C++ release with debug info flags"
)

# put together the final flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${EXTRA_FLAGS} ${EXTRA_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${EXTRA_FLAGS} ${EXTRA_CXX_FLAGS}")

################################################################################
## Runtime
################################################################################
option(STATIC_EXECUTABLES "produce static executables" OFF)

execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} --print-libgcc-file-name --rtlib=compiler-rt
    OUTPUT_VARIABLE BUILTINS_LIBRARY_CLANG
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
# link nothing by default and use clang_rt for builtins
set(DEFAULT_LIBS "-nodefaultlibs ${BUILTINS_LIBRARY_CLANG}")

cmake_host_system_information(RESULT DISTRIB_NAME QUERY DISTRIB_NAME)
message(STATUS "DISTRIB_NAME=${DISTRIB_NAME}")
if("${DISTRIB_NAME}" MATCHES "Alpine") # musl distr
    set(DEFAULT_LIBS "${DEFAULT_LIBS} -lc")
else() # glibc distr
    # TODO(mbkkt) think about -lrt -lpthread -ldl
    set(DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm")
endif()
if(STATIC_EXECUTABLES)
    set(CMAKE_POSITION_INDEPENDENT_CODE OFF CACHE BOOL "" FORCE)
    set(DEFAULT_LIBS "-static ${DEFAULT_LIBS}")
endif()

message(STATUS "DEFAULT_LIBS=${DEFAULT_LIBS}")
set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

if(USE_V8)
    set(SDB_SANITIZE "" CACHE STRING "" FORCE)
endif()

set(SDB_SANITIZE_WAS "${SDB_SANITIZE}")
if(NOT "${SDB_SANITIZE}" STREQUAL "")
    # TODO(mbkkt) we also can apply sanitize-recover
    if("${SDB_SANITIZE}" STREQUAL "Leak")
        add_link_options(-fsanitize=leak)
    elseif("${SDB_SANITIZE}" STREQUAL "Undefined")
        add_link_options(-fsanitize=undefined)
    elseif("${SDB_SANITIZE}" MATCHES "(Undefined)?Address(Light)?")
        if("${SDB_SANITIZE_WAS}" MATCHES ".*Undefined.*")
            set(SDB_SANITIZE "Undefined;Address")
            add_link_options(-fsanitize=undefined)
        else()
            set(SDB_SANITIZE "Address")
        endif()
        add_link_options(-fsanitize=address)
    elseif("${SDB_SANITIZE}" STREQUAL "Thread")
        add_link_options(-fsanitize=thread)
    elseif("${SDB_SANITIZE}" MATCHES "Memory(WithOrigins)?")
        # TODO(mbkkt) patch libc++ to be able work with -fsanitize-memory-track-origins=1 (default is 2)
        # TODO(mbkkt) add ability to apply -fno-sanitize-memory-use-after-dtor
        add_link_options(-fsanitize=memory)
    else()
        # TODO(mbkkt) Maybe support cfi, safe-stack?
        message(FATAL_ERROR "SDB_SANITIZE=${SDB_SANITIZE} not supported yet")
    endif()
endif()

include(Runtime)

add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-nostdinc++>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-nostdlib++>)

if(NOT "${SDB_SANITIZE}" STREQUAL "")
    # if will be some strange link error just enable this
    # add_compile_options(-fPIC -fPIE)
    if("${SDB_SANITIZE}" STREQUAL "Leak")
        add_compile_options(-fsanitize=leak)
        link_libraries(unwind_static cxxabi_static cxx_static)
    elseif("${SDB_SANITIZE}" STREQUAL "Undefined")
        # TODO(mbkkt) enable nullability checks and local-bounds
        # TODO(mbkkt) apply these settings to libc++?
        add_compile_options(-fsanitize=undefined)
        link_libraries(unwind_static cxxabi_static cxx_static)
    elseif("${SDB_SANITIZE}" MATCHES ".*Address.*")
        if("${SDB_SANITIZE_WAS}" MATCHES ".*Undefined.*")
            add_compile_options(-fsanitize=undefined)
        endif()
        add_compile_options(-fsanitize=address)
        # TODO(mbkkt) apply these settings to libc++?
        if("${SDB_SANITIZE_WAS}" MATCHES ".*Light.*")
            add_compile_options(
                -fsanitize-address-use-after-return=never
                -fno-sanitize-address-use-after-scope
                -fno-sanitize-address-use-odr-indicator
            )
        else()
            add_compile_options(
                -fsanitize-address-use-after-return=always
                -fsanitize-address-use-after-scope
                -fsanitize-address-use-odr-indicator
            )
        endif()
        link_libraries(unwind_static cxxabi_static cxx_static)
    elseif("${SDB_SANITIZE}" MATCHES "Memory(WithOrigins)?")
        add_compile_options(-fsanitize=memory -fsanitize-memory-use-after-dtor)
        if("${SDB_SANITIZE}" STREQUAL "MemoryWithOrigins")
            add_compile_options(-fsanitize-memory-track-origins)
        endif()
        # crashes with llvm libunwinder
        link_libraries(cxxabi_static cxx_static)
    elseif("${SDB_SANITIZE}" STREQUAL "Thread")
        add_compile_options(-fsanitize=thread)
        # crashes with llvm libunwinder
        link_libraries(cxxabi_static cxx_static)
    else()
        message(FATAL_ERROR "SDB_SANITIZE=${SDB_SANITIZE} not supported yet")
    endif()
else()
    link_libraries(unwind_static cxxabi_static cxx_static)
endif()

################################################################################
## VTUNE Symbols for profiling our build.
## As we link statically we would miss some essential symbols for profiler.
## So we force them to be included.
## See https://software.intel.com/content/www/us/en/develop/documentation/vtune-help/top/set-up-analysis-target/linux-targets/analyzing-statically-linked-binaries-on-linux-targets.html
################################################################################

option(
    ENSURE_VTUNE_SYMBOLS
    "force needed by VTUNE profiler symbols to be included"
    OFF
)

if(ENSURE_VTUNE_SYMBOLS)
    set(CMAKE_EXE_LINKER_FLAGS
        "${CMAKE_EXE_LINKER_FLAGS} -upthread_getattr_np -upthread_create\
    -upthread_key_create -upthread_setspecific -upthread_getspecific -upthread_self -upthread_attr_destroy\
    -upthread_attr_setstack -upthread_attr_getstack -upthread_attr_getstacksize -upthread_attr_setstacksize\
    -upthread_cancel -u_pthread_cleanup_push -u_pthread_cleanup_pop -upthread_mutex_lock -upthread_mutex_trylock\
    -upthread_spin_lock -upthread_spin_trylock"
    )
endif()

################################################################################
## 3RD PARTY
################################################################################

add_subdirectory(third_party EXCLUDE_FROM_ALL)

if("${SDB_ALLOC}" MATCHES "JE")
    link_libraries(jemalloc)
elseif("${SDB_ALLOC}" MATCHES "TC")
    link_libraries(tcmalloc::tcmalloc)
endif()

add_definitions("-DSERENEDB_BOOST_VERSION=\"${BOOST_VERSION}\"")

################################################################################
## V8
################################################################################

set(V8_LINK_DIRECTORIES "${LINK_DIRECTORIES}" CACHE INTERNAL "" FORCE)

add_definitions("-DSERENEDB_V8_VERSION=\"${V8_VERSION}\"")

foreach(LINK_DIR ${V8_LINK_DIRECTORIES})
    link_directories("${LINK_DIR}")
endforeach()

################################################################################
## PATHS, installation, packages, frontend
################################################################################

add_subdirectory(resources)
include(SereneDBInstall)

################################################################################
## ERRORS FILES
################################################################################

# If "make clean" removes these files, afterwards neither "make" nor "cmake" work any more.
set_directory_properties(PROPERTIES CLEAN_NO_CUSTOM "On")

set(ERROR_FILES libs/basics/errors.h libs/basics/error-registry.h)
if(SDB_CLUSTER)
    list(APPEND ERROR_FILES private/js/common/bootstrap/errors.js)
endif()

set(ERROR_FILES_GEN)
set(ERRORS_DAT libs/basics/errors.dat)

foreach(m IN LISTS ERROR_FILES)
    get_filename_component(GEN_BASENAME "${m}" NAME)
    if(GEN_BASENAME STREQUAL "errors.js")
        # generated errors.js is copied into the source dir
        set(TARGET_FILENAME ${CMAKE_SOURCE_DIR}/${m})
    else()
        # generated C++ header files are copied into the build dir
        set(TARGET_FILENAME ${CMAKE_BINARY_DIR}/${m})
    endif()
    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/${m}
        COMMAND
            ${PYTHON_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/scripts/generateErrorfile.py
            ${CMAKE_SOURCE_DIR}/${ERRORS_DAT}
            ${CMAKE_BINARY_DIR}/${GEN_BASENAME}.tmp
        COMMAND
            ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_BINARY_DIR}/${GEN_BASENAME}.tmp ${TARGET_FILENAME}
        COMMAND
            ${CMAKE_COMMAND} -E remove ${CMAKE_BINARY_DIR}/${GEN_BASENAME}.tmp
        DEPENDS
            ${CMAKE_SOURCE_DIR}/${ERRORS_DAT}
            ${CMAKE_SOURCE_DIR}/scripts/generateErrorfile.py
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Building errors file ${m}"
        VERBATIM
    )

    list(APPEND ERROR_FILES_GEN ${CMAKE_BINARY_DIR}/${m})
endforeach()

add_custom_target(errorfiles ALL DEPENDS ${ERROR_FILES_GEN})

set(EXIT_CODE_FILES libs/basics/exitcodes.h js/common/bootstrap/exitcodes.js)

set(EXIT_CODE_FILES_GEN)
set(EXIT_CODES_DAT libs/basics/exitcodes.dat)

foreach(m IN LISTS EXIT_CODE_FILES)
    get_filename_component(GEN_BASENAME "${m}" NAME)
    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/${m}
        COMMAND
            ${PYTHON_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/scripts/generateExitCodesFiles.py
            ${CMAKE_SOURCE_DIR}/${EXIT_CODES_DAT}
            ${CMAKE_BINARY_DIR}/${GEN_BASENAME}.tmp
        COMMAND
            ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_BINARY_DIR}/${GEN_BASENAME}.tmp ${CMAKE_BINARY_DIR}/${m}
        COMMAND
            ${CMAKE_COMMAND} -E remove ${CMAKE_BINARY_DIR}/${GEN_BASENAME}.tmp
        DEPENDS
            ${CMAKE_SOURCE_DIR}/${EXIT_CODES_DAT}
            ${CMAKE_SOURCE_DIR}/scripts/generateExitCodesFiles.py
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Building exitcode file ${m}"
        VERBATIM
    )

    list(APPEND EXIT_CODE_FILES_GEN ${CMAKE_BINARY_DIR}/${m})
endforeach()

add_custom_target(exitcodefiles ALL DEPENDS ${EXIT_CODE_FILES_GEN})

################################################################################
## SUB-PROJECTS
################################################################################

list(INSERT SYSTEM_LIBRARIES 0 ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

add_subdirectory(libs)
add_subdirectory(server)
add_subdirectory(tests)
if(SDB_CLUSTER)
    add_subdirectory(private)
endif()

add_custom_target(packages DEPENDS ${PACKAGES_LIST})

add_custom_target(copy_packages DEPENDS ${COPY_PACKAGES_LIST})

add_custom_target(clean_packages DEPENDS ${CLEAN_PACKAGES_LIST})

add_custom_target(
    clean_autogenerated_files
    DEPENDS ${CLEAN_AUTOGENERATED_FILES}
)

message(STATUS "building for git revision: ${SERENEDB_BUILD_REPOSITORY}")

add_custom_target(serenedb DEPENDS serened)
if(SDB_CLUSTER)
    add_dependencies(serenedb serene-clients)
endif()
