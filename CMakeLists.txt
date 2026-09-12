cmake_minimum_required(VERSION 3.26)
message(STATUS "CMake version: ${CMAKE_VERSION}")
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

option(USE_FAIL_ON_WARNINGS "treat warnings as errors" ON)

option(AUTO_UPDATE_MODULES "automatically update git modules" ON)

option(SDB_IOURING "allow to use iouring" ON)

option(SDB_EMBEDDED_DOCS "embed the documentation from docs/ into serened" ON)

option(SDB_GTEST "build the gtest suites under tests/" OFF)

set(SDB_ALLOC
    ""
    CACHE STRING
    "allocator: JE (jemalloc), TC (tcmalloc), empty for system"
)
set_property(CACHE SDB_ALLOC PROPERTY STRINGS "" JE TC)

set(SDB_SANITIZE
    ""
    CACHE STRING
    "sanitizer: Leak, Undefined, Address, AddressLight, UndefinedAddress, UndefinedAddressLight, Thread, Memory, MemoryWithOrigins, empty for none"
)
set_property(
    CACHE SDB_SANITIZE
    PROPERTY
        STRINGS
            ""
            Leak
            Undefined
            Address
            AddressLight
            UndefinedAddress
            UndefinedAddressLight
            Thread
            Memory
            MemoryWithOrigins
)

set(USE_COVERAGE
    ""
    CACHE STRING
    "coverage: SourceBased, SourceBasedMCDC, GCOV, empty for none"
)
set_property(
    CACHE USE_COVERAGE
    PROPERTY STRINGS "" SourceBased SourceBasedMCDC GCOV
)

set(USE_DEBUG_INFO
    "FULL"
    CACHE STRING
    "debug info: NONE, LINES, COLUMNS, SANITIZE, COVERAGE, PROFILE, LLDB, GDB, FULL"
)
set_property(
    CACHE USE_DEBUG_INFO
    PROPERTY STRINGS NONE LINES COLUMNS SANITIZE COVERAGE PROFILE LLDB GDB FULL
)

set(SDB_GEN_PGO OFF CACHE BOOL "build instrumented for PGO profile generation")
set(SDB_GEN_CS_PGO
    ""
    CACHE PATH
    "profile to use while generating a context-sensitive PGO profile"
)
set(SDB_USE_PGO "" CACHE PATH "PGO profile to optimize with")

find_package(Git REQUIRED)

# ------------------------------------------------------------------------------
# VERSION information
# ------------------------------------------------------------------------------

# stable release: MAJOR.MINOR.PATCH
# LTS release:    MAJOR.MINOR.PATCH-lts
#
# MAJOR = calendar year, 2-digit, UTC (e.g. 26 for 2026)
# MINOR = month, 2-digit zero-padded, UTC (e.g. 03 for March)
# PATCH = growing patch number; 0 means dev/unversioned build
#
# MAJOR and MINOR are derived from the UTC clock at configure time.
# PATCH defaults to 0; pass -DSERENEDB_VERSION_PATCH=N (N > 0) for releases.
#
# These are mapped to the following variables:
#
# SERENEDB_VERSION_MAJOR        = MAJOR
# SERENEDB_VERSION_MINOR        = MINOR
# SERENEDB_VERSION_PATCH        = PATCH
# SERENEDB_VERSION_RELEASE_TYPE = "lts" or empty

# Derive MAJOR and MINOR from the UTC clock at configure time
string(TIMESTAMP SERENEDB_VERSION_MAJOR "%y" UTC)
string(TIMESTAMP SERENEDB_VERSION_MINOR "%m" UTC)

if(NOT DEFINED SERENEDB_VERSION_PATCH)
    set(SERENEDB_VERSION_PATCH "0")
elseif(SERENEDB_VERSION_PATCH EQUAL 0)
    message(
        FATAL_ERROR
        "SERENEDB_VERSION_PATCH must be > 0 when specified explicitly. "
        "Omit -DSERENEDB_VERSION_PATCH for dev builds (patch defaults to 0)."
    )
endif()

# unset RELEASE_TYPE if empty
if(DEFINED SERENEDB_VERSION_RELEASE_TYPE)
    if(SERENEDB_VERSION_RELEASE_TYPE STREQUAL "")
        unset(SERENEDB_VERSION_RELEASE_TYPE)
    endif()
endif()

# semantic version
set(SERENEDB_PLAIN_VERSION
    "${SERENEDB_VERSION_MAJOR}.${SERENEDB_VERSION_MINOR}.${SERENEDB_VERSION_PATCH}"
)

if(DEFINED SERENEDB_VERSION_RELEASE_TYPE)
    set(SERENEDB_VERSION
        "${SERENEDB_PLAIN_VERSION}-${SERENEDB_VERSION_RELEASE_TYPE}"
    )
else()
    set(SERENEDB_VERSION "${SERENEDB_PLAIN_VERSION}")
endif()
message(STATUS "SERENEDB PLAIN VERSION: ${SERENEDB_PLAIN_VERSION}")
message(STATUS "SERENEDB VERSION: ${SERENEDB_VERSION}")

project(
    serenedb
    LANGUAGES CXX C ASM
    VERSION ${SERENEDB_VERSION_MAJOR}.${SERENEDB_VERSION_MINOR}
)

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

if(SDB_GEN_PGO)
    # TODO(mbkkt) Does it make sense to use -ftemporal-profile ?
    add_compile_options(-fprofile-generate)
    add_link_options(-fprofile-generate)
elseif(SDB_GEN_CS_PGO)
    # TODO(mbkkt) seems like cs pgo didn't produce results with LTO :/
    # https://github.com/llvm/llvm-project/issues/112103
    add_compile_options(-fprofile-use=${SDB_GEN_CS_PGO} -fcs-profile-generate)
    add_link_options(-fprofile-use=${SDB_GEN_CS_PGO} -fcs-profile-generate)
elseif(SDB_USE_PGO)
    add_compile_options(-fprofile-use=${SDB_USE_PGO})
    add_link_options(-fprofile-use=${SDB_USE_PGO})
    # TODO(mbkkt) for -ftemporal-profile
    #   add_link_options(-Wl,--symbol-ordering-file=path-to-code.orderfile)
endif()

if(USE_COVERAGE STREQUAL "SourceBased")
    add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
    add_link_options(-fprofile-instr-generate -fcoverage-mapping)
elseif(USE_COVERAGE STREQUAL "SourceBasedMCDC")
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
    add_compile_options(--coverage)
    add_link_options(--coverage)
elseif(NOT "${USE_COVERAGE}" STREQUAL "")
    message(FATAL_ERROR "Invalid USE_COVERAGE value ${USE_COVERAGE}")
endif()

# ------------------------------------------------------------------------------
# update files containing VERSION information
# ------------------------------------------------------------------------------

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/server/utils/build.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/server/utils/build.h"
    NEWLINE_STYLE UNIX
)

################################################################################
## EXTERNAL PROGRAMS
################################################################################

find_package(Python3 REQUIRED)
get_filename_component(PYTHON_EXECUTABLE "${Python3_EXECUTABLE}" REALPATH)

set(CMAKE_CXX_STANDARD 26 CACHE STRING "" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

################################################################################
## TARGET ARCHITECTURE
################################################################################

include(OptimizeForArchitecture)

if(APPLE)
    set(OS_DARWIN 1)
    set(SDB_IOURING OFF CACHE BOOL "" FORCE)
    if(NOT CMAKE_CXX_COMPILER_AR)
        message(
            FATAL_ERROR
            "llvm-ar (CMAKE_CXX_COMPILER_AR) is required on macOS"
        )
    endif()
    set(CMAKE_AR "${CMAKE_CXX_COMPILER_AR}")
    set(CMAKE_RANLIB "${CMAKE_CXX_COMPILER_RANLIB}")
else()
    set(OS_LINUX 1)
endif()

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
    add_definitions("-DDEBUG")

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

function(add_compile_warnings_flags)
    if(USE_FAIL_ON_WARNINGS)
        add_compile_options(-Werror)
    endif()
endfunction()

# compiler options
set(EXTRA_FLAGS "-Wall -Wextra -Wthread-safety -Wdeprecated")
set(EXTRA_FLAGS "${EXTRA_FLAGS} -Wno-unused-parameter")
set(EXTRA_FLAGS "${EXTRA_FLAGS} ${ARCHITECTURE_OPTIMIZATIONS}")

# TODO(mbkkt) add -Wheader-hygiene -Wignored-base-class-qualifiers
set(EXTRA_CXX_FLAGS "-Wsuggest-override -Wsuggest-destructor-override")
set(EXTRA_CXX_FLAGS
    "${EXTRA_CXX_FLAGS} -Wno-missing-designated-field-initializers -Wno-deprecated-declarations -Wno-deprecated-copy-with-dtor"
)
set(EXTRA_CXX_FLAGS
    "${EXTRA_CXX_FLAGS} -fsized-deallocation -fstrict-vtable-pointers"
)
if(IPO_ENABLED)
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
set(FULL_DEBUG_INFO "${BASE_DEBUG_INFO} -g3 -fstandalone-debug")
# -fdebug-macro leads to crash on macOS, see issue
# https://github.com/llvm/llvm-project/issues/212345
if(NOT OS_DARWIN)
    set(FULL_DEBUG_INFO "${FULL_DEBUG_INFO} -fdebug-macro")
endif()
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
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${EXTRA_FLAGS}")
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

# One dispatch feeds both passes: the link options go on here, before the llvm
# runtimes subdir is added, so libc++ links the sanitizer runtime; the compile
# options go on after it, so libc++ itself is not instrumented. SDB_SANITIZE is
# normalized to a name LLVM_USE_SANITIZER accepts.
set(SDB_SANITIZE_WAS "${SDB_SANITIZE}")
set(SDB_SANITIZE_FLAGS "")
set(SDB_SANITIZE_EXTRA "")
set(SDB_SANITIZE_UNWINDER ON)
if(NOT SDB_SANITIZE STREQUAL "")
    # TODO(mbkkt) we also can apply sanitize-recover
    if(SDB_SANITIZE STREQUAL "Leak")
        set(SDB_SANITIZE_FLAGS -fsanitize=leak)
    elseif(SDB_SANITIZE STREQUAL "Undefined")
        # TODO(mbkkt) enable nullability checks and local-bounds
        set(SDB_SANITIZE_FLAGS -fsanitize=undefined)
    elseif(SDB_SANITIZE MATCHES "^(Undefined)?Address(Light)?$")
        if(SDB_SANITIZE MATCHES "Undefined")
            set(SDB_SANITIZE_FLAGS -fsanitize=undefined -fsanitize=address)
            set(SDB_SANITIZE "Undefined;Address")
        else()
            set(SDB_SANITIZE_FLAGS -fsanitize=address)
            set(SDB_SANITIZE "Address")
        endif()
        # TODO(mbkkt) apply these settings to libc++?
        if(SDB_SANITIZE_WAS MATCHES "Light")
            set(SDB_SANITIZE_EXTRA
                -fsanitize-address-use-after-return=never
                -fno-sanitize-address-use-after-scope
                -fno-sanitize-address-use-odr-indicator
            )
        else()
            set(SDB_SANITIZE_EXTRA
                -fsanitize-address-use-after-return=always
                -fsanitize-address-use-after-scope
                -fsanitize-address-use-odr-indicator
            )
        endif()
    elseif(SDB_SANITIZE STREQUAL "Thread")
        set(SDB_SANITIZE_FLAGS -fsanitize=thread)
        set(SDB_SANITIZE_UNWINDER OFF) # crashes with llvm libunwinder
    elseif(SDB_SANITIZE MATCHES "^Memory(WithOrigins)?$")
        # TODO(mbkkt) patch libc++ to be able work with -fsanitize-memory-track-origins=1 (default is 2)
        # TODO(mbkkt) add ability to apply -fno-sanitize-memory-use-after-dtor
        set(SDB_SANITIZE_FLAGS -fsanitize=memory)
        set(SDB_SANITIZE_EXTRA -fsanitize-memory-use-after-dtor)
        if(SDB_SANITIZE STREQUAL "MemoryWithOrigins")
            list(APPEND SDB_SANITIZE_EXTRA -fsanitize-memory-track-origins)
        endif()
        set(SDB_SANITIZE_UNWINDER OFF) # crashes with llvm libunwinder
    else()
        # TODO(mbkkt) Maybe support cfi, safe-stack?
        message(FATAL_ERROR "SDB_SANITIZE=${SDB_SANITIZE} not supported yet")
    endif()
    add_link_options(${SDB_SANITIZE_FLAGS})
endif()

add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-nostdinc++>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-nostdlib++>)

include(Runtime)

if(NOT SDB_SANITIZE STREQUAL "")
    # if will be some strange link error just enable this
    # add_compile_options(-fPIC -fPIE)
    add_compile_options(${SDB_SANITIZE_FLAGS} ${SDB_SANITIZE_EXTRA})
endif()

if(SDB_SANITIZE_UNWINDER)
    link_libraries(
        $<BUILD_LOCAL_INTERFACE:unwind_static>
        $<BUILD_LOCAL_INTERFACE:cxxabi_static>
        $<BUILD_LOCAL_INTERFACE:cxx_static>
    )
else()
    link_libraries(
        $<BUILD_LOCAL_INTERFACE:cxxabi_static>
        $<BUILD_LOCAL_INTERFACE:cxx_static>
    )
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

set(CMAKE_SKIP_INSTALL_RULES TRUE)
add_subdirectory(third_party EXCLUDE_FROM_ALL)
set(CMAKE_SKIP_INSTALL_RULES FALSE)

if("${SDB_ALLOC}" MATCHES "JE")
    link_libraries(jemalloc)
elseif("${SDB_ALLOC}" MATCHES "TC")
    link_libraries(tcmalloc::tcmalloc)
endif()

################################################################################
## PATHS, installation, packages, frontend
################################################################################

include(SereneDBInstall)

################################################################################
## SUB-PROJECTS
################################################################################

add_subdirectory(iresearch)
add_subdirectory(server/utils)
add_subdirectory(server)
add_subdirectory(tests)

add_custom_target(serenedb DEPENDS serened)
