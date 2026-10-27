cmake_minimum_required(VERSION 3.26)
message(STATUS "CMake version: ${CMAKE_VERSION}")
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE
        Release
        CACHE STRING
        "Choose the type of build, options are: Debug RelWithDebInfo Release."
        FORCE
    )
endif()

if(NOT CMAKE_BUILD_TYPE MATCHES "^(Debug|RelWithDebInfo|Release)$")
    message(
        FATAL_ERROR
        "expecting CMAKE_BUILD_TYPE: Debug RelWithDebInfo Release, got ${CMAKE_BUILD_TYPE}."
    )
endif()

# ------------------------------------------------------------------------------
# VERSION information
# ------------------------------------------------------------------------------

# stable release: MAJOR.MINOR.PATCH
# hotfix release: MAJOR.MINOR.PATCH.HOTFIX
# LTS release:    MAJOR.MINOR.PATCH-lts
#
# MAJOR = calendar year, 2-digit, UTC (e.g. 26 for 2026)
# MINOR = month, 2-digit zero-padded, UTC (e.g. 03 for March)
# PATCH = growing patch number, optionally with a hotfix number after a dot
#         (e.g. 1 or 1.1); 0 means dev/unversioned build
#
# A hotfix ships a fix on top of an already published patch release without
# moving that release line forward: 26.09.1.1 follows 26.09.1 and leaves
# 26.09.2 free for the next ordinary release.
#
# MAJOR and MINOR are derived from the UTC clock at configure time.
# PATCH defaults to 0; pass -DSERENEDB_VERSION_PATCH=N or N.M for releases.
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
elseif(NOT SERENEDB_VERSION_PATCH MATCHES "^[1-9][0-9]*(\\.[1-9][0-9]*)?$")
    message(
        FATAL_ERROR
        "SERENEDB_VERSION_PATCH must be N or N.M, both > 0, got "
        "'${SERENEDB_VERSION_PATCH}'. Omit -DSERENEDB_VERSION_PATCH for dev "
        "builds (patch defaults to 0)."
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

list(PREPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(SereneDBUtils)

################################################################################
## OPTIONS
################################################################################

option(SDB_WERROR "treat warnings as errors" ON)

option(
    SDB_UPDATE_SUBMODULES
    "check out every submodule at its recorded commit while configuring"
    ON
)

option(SDB_EMBEDDED_DOCS "embed the documentation from docs/ into serened" ON)

option(SDB_GTEST "build the gtest suites under tests/" OFF)

option(
    IRESEARCH_BENCH
    "build only search-benchmark-game, against a minimal iresearch: no server, vector, geo, fastText or DuckDB extensions besides icu"
    OFF
)
option(
    IRESEARCH_LIBRARY
    "build and install the standalone IResearch C++ library"
    OFF
)
if(IRESEARCH_LIBRARY AND IRESEARCH_BENCH)
    message(
        FATAL_ERROR
        "IRESEARCH_LIBRARY and IRESEARCH_BENCH are mutually exclusive"
    )
endif()
if(IRESEARCH_BENCH AND SDB_GTEST)
    message(
        FATAL_ERROR
        "IRESEARCH_BENCH builds only search-benchmark-game: turn SDB_GTEST off"
    )
endif()

set(SDB_ARCH
    ""
    CACHE STRING
    "instruction set: sandybridge, haswell, icelake, graviton1, graviton2, graviton3 or native; empty for haswell on x86-64 and graviton1 on arm64"
)
set_property(
    CACHE SDB_ARCH
    PROPERTY
        STRINGS
            ""
            sandybridge
            haswell
            icelake
            graviton1
            graviton2
            graviton3
            native
)

set(SDB_ALLOC
    SYS
    CACHE STRING
    "allocator: SYS (system), JE (jemalloc) or TC (tcmalloc)"
)
set_property(CACHE SDB_ALLOC PROPERTY STRINGS SYS JE TC)
if(NOT SDB_ALLOC MATCHES "^(SYS|JE|TC)$")
    message(FATAL_ERROR "SDB_ALLOC=${SDB_ALLOC}, expecting SYS, JE or TC")
endif()
option(SDB_JEMALLOC_PROF "jemalloc heap profiling, with SDB_ALLOC=JE" OFF)

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

option(
    SDB_COVERAGE
    "LLVM source-based coverage of our code, DuckDB and re2"
    OFF
)

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(SDB_RELEASE_DEFAULT ON)
else()
    set(SDB_RELEASE_DEFAULT OFF)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(SDB_DEBUG_INFO_DEFAULT FULL)
else()
    set(SDB_DEBUG_INFO_DEFAULT STACK)
endif()
set(SDB_DEBUG_INFO
    ${SDB_DEBUG_INFO_DEFAULT}
    CACHE STRING
    "debug info: NONE, STACK (line tables for symbolized stacks) or FULL (variables and types for lldb); FULL in Debug builds, STACK in the others"
)
set_property(CACHE SDB_DEBUG_INFO PROPERTY STRINGS NONE STACK FULL)

if(SDB_RELEASE_DEFAULT)
    set(SDB_FRAME_POINTERS_DEFAULT OFF)
else()
    set(SDB_FRAME_POINTERS_DEFAULT ON)
endif()
option(
    SDB_FRAME_POINTERS
    "keep frame pointers; off in Release builds"
    ${SDB_FRAME_POINTERS_DEFAULT}
)

if(APPLE)
    set(SDB_SPLIT_DWARF_DEFAULT OFF)
else()
    set(SDB_SPLIT_DWARF_DEFAULT ON)
endif()
option(
    SDB_SPLIT_DWARF
    "debug info in a .dwo file beside each object instead of in every binary"
    ${SDB_SPLIT_DWARF_DEFAULT}
)

option(SDB_GEN_PGO "build instrumented for PGO profile generation" OFF)
set(SDB_GEN_CS_PGO
    ""
    CACHE PATH
    "profile to use while generating a context-sensitive PGO profile"
)
set(SDB_USE_PGO "" CACHE PATH "PGO profile to optimize with")

option(
    SDB_LTO
    "ThinLTO, identical code folding and section GC; on in Release builds"
    ${SDB_RELEASE_DEFAULT}
)

option(
    SDB_REMAP_EXECUTABLE
    "default of serened's --remap_executable, which copies its machine code into huge pages at startup; on in Release builds"
    ${SDB_RELEASE_DEFAULT}
)

option(
    SDB_DEV
    "whether we want to have assertions and other development features"
    OFF
)

option(
    SDB_FAULT_INJECTION
    "whether we want to have failure tests compiled in"
    OFF
)

option(SDB_STATIC "link executables statically" OFF)
if(IRESEARCH_LIBRARY AND SDB_STATIC)
    message(FATAL_ERROR "IRESEARCH_LIBRARY requires SDB_STATIC=OFF")
endif()
if(IRESEARCH_LIBRARY)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif()

option(
    SDB_VTUNE
    "keep the pthread symbols VTune needs in a statically linked executable"
    OFF
)

################################################################################
## PLATFORM
################################################################################

if(APPLE)
    if(NOT CMAKE_CXX_COMPILER_AR)
        message(
            FATAL_ERROR
            "llvm-ar (CMAKE_CXX_COMPILER_AR) is required on macOS"
        )
    endif()
    set(CMAKE_AR "${CMAKE_CXX_COMPILER_AR}")
    set(CMAKE_RANLIB "${CMAKE_CXX_COMPILER_RANLIB}")
else()
    foreach(lang C CXX ASM)
        set(CMAKE_${lang}_ARCHIVE_CREATE
            "<CMAKE_AR> qcT <TARGET> <LINK_FLAGS> <OBJECTS>"
        )
        set(CMAKE_${lang}_ARCHIVE_APPEND
            "<CMAKE_AR> qT <TARGET> <LINK_FLAGS> <OBJECTS>"
        )
    endforeach()
endif()

include(OptimizeForArchitecture)

if(SDB_UPDATE_SUBMODULES)
    find_package(Git REQUIRED)
endif()
find_package(Python3 REQUIRED COMPONENTS Interpreter)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

# required for clang completion in editors - must be set after creating project
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# TODO(mbkkt) maybe disable in CI
set(CMAKE_COLOR_DIAGNOSTICS ON)

set(CMAKE_CXX_STANDARD 26 CACHE STRING "" FORCE)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF CACHE BOOL "" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

################################################################################
## DEFINITIONS
################################################################################

# disable not fatal logs and all vlogs in compile time
add_compile_definitions(ABSL_MAX_VLOG_VERBOSITY=-1 ABSL_MIN_LOG_LEVEL=3)

if(SDB_DEV)
    add_compile_definitions(SDB_DEV=1 DEBUG)

    # TODO(mbkkt) add_compile_definitions(_FORTIFY_SOURCE=3)
else()
    add_compile_definitions(NDEBUG)
endif()

if(SDB_GTEST)
    add_compile_definitions(SDB_GTEST=1)
endif()

if(SDB_FAULT_INJECTION)
    add_compile_definitions(SDB_FAULT_INJECTION=1)
endif()

# TODO(mbkkt) -fnew-alignment=8 and tcmalloc is nothrow -- ABSL_ALLOCATOR_NOTHROW
if(SDB_ALLOC STREQUAL "JE")
    add_compile_definitions(SERENEDB_HAVE_JEMALLOC=1)
elseif(SDB_ALLOC STREQUAL "TC")
    add_compile_definitions(SERENEDB_HAVE_TCMALLOC=1)
endif()

################################################################################
## FLAGS
################################################################################

set(SDB_CODEGEN_FLAGS
    "${SDB_ARCH_FLAGS} -falign-functions=64 -falign-loops=64 -fno-math-errno"
)
if(SDB_LTO)
    # TODO(mbkkt) Try to apply -fsplit-lto-unit, -flo=full -fbasic-block-sections=all
    # -fexperimental-relative-c++-abi-vtables, -fassume-unique-vtables -fvirtual-function-elimination
    # -fassume-nothrow-exception-dtor -fasync-exceptions -fasynchronous-unwind-tables
    string(
        APPEND SDB_CODEGEN_FLAGS
        " -flto=thin -faddrsig -ffunction-sections -fdata-sections"
    )
    # -fforce-emit-vtables -fwhole-program-vtables
    string(APPEND CMAKE_EXE_LINKER_FLAGS " -Wl,-icf=safe -Wl,--gc-sections")
endif()
string(APPEND CMAKE_C_FLAGS " ${SDB_CODEGEN_FLAGS}")
string(
    APPEND CMAKE_CXX_FLAGS
    " ${SDB_CODEGEN_FLAGS} -fsized-deallocation -fstrict-vtable-pointers"
)

file(
    RELATIVE_PATH
    SDB_SOURCE_FROM_BUILD
    ${CMAKE_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}
)
file(
    RELATIVE_PATH
    SDB_BUILD_FROM_SOURCE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
)
if(SDB_BUILD_FROM_SOURCE STREQUAL "")
    message(
        FATAL_ERROR
        "build in a directory of its own, such as the presets' build_<preset>"
    )
endif()
string(REGEX REPLACE "/+$" "" SDB_SOURCE_FROM_BUILD "${SDB_SOURCE_FROM_BUILD}")
string(APPEND SDB_SOURCE_FROM_BUILD "/")

add_compile_options(
    $<$<BOOL:${SDB_WERROR}>:-Werror>
    -ffile-prefix-map=${CMAKE_SOURCE_DIR}/=
    -ffile-prefix-map=${SDB_SOURCE_FROM_BUILD}=
    -fdebug-prefix-map=${CMAKE_SOURCE_DIR}/=${SDB_SOURCE_FROM_BUILD}
    -fdebug-prefix-map=${SDB_SOURCE_FROM_BUILD}=${SDB_SOURCE_FROM_BUILD}
    -fdebug-compilation-dir=${SDB_BUILD_FROM_SOURCE}
    -fcoverage-compilation-dir=.
    $<$<COMPILE_LANGUAGE:CXX>:-nostdinc++>
    $<$<COMPILE_LANGUAGE:CXX>:-nostdlib++>
)

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

if(SDB_COVERAGE)
    set(SDB_COVERAGE_FLAGS
        -fprofile-instr-generate
        -fcoverage-mapping
        -fcoverage-mcdc
        "SHELL:-mllvm -enable-single-byte-coverage=true"
        "SHELL:-mllvm -conditional-counter-update"
        -Wno-pass-failed
    )
    add_link_options(-fprofile-instr-generate)
endif()

# VTune needs these symbols in a statically linked binary, see
# https://software.intel.com/content/www/us/en/develop/documentation/vtune-help/top/set-up-analysis-target/linux-targets/analyzing-statically-linked-binaries-on-linux-targets.html
if(SDB_VTUNE)
    string(
        APPEND CMAKE_EXE_LINKER_FLAGS
        " -upthread_getattr_np -upthread_create -upthread_key_create -upthread_setspecific -upthread_getspecific -upthread_self -upthread_attr_destroy -upthread_attr_setstack -upthread_attr_getstack -upthread_attr_getstacksize -upthread_attr_setstacksize -upthread_cancel -u_pthread_cleanup_push -u_pthread_cleanup_pop -upthread_mutex_lock -upthread_mutex_trylock -upthread_spin_lock -upthread_spin_trylock"
    )
endif()

if(SDB_FRAME_POINTERS)
    set(SDB_FRAME_POINTER_FLAGS
        "-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer"
    )
else()
    set(SDB_FRAME_POINTER_FLAGS
        "-fomit-frame-pointer -momit-leaf-frame-pointer"
    )
endif()

set(SDB_DEBUG_INFO_NONE "-g0")
set(SDB_DEBUG_INFO_STACK "-gline-tables-only")
set(SDB_DEBUG_INFO_FULL "-g -glldb")
if(NOT DEFINED SDB_DEBUG_INFO_${SDB_DEBUG_INFO})
    message(
        FATAL_ERROR
        "SDB_DEBUG_INFO=${SDB_DEBUG_INFO}, expecting NONE, STACK or FULL"
    )
endif()
set(SDB_DEBUG_FLAGS "${SDB_DEBUG_INFO_${SDB_DEBUG_INFO}}")

if(SDB_SPLIT_DWARF AND NOT SDB_DEBUG_INFO STREQUAL "NONE")
    string(APPEND SDB_DEBUG_FLAGS " -gsplit-dwarf")
    if(SDB_LTO)
        add_link_options("-Wl,--plugin-opt=dwo_dir=dwo/$<TARGET_PROPERTY:NAME>")
    endif()
endif()

# note: when building one of the build types, CMake will automatically combine
# the base flags from CMAKE_<LANG>_FLAGS with build type-specific flags in
# CMAKE_<LANG>_FLAGS_${CMAKE_BUILD_TYPE}.
# there is no need to repeat the base flags in the build-type specific flags!
foreach(lang C CXX)
    set(CMAKE_${lang}_FLAGS_DEBUG
        "-O0 ${SDB_FRAME_POINTER_FLAGS} ${SDB_DEBUG_FLAGS}"
        CACHE INTERNAL
        ""
    )
    set(CMAKE_${lang}_FLAGS_RELWITHDEBINFO
        "-O3 ${SDB_FRAME_POINTER_FLAGS} ${SDB_DEBUG_FLAGS}"
        CACHE INTERNAL
        ""
    )
    set(CMAKE_${lang}_FLAGS_RELEASE
        "-O3 ${SDB_FRAME_POINTER_FLAGS} ${SDB_DEBUG_FLAGS}"
        CACHE INTERNAL
        ""
    )
endforeach()

################################################################################
## Runtime
################################################################################

cmake_host_system_information(RESULT SDB_DISTRIB_NAME QUERY DISTRIB_NAME)
if(SDB_DISTRIB_NAME MATCHES "Alpine")
    set(SDB_MUSL ON)
else()
    set(SDB_MUSL OFF)
endif()

execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} --print-libgcc-file-name --rtlib=compiler-rt
    OUTPUT_VARIABLE SDB_BUILTINS_LIBRARY
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
# link nothing by default and use clang_rt for builtins
set(SDB_DEFAULT_LIBS "-nodefaultlibs ${SDB_BUILTINS_LIBRARY}")
if(SDB_MUSL)
    string(APPEND SDB_DEFAULT_LIBS " -lc")
else()
    # TODO(mbkkt) think about -lrt -lpthread -ldl
    string(APPEND SDB_DEFAULT_LIBS " -lc -lm")
endif()
if(SDB_STATIC)
    set(CMAKE_POSITION_INDEPENDENT_CODE OFF CACHE BOOL "" FORCE)
    set(SDB_DEFAULT_LIBS "-static ${SDB_DEFAULT_LIBS}")
endif()
set(CMAKE_CXX_STANDARD_LIBRARIES ${SDB_DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${SDB_DEFAULT_LIBS})

# One dispatch feeds both passes: the link options go on here, before the llvm
# runtimes subdir is added, so libc++ links the sanitizer runtime; the compile
# options go on after it, so libc++ itself is not instrumented.
# SDB_LLVM_SANITIZER is the same sanitizer in the names LLVM_USE_SANITIZER takes.
set(SDB_SANITIZE_FLAGS "")
set(SDB_SANITIZE_EXTRA "")
set(SDB_LLVM_SANITIZER "${SDB_SANITIZE}")
set(SDB_LLVM_UNWINDER ON)
if(NOT SDB_SANITIZE STREQUAL "")
    # TODO(mbkkt) we also can apply sanitize-recover
    if(SDB_SANITIZE STREQUAL "Leak")
        set(SDB_SANITIZE_FLAGS -fsanitize=leak)
        set(SDB_LLVM_SANITIZER "Leaks")
    elseif(SDB_SANITIZE STREQUAL "Undefined")
        # TODO(mbkkt) enable nullability checks and local-bounds
        set(SDB_SANITIZE_FLAGS -fsanitize=undefined)
    elseif(SDB_SANITIZE MATCHES "^(Undefined)?Address(Light)?$")
        if(SDB_SANITIZE MATCHES "^Undefined")
            set(SDB_SANITIZE_FLAGS -fsanitize=undefined -fsanitize=address)
            set(SDB_LLVM_SANITIZER "Undefined;Address")
        else()
            set(SDB_SANITIZE_FLAGS -fsanitize=address)
            set(SDB_LLVM_SANITIZER "Address")
        endif()
        # TODO(mbkkt) apply these settings to libc++?
        if(SDB_SANITIZE MATCHES "Light$")
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
        set(SDB_LLVM_UNWINDER OFF) # crashes with llvm libunwinder
    elseif(SDB_SANITIZE MATCHES "^Memory(WithOrigins)?$")
        # TODO(mbkkt) patch libc++ to be able work with -fsanitize-memory-track-origins=1 (default is 2)
        # TODO(mbkkt) add ability to apply -fno-sanitize-memory-use-after-dtor
        set(SDB_SANITIZE_FLAGS -fsanitize=memory)
        set(SDB_SANITIZE_EXTRA
            -fsanitize-memory-use-after-dtor
            -fsanitize-blacklist=${CMAKE_SOURCE_DIR}/resources/suppressions/msan.txt
        )
        if(SDB_SANITIZE STREQUAL "MemoryWithOrigins")
            list(APPEND SDB_SANITIZE_EXTRA -fsanitize-memory-track-origins)
        endif()
        set(SDB_LLVM_UNWINDER OFF) # crashes with llvm libunwinder
    else()
        # TODO(mbkkt) Maybe support cfi, safe-stack?
        message(FATAL_ERROR "SDB_SANITIZE=${SDB_SANITIZE} not supported yet")
    endif()
    add_link_options(${SDB_SANITIZE_FLAGS})
endif()

include(Runtime)

if(NOT SDB_SANITIZE STREQUAL "")
    # if will be some strange link error just enable this
    # add_compile_options(-fPIC -fPIE)
    add_compile_options(
        ${SDB_SANITIZE_FLAGS}
        ${SDB_SANITIZE_EXTRA}
        -Wno-pass-failed
    )
endif()

if(SDB_LLVM_UNWINDER)
    link_libraries($<BUILD_LOCAL_INTERFACE:unwind_static>)
endif()
link_libraries(
    $<BUILD_LOCAL_INTERFACE:cxxabi_static>
    $<BUILD_LOCAL_INTERFACE:cxx_static>
)

################################################################################
## 3RD PARTY
################################################################################

set(CMAKE_SKIP_INSTALL_RULES TRUE)
add_subdirectory(third_party EXCLUDE_FROM_ALL SYSTEM)
set(CMAKE_SKIP_INSTALL_RULES FALSE)

if(SDB_ALLOC STREQUAL "JE")
    link_libraries(jemalloc)
elseif(SDB_ALLOC STREQUAL "TC")
    link_libraries(tcmalloc::tcmalloc)
endif()

################################################################################
## INSTALL
################################################################################

include(GNUInstallDirs)
if(NOT IRESEARCH_LIBRARY)
    install(
        FILES ${CMAKE_SOURCE_DIR}/LICENSE
        DESTINATION ${CMAKE_INSTALL_DOCDIR}
        RENAME LICENSE.txt
    )
    # glibc license files (required for static linking, see glibc LICENSES file)
    install(
        DIRECTORY ${CMAKE_SOURCE_DIR}/resources/licenses/glibc/
        DESTINATION ${CMAKE_INSTALL_DOCDIR}/glibc
    )
    install(DIRECTORY DESTINATION ${CMAKE_INSTALL_LOCALSTATEDIR}/lib/serenedb)
    install(DIRECTORY DESTINATION ${CMAKE_INSTALL_LOCALSTATEDIR}/log/serenedb)
    # systemd service file is installed via debian packaging
    # (packages/debian/source/common/serenedb.service)
endif()

################################################################################
## SUB-PROJECTS
################################################################################

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/")

add_compile_options(
    -Wall
    -Wcomma
    -Wdeprecated
    -Wextra
    -Wextra-semi
    -Wfloat-conversion
    -Wformat-signedness
    -Wformat=2
    -Wheader-hygiene
    -Wignored-base-class-qualifiers
    -Wimplicit-fallthrough
    -Wimplicit-int-conversion-on-negation
    -Wloop-analysis
    -Wmissing-variable-declarations
    -Wnon-virtual-dtor
    -Wnull-dereference
    -Wsuggest-destructor-override
    -Wsuggest-override
    -Wthread-safety
    -Wthread-safety-beta
    -Wundef
    -Wunreachable-code-break
    -Wunreachable-code-return
    -Wunused-exception-parameter
    -Wvla
    -Wno-c++98-compat-extra-semi
    -Wno-deprecated-copy-with-dtor
    -Wno-deprecated-declarations
    -Wno-missing-designated-field-initializers
    -Wno-unused-parameter
)
if(SDB_COVERAGE)
    add_compile_options(${SDB_COVERAGE_FLAGS})
    add_compile_definitions(SDB_COVERAGE)
endif()

add_subdirectory(iresearch)
if(IRESEARCH_LIBRARY)
    include(cmake/IResearchLibrary.cmake)
    if(SDB_GTEST)
        add_subdirectory(tests)
    endif()
    return()
endif()
if(NOT IRESEARCH_BENCH)
    add_subdirectory(server)
    add_subdirectory(examples/iresearch EXCLUDE_FROM_ALL)
    add_custom_target(serenedb)
    add_dependencies(serenedb serened)
endif()
add_subdirectory(tests)
