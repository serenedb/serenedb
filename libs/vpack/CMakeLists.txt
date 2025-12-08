option(BUILD_VPACK_SSE "Build vpack with SSE optimization instructions" ON)

if(NOT BUILD_VPACK_SSE)
    add_definitions("-DVPACK_ASM_OPTIMIZATIONS=0")
else()
    if(ADD_SSE_FLAG)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
    elseif(ARCH_AARCH64)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__ARM_NEON")
    endif()
    add_definitions("-DVPACK_ASM_OPTIMIZATIONS=1")
endif()

add_library(
    vpack
    STATIC
    src/common.cpp
    src/literal.cpp
    src/builder.cpp
    src/collection.cpp
    src/dumper.cpp
    src/exception.cpp
    src/hex_dump.cpp
    src/iterator.cpp
    src/options.cpp
    src/parser.cpp
    src/slice.cpp
    src/utf8_helper.cpp
    src/validator.cpp
    src/value.cpp
    src/value_type.cpp
    src/asm-functions.cpp
    src/asm-utf8check.cpp
    src/string-functions.cpp
    src/string.cpp
    src/validation.cpp
    src/vpack_helper.cpp
)

target_include_directories(vpack PRIVATE src PUBLIC include)

target_link_libraries(
    vpack
    PUBLIC absl::strings
    PUBLIC absl::cord
    PUBLIC absl::flat_hash_set
    PUBLIC absl::flat_hash_map
    PUBLIC taocpp::json
    PRIVATE sse2neon
)
