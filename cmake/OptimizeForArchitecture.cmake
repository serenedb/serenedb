# https://learn.microsoft.com/en-us/windows/win32/winprog64/wow64-implementation-details
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(amd64.*|AMD64.*|x86_64.*)")
    set(ARCH_AMD64 1)
    set(SDB_ARCH_DEFAULT haswell)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm64.*|ARM64.*|aarch64.*|AARCH64.*)")
    # armv8.*|ARMV8.* is 32 bit builded aarch64, we don't need to support it now
    # also even if we want to support it, it's more like armv7 with extensions not aarch64
    set(ARCH_AARCH64 1)
    set(SDB_ARCH_DEFAULT graviton1)
    # elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc64le.*|ppc64le.*|PPC64LE.*)")
    # elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^(s390x.*|S390X.*)")
    # elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "riscv64")
else()
    message(
        WARNING
        "Platform ${CMAKE_SYSTEM_PROCESSOR} is probably not supported"
    )
endif()

# Names and design inspired by
# https://github.com/simdjson/simdjson/blob/master/doc/implementation-selection.md
# https://gitlab.com/x86-psABIs/x86-64-ABI/-/blob/master/x86-64-ABI/low-level-sys-info.tex
set(AMD64_V1 -mfxsr -mmmx -msse -msse2)
# "cx16 sse4.2 popcnt pclmul" are specially important
# aes needed for sse4.2 mode in highway, it's available for all sandybridge or newer
# except some mobile (for smartphone) intel proccessors
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=71235
set(AMD64_V2
    -mcx16
    -msahf
    -mpopcnt
    -msse3
    -msse4.1
    -msse4.2
    -mssse3
    -mpclmul
    -mavx
    -mxsave
    -maes
)
set(AMD64_V3
    -mavx2
    -mbmi
    -mbmi2
    -mf16c
    -mfma
    -mlzcnt
    -mmovbe
)
set(AMD64_V4
    -mavx512f
    -mavx512bw
    -mavx512cd
    -mavx512dq
    -mavx512vl
    -mavx512vbmi
    -mavx512vbmi2
)

set(SDB_ARCH_FLAGS_sandybridge ${AMD64_V1} ${AMD64_V2})
set(SDB_ARCH_FLAGS_haswell ${SDB_ARCH_FLAGS_sandybridge} ${AMD64_V3})
set(SDB_ARCH_FLAGS_icelake ${SDB_ARCH_FLAGS_haswell} ${AMD64_V4})

# Simplified for servers we need to support only AWS Graviton, Ampere Altra, Apple M1
# https://github.com/aws/aws-graviton-getting-started
# https://gcc.gnu.org/onlinedocs/gcc/AArch64-Options.html#AArch64-Options
# little-endian enabled by default, but only if target is not something like aarch64_be
# so force to generate little-endian code, because it's simpler and less error-prone
# "fp simd" are included to armv8-a
set(SDB_ARCH_FLAGS_graviton1 -mlittle-endian -march=armv8-a+crc+crypto)
set(SDB_ARCH_FLAGS_graviton2
    -mlittle-endian
    -march=armv8.2-a+fp16+rcpc+dotprod+crypto
)
set(SDB_ARCH_FLAGS_graviton3
    -mlittle-endian
    -march=armv8.4-a+sve+rng+bf16+int8+crypto
)

set(SDB_ARCH_FLAGS_native -march=native)

if(SDB_ARCH STREQUAL "")
    set(SDB_ARCH_SELECTED "${SDB_ARCH_DEFAULT}")
else()
    set(SDB_ARCH_SELECTED "${SDB_ARCH}")
endif()
if(
    NOT SDB_ARCH_SELECTED STREQUAL ""
    AND NOT DEFINED SDB_ARCH_FLAGS_${SDB_ARCH_SELECTED}
)
    message(FATAL_ERROR "SDB_ARCH=${SDB_ARCH} is not supported")
endif()
message(STATUS "Optimize for ${SDB_ARCH_SELECTED}")
string(JOIN " " SDB_ARCH_FLAGS ${SDB_ARCH_FLAGS_${SDB_ARCH_SELECTED}})
