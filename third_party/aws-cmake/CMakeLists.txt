# Monolithic AWS SDK build (ClickHouse-style)
# Compiles all needed AWS CRT + SDK sources into a single _aws static library.

set(AWS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")

# AWS cmake utilities from aws-c-common
set(AWS_C_COMMON_DIR "${AWS_DIR}/aws-c-common")

list(APPEND CMAKE_MODULE_PATH "${AWS_C_COMMON_DIR}/cmake")
include(AwsFeatureTests)
include(AwsSIMD)
include(AwsThreadAffinity)
include(AwsThreadName)

# Configure headers
set(AWS_COMMON_CONFIG_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/aws-common-config")
configure_file(
    "${AWS_C_COMMON_DIR}/include/aws/common/config.h.in"
    "${AWS_COMMON_CONFIG_BINARY_DIR}/aws/common/config.h"
)

# aws-crt-cpp Config.h
set(AWS_CRT_CONFIG_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/aws-crt-config")
set(FULL_VERSION "0.37.3")
set(PROJECT_VERSION_MAJOR 0)
set(PROJECT_VERSION_MINOR 37)
set(PROJECT_VERSION_PATCH 3)
set(GIT_HASH "unknown")
configure_file(
    "${AWS_DIR}/aws-crt-cpp/include/aws/crt/Config.h.in"
    "${AWS_CRT_CONFIG_BINARY_DIR}/aws/crt/Config.h"
)

# aws-cpp-sdk-core SDKConfig.h
set(AWS_SDK_CONFIG_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-config")
set(USE_AWS_MEMORY_MANAGEMENT OFF)
configure_file(
    "${AWS_DIR}/aws-sdk-cpp/src/aws-cpp-sdk-core/include/aws/core/SDKConfig.h.in"
    "${AWS_SDK_CONFIG_BINARY_DIR}/aws/core/SDKConfig.h"
)

# Gather C sources from CRT libraries

# --- aws-c-common ---
file(GLOB AWS_C_COMMON_SRC "${AWS_C_COMMON_DIR}/source/*.c")
file(GLOB AWS_C_COMMON_EXTERNAL_SRC "${AWS_C_COMMON_DIR}/source/external/*.c")
file(GLOB_RECURSE AWS_C_COMMON_LIBCBOR_SRC "${AWS_C_COMMON_DIR}/source/external/libcbor/*.c")
file(GLOB AWS_C_COMMON_POSIX_SRC "${AWS_C_COMMON_DIR}/source/posix/*.c")
file(GLOB AWS_C_COMMON_LINUX_SRC "${AWS_C_COMMON_DIR}/source/linux/*.c")

file(GLOB AWS_C_COMMON_ARCH_GENERIC_SRC "${AWS_C_COMMON_DIR}/source/arch/generic/*.c")

set(AWS_C_COMMON_ALL_SRC
    ${AWS_C_COMMON_SRC}
    ${AWS_C_COMMON_EXTERNAL_SRC}
    ${AWS_C_COMMON_LIBCBOR_SRC}
    ${AWS_C_COMMON_POSIX_SRC}
    ${AWS_C_COMMON_LINUX_SRC}
    ${AWS_C_COMMON_ARCH_GENERIC_SRC}
)

# --- aws-checksums ---
set(AWS_CHECKSUMS_DIR "${AWS_DIR}/aws-checksums")
file(GLOB AWS_CHECKSUMS_SRC "${AWS_CHECKSUMS_DIR}/source/*.c")

set(AWS_CHECKSUMS_ARCH_SRC "")
if(AWS_ARCH_INTEL)
    file(GLOB AWS_CHECKSUMS_INTEL_ASM_SRC "${AWS_CHECKSUMS_DIR}/source/intel/asm/*.c")
    file(GLOB AWS_CHECKSUMS_INTEL_INTRIN_SRC "${AWS_CHECKSUMS_DIR}/source/intel/intrin/*.c")
    list(APPEND AWS_CHECKSUMS_ARCH_SRC ${AWS_CHECKSUMS_INTEL_ASM_SRC} ${AWS_CHECKSUMS_INTEL_INTRIN_SRC})
elseif(AWS_ARCH_ARM64)
    file(GLOB AWS_CHECKSUMS_ARM_SRC "${AWS_CHECKSUMS_DIR}/source/arm/*.c")
    list(APPEND AWS_CHECKSUMS_ARCH_SRC ${AWS_CHECKSUMS_ARM_SRC})
endif()
list(APPEND AWS_CHECKSUMS_SRC ${AWS_CHECKSUMS_ARCH_SRC})

# --- aws-c-cal ---
set(AWS_C_CAL_DIR "${AWS_DIR}/aws-c-cal")
file(GLOB AWS_C_CAL_SRC "${AWS_C_CAL_DIR}/source/*.c")
file(GLOB AWS_C_CAL_UNIX_SRC "${AWS_C_CAL_DIR}/source/unix/*.c")
file(GLOB AWS_C_CAL_SHARED_SRC "${AWS_C_CAL_DIR}/source/shared/*.c")
list(APPEND AWS_C_CAL_SRC ${AWS_C_CAL_UNIX_SRC} ${AWS_C_CAL_SHARED_SRC})

# --- aws-c-io ---
set(AWS_C_IO_DIR "${AWS_DIR}/aws-c-io")
file(GLOB AWS_C_IO_SRC "${AWS_C_IO_DIR}/source/*.c")
file(GLOB AWS_C_IO_POSIX_SRC "${AWS_C_IO_DIR}/source/posix/*.c")
file(GLOB AWS_C_IO_LINUX_SRC "${AWS_C_IO_DIR}/source/linux/*.c")
list(APPEND AWS_C_IO_SRC ${AWS_C_IO_POSIX_SRC} ${AWS_C_IO_LINUX_SRC})

# --- aws-c-compression ---
set(AWS_C_COMPRESSION_DIR "${AWS_DIR}/aws-c-compression")
file(GLOB AWS_C_COMPRESSION_SRC "${AWS_C_COMPRESSION_DIR}/source/*.c")
list(FILTER AWS_C_COMPRESSION_SRC EXCLUDE REGEX "huffman_testing\\.c$")

# --- aws-c-http ---
set(AWS_C_HTTP_DIR "${AWS_DIR}/aws-c-http")
file(GLOB AWS_C_HTTP_SRC "${AWS_C_HTTP_DIR}/source/*.c")

# --- aws-c-sdkutils ---
set(AWS_C_SDKUTILS_DIR "${AWS_DIR}/aws-c-sdkutils")
file(GLOB AWS_C_SDKUTILS_SRC "${AWS_C_SDKUTILS_DIR}/source/*.c")

# --- aws-c-auth ---
set(AWS_C_AUTH_DIR "${AWS_DIR}/aws-c-auth")
file(GLOB AWS_C_AUTH_SRC "${AWS_C_AUTH_DIR}/source/*.c")

# --- aws-c-event-stream ---
set(AWS_C_EVENT_STREAM_DIR "${AWS_DIR}/aws-c-event-stream")
file(GLOB AWS_C_EVENT_STREAM_SRC "${AWS_C_EVENT_STREAM_DIR}/source/*.c")

# --- aws-c-s3 ---
set(AWS_C_S3_DIR "${AWS_DIR}/aws-c-s3")
file(GLOB AWS_C_S3_SRC "${AWS_C_S3_DIR}/source/*.c")
file(GLOB AWS_C_S3_ENDPOINT_RESOLVER_SRC "${AWS_C_S3_DIR}/source/s3_endpoint_resolver/*.c")
list(APPEND AWS_C_S3_SRC ${AWS_C_S3_ENDPOINT_RESOLVER_SRC})

# --- aws-c-mqtt ---
set(AWS_C_MQTT_DIR "${AWS_DIR}/aws-c-mqtt")
file(GLOB AWS_C_MQTT_SRC "${AWS_C_MQTT_DIR}/source/*.c")
file(GLOB AWS_C_MQTT_V5_SRC "${AWS_C_MQTT_DIR}/source/v5/*.c")
file(GLOB AWS_C_MQTT_RR_SRC "${AWS_C_MQTT_DIR}/source/request-response/*.c")
list(APPEND AWS_C_MQTT_SRC ${AWS_C_MQTT_V5_SRC} ${AWS_C_MQTT_RR_SRC})

# Gather C++ sources from aws-crt-cpp
file(GLOB AWS_CRT_CPP_ALL_SRC
    "${AWS_DIR}/aws-crt-cpp/source/*.cpp"
    "${AWS_DIR}/aws-crt-cpp/source/auth/*.cpp"
    "${AWS_DIR}/aws-crt-cpp/source/crypto/*.cpp"
    "${AWS_DIR}/aws-crt-cpp/source/endpoints/*.cpp"
    "${AWS_DIR}/aws-crt-cpp/source/external/*.cpp"
    "${AWS_DIR}/aws-crt-cpp/source/http/*.cpp"
    "${AWS_DIR}/aws-crt-cpp/source/io/*.cpp"
    "${AWS_DIR}/aws-crt-cpp/source/checksum/*.cpp"
    "${AWS_DIR}/aws-crt-cpp/source/cbor/*.cpp"
)

# Gather C++ sources from aws-cpp-sdk-core

set(AWS_SDK_CORE_DIR "${AWS_DIR}/aws-sdk-cpp/src/aws-cpp-sdk-core")

file(GLOB_RECURSE AWS_SDK_CORE_ALL_SRC "${AWS_SDK_CORE_DIR}/source/*.cpp")
list(FILTER AWS_SDK_CORE_ALL_SRC EXCLUDE REGEX "/http/curl/")
list(FILTER AWS_SDK_CORE_ALL_SRC EXCLUDE REGEX "/http/windows/")
list(FILTER AWS_SDK_CORE_ALL_SRC EXCLUDE REGEX "/net/windows/")
list(FILTER AWS_SDK_CORE_ALL_SRC EXCLUDE REGEX "/platform/android/")
list(FILTER AWS_SDK_CORE_ALL_SRC EXCLUDE REGEX "/platform/windows/")
list(FILTER AWS_SDK_CORE_ALL_SRC EXCLUDE REGEX "opentelemetry")

# Gather C++ sources from service SDKs

# --- aws-cpp-sdk-s3 ---
file(GLOB_RECURSE AWS_SDK_S3_SRC
    "${AWS_DIR}/aws-sdk-cpp/generated/src/aws-cpp-sdk-s3/source/*.cpp"
)

# --- aws-cpp-sdk-sts ---
file(GLOB_RECURSE AWS_SDK_STS_SRC
    "${AWS_DIR}/aws-sdk-cpp/generated/src/aws-cpp-sdk-sts/source/*.cpp"
)

# --- aws-cpp-sdk-cognito-identity ---
file(GLOB_RECURSE AWS_SDK_COGNITO_SRC
    "${AWS_DIR}/aws-sdk-cpp/generated/src/aws-cpp-sdk-cognito-identity/source/*.cpp"
)

# --- aws-cpp-sdk-identity-management ---
file(GLOB_RECURSE AWS_SDK_IDENTITY_MGMT_SRC
    "${AWS_DIR}/aws-sdk-cpp/src/aws-cpp-sdk-identity-management/source/*.cpp"
)

# Weak symbol stubs for TLS/MQTT (replaces BYO_CRYPTO approach)
set(AWS_STUBS "${CMAKE_CURRENT_SOURCE_DIR}/aws_stubs.cpp")

add_library(_aws STATIC
    # C sources - CRT libs
    ${AWS_C_COMMON_ALL_SRC}
    ${AWS_CHECKSUMS_SRC}
    ${AWS_C_CAL_SRC}
    ${AWS_C_IO_SRC}
    ${AWS_C_COMPRESSION_SRC}
    ${AWS_C_HTTP_SRC}
    ${AWS_C_SDKUTILS_SRC}
    ${AWS_C_AUTH_SRC}
    ${AWS_C_EVENT_STREAM_SRC}
    ${AWS_C_S3_SRC}
    ${AWS_C_MQTT_SRC}
    # C++ sources - CRT wrapper
    ${AWS_CRT_CPP_ALL_SRC}
    # C++ sources - SDK core
    ${AWS_SDK_CORE_ALL_SRC}
    # C++ sources - Service SDKs
    ${AWS_SDK_S3_SRC}
    ${AWS_SDK_STS_SRC}
    ${AWS_SDK_COGNITO_SRC}
    ${AWS_SDK_IDENTITY_MGMT_SRC}
    # Stubs
    ${AWS_STUBS}
)

target_include_directories(_aws SYSTEM PUBLIC
    # Generated config headers
    "${AWS_COMMON_CONFIG_BINARY_DIR}"
    "${AWS_CRT_CONFIG_BINARY_DIR}"
    "${AWS_SDK_CONFIG_BINARY_DIR}"

    # CRT C library headers
    "${AWS_C_COMMON_DIR}/include"
    "${AWS_CHECKSUMS_DIR}/include"
    "${AWS_C_CAL_DIR}/include"
    "${AWS_C_IO_DIR}/include"
    "${AWS_C_COMPRESSION_DIR}/include"
    "${AWS_C_HTTP_DIR}/include"
    "${AWS_C_SDKUTILS_DIR}/include"
    "${AWS_C_AUTH_DIR}/include"
    "${AWS_C_EVENT_STREAM_DIR}/include"
    "${AWS_C_S3_DIR}/include"
    "${AWS_C_MQTT_DIR}/include"

    # CRT C++ wrapper
    "${AWS_DIR}/aws-crt-cpp/include"

    # SDK core
    "${AWS_SDK_CORE_DIR}/include"

    # Service SDKs
    "${AWS_DIR}/aws-sdk-cpp/generated/src/aws-cpp-sdk-s3/include"
    "${AWS_DIR}/aws-sdk-cpp/generated/src/aws-cpp-sdk-sts/include"
    "${AWS_DIR}/aws-sdk-cpp/generated/src/aws-cpp-sdk-cognito-identity/include"
    "${AWS_DIR}/aws-sdk-cpp/src/aws-cpp-sdk-identity-management/include"
)

# Private include for libcbor headers
target_include_directories(_aws PRIVATE
    "${AWS_C_COMMON_DIR}/source/external/libcbor"
)

# Compile definitions
target_compile_definitions(_aws PUBLIC
    AWS_SDK_USE_CRT_HTTP
    ENABLE_OPENSSL_ENCRYPTION
    AWS_SDK_VERSION_MAJOR=1
    AWS_SDK_VERSION_MINOR=11
    AWS_SDK_VERSION_PATCH=0
    AWS_DEEP_CHECKS=0
    AWS_ENABLE_EPOLL
    INTEL_NO_ITTNOTIFY_API
    AWS_MQTT_WITH_ACCOUNT_ENDPOINTS
    AWS_CHECKSUMS_USE_HW_CRC32=0
)

# Compiler flags
target_compile_options(_aws PRIVATE
    -Wno-extra
    -Wno-sign-compare
    -Wno-unused-parameter
    -Wno-missing-field-initializers
    -Wno-deprecated-declarations
    -Wno-cast-function-type
)

# Set per-file SIMD flags for checksums
if(AWS_ARCH_INTEL)
    if(AWS_HAVE_AVX2_INTRINSICS)
        set_source_files_properties(
            "${AWS_CHECKSUMS_DIR}/source/intel/intrin/crc32c_sse42_avx512.c"
            PROPERTIES COMPILE_FLAGS "${AWS_AVX512_FLAG} ${AWS_CLMUL_FLAG} ${AWS_SSE4_2_FLAG}"
        )
        set_source_files_properties(
            "${AWS_CHECKSUMS_DIR}/source/intel/intrin/crc64nvme_avx512.c"
            PROPERTIES COMPILE_FLAGS "${AWS_AVX512_FLAG} ${AWS_CLMUL_FLAG}"
        )
        set_source_files_properties(
            "${AWS_CHECKSUMS_DIR}/source/intel/intrin/crc64nvme_clmul.c"
            PROPERTIES COMPILE_FLAGS "${AWS_CLMUL_FLAG} ${AWS_SSE4_2_FLAG}"
        )
    endif()
    set_source_files_properties(
        "${AWS_CHECKSUMS_DIR}/source/intel/asm/crc32c_sse42_asm.c"
        PROPERTIES COMPILE_FLAGS "${AWS_SSE4_2_FLAG}"
    )
elseif(AWS_ARCH_ARM64)
    set_source_files_properties(
        "${AWS_CHECKSUMS_DIR}/source/arm/crc32c_arm.c"
        "${AWS_CHECKSUMS_DIR}/source/arm/crc64_arm.c"
        PROPERTIES COMPILE_FLAGS "${AWS_ARMv8_1_FLAG}"
    )
endif()

# SIMD flags for aws-c-common encoding
if(AWS_HAVE_AVX2_INTRINSICS)
    set_source_files_properties(
        "${AWS_C_COMMON_DIR}/source/arch/intel/encoding_avx2.c"
        PROPERTIES COMPILE_FLAGS "${AWS_AVX2_FLAG}"
    )
endif()

target_link_libraries(_aws PUBLIC
    OpenSSL::Crypto
    OpenSSL::SSL
)

aws_set_thread_affinity_method(_aws)
aws_set_thread_name_method(_aws)
