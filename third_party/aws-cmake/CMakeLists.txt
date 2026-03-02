# Monolithic AWS SDK build (ClickHouse-style)
# Compiles all needed AWS CRT + SDK sources into a single _aws static library.

set(AWS_SDK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../aws-sdk-cpp")
set(AWS_CRT_DIR "${AWS_SDK_DIR}/crt/aws-crt-cpp")
set(AWS_CRT_CRT_DIR "${AWS_CRT_DIR}/crt")

# ===========================================================================
# AWS cmake utilities from aws-c-common
# ===========================================================================
set(AWS_C_COMMON_DIR "${AWS_CRT_CRT_DIR}/aws-c-common")

list(APPEND CMAKE_MODULE_PATH "${AWS_C_COMMON_DIR}/cmake")
include(AwsFeatureTests)
include(AwsSIMD)
include(AwsThreadAffinity)
include(AwsThreadName)

# ===========================================================================
# Configure headers
# ===========================================================================
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
    "${AWS_CRT_DIR}/include/aws/crt/Config.h.in"
    "${AWS_CRT_CONFIG_BINARY_DIR}/aws/crt/Config.h"
)

# aws-cpp-sdk-core SDKConfig.h
set(AWS_SDK_CONFIG_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-config")
set(USE_AWS_MEMORY_MANAGEMENT OFF)
configure_file(
    "${AWS_SDK_DIR}/src/aws-cpp-sdk-core/include/aws/core/SDKConfig.h.in"
    "${AWS_SDK_CONFIG_BINARY_DIR}/aws/core/SDKConfig.h"
)

# ===========================================================================
# Gather C sources from CRT libraries
# ===========================================================================

# --- aws-c-common ---
file(GLOB AWS_C_COMMON_SRC "${AWS_C_COMMON_DIR}/source/*.c")
file(GLOB AWS_C_COMMON_EXTERNAL_SRC "${AWS_C_COMMON_DIR}/source/external/*.c")
file(GLOB_RECURSE AWS_C_COMMON_LIBCBOR_SRC "${AWS_C_COMMON_DIR}/source/external/libcbor/*.c")
file(GLOB AWS_C_COMMON_POSIX_SRC "${AWS_C_COMMON_DIR}/source/posix/*.c")
file(GLOB AWS_C_COMMON_LINUX_SRC "${AWS_C_COMMON_DIR}/source/linux/*.c")

# Arch-specific sources for aws-c-common
file(GLOB AWS_C_COMMON_ARCH_GENERIC_SRC "${AWS_C_COMMON_DIR}/source/arch/generic/*.c")

set(AWS_C_COMMON_ALL_SRC
    ${AWS_C_COMMON_SRC}
    ${AWS_C_COMMON_EXTERNAL_SRC}
    ${AWS_C_COMMON_LIBCBOR_SRC}
    ${AWS_C_COMMON_POSIX_SRC}
    ${AWS_C_COMMON_LINUX_SRC}
    ${AWS_C_COMMON_ARCH_GENERIC_SRC}
)

# Platform fallback stubs for aws-c-common (Linux has direct_io; others need fallbacks)
file(GLOB AWS_C_COMMON_FALLBACK_SRC "${AWS_C_COMMON_DIR}/source/platform_fallback_stubs/*.c")
# system_info fallback is only for non-linux, file_direct_io fallback is only for non-linux
# On linux, these are provided by linux/ directory, so skip the fallbacks
# Actually include them and let the linker sort it out? No. On linux, these symbols come
# from linux/ dir. Let's be explicit:
# Don't include fallback stubs on linux since linux/ provides the real implementations.

# --- aws-checksums ---
set(AWS_CHECKSUMS_DIR "${AWS_CRT_CRT_DIR}/aws-checksums")
file(GLOB AWS_CHECKSUMS_SRC "${AWS_CHECKSUMS_DIR}/source/*.c")

# Checksums arch-specific: Intel
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
set(AWS_C_CAL_DIR "${AWS_CRT_CRT_DIR}/aws-c-cal")
file(GLOB AWS_C_CAL_SRC "${AWS_C_CAL_DIR}/source/*.c")
file(GLOB AWS_C_CAL_UNIX_SRC "${AWS_C_CAL_DIR}/source/unix/*.c")
file(GLOB AWS_C_CAL_SHARED_SRC "${AWS_C_CAL_DIR}/source/shared/*.c")
list(APPEND AWS_C_CAL_SRC ${AWS_C_CAL_UNIX_SRC} ${AWS_C_CAL_SHARED_SRC})

# --- aws-c-io ---
set(AWS_C_IO_DIR "${AWS_CRT_CRT_DIR}/aws-c-io")
file(GLOB AWS_C_IO_SRC "${AWS_C_IO_DIR}/source/*.c")
file(GLOB AWS_C_IO_POSIX_SRC "${AWS_C_IO_DIR}/source/posix/*.c")
file(GLOB AWS_C_IO_LINUX_SRC "${AWS_C_IO_DIR}/source/linux/*.c")
# Exclude s2n/ (we use BYO_CRYPTO)
list(APPEND AWS_C_IO_SRC ${AWS_C_IO_POSIX_SRC} ${AWS_C_IO_LINUX_SRC})

# --- aws-c-compression ---
set(AWS_C_COMPRESSION_DIR "${AWS_CRT_CRT_DIR}/aws-c-compression")
file(GLOB AWS_C_COMPRESSION_SRC "${AWS_C_COMPRESSION_DIR}/source/*.c")
# Exclude huffman_testing.c (test-only file)
list(FILTER AWS_C_COMPRESSION_SRC EXCLUDE REGEX "huffman_testing\\.c$")

# --- aws-c-http ---
set(AWS_C_HTTP_DIR "${AWS_CRT_CRT_DIR}/aws-c-http")
file(GLOB AWS_C_HTTP_SRC "${AWS_C_HTTP_DIR}/source/*.c")

# --- aws-c-sdkutils ---
set(AWS_C_SDKUTILS_DIR "${AWS_CRT_CRT_DIR}/aws-c-sdkutils")
file(GLOB AWS_C_SDKUTILS_SRC "${AWS_C_SDKUTILS_DIR}/source/*.c")

# --- aws-c-auth ---
set(AWS_C_AUTH_DIR "${AWS_CRT_CRT_DIR}/aws-c-auth")
file(GLOB AWS_C_AUTH_SRC "${AWS_C_AUTH_DIR}/source/*.c")

# --- aws-c-event-stream ---
set(AWS_C_EVENT_STREAM_DIR "${AWS_CRT_CRT_DIR}/aws-c-event-stream")
file(GLOB AWS_C_EVENT_STREAM_SRC "${AWS_C_EVENT_STREAM_DIR}/source/*.c")

# --- aws-c-s3 ---
set(AWS_C_S3_DIR "${AWS_CRT_CRT_DIR}/aws-c-s3")
file(GLOB AWS_C_S3_SRC "${AWS_C_S3_DIR}/source/*.c")
file(GLOB AWS_C_S3_ENDPOINT_RESOLVER_SRC "${AWS_C_S3_DIR}/source/s3_endpoint_resolver/*.c")
list(APPEND AWS_C_S3_SRC ${AWS_C_S3_ENDPOINT_RESOLVER_SRC})

# --- aws-c-mqtt (needed for aws-crt-cpp headers) ---
set(AWS_C_MQTT_DIR "${AWS_CRT_CRT_DIR}/aws-c-mqtt")
file(GLOB AWS_C_MQTT_SRC "${AWS_C_MQTT_DIR}/source/*.c")
file(GLOB AWS_C_MQTT_V5_SRC "${AWS_C_MQTT_DIR}/source/v5/*.c")
file(GLOB AWS_C_MQTT_RR_SRC "${AWS_C_MQTT_DIR}/source/request-response/*.c")
list(APPEND AWS_C_MQTT_SRC ${AWS_C_MQTT_V5_SRC} ${AWS_C_MQTT_RR_SRC})

# ===========================================================================
# Gather C++ sources from aws-crt-cpp
# ===========================================================================
file(GLOB AWS_CRT_CPP_SRC "${AWS_CRT_DIR}/source/*.cpp")
file(GLOB_RECURSE AWS_CRT_CPP_AUTH_SRC "${AWS_CRT_DIR}/source/auth/*.cpp")
file(GLOB_RECURSE AWS_CRT_CPP_CBOR_SRC "${AWS_CRT_DIR}/source/cbor/*.cpp")
file(GLOB_RECURSE AWS_CRT_CPP_CHECKSUM_SRC "${AWS_CRT_DIR}/source/checksum/*.cpp")
file(GLOB_RECURSE AWS_CRT_CPP_CRYPTO_SRC "${AWS_CRT_DIR}/source/crypto/*.cpp")
file(GLOB_RECURSE AWS_CRT_CPP_ENDPOINTS_SRC "${AWS_CRT_DIR}/source/endpoints/*.cpp")
file(GLOB_RECURSE AWS_CRT_CPP_HTTP_SRC "${AWS_CRT_DIR}/source/http/*.cpp")
file(GLOB_RECURSE AWS_CRT_CPP_IO_SRC "${AWS_CRT_DIR}/source/io/*.cpp")
file(GLOB_RECURSE AWS_CRT_CPP_IOT_SRC "${AWS_CRT_DIR}/source/iot/*.cpp")
file(GLOB_RECURSE AWS_CRT_CPP_MQTT_SRC "${AWS_CRT_DIR}/source/mqtt/*.cpp")

set(AWS_CRT_CPP_ALL_SRC
    ${AWS_CRT_CPP_SRC}
    ${AWS_CRT_CPP_AUTH_SRC}
    ${AWS_CRT_CPP_CBOR_SRC}
    ${AWS_CRT_CPP_CHECKSUM_SRC}
    ${AWS_CRT_CPP_CRYPTO_SRC}
    ${AWS_CRT_CPP_ENDPOINTS_SRC}
    ${AWS_CRT_CPP_HTTP_SRC}
    ${AWS_CRT_CPP_IO_SRC}
    ${AWS_CRT_CPP_IOT_SRC}
    ${AWS_CRT_CPP_MQTT_SRC}
)

# ===========================================================================
# Gather C++ sources from aws-cpp-sdk-core
# ===========================================================================
set(AWS_SDK_CORE_DIR "${AWS_SDK_DIR}/src/aws-cpp-sdk-core")

file(GLOB AWS_SDK_CORE_SRC "${AWS_SDK_CORE_DIR}/source/*.cpp")
file(GLOB_RECURSE AWS_SDK_CORE_AUTH_SRC "${AWS_SDK_CORE_DIR}/source/auth/*.cpp")
file(GLOB_RECURSE AWS_SDK_CORE_CLIENT_SRC "${AWS_SDK_CORE_DIR}/source/client/*.cpp")
file(GLOB_RECURSE AWS_SDK_CORE_CONFIG_SRC "${AWS_SDK_CORE_DIR}/source/config/*.cpp")
file(GLOB_RECURSE AWS_SDK_CORE_ENDPOINT_SRC "${AWS_SDK_CORE_DIR}/source/endpoint/*.cpp")
file(GLOB_RECURSE AWS_SDK_CORE_EXTERNAL_SRC "${AWS_SDK_CORE_DIR}/source/external/*.cpp")
file(GLOB_RECURSE AWS_SDK_CORE_INTERNAL_SRC "${AWS_SDK_CORE_DIR}/source/internal/*.cpp")
file(GLOB_RECURSE AWS_SDK_CORE_MONITORING_SRC "${AWS_SDK_CORE_DIR}/source/monitoring/*.cpp")
file(GLOB_RECURSE AWS_SDK_CORE_SMITHY_SRC "${AWS_SDK_CORE_DIR}/source/smithy/*.cpp")
file(GLOB_RECURSE AWS_SDK_CORE_UTILS_SRC "${AWS_SDK_CORE_DIR}/source/utils/*.cpp")

# HTTP sources: include curl + standard, exclude windows + CRT client
file(GLOB AWS_SDK_CORE_HTTP_TOP_SRC "${AWS_SDK_CORE_DIR}/source/http/*.cpp")
file(GLOB AWS_SDK_CORE_HTTP_CURL_SRC "${AWS_SDK_CORE_DIR}/source/http/curl/*.cpp")
file(GLOB AWS_SDK_CORE_HTTP_STANDARD_SRC "${AWS_SDK_CORE_DIR}/source/http/standard/*.cpp")
# Exclude: http/crt/ (CRTHttpClient) and http/windows/

# Net sources: linux-shared only
file(GLOB AWS_SDK_CORE_NET_TOP_SRC "${AWS_SDK_CORE_DIR}/source/net/*.cpp")
file(GLOB AWS_SDK_CORE_NET_LINUX_SRC "${AWS_SDK_CORE_DIR}/source/net/linux-shared/*.cpp")

# Platform sources: linux-shared only
file(GLOB AWS_SDK_CORE_PLATFORM_LINUX_SRC "${AWS_SDK_CORE_DIR}/source/platform/linux-shared/*.cpp")

# Remove OpenTelemetry impl files (need OpenTelemetry SDK, which we don't have)
set(AWS_SDK_CORE_SMITHY_SRC_FILTERED ${AWS_SDK_CORE_SMITHY_SRC})
list(FILTER AWS_SDK_CORE_SMITHY_SRC_FILTERED EXCLUDE REGEX "opentelemetry")

set(AWS_SDK_CORE_ALL_SRC
    ${AWS_SDK_CORE_SRC}
    ${AWS_SDK_CORE_AUTH_SRC}
    ${AWS_SDK_CORE_CLIENT_SRC}
    ${AWS_SDK_CORE_CONFIG_SRC}
    ${AWS_SDK_CORE_ENDPOINT_SRC}
    ${AWS_SDK_CORE_EXTERNAL_SRC}
    ${AWS_SDK_CORE_INTERNAL_SRC}
    ${AWS_SDK_CORE_MONITORING_SRC}
    ${AWS_SDK_CORE_SMITHY_SRC_FILTERED}
    ${AWS_SDK_CORE_UTILS_SRC}
    ${AWS_SDK_CORE_HTTP_TOP_SRC}
    ${AWS_SDK_CORE_HTTP_CURL_SRC}
    ${AWS_SDK_CORE_HTTP_STANDARD_SRC}
    ${AWS_SDK_CORE_NET_TOP_SRC}
    ${AWS_SDK_CORE_NET_LINUX_SRC}
    ${AWS_SDK_CORE_PLATFORM_LINUX_SRC}
)

# ===========================================================================
# Gather C++ sources from service SDKs
# ===========================================================================

# --- aws-cpp-sdk-s3 ---
file(GLOB_RECURSE AWS_SDK_S3_SRC
    "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-s3/source/*.cpp"
)

# --- aws-cpp-sdk-sts ---
file(GLOB_RECURSE AWS_SDK_STS_SRC
    "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-sts/source/*.cpp"
)

# --- aws-cpp-sdk-cognito-identity ---
file(GLOB_RECURSE AWS_SDK_COGNITO_SRC
    "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-cognito-identity/source/*.cpp"
)

# --- aws-cpp-sdk-identity-management ---
file(GLOB_RECURSE AWS_SDK_IDENTITY_MGMT_SRC
    "${AWS_SDK_DIR}/src/aws-cpp-sdk-identity-management/source/*.cpp"
)

# ===========================================================================
# Stubs
# ===========================================================================
set(AWS_STUBS_SRC "${CMAKE_CURRENT_SOURCE_DIR}/aws_stubs.cpp")

# ===========================================================================
# Build single static library
# ===========================================================================
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
    ${AWS_STUBS_SRC}
)

# ===========================================================================
# Include directories
# ===========================================================================
target_include_directories(_aws SYSTEM PUBLIC
    # Generated config headers
    "${AWS_COMMON_CONFIG_BINARY_DIR}"
    "${AWS_CRT_CONFIG_BINARY_DIR}"
    "${AWS_SDK_CONFIG_BINARY_DIR}"

    # CRT C library headers
    "${AWS_C_COMMON_DIR}/include"
    "${AWS_CRT_CRT_DIR}/aws-checksums/include"
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
    "${AWS_CRT_DIR}/include"

    # SDK core
    "${AWS_SDK_CORE_DIR}/include"

    # Service SDKs
    "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-s3/include"
    "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-sts/include"
    "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-cognito-identity/include"
    "${AWS_SDK_DIR}/src/aws-cpp-sdk-identity-management/include"
)

# Private include for libcbor headers
target_include_directories(_aws PRIVATE
    "${AWS_C_COMMON_DIR}/source/external/libcbor"
)

# ===========================================================================
# Compile definitions
# ===========================================================================
target_compile_definitions(_aws PUBLIC
    ENABLE_CURL_CLIENT
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

# Note: Do NOT define *_EXPORTS macros. On Linux they are unused (no DLL semantics)
# and they suppress explicit template instantiations needed by the SDK
# (e.g. DefaultEndpointProvider in S3EndpointProvider.cpp).

# ===========================================================================
# Compiler flags
# ===========================================================================
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

# ===========================================================================
# BYO_CRYPTO: only for aws-c-io (skip s2n) and aws-crt-cpp (TLS callbacks)
# Do NOT apply to aws-c-cal — it needs to register OpenSSL crypto directly.
# ===========================================================================
set_source_files_properties(${AWS_C_IO_SRC} PROPERTIES COMPILE_DEFINITIONS "BYO_CRYPTO")
set_source_files_properties(${AWS_CRT_CPP_ALL_SRC} PROPERTIES COMPILE_DEFINITIONS "BYO_CRYPTO")

# ===========================================================================
# Link dependencies
# ===========================================================================
target_link_libraries(_aws PUBLIC
    OpenSSL::Crypto
    OpenSSL::SSL
    CURL::libcurl
)

# ===========================================================================
# Thread affinity and thread name detection
# ===========================================================================
aws_set_thread_affinity_method(_aws)
aws_set_thread_name_method(_aws)
