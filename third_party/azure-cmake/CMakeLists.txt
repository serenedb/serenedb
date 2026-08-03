# Builds the vendored Azure SDK for C++ into a single _azure_sdk static library

set(AZURE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../azure-sdk-for-cpp")
set(AZURE_SDK_LIBRARY_DIR "${AZURE_DIR}/sdk")

file(GLOB AZURE_SDK_SRC
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/credentials/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/cryptography/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/http/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/io/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/tracing/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/identity/azure-identity/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/src/private/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-common/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-files-datalake/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-files-datalake/src/private/*.cpp"
)

set(AZURE_SDK_INCLUDES
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/identity/azure-identity/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-common/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-files-datalake/inc/"
)

add_library(_azure_sdk STATIC ${AZURE_SDK_SRC})

# The transport is provided by duckdb_azure (see azure_duckdb_transport.cpp),
# resolved at final link via AzureSdkGetCustomHttpTransport.
target_compile_definitions(_azure_sdk PRIVATE BUILD_TRANSPORT_CUSTOM_ADAPTER)

target_include_directories(_azure_sdk SYSTEM BEFORE PUBLIC ${AZURE_SDK_INCLUDES})

target_link_libraries(_azure_sdk PRIVATE
    OpenSSL::Crypto
    OpenSSL::SSL
    _libxml2
)
