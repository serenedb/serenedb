cmake_minimum_required(VERSION 3.24)
project(sdb_ivf_cuda VERSION 1.0.0 LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_STANDARD 20)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(CUDAToolkit REQUIRED)

add_library(sdb::blas STATIC IMPORTED)
set_target_properties(
    sdb::blas
    PROPERTIES IMPORTED_LOCATION "${OPENBLAS_ARCHIVE}"
)

set(FAISS_ENABLE_GPU ON)
set(FAISS_ENABLE_CUVS OFF)
set(FAISS_ENABLE_ROCM OFF)
set(FAISS_ENABLE_METAL OFF)
set(FAISS_ENABLE_PYTHON OFF)
set(FAISS_ENABLE_EXTRAS OFF)
set(BUILD_SHARED_LIBS OFF)
set(BUILD_TESTING OFF)
include_directories(SYSTEM "${PCG_INCLUDE_DIR}")
add_subdirectory("${FAISS_SOURCE_DIR}" faiss)

set_property(
    SOURCE
        "${FAISS_SOURCE_DIR}/faiss/gpu/GpuIndex.cu"
        TARGET_DIRECTORY
        faiss_gpu_objs
    APPEND
    PROPERTY COMPILE_OPTIONS -Xcompiler=-ffunction-sections
)

add_library(sdb_ivf_cuda SHARED bridge.cpp)

if(FAISS_GPU_STATIC)
    target_link_libraries(
        sdb_ivf_cuda
        PRIVATE faiss CUDA::cudart_static CUDA::cublas_static
    )
else()
    target_link_libraries(sdb_ivf_cuda PRIVATE faiss CUDA::cudart CUDA::cublas)
endif()

set(exports "${CMAKE_CURRENT_SOURCE_DIR}/exports.map")
target_link_options(
    sdb_ivf_cuda
    PRIVATE
        "LINKER:--gc-sections"
        "LINKER:--version-script,${exports}"
        "LINKER:-z,defs"
)
set_property(TARGET sdb_ivf_cuda APPEND PROPERTY LINK_DEPENDS "${exports}")
