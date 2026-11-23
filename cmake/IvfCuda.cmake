include(ExternalProject)

find_package(CUDAToolkit REQUIRED)
find_program(SDB_IVF_CUDA_CXX NAMES g++ HINTS /usr/bin REQUIRED)

set(ivf_cuda_binary_dir "${CMAKE_BINARY_DIR}/ivf-cuda")
set(ivf_cuda_library "${ivf_cuda_binary_dir}/libsdb_ivf_cuda.so")
set(ivf_cuda_architecture_arg)

if(CMAKE_CUDA_ARCHITECTURES)
    string(REPLACE ";" "|" ivf_cuda_architectures "${CMAKE_CUDA_ARCHITECTURES}")
    set(ivf_cuda_architecture_arg
        "-DCMAKE_CUDA_ARCHITECTURES=${ivf_cuda_architectures}"
    )
endif()

ExternalProject_Add(
    sdb_ivf_cuda_build
    SOURCE_DIR "${CMAKE_SOURCE_DIR}/iresearch/formats/ivf/cuda"
    BINARY_DIR "${ivf_cuda_binary_dir}"
    LIST_SEPARATOR "|"
    CMAKE_ARGS
        "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
        "-DCMAKE_CXX_COMPILER=${SDB_IVF_CUDA_CXX}"
        "-DCMAKE_CUDA_COMPILER=${CUDAToolkit_NVCC_EXECUTABLE}"
        "-DCMAKE_CUDA_HOST_COMPILER=${SDB_IVF_CUDA_CXX}"
        "-DFAISS_GPU_STATIC=${SDB_STATIC}" ${ivf_cuda_architecture_arg}
        "-DCUDAToolkit_ROOT=${CUDAToolkit_ROOT}"
        "-DFAISS_SOURCE_DIR=${CMAKE_SOURCE_DIR}/third_party/faiss"
        "-DPCG_INCLUDE_DIR=${CMAKE_SOURCE_DIR}/third_party/duckdb/third_party/pcg"
        "-DOPENBLAS_ARCHIVE=$<TARGET_FILE:openblas>"
    BUILD_ALWAYS ON
    BUILD_BYPRODUCTS "${ivf_cuda_library}"
    INSTALL_COMMAND ""
    DEPENDS openblas
)

add_library(sdb_ivf_cuda SHARED IMPORTED GLOBAL)
set_target_properties(
    sdb_ivf_cuda
    PROPERTIES IMPORTED_LOCATION "${ivf_cuda_library}"
)
add_dependencies(sdb_ivf_cuda sdb_ivf_cuda_build)
install(FILES "${ivf_cuda_library}" DESTINATION "${CMAKE_INSTALL_LIBDIR}")
