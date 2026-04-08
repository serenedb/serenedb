add_compile_options(-Wno-error)

nanobind_add_module(pyiresearch NB_STATIC MUSL_DYNAMIC_LIBCPP pyiresearch.cpp)
target_include_directories(pyiresearch
    PRIVATE
        ${CMAKE_SOURCE_DIR}/libs
)
target_link_libraries(pyiresearch
    PRIVATE
        iresearch-static
)
