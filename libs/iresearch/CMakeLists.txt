option(USE_URING "Build iresearch with uring support" OFF)

if(uring_FOUND)
    add_definitions(-DIRESEARCH_URING)
endif()

set(IResearch_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

add_definitions(-DUNICODE -D_UNICODE)

add_subdirectory(include/iresearch)

add_subdirectory(examples)
