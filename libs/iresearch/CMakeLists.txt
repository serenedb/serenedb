option(USE_URING "Build iresearch with uring support" OFF)

# fix strange issue with utf_8_to_32_iterator failing
# to follow std::bidirectional_iterator concept
add_definitions(-DBOOST_TEXT_DISABLE_CONCEPTS)

if(uring_FOUND)
    add_definitions(-DIRESEARCH_URING)
endif()

set(IResearch_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

add_definitions(-DUNICODE -D_UNICODE)

add_subdirectory(include/iresearch)

option(IRESEARCH_BUILD_EXAMPLES "Build iresearch examples" OFF)
if(IRESEARCH_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
