cmake_minimum_required(VERSION 3.30)
project(IResearchConsumer LANGUAGES CXX)

find_package(IResearch CONFIG REQUIRED)

add_executable(consumer consumer.cpp)
target_link_libraries(consumer PRIVATE iresearch::iresearch)

add_library(consumer-shared SHARED shared.cpp)
target_link_libraries(consumer-shared PRIVATE iresearch::iresearch)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_options(consumer-shared PRIVATE LINKER:--no-undefined)
endif()

enable_testing()
add_test(NAME iresearch-library-consumer COMMAND consumer)
