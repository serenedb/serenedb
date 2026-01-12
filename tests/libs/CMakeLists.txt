add_subdirectory(fuerte)
add_subdirectory(vpack)

# set test resource directory
set(IRS_TEST_RESOURCE_DIR ${CMAKE_SOURCE_DIR}/resources/tests/iresearch)

# generate tests_config.hpp here instead of inside 'iresearch' to make available externally
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/iresearch/tests_config.hpp.in"
    "${CMAKE_CURRENT_BINARY_DIR}/iresearch/tests_config.hpp"
)

add_subdirectory(iresearch)
add_test(NAME iresearch-tests COMMAND iresearch-tests)
