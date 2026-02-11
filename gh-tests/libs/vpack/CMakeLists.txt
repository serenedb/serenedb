add_subdirectory(tools)

set(Tests
    buffer
    builder
    collection
    common
    compare
    dumper
    example
    exception
    files
    hex_dump
    iterator
    lookup
    parser
    shared_slice
    sink
    slice
    type
    validator
    large
)

add_custom_target(vpack-tests)

foreach(testName ${Tests})
    add_executable(vpack-tests-${testName} tests_${testName}.cpp)
    target_link_libraries(vpack-tests-${testName} gtest_main vpack)
    add_dependencies(vpack-tests vpack-tests-${testName})
    add_test(NAME vpack-tests-${testName} COMMAND vpack-tests-${testName})
endforeach()

add_executable(
    vpack-tests-asm
    "${CMAKE_SOURCE_DIR}/libs/vpack/src/asm-functions.cpp"
)
target_compile_definitions(vpack-tests-asm PRIVATE COMPILE_VPACK_ASM_UNITTESTS)
target_link_libraries(vpack-tests-asm vpack)
add_dependencies(vpack-tests vpack-tests-asm)
add_test(NAME vpack-tests-asm COMMAND vpack-tests-asm 300 10000000 100)

add_dependencies(vpack-tests vpack_to_json vpack_validate vpack_fuzzer)

add_test(
    NAME vpack_fuzzer_vpack_good
    COMMAND vpack_fuzzer --vpack --iterations 10000 --threads 4
)
add_test(
    NAME vpack_fuzzer_vpack_evil
    COMMAND vpack_fuzzer --vpack --iterations 10000 --threads 4
)
add_test(
    NAME vpack_fuzzer_json_good
    COMMAND vpack_fuzzer --json --iterations 1000 --threads 4
)
