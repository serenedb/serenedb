add_executable(vpack_to_json vpack-to-json.cpp)
target_link_libraries(vpack_to_json vpack)

add_executable(vpack_validate vpack-validate.cpp)
target_link_libraries(vpack_validate vpack)

add_executable(vpack_fuzzer fuzzer.cpp)
target_link_libraries(vpack_fuzzer PRIVATE vpack absl::random_random)
