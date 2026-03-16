add_executable(ann-benchmark-profile main.cpp)
target_link_libraries(ann-benchmark-profile iresearch-static simdjson serene_base)

add_executable(ann-benchmark main.cpp)
target_link_libraries(ann-benchmark iresearch-static simdjson serene_base)
