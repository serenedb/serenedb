add_executable(iresearch-example-basic basic.cpp)
target_link_libraries(iresearch-example-basic iresearch-static)

add_executable(iresearch-example-geo geo.cpp)
target_link_libraries(iresearch-example-geo iresearch-static)

add_executable(iresearch-example-text-filters text_filters.cpp)
target_link_libraries(iresearch-example-text-filters iresearch-static)

add_custom_target(
    iresearch-examples
    DEPENDS
        iresearch-example-basic
        iresearch-example-geo
        iresearch-example-text-filters
)
