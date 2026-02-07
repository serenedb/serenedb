add_library(
    serene_geo
    STATIC
    geo_json.cpp
    geo_params.cpp
    shape_container.cpp
    utils.cpp
    coding.cpp
)

target_link_libraries(serene_geo geographiclib-c serene boost_boost s2)

add_subdirectory(s2)
