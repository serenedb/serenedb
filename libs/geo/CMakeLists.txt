add_library(
    serene_geo
    STATIC
    geo_json.cpp
    shape_container.cpp
    coding.cpp
    wkb.cpp
)

target_link_libraries(
    serene_geo
    geographiclib-c
    serene
    boost_boost
    s2
    simdjson
)

add_subdirectory(s2)
