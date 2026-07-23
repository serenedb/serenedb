add_library(serene_build_id STATIC src/build_id.cpp)

target_include_directories(serene_build_id PUBLIC include)

if(NOT APPLE)
    target_link_options(
        serene_build_id
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/BuildId.ld -Wl,--build-id=sha1
    )
endif()
