function(sdb_assert_duckdb_core_only directory)
    get_property(_targets DIRECTORY ${directory} PROPERTY BUILDSYSTEM_TARGETS)
    foreach(_target IN LISTS _targets)
        get_property(
            _extension
            TARGET ${_target}
            PROPERTY DUCKDB_EXTENSION_KIND
            SET
        )
        if(_extension)
            message(
                FATAL_ERROR
                "Library-only mode configured DuckDB extension: ${_target}"
            )
        endif()
    endforeach()
    get_property(_children DIRECTORY ${directory} PROPERTY SUBDIRECTORIES)
    foreach(_child IN LISTS _children)
        sdb_assert_duckdb_core_only(${_child})
    endforeach()
endfunction()

sdb_assert_duckdb_core_only(${CMAKE_CURRENT_SOURCE_DIR}/duckdb)

function(sdb_build_iresearch_icu)
    set(_icu ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/extension/icu)
    add_library(
        iresearch-icu
        STATIC
        ${_icu}/collation/collation_collator.cpp
        ${_icu}/collation/collation_loader.cpp
        ${_icu}/collation/collation_normalizer.cpp
        ${_icu}/collation/generated/collation_data.cpp
        ${_icu}/properties/unicode_properties.cpp
        ${_icu}/properties/generated/property_data.cpp
        ${_icu}/text/text_break_iterator.cpp
        ${_icu}/text/text_casing.cpp
        ${_icu}/text/text_dictionary.cpp
        ${_icu}/text/text_locale.cpp
        ${_icu}/text/text_normalizer.cpp
        ${_icu}/text/text_transform.cpp
        ${_icu}/text/text_unit.cpp
        ${_icu}/text/generated/text_data.cpp
    )
    target_link_libraries(iresearch-icu PRIVATE zstd::zstd absl::node_hash_map)
    target_include_directories(
        iresearch-icu
        SYSTEM
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/src/include
            ${_icu}/include
            ${_icu}/collation/include
            ${_icu}/properties/include
            ${_icu}/text/include
    )
    target_link_libraries(sdb_icu INTERFACE iresearch-icu)
endfunction()

sdb_build_iresearch_icu()
