target_sources(
    serene_connector
    PRIVATE
        iresearch_plan.cpp
        rocksdb_plan.cpp
        serene_filter_rules.cpp
        wrap_unsupported_types.cpp
)
