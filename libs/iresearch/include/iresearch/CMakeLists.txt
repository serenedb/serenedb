# set version components
if(NOT IResearch_version_major)
    set(IResearch_version_major "1")
endif()
if(NOT IResearch_version_minor)
    set(IResearch_version_minor "3")
endif()
if(NOT IResearch_version_revision)
    set(IResearch_version_revision "0")
endif()
if(NOT IResearch_version_patch)
    set(IResearch_version_patch "0")
endif()

set(SIMD_LIBRARY_STATIC "simdcomp-static")
set(FASTTEXT_LIBRARY_STATIC "fasttext-static")

# calculate version
math(
    EXPR
    IResearch_int_version
    "(${IResearch_version_major} * 1000000) + (${IResearch_version_minor} * 10000) + (${IResearch_version_revision} * 100) + (${IResearch_version_patch} * 1)"
)
set(IResearch_version
    "${IResearch_version_major}.${IResearch_version_minor}.${IResearch_version_revision}.${IResearch_version_patch}"
)

if(MSVC)
    # disable "checked iterators" feature
    add_definitions(-D_SCL_SECURE_NO_WARNINGS)

    # set OS specific sources
    set(IResearch_core_os_specific_sources ./utils/mman_win32.cpp)

    # set OS specific headers
    set(IResearch_core_os_specific_headers ./utils/mman_win32.hpp)
else()
    if(
        NOT APPLE
        AND NOT (
            CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo"
            OR CMAKE_BUILD_TYPE STREQUAL "Debug"
        )
    )
        add_link_options(-s)
    endif()

    # set OS specific headers
    set(IResearch_core_os_specific_headers ./utils/mman_posix.hpp)
endif()

set(IResearch_core_sources
    ./utils/snowball_stemmer.cpp
    ./analysis/analyzers.cpp
    ./analysis/token_attributes.cpp
    ./analysis/tokenizers.cpp
    ./analysis/stemming_tokenizer.cpp
    ./analysis/stopwords_tokenizer.cpp
    ./analysis/ngram_tokenizer.cpp
    ./analysis/pipeline_tokenizer.cpp
    ./analysis/segmentation_tokenizer.cpp
    ./analysis/classification_tokenizer.cpp
    ./analysis/nearest_neighbors_tokenizer.cpp
    ./analysis/normalizing_tokenizer.cpp
    ./analysis/minhash_tokenizer.cpp
    ./analysis/collation_tokenizer.cpp
    ./analysis/text_tokenizer.cpp
    ./analysis/multi_delimited_tokenizer.cpp
    ./analysis/delimited_tokenizer.cpp
    ./analysis/solr_synonyms_tokenizer.cpp
    ./analysis/wordnet_synonyms_tokenizer.cpp
    ./error/error.cpp
    ./formats/columnstore2.cpp
    ./formats/formats.cpp
    ./formats/format_utils.cpp
    ./formats/skip_list.cpp
    ./formats/sparse_bitmap.cpp
    ./formats/formats_impl.cpp
    ./formats/formats_burst_trie.cpp
    ./formats/hnsw_index.cpp
    ./index/buffered_column.cpp
    ./index/directory_reader.cpp
    ./index/directory_reader_impl.cpp
    ./index/field_data.cpp
    ./index/field_meta.cpp
    ./index/file_names.cpp
    ./index/index_writer.cpp
    ./index/index_reader.cpp
    ./index/iterators.cpp
    ./index/merge_writer.cpp
    ./index/norm.cpp
    ./index/postings.cpp
    ./index/segment_reader.cpp
    ./index/segment_reader_impl.cpp
    ./index/segment_writer.cpp
    ./search/all_docs_provider.cpp
    ./search/all_filter.cpp
    ./search/all_iterator.cpp
    ./search/boost_scorer.cpp
    ./search/granular_range_filter.cpp
    ./search/scorers.cpp
    ./search/scorer.cpp
    ./search/score_function.cpp
    ./search/cost.cpp
    ./search/collectors.cpp
    ./search/score.cpp
    ./search/bitset_doc_iterator.cpp
    ./search/filter.cpp
    ./search/term_filter.cpp
    ./search/nested_filter.cpp
    ./search/terms_filter.cpp
    ./search/prefix_filter.cpp
    ./search/range_filter.cpp
    ./search/phrase_filter.cpp
    ./search/phrase_query.cpp
    ./search/column_existence_filter.cpp
    ./search/same_position_filter.cpp
    ./search/wildcard_filter.cpp
    ./search/levenshtein_filter.cpp
    ./search/multiterm_query.cpp
    ./search/term_query.cpp
    ./search/boolean_filter.cpp
    ./search/boolean_query.cpp
    ./search/ngram_similarity_filter.cpp
    ./search/ngram_similarity_query.cpp
    ./search/proxy_filter.cpp
    ./search/tfidf.cpp
    ./search/bm25.cpp
    ./store/data_input.cpp
    ./store/directory.cpp
    ./store/directory_attributes.cpp
    ./store/directory_cleaner.cpp
    ./store/fs_directory.cpp
    ./store/mmap_directory.cpp
    ./store/memory_directory.cpp
    ./store/store_utils.cpp
    ./utils/attributes.cpp
    ./utils/automaton_utils.cpp
    ./utils/encryption.cpp
    ./utils/ctr_encryption.cpp
    ./utils/compression.cpp
    ./utils/delta_compression.cpp
    ./utils/lz4compression.cpp
    ./utils/directory_utils.cpp
    ./utils/mmap_utils.cpp
    ./utils/index_utils.cpp
    ./utils/levenshtein_utils.cpp
    ./utils/wildcard_utils.cpp
    ./utils/levenshtein_default_pdp.cpp
    ./utils/timer_utils.cpp
    ./utils/numeric_utils.cpp
    ${IResearch_core_os_specific_sources}
    ${IResearch_core_optimized_sources}
)

if(uring_FOUND)
    list(APPEND IResearch_core_sources ./store/async_directory.cpp)
endif()

set(IRESEARCH_EXTERNAL_INCLUDES
    ${Boost_INCLUDE_DIRS} # ensure Boost paths take precedence over other system libraries as Boost may be defined elsewhere
    ${Fasttext_INCLUDE_DIR}
    ${FROZEN_INCLUDE_DIR}
)

include_directories(SYSTEM ${IRESEARCH_EXTERNAL_INCLUDES})

add_library(iresearch-static STATIC ${IResearch_core_sources})

target_include_directories(
    iresearch-static
    SYSTEM
    PUBLIC ${IResearch_INCLUDE_DIR}
    PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
    PUBLIC
        $<TARGET_PROPERTY:${FASTTEXT_LIBRARY_STATIC},INTERFACE_INCLUDE_DIRECTORIES>
    PRIVATE $<TARGET_PROPERTY:${FASTTEXT_LIBRARY_STATIC},INCLUDE_DIRECTORIES>
    PRIVATE ${SNOWBALL_INCLUDE_DIR}
)

# setup MSVC specific compiler flags
if(MSVC)
    target_compile_options(
        iresearch-static
        PRIVATE "$<$<CONFIG:Debug>:/bigobj>"
        PRIVATE "/EHa"
    )
endif()

target_link_libraries(
    iresearch-static
    PUBLIC icu
    PUBLIC absl::flat_hash_map
    PUBLIC absl::flat_hash_set
    PUBLIC absl::crc32c
    PUBLIC vpack
    PUBLIC iresearch-ofst
    PUBLIC ${FASTTEXT_LIBRARY_STATIC}
    PUBLIC ${SIMD_LIBRARY_STATIC}
    PUBLIC ${FAISS_LIB}
    PUBLIC re2
    PRIVATE lz4_static
    PRIVATE text
    PRIVATE stemmer-static
)

if(uring_FOUND)
    target_link_libraries(iresearch-static PRIVATE uring::uring)
endif()
