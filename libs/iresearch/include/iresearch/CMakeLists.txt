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
    ./analysis/token_attributes.cpp
    ./analysis/tokenizers.cpp
    ./analysis/stemming_tokenizer.cpp
    ./analysis/stopwords_tokenizer.cpp
    ./analysis/ngram_tokenizer.cpp
    ./analysis/sparse_ngram_tokenizer.cpp
    ./analysis/pipeline_tokenizer.cpp
    ./analysis/union_tokenizer.cpp
    ./analysis/segmentation_tokenizer.cpp
    ./analysis/classification_tokenizer.cpp
    ./analysis/nearest_neighbors_tokenizer.cpp
    ./analysis/normalizing_tokenizer.cpp
    ./analysis/minhash_tokenizer.cpp
    ./analysis/collation_tokenizer.cpp
    ./analysis/text_tokenizer.cpp
    ./analysis/multi_delimited_tokenizer.cpp
    ./analysis/delimited_tokenizer.cpp
    ./analysis/path_hierarchy_tokenizer.cpp
    ./analysis/solr_synonyms_tokenizer.cpp
    ./analysis/wordnet_synonyms_tokenizer.cpp
    ./analysis/fast_text_model.cpp
    ./analysis/geo_analyzer.cpp
    ./analysis/pattern_tokenizer.cpp
    ./analysis/wildcard_analyzer.cpp
    ./error/error.cpp
    ./formats/column/col_reader.cpp
    ./formats/column/col_writer.cpp
    ./formats/column/column_writer.cpp
    ./formats/column/column_reader.cpp
    ./formats/column/variant_column_reader.cpp
    ./formats/column/norm_writer.cpp
    ./formats/column/norm_column_reader.cpp
    ./formats/column/merge.cpp
    ./formats/column/internal/write_context.cpp
    ./formats/column/read_context.cpp
    ./formats/formats.cpp
    ./formats/format_utils.cpp
    ./formats/posting/skip_list.cpp
    ./formats/format_block_128.cpp
    ./formats/index/burst_trie.cpp
    ./formats/index/idx_reader.cpp
    ./formats/index/idx_writer.cpp
    ./formats/ivf/centroids.cpp
    ./formats/ivf/clustering.cpp
    ./formats/ivf/ivf_writer.cpp
    ./formats/ivf/ivf_reader.cpp
    ./formats/ivf/quantizer.cpp
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
    ./index/hit_batcher.cpp
    ./index/table_filter_iterator.cpp
    ./index/column_extract.cpp
    ./index/postings.cpp
    ./index/segment_reader.cpp
    ./index/segment_reader_impl.cpp
    ./index/segment_writer.cpp
    ./search/all_filter.cpp
    ./search/granular_range_filter.cpp
    ./search/score_function.cpp
    ./search/common/score/make_conjunction.cpp
    ./search/common/score/make_probe.cpp
    ./search/common/score/make_window.cpp
    ./search/common/score/root_window.cpp
    ./search/collectors.cpp
    ./search/filter.cpp
    ./search/term_filter.cpp
    ./search/nested_filter.cpp
    ./search/term_set.cpp
    ./search/prefix_filter.cpp
    ./search/range_filter.cpp
    ./search/phrase_filter.cpp
    ./search/automaton_filter.cpp
    ./search/wildcard_filter.cpp
    ./search/regexp_filter.cpp
    ./search/wildcard_ngram_filter.cpp
    ./search/levenshtein_filter.cpp
    ./search/count/make.cpp
    ./search/count/make_constant.cpp
    ./search/count/make_bitset_disjunction.cpp
    ./search/count/make_window_disjunction.cpp
    ./search/count/make_bitset_conjunction.cpp
    ./search/count/make_window_conjunction.cpp
    ./search/count/make_sparse_conjunction.cpp
    ./search/count/make_bitset_exclusion.cpp
    ./search/count/make_window_exclusion.cpp
    ./search/count/make_sparse_exclusion.cpp
    ./search/count/make_subtract_conjunction.cpp
    ./search/count/make_subtract_disjunction.cpp
    ./search/count/make_bits_threshold.cpp
    ./search/count/make_count_threshold.cpp
    ./search/count/make_fixed_phrase.cpp
    ./search/count/make_fixed_phrase_intervals.cpp
    ./search/count/make_fixed_phrase_slop.cpp
    ./search/count/make_variadic_phrase.cpp
    ./search/count/make_variadic_phrase_intervals.cpp
    ./search/count/make_variadic_phrase_slop.cpp
    ./search/count/make_ngram.cpp
    ./search/count/make_ngram_all.cpp
    ./search/count/make_wildcard_ngram.cpp
    ./search/count/make_geo_intersects.cpp
    ./search/count/make_geo_contains.cpp
    ./search/count/make_geo_is_contained.cpp
    ./search/count/make_geo_distance.cpp
    ./search/count/make_masked.cpp
    ./search/count/make_term_counts.cpp
    ./search/lead/make.cpp
    ./search/lead/make_posting_docs.cpp
    ./search/lead/make_posting_scored.cpp
    ./search/lead/make_all_docs.cpp
    ./search/lead/make_all_scored.cpp
    ./search/lead/make_bitset_disjunction_docs.cpp
    ./search/lead/make_window_disjunction_docs.cpp
    ./search/lead/make_window_disjunction_scored.cpp
    ./search/lead/make_bitset_conjunction_docs.cpp
    ./search/lead/make_window_conjunction_docs.cpp
    ./search/lead/make_sparse_conjunction_docs.cpp
    ./search/lead/make_sparse_conjunction_scored.cpp
    ./search/lead/make_bitset_exclusion_docs.cpp
    ./search/lead/make_window_exclusion_docs.cpp
    ./search/lead/make_sparse_exclusion_docs.cpp
    ./search/lead/make_sparse_exclusion_scored.cpp
    ./search/lead/make_bits_threshold_docs.cpp
    ./search/lead/make_bits_threshold_scored.cpp
    ./search/lead/make_count_threshold_docs.cpp
    ./search/lead/make_count_threshold_scored.cpp
    ./search/lead/make_sparse_boost_scored.cpp
    ./search/lead/make_fixed_phrase_docs.cpp
    ./search/lead/make_fixed_phrase_scored.cpp
    ./search/lead/make_fixed_phrase_intervals_docs.cpp
    ./search/lead/make_fixed_phrase_intervals_scored.cpp
    ./search/lead/make_fixed_phrase_slop_docs.cpp
    ./search/lead/make_fixed_phrase_slop_scored.cpp
    ./search/lead/make_variadic_phrase_docs.cpp
    ./search/lead/make_variadic_phrase_scored.cpp
    ./search/lead/make_variadic_phrase_intervals_docs.cpp
    ./search/lead/make_variadic_phrase_intervals_scored.cpp
    ./search/lead/make_variadic_phrase_slop_docs.cpp
    ./search/lead/make_variadic_phrase_slop_scored.cpp
    ./search/lead/make_ngram_docs.cpp
    ./search/lead/make_ngram_scored.cpp
    ./search/lead/make_ngram_all_docs.cpp
    ./search/lead/make_ngram_all_scored.cpp
    ./search/lead/make_wildcard_ngram_docs.cpp
    ./search/lead/make_wildcard_ngram_scored.cpp
    ./search/lead/make_geo_intersects_docs.cpp
    ./search/lead/make_geo_intersects_scored.cpp
    ./search/lead/make_geo_contains_docs.cpp
    ./search/lead/make_geo_contains_scored.cpp
    ./search/lead/make_geo_is_contained_docs.cpp
    ./search/lead/make_geo_is_contained_scored.cpp
    ./search/lead/make_geo_distance_docs.cpp
    ./search/lead/make_geo_distance_scored.cpp
    ./search/offsets/make.cpp
    ./search/offsets/make_posting.cpp
    ./search/offsets/make_fixed_phrase.cpp
    ./search/offsets/make_fixed_phrase_intervals.cpp
    ./search/offsets/make_fixed_phrase_slop.cpp
    ./search/offsets/make_variadic_phrase.cpp
    ./search/offsets/make_variadic_phrase_intervals.cpp
    ./search/offsets/make_variadic_phrase_slop.cpp
    ./search/offsets/make_ngram.cpp
    ./search/offsets/make_ngram_all.cpp
    ./search/probe/make.cpp
    ./search/probe/make_posting_docs.cpp
    ./search/probe/make_posting_scored.cpp
    ./search/probe/make_single_posting_scored.cpp
    ./search/probe/make_all_docs.cpp
    ./search/probe/make_all_scored.cpp
    ./search/probe/make_bitset_disjunction_docs.cpp
    ./search/probe/make_sparse_disjunction_scored.cpp
    ./search/probe/make_sparse_conjunction_docs.cpp
    ./search/probe/make_sparse_conjunction_scored.cpp
    ./search/probe/make_sparse_threshold_docs.cpp
    ./search/probe/make_sparse_threshold_scored.cpp
    ./search/probe/make_sparse_exclusion_docs.cpp
    ./search/probe/make_sparse_exclusion_scored.cpp
    ./search/probe/make_sparse_boost_scored.cpp
    ./search/probe/make_fixed_phrase_docs.cpp
    ./search/probe/make_fixed_phrase_scored.cpp
    ./search/probe/make_fixed_phrase_intervals_docs.cpp
    ./search/probe/make_fixed_phrase_intervals_scored.cpp
    ./search/probe/make_fixed_phrase_slop_docs.cpp
    ./search/probe/make_fixed_phrase_slop_scored.cpp
    ./search/probe/make_variadic_phrase_docs.cpp
    ./search/probe/make_variadic_phrase_scored.cpp
    ./search/probe/make_variadic_phrase_intervals_docs.cpp
    ./search/probe/make_variadic_phrase_intervals_scored.cpp
    ./search/probe/make_variadic_phrase_slop_docs.cpp
    ./search/probe/make_variadic_phrase_slop_scored.cpp
    ./search/probe/make_ngram_docs.cpp
    ./search/probe/make_ngram_scored.cpp
    ./search/probe/make_ngram_all_docs.cpp
    ./search/probe/make_ngram_all_scored.cpp
    ./search/probe/make_wildcard_ngram_docs.cpp
    ./search/probe/make_wildcard_ngram_scored.cpp
    ./search/probe/make_geo_intersects_docs.cpp
    ./search/probe/make_geo_intersects_scored.cpp
    ./search/probe/make_geo_contains_docs.cpp
    ./search/probe/make_geo_contains_scored.cpp
    ./search/probe/make_geo_is_contained_docs.cpp
    ./search/probe/make_geo_is_contained_scored.cpp
    ./search/probe/make_geo_distance_docs.cpp
    ./search/probe/make_geo_distance_scored.cpp
    ./search/fill/make.cpp
    ./search/fill/make_posting_docs.cpp
    ./search/fill/make_posting_scored.cpp
    ./search/fill/make_single_posting_docs.cpp
    ./search/fill/make_single_posting_scored.cpp
    ./search/fill/make_all_docs.cpp
    ./search/fill/make_all_scored.cpp
    ./search/fill/make_bitset_disjunction_docs.cpp
    ./search/fill/make_window_disjunction_docs.cpp
    ./search/fill/make_window_disjunction_scored.cpp
    ./search/fill/make_bitset_conjunction_docs.cpp
    ./search/fill/make_window_conjunction_docs.cpp
    ./search/fill/make_sparse_conjunction_docs.cpp
    ./search/fill/make_sparse_conjunction_scored.cpp
    ./search/fill/make_bitset_exclusion_docs.cpp
    ./search/fill/make_window_exclusion_docs.cpp
    ./search/fill/make_sparse_exclusion_docs.cpp
    ./search/fill/make_sparse_exclusion_scored.cpp
    ./search/fill/make_bits_threshold_docs.cpp
    ./search/fill/make_bits_threshold_scored.cpp
    ./search/fill/make_count_threshold_docs.cpp
    ./search/fill/make_count_threshold_scored.cpp
    ./search/fill/make_sparse_boost_scored.cpp
    ./search/fill/make_fixed_phrase_docs.cpp
    ./search/fill/make_fixed_phrase_scored.cpp
    ./search/fill/make_fixed_phrase_intervals_docs.cpp
    ./search/fill/make_fixed_phrase_intervals_scored.cpp
    ./search/fill/make_fixed_phrase_slop_docs.cpp
    ./search/fill/make_fixed_phrase_slop_scored.cpp
    ./search/fill/make_variadic_phrase_docs.cpp
    ./search/fill/make_variadic_phrase_scored.cpp
    ./search/fill/make_variadic_phrase_intervals_docs.cpp
    ./search/fill/make_variadic_phrase_intervals_scored.cpp
    ./search/fill/make_variadic_phrase_slop_docs.cpp
    ./search/fill/make_variadic_phrase_slop_scored.cpp
    ./search/fill/make_ngram_docs.cpp
    ./search/fill/make_ngram_scored.cpp
    ./search/fill/make_ngram_all_docs.cpp
    ./search/fill/make_ngram_all_scored.cpp
    ./search/fill/make_wildcard_ngram_docs.cpp
    ./search/fill/make_wildcard_ngram_scored.cpp
    ./search/fill/make_geo_intersects_docs.cpp
    ./search/fill/make_geo_intersects_scored.cpp
    ./search/fill/make_geo_contains_docs.cpp
    ./search/fill/make_geo_contains_scored.cpp
    ./search/fill/make_geo_is_contained_docs.cpp
    ./search/fill/make_geo_is_contained_scored.cpp
    ./search/fill/make_geo_distance_docs.cpp
    ./search/fill/make_geo_distance_scored.cpp
    ./search/docs/make.cpp
    ./search/docs/make_posting.cpp
    ./search/docs/make_single_posting.cpp
    ./search/docs/make_all.cpp
    ./search/docs/make_bitset_disjunction.cpp
    ./search/docs/make_window_disjunction.cpp
    ./search/docs/make_bitset_conjunction.cpp
    ./search/docs/make_window_conjunction.cpp
    ./search/docs/make_sparse_conjunction.cpp
    ./search/docs/make_bitset_exclusion.cpp
    ./search/docs/make_window_exclusion.cpp
    ./search/docs/make_sparse_exclusion.cpp
    ./search/docs/make_bits_threshold.cpp
    ./search/docs/make_count_threshold.cpp
    ./search/docs/make_fixed_phrase.cpp
    ./search/docs/make_fixed_phrase_intervals.cpp
    ./search/docs/make_fixed_phrase_slop.cpp
    ./search/docs/make_variadic_phrase.cpp
    ./search/docs/make_variadic_phrase_intervals.cpp
    ./search/docs/make_variadic_phrase_slop.cpp
    ./search/docs/make_ngram.cpp
    ./search/docs/make_ngram_all.cpp
    ./search/docs/make_wildcard_ngram.cpp
    ./search/docs/make_geo_intersects.cpp
    ./search/docs/make_geo_contains.cpp
    ./search/docs/make_geo_is_contained.cpp
    ./search/docs/make_geo_distance.cpp
    ./search/scored/make.cpp
    ./search/scored/make_posting.cpp
    ./search/scored/make_single_posting.cpp
    ./search/scored/make_all.cpp
    ./search/scored/make_window_disjunction.cpp
    ./search/scored/make_sparse_conjunction.cpp
    ./search/scored/make_sparse_exclusion.cpp
    ./search/scored/make_window_exclusion.cpp
    ./search/scored/make_bits_threshold.cpp
    ./search/scored/make_count_threshold.cpp
    ./search/scored/make_fixed_phrase.cpp
    ./search/scored/make_fixed_phrase_intervals.cpp
    ./search/scored/make_fixed_phrase_slop.cpp
    ./search/scored/make_variadic_phrase.cpp
    ./search/scored/make_variadic_phrase_intervals.cpp
    ./search/scored/make_variadic_phrase_slop.cpp
    ./search/scored/make_ngram.cpp
    ./search/scored/make_ngram_all.cpp
    ./search/scored/make_wildcard_ngram.cpp
    ./search/scored/make_geo_intersects.cpp
    ./search/scored/make_geo_contains.cpp
    ./search/scored/make_geo_is_contained.cpp
    ./search/scored/make_geo_distance.cpp
    ./search/top/make.cpp
    ./search/top/make_posting.cpp
    ./search/top/make_single_posting.cpp
    ./search/top/make_all.cpp
    ./search/top/make_window_disjunction.cpp
    ./search/top/make_sparse_conjunction.cpp
    ./search/top/make_sparse_exclusion.cpp
    ./search/top/make_window_exclusion.cpp
    ./search/top/make_bits_threshold.cpp
    ./search/top/make_count_threshold.cpp
    ./search/top/make_fixed_phrase.cpp
    ./search/top/make_fixed_phrase_intervals.cpp
    ./search/top/make_fixed_phrase_slop.cpp
    ./search/top/make_variadic_phrase.cpp
    ./search/top/make_variadic_phrase_intervals.cpp
    ./search/top/make_variadic_phrase_slop.cpp
    ./search/top/make_ngram.cpp
    ./search/top/make_ngram_all.cpp
    ./search/top/make_wildcard_ngram.cpp
    ./search/top/make_geo_intersects.cpp
    ./search/top/make_geo_contains.cpp
    ./search/top/make_geo_is_contained.cpp
    ./search/top/make_geo_distance.cpp
    ./search/top/make_masked.cpp
    ./search/top/make_pruned_posting.cpp
    ./search/top/make_fixed_phrase_pruned.cpp
    ./search/top/make_fixed_phrase_intervals_pruned.cpp
    ./search/top/make_max_score_disjunction.cpp
    ./search/top/make_wand_conjunction.cpp
    ./search/multiterm_query.cpp
    ./search/term_query.cpp
    ./search/boolean_filter.cpp
    ./search/boolean_query.cpp
    ./search/filter_optimizer.cpp
    ./search/optimizer/boolean_rules.cpp
    ./search/optimizer/range_rules.cpp
    ./search/optimizer/lowering_rules.cpp
    ./search/optimizer/levenshtein_prefix_rules.cpp
    ./search/ngram_similarity_filter.cpp
    ./search/vector_similarity_filter.cpp
    ./search/vector_similarity_query.cpp
    ./search/vector_similarity_scorer.cpp
    ./search/vector_radius_filter.cpp
    ./search/geo_filter.cpp
    ./search/tfidf.cpp
    ./search/bm25.cpp
    ./search/lm_jelinek_mercer.cpp
    ./search/lm_dirichlet.cpp
    ./search/indri_dirichlet.cpp
    ./search/dfi.cpp
    ./search/idf.cpp
    ./search/raw_boost.cpp
    ./search/constant_score.cpp
    ./search/raw_tf.cpp
    ./search/raw_dl.cpp
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
    ./utils/directory_utils.cpp
    ./utils/mmap_utils.cpp
    ./utils/index_utils.cpp
    ./utils/levenshtein_utils.cpp
    ./utils/wildcard_utils.cpp
    ./utils/regexp_utils.cpp
    ./utils/levenshtein_default_pdp.cpp
    ./utils/timer_utils.cpp
    ./utils/numeric_utils.cpp
    ./parser/lucene_parser.cpp
    ./parser/lucene_lexer.cpp
    ${IResearch_core_os_specific_sources}
    ${IResearch_core_optimized_sources}
)

find_package(FLEX)
find_package(BISON)

set_directory_properties(PROPERTIES CLEAN_NO_CUSTOM "On")

if(FLEX_FOUND AND BISON_FOUND)
    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/parser/lucene_lexer.cpp
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND
            ${CMAKE_SOURCE_DIR}/scripts/generate_c.sh ${FLEX_EXECUTABLE}
            libs/iresearch/include/iresearch/parser/lucene_lexer.cpp
            libs/iresearch/include/iresearch/parser/lucene_lexer.l
        MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/parser/lucene_lexer.l
        VERBATIM
    )

    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/parser/lucene_parser.cpp
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND
            ${CMAKE_SOURCE_DIR}/scripts/generate_c.sh ${BISON_EXECUTABLE}
            libs/iresearch/include/iresearch/parser/lucene_parser.cpp
            libs/iresearch/include/iresearch/parser/lucene_parser.y
        MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/parser/lucene_parser.y
        VERBATIM
    )
else()
    set(_missing_tools "")
    if(NOT FLEX_FOUND)
        list(APPEND _missing_tools "FLEX")
    endif()
    if(NOT BISON_FOUND)
        list(APPEND _missing_tools "BISON")
    endif()
    string(JOIN " and " _missing_tools_str ${_missing_tools})
    message(
        STATUS
        "${_missing_tools_str} not found; skipping grammar generation and using pre-generated parser/lucene_lexer.cpp and parser/lucene_parser.cpp"
    )
    unset(_missing_tools)
    unset(_missing_tools_str)
endif()

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

set_source_files_properties(
    search/bm25.cpp
    search/tfidf.cpp
    PROPERTIES
        COMPILE_OPTIONS
            "-fapprox-func -fno-math-errno -fassociative-math -freciprocal-math -fno-signed-zeros -fno-trapping-math -fno-rounding-math -ffp-contract=fast"
)

target_include_directories(
    iresearch-static
    SYSTEM
    PUBLIC ${IResearch_INCLUDE_DIR}
    PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
    PUBLIC
        $<TARGET_PROPERTY:${FASTTEXT_LIBRARY_STATIC},INTERFACE_INCLUDE_DIRECTORIES>
    PRIVATE $<TARGET_PROPERTY:${FASTTEXT_LIBRARY_STATIC},INCLUDE_DIRECTORIES>
    PRIVATE ${SNOWBALL_INCLUDE_DIR}
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/duckdb/third_party/fastpforlib
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/duckdb/third_party/utf8proc/include
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
    PUBLIC serene_base
    PUBLIC simdjson
    PUBLIC iresearch-ofst
    PUBLIC ${FASTTEXT_LIBRARY_STATIC}
    PUBLIC ${SIMD_LIBRARY_STATIC}
    PUBLIC ${FAISS_LIB}
    PUBLIC re2
    PUBLIC serene_geo
    PUBLIC streamvbyte
    PUBLIC duckdb_static
    PRIVATE lz4_static
    PRIVATE text
    PRIVATE stemmer-static
)

if(uring_FOUND)
    target_link_libraries(iresearch-static PRIVATE uring::uring)
endif()
