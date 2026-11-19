target_sources(
    iresearch
    PRIVATE
        count/make.cpp
        count/make_boolean.cpp
        count/make_boolean_negation.cpp
        count/make_constant.cpp
        count/make_subtract_conjunction.cpp
        count/make_subtract_disjunction.cpp
        count/make_term_counts.cpp
        detail/collectors.cpp
        detail/compiled_phrase.cpp
        detail/pattern_cache.cpp
        detail/row_values.cpp
        detail/term_acceptor.cpp
        detail/token_phrase.cpp
        docs/make.cpp
        docs/make_all.cpp
        docs/make_posting.cpp
        fill/make_all_docs.cpp
        fill/make_all_scored.cpp
        fill/make_posting.cpp
        fill/make_root.cpp
        filters/all_filter.cpp
        filters/automaton_filter.cpp
        filters/boolean_filter.cpp
        filters/boolean_rules.cpp
        filters/deferral.cpp
        filters/filter.cpp
        filters/filter_optimizer.cpp
        filters/granular_range_filter.cpp
        filters/levenshtein_filter.cpp
        filters/levenshtein_prefix_rules.cpp
        filters/lowering_rules.cpp
        filters/ngram_similarity_filter.cpp
        filters/phrase_filter.cpp
        filters/prefix_filter.cpp
        filters/query_profile.cpp
        filters/range_filter.cpp
        filters/range_rules.cpp
        filters/regexp_filter.cpp
        filters/term_filter.cpp
        filters/wildcard_filter.cpp
        filters/wildcard_ngram_filter.cpp
        hits/make.cpp
        hits/make_all.cpp
        hits/make_boosted_posting.cpp
        hits/make_posting.cpp
        lead/make_all_docs.cpp
        lead/make_all_scored.cpp
        lead/make_posting.cpp
        probe/make_all_docs.cpp
        probe/make_all_scored.cpp
        probe/make_posting.cpp
        queries/boolean_query.cpp
        queries/docs_mask_query.cpp
        queries/multiterm_query.cpp
        queries/term_query.cpp
        scorers/bm25.cpp
        scorers/constant_score.cpp
        scorers/make_conjunction.cpp
        scorers/make_probe.cpp
        scorers/make_window.cpp
        scorers/root_window.cpp
        scorers/score_function.cpp
        top/make.cpp
        top/make_all.cpp
        top/make_posting.cpp
        top/make_pruned.cpp
        top/make_pruned_posting.cpp
        top/make_pruned_req_opt.cpp
        top/make_token_phrase.cpp
)

sdb_instantiation_axis(
    IRS_PLANS
    "count FAMILY=count::Family CTX=count::Context MAKE=IRS_MAKE_DOCS GEO=IRS_MAKE_GEO_DOCS INCLUDE=iresearch/search/count/family.hpp"
    "docs FAMILY=docs::Family CTX=docs::Context MAKE=IRS_MAKE_DOCS GEO=IRS_MAKE_GEO_DOCS INCLUDE=iresearch/search/docs/family.hpp"
    "fill_docs FAMILY=fill::Family CTX=utils::Empty MAKE=IRS_MAKE_DOCS GEO=IRS_MAKE_GEO_DOCS INCLUDE=iresearch/search/fill/family.hpp"
    "fill_scored FAMILY=fill::Family CTX=fill::Family::ScoredContext MAKE=IRS_MAKE_SCORED GEO=IRS_MAKE_GEO_SCORED INCLUDE=iresearch/search/fill/family.hpp"
    "hits FAMILY=hits::Family CTX=hits::Context MAKE=IRS_MAKE_SCORED GEO=IRS_MAKE_GEO_SCORED INCLUDE=iresearch/search/hits/family.hpp"
    "lead_docs FAMILY=lead::Family CTX=utils::Empty MAKE=IRS_MAKE_DOCS GEO=IRS_MAKE_GEO_DOCS INCLUDE=iresearch/search/lead/family.hpp"
    "lead_scored FAMILY=lead::Family CTX=ScoredCtx MAKE=IRS_MAKE_SCORED GEO=IRS_MAKE_GEO_SCORED INCLUDE=iresearch/search/lead/family.hpp"
    "probe_docs FAMILY=probe::Family CTX=probe::Family::DocsContext MAKE=IRS_MAKE_DOCS GEO=IRS_MAKE_GEO_DOCS INCLUDE=iresearch/search/probe/family.hpp PLANNER=iresearch/search/probe/boolean_planner.hpp"
    "probe_scored FAMILY=probe::Family CTX=probe::Family::ScoredContext MAKE=IRS_MAKE_SCORED GEO=IRS_MAKE_GEO_SCORED INCLUDE=iresearch/search/probe/family.hpp PLANNER=iresearch/search/probe/boolean_planner.hpp"
    "top FAMILY=top::Family CTX=top::Context MAKE=IRS_MAKE_SCORED GEO=IRS_MAKE_GEO_SCORED INCLUDE=iresearch/search/top/family.hpp"
)
sdb_instantiation_axis(
    IRS_MASK_SLICES
    "unmasked PIN=irs::Pin<irs::detail::MaskedAxis,false>"
    "bitsets PIN=irs::Pin<irs::detail::MaskedAxis,true>,irs::Pin<irs::DocsMaskAxis,irs::MaskKind::Bitsets>"
    "arrays PIN=irs::Pin<irs::detail::MaskedAxis,true>,irs::Pin<irs::DocsMaskAxis,irs::MaskKind::Arrays>"
    "runs PIN=irs::Pin<irs::detail::MaskedAxis,true>,irs::Pin<irs::DocsMaskAxis,irs::MaskKind::Runs>"
    "mixed PIN=irs::Pin<irs::detail::MaskedAxis,true>,irs::Pin<irs::DocsMaskAxis,irs::MaskKind::Mixed>"
    "bitset PIN=irs::Pin<irs::detail::MaskedAxis,true>,irs::Pin<irs::DocsMaskAxis,irs::MaskKind::Bitset>"
    "array PIN=irs::Pin<irs::detail::MaskedAxis,true>,irs::Pin<irs::DocsMaskAxis,irs::MaskKind::Array>"
    "run PIN=irs::Pin<irs::detail::MaskedAxis,true>,irs::Pin<irs::DocsMaskAxis,irs::MaskKind::Run>"
)
sdb_instantiation_axis(
    IRS_INPUTS
    "mapped PIN=irs::Pin<irs::detail::InputAxis,irs::detail::InputKey::Mapped>"
    "stream PIN=irs::Pin<irs::detail::InputAxis,irs::detail::InputKey::Stream>"
)
set(irs_slice "irs::Pins<@MASK.PIN@,@INPUT.PIN@>")
set(irs_input_slice "irs::Pins<@INPUT.PIN@>")
sdb_instantiation_axis(
    IRS_SCORING
    "docs SCORING=detail::NoScoring"
    "scored SCORING=detail::TermScoring"
)
sdb_instantiation_axis(
    IRS_PHRASES
    "fixed QUERY=FixedPhraseQuery"
    "variadic QUERY=VariadicPhraseQuery"
)
sdb_instantiation_axis(
    IRS_PHRASE_MATCHES
    "plain MATCH=Plain"
    "intervals MATCH=Intervals"
    "slop MATCH=Slop"
)
sdb_instantiation_axis(IRS_NGRAM_MATCHES "all MATCH=All" "threshold MATCH=Threshold")
sdb_instantiation_axis(
    IRS_QUERY_BUILDERS
    "all TYPE=AllQuery MAKE=iresearch/search/detail/all_of.hpp"
    "approx TYPE=ApproxQuery MAKE=iresearch/search/detail/approx_of.hpp"
    "boolean TYPE=BooleanQuery MAKE=iresearch/search/detail/make_query.hpp HEADER=iresearch/search/queries/boolean_query.hpp"
    "multi_term TYPE=MultiTermQuery MAKE=iresearch/search/detail/make_term.hpp HEADER=iresearch/search/queries/multiterm_query.hpp"
    "ngram_similarity TYPE=NGramSimilarityQuery MAKE=iresearch/search/detail/ngram_of.hpp"
    "phrase TYPE=FixedPhraseQuery,PhraseQuery MAKE=iresearch/search/detail/phrase_of.hpp"
    "phrase_variadic TYPE=VariadicPhraseQuery,PhraseQuery MAKE=iresearch/search/detail/phrase_of.hpp"
    "term TYPE=TermQuery MAKE=iresearch/search/detail/make_term.hpp HEADER=iresearch/search/queries/term_query.hpp"
    "token_phrase TYPE=TokenPhraseQuery MAKE=iresearch/search/detail/token_phrase_of.hpp"
    "wildcard_ngram TYPE=WildcardNGramQuery MAKE=iresearch/search/detail/wildcard_ngram_of.hpp"
)

sdb_instantiate(
    iresearch
    DIR search/boolean
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/boolean_planner.hpp
        iresearch/search/detail/make_boolean.hpp
        @PLAN.INCLUDE@
        @PLAN.PLANNER@
    CALL "IRS_MAKE_BOOLEAN(@PLAN.FAMILY@, @PLAN.CTX@);"
    AXES PLAN=IRS_PLANS
)
sdb_instantiate(
    iresearch
    DIR search/boolean
    PREFIX required
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/boolean_required.hpp
        iresearch/search/detail/make_boolean.hpp
        @PLAN.INCLUDE@
    CALL
        "IRS_MAKE_BOOLEAN_REQUIRED(@PLAN.FAMILY@, @PLAN.CTX@, ${irs_input_slice});"
    AXES PLAN=IRS_PLANS INPUT=IRS_INPUTS
    SKIP probe_docs probe_scored
)
sdb_instantiate(
    iresearch
    DIR search/boolean
    PREFIX exclusion
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/boolean_exclusion.hpp
        iresearch/search/detail/make_boolean.hpp
        @PLAN.INCLUDE@
    CALL
        "IRS_MAKE_BOOLEAN_EXCLUSION(@PLAN.FAMILY@, @PLAN.CTX@, ${irs_slice});"
    AXES PLAN=IRS_PLANS MASK=IRS_MASK_SLICES INPUT=IRS_INPUTS
    SKIP probe_docs probe_scored
)
sdb_instantiate(
    iresearch
    DIR search/boolean
    PREFIX exclusion_window
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/boolean_exclusion_window.hpp
        iresearch/search/detail/make_boolean.hpp
        @PLAN.INCLUDE@
    CALL
        "IRS_MAKE_BOOLEAN_EXCLUSION_WINDOW(@PLAN.FAMILY@, @PLAN.CTX@, ${irs_slice});"
    AXES PLAN=IRS_PLANS MASK=IRS_MASK_SLICES INPUT=IRS_INPUTS
    SKIP fill_scored lead_scored probe_docs probe_scored
)
sdb_instantiate(
    iresearch
    DIR search/boolean
    PREFIX negation
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/boolean_negation.hpp
        iresearch/search/detail/make_boolean.hpp
        @PLAN.INCLUDE@
    CALL "IRS_MAKE_BOOLEAN_NEGATION(@PLAN.FAMILY@, @PLAN.CTX@);"
    AXES PLAN=IRS_PLANS
    SKIP count fill_scored hits lead_scored probe_docs probe_scored top
)
sdb_instantiate(
    iresearch
    DIR search/boolean
    PREFIX probe_exclusion
    NAMESPACE irs::probe
    INCLUDE iresearch/search/probe/boolean_exclusion.hpp
    CALL "IRS_MAKE_PROBE_EXCLUSION(@SCORING.SCORING@, ${irs_slice});"
    AXES SCORING=IRS_SCORING MASK=IRS_MASK_SLICES INPUT=IRS_INPUTS
)
sdb_instantiate(
    iresearch
    DIR search/boolean
    PREFIX probe_optional
    NAMESPACE irs::probe
    INCLUDE iresearch/search/probe/boolean_optional.hpp
    CALL "IRS_MAKE_PROBE_OPTIONAL(@SCORING.SCORING@);"
    AXES SCORING=IRS_SCORING
)
sdb_instantiate(
    iresearch
    DIR search/top
    PREFIX pruned_conjunction
    NAMESPACE irs::top
    INCLUDE iresearch/search/top/make_pruned_conjunction.hpp
    CALL "IRS_MAKE_PRUNED_CONJUNCTION(${irs_slice});"
    AXES MASK=IRS_MASK_SLICES INPUT=IRS_INPUTS
)
sdb_instantiate(
    iresearch
    DIR search/top
    PREFIX pruned_conjunction_nested
    NAMESPACE irs::top
    INCLUDE iresearch/search/top/make_pruned_conjunction_nested.hpp
    CALL "IRS_MAKE_NESTED_PRUNED_CONJUNCTION(${irs_slice});"
    AXES MASK=IRS_MASK_SLICES INPUT=IRS_INPUTS
)
sdb_instantiate(
    iresearch
    DIR search/top
    PREFIX pruned_disjunction
    NAMESPACE irs::top
    INCLUDE iresearch/search/top/make_pruned_disjunction.hpp
    CALL "IRS_MAKE_PRUNED_DISJUNCTION(${irs_slice});"
    AXES MASK=IRS_MASK_SLICES INPUT=IRS_INPUTS
)
sdb_instantiate(
    iresearch
    DIR search/top
    PREFIX pruned_phrase
    NAMESPACE irs::top
    INCLUDE iresearch/search/top/make_phrase_pruned.hpp
    CALL "IRS_TOP_PHRASE_PRUNED(@PHRASE.QUERY@, @MATCH.MATCH@);"
    AXES PHRASE=IRS_PHRASES MATCH=IRS_PHRASE_MATCHES
    SKIP slop
)
sdb_instantiate(
    iresearch
    DIR search/phrase
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/make_impl.hpp
        iresearch/search/detail/phrase_of.hpp
        @PLAN.INCLUDE@
    CALL
        "@PLAN.MAKE@(@PLAN.FAMILY@, PhraseKind<PhraseMatch::@MATCH.MATCH@>, @PHRASE.QUERY@);"
    AXES PLAN=IRS_PLANS PHRASE=IRS_PHRASES MATCH=IRS_PHRASE_MATCHES
)
sdb_instantiate(
    iresearch
    DIR search/ngram
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/make_impl.hpp
        iresearch/search/detail/ngram_of.hpp
        @PLAN.INCLUDE@
    CALL
        "@PLAN.MAKE@(@PLAN.FAMILY@, NGramKind<NGramMatch::@MATCH.MATCH@>, NGramSimilarityQuery);"
    AXES PLAN=IRS_PLANS MATCH=IRS_NGRAM_MATCHES
)
sdb_instantiate(
    iresearch
    DIR search/token_phrase
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/make_impl.hpp
        iresearch/search/detail/token_phrase_of.hpp
        @PLAN.INCLUDE@
    CALL "@PLAN.MAKE@(@PLAN.FAMILY@, TokenPhraseKind, TokenPhraseQuery);"
    AXES PLAN=IRS_PLANS
)
sdb_instantiate(
    iresearch
    DIR search/wildcard_ngram
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/make_impl.hpp
        iresearch/search/detail/wildcard_ngram_of.hpp
        @PLAN.INCLUDE@
    CALL "@PLAN.MAKE@(@PLAN.FAMILY@, WildcardNGramKind, WildcardNGramQuery);"
    AXES PLAN=IRS_PLANS
)
sdb_instantiate(
    iresearch
    DIR search/query
    NAMESPACE irs
    INCLUDE
        iresearch/search/queries/query_builder_impl.tpp
        @QUERY.MAKE@
        @QUERY.HEADER@
    CALL "template class QueryBuilderImpl<@QUERY.TYPE@>;"
    AXES QUERY=IRS_QUERY_BUILDERS
)

if(IRESEARCH_BENCH)
    return()
endif()

target_sources(
    iresearch
    PRIVATE
        ann/make.cpp
        ann/make_exact.cpp
        ann/make_hnsw.cpp
        ann/make_ivf.cpp
        count/make_exact.cpp
        count/make_hnsw.cpp
        docs/make_exact.cpp
        docs/make_hnsw.cpp
        filters/geo_filter.cpp
        filters/nested_filter.cpp
        filters/shingle_phrase.cpp
        filters/vector_exact_filter.cpp
        filters/vector_radius_filter.cpp
        filters/vector_similarity_filter.cpp
        hits/make_exact.cpp
        hits/make_hnsw.cpp
        hits/make_ivf.cpp
        lead/make_hnsw.cpp
        lead/make_ivf.cpp
        offsets/make.cpp
        offsets/make_posting.cpp
        queries/hnsw_query.cpp
        queries/nested_query.cpp
        queries/vector_similarity_query.cpp
        scorers/dfi.cpp
        scorers/idf.cpp
        scorers/indri_dirichlet.cpp
        scorers/lm_dirichlet.cpp
        scorers/lm_jelinek_mercer.cpp
        scorers/raw_boost.cpp
        scorers/raw_dl.cpp
        scorers/raw_tf.cpp
        scorers/tfidf.cpp
        scorers/vector_similarity_scorer.cpp
)

sdb_instantiation_axis(
    IRS_GEO_ACCEPTORS
    "contains ACCEPTOR=GeoContainsAcceptor"
    "intersects ACCEPTOR=GeoIntersectsAcceptor"
    "is_contained ACCEPTOR=GeoIsContainedAcceptor"
)
sdb_instantiation_axis(
    IRS_FULL_QUERY_BUILDERS
    "exact_vector TYPE=ExactVectorQuery MAKE=iresearch/search/detail/make_query.hpp HEADER=iresearch/search/queries/vector_exact_query.hpp"
    "hnsw TYPE=HnswQuery MAKE=iresearch/search/detail/make_query.hpp HEADER=iresearch/search/queries/hnsw_query.hpp"
    "knn_vector TYPE=KnnVectorQuery MAKE=iresearch/search/detail/make_query.hpp HEADER=iresearch/search/queries/vector_similarity_query.hpp"
    "nested TYPE=ByNestedQuery MAKE=iresearch/search/detail/nested_of.hpp"
    "range_vector TYPE=RangeVectorQuery MAKE=iresearch/search/detail/vector_of.hpp"
)

sdb_instantiate(
    iresearch
    DIR search/geo
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/geo_instantiate.hpp
        iresearch/search/detail/geo_of.hpp
        iresearch/search/detail/make_impl.hpp
        @PLAN.INCLUDE@
    CALL "IRS_GEO_PARSERS(@PLAN.GEO@, @PLAN.FAMILY@, @ACCEPTOR.ACCEPTOR@)"
    AXES PLAN=IRS_PLANS ACCEPTOR=IRS_GEO_ACCEPTORS
)
sdb_instantiate(
    iresearch
    DIR search/geo
    PREFIX distance
    NAMESPACE irs::detail
    INCLUDE
        iresearch/search/detail/geo_instantiate.hpp
        iresearch/search/detail/geo_of.hpp
        iresearch/search/detail/make_impl.hpp
        @PLAN.INCLUDE@
    CALL "IRS_GEO_DISTANCE_ACCEPTORS(@PLAN.GEO@, @PLAN.FAMILY@)"
    AXES PLAN=IRS_PLANS
)
sdb_instantiate(
    iresearch
    DIR search/query
    NAMESPACE irs
    INCLUDE
        iresearch/search/queries/query_builder_impl.tpp
        @QUERY.MAKE@
        @QUERY.HEADER@
    CALL "template class QueryBuilderImpl<@QUERY.TYPE@>;"
    AXES QUERY=IRS_FULL_QUERY_BUILDERS
)
sdb_instantiate(
    iresearch
    DIR search/offsets
    PREFIX phrase
    NAMESPACE irs::offsets
    INCLUDE iresearch/search/offsets/make_impl.hpp
    CALL "IRS_OFFSETS_PHRASE(@PHRASE.QUERY@, @MATCH.MATCH@);"
    AXES PHRASE=IRS_PHRASES MATCH=IRS_PHRASE_MATCHES
)
sdb_instantiate(
    iresearch
    DIR search/offsets
    PREFIX ngram
    NAMESPACE irs::offsets
    INCLUDE iresearch/search/offsets/make_impl.hpp
    CALL "IRS_OFFSETS_NGRAM(@MATCH.MATCH@);"
    AXES MATCH=IRS_NGRAM_MATCHES
)
