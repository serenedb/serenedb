////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

// Correctness coverage for the sloppy-phrase gather machinery and the n == 2
// fused merge-join.
//
// n == 2 phrases - fixed and variadic alike - route to the merge-join in
// production and never reach gather; gPairJoinDisabled exposes the generic
// gather + Run path for the equivalence tests, which compile only under
// SDB_DEV. The SlopOverlapMatcher tests at the end pin the n >= 3
// same-position (term-group) semantics of spm::Run and run in any build.

#include "filter_test_case_base.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/slop_phrase.hpp"
#include "tests_shared.hpp"

namespace {

namespace spm = irs::detail::slop;

#ifdef SDB_DEV

constexpr irs::field_id kField = tests::FieldIdFor("phrase_anl");

// Routes n == 2 phrases through the generic gather + Run path for the
// duration of the scope (the production default is the fused merge-join,
// which bypasses gather entirely).
class PairJoinGuard {
 public:
  PairJoinGuard() noexcept { spm::gPairJoinDisabled = true; }
  ~PairJoinGuard() { spm::gPairJoinDisabled = false; }

  PairJoinGuard(const PairJoinGuard&) = delete;
  PairJoinGuard& operator=(const PairJoinGuard&) = delete;
};

// Routes the offset-enabled read-all gather through the scalar per-position
// loop for the duration of the scope (the production default is the bulk
// three-array ReadAll).
class OffsBulkScalarGuard {
 public:
  OffsBulkScalarGuard() noexcept { spm::gOffsBulkGatherDisabled = true; }
  ~OffsBulkScalarGuard() { spm::gOffsBulkGatherDisabled = false; }

  OffsBulkScalarGuard(const OffsBulkScalarGuard&) = delete;
  OffsBulkScalarGuard& operator=(const OffsBulkScalarGuard&) = delete;
};

irs::bytes_view Term(std::string_view s) {
  return irs::ViewCast<irs::byte_type>(s);
}

// Collects matched doc ids across all segments for the prepared query.
// The path toggles are read at iteration time, so the same prepared
// query can be re-run under different guard scopes.
std::vector<irs::doc_id_t> CollectDocs(const tests::PreparedFilter& prepared) {
  std::vector<irs::doc_id_t> out;
  for (size_t i = 0; i < prepared.size(); ++i) {
    auto docs = prepared.Execute(i);
    while (!irs::doc_limits::eof(docs->advance())) {
      out.push_back(docs->value());
    }
  }
  return out;
}

// Collects (doc, [(start,end)...]) per matched doc via ExecuteWithOffsets.
struct OffsetMatch {
  irs::doc_id_t doc;
  std::vector<std::pair<uint32_t, uint32_t>> offsets;

  bool operator==(const OffsetMatch&) const = default;
};

template<typename PhraseQueryT>
std::vector<OffsetMatch> CollectOffsets(const tests::PreparedFilter& prepared,
                                        const irs::DirectoryReader& rdr) {
  std::vector<OffsetMatch> out;
  size_t i = 0;
  for (auto sub = rdr.begin(); sub != rdr.end(); ++sub, ++i) {
    const auto* phrase_query =
      dynamic_cast<const PhraseQueryT*>(prepared.Query(i));
    EXPECT_NE(nullptr, phrase_query);
    if (!phrase_query) {
      continue;
    }
    auto docs = phrase_query->ExecuteWithOffsets(*sub);
    if (!docs) {
      continue;
    }
    auto* pos = irs::GetMutable<irs::PosAttr>(docs.get());
    if (!pos) {
      continue;
    }
    auto* offs = irs::get<irs::OffsAttr>(*pos);
    while (!irs::doc_limits::eof(docs->advance())) {
      OffsetMatch m{.doc = docs->value()};
      while (pos->next()) {
        m.offsets.emplace_back(offs ? offs->start : 0, offs ? offs->end : 0);
      }
      out.push_back(std::move(m));
    }
  }
  return out;
}

#endif  // SDB_DEV

}  // namespace

#ifdef SDB_DEV

class SlopGatherTestCase : public tests::FilterTestCaseBase {};

// Pair-join equivalence: the n == 2 merge-join (production default) must
// produce exactly the docs the generic gather + Run path does.
TEST_P(SlopGatherTestCase, pair_join_equivalence_fixed) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::PayloadedJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  struct Spec {
    std::string_view ctx;
    std::vector<std::string_view> terms;
    size_t gap_offs;  // extra offset before the second term (0 == none)
    irs::PosAttr::value_t slop;
  };
  const Spec specs[] = {
    {"quick fox s1", {"quick", "fox"}, 0, 1},
    {"quick moved s3", {"quick", "moved"}, 0, 3},
    {"fox brown s2 (reversal)", {"fox", "brown"}, 0, 2},
    {"fox fox s1 (repeated term)", {"fox", "fox"}, 0, 1},
    {"quick __ moved s1 (gap)", {"quick", "moved"}, 1, 1},
  };

  for (const auto& s : specs) {
    irs::ByPhrase q;
    *q.mutable_field_id() = kField;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      Term(s.terms[0]);
    q.mutable_options()->push_back<irs::ByTermOptions>(s.gap_offs).term =
      Term(s.terms[1]);
    q.mutable_options()->set_slop(s.slop);

    tests::PreparedFilter prepared{q, rdr};

    const auto join = CollectDocs(prepared);
    std::vector<irs::doc_id_t> legacy;
    {
      PairJoinGuard pj;
      legacy = CollectDocs(prepared);
    }
    ASSERT_EQ(legacy, join) << "pair join diverged from gather: " << s.ctx;
    ASSERT_FALSE(join.empty()) << "expected matches for: " << s.ctx;
  }
}

// Same for the offsets path: per-match offsets (and, via the match
// count, freq) from the join must be identical to the generic path's.
TEST_P(SlopGatherTestCase, pair_join_equivalence_offsets) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::PayloadedJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  irs::ByPhrase q;
  *q.mutable_field_id() = kField;
  q.mutable_options()->push_back<irs::ByTermOptions>().term = Term("quick");
  q.mutable_options()->push_back<irs::ByTermOptions>().term = Term("fox");
  q.mutable_options()->set_slop(1);

  tests::PreparedFilter prepared{q, rdr};

  const auto join = CollectOffsets<irs::FixedPhraseQuery>(prepared, rdr);
  std::vector<OffsetMatch> legacy;
  {
    PairJoinGuard pj;
    legacy = CollectOffsets<irs::FixedPhraseQuery>(prepared, rdr);
  }
  ASSERT_FALSE(join.empty());
  ASSERT_EQ(legacy, join);
}

// Variadic pair-join equivalence: an n == 2 variadic phrase (a term set per
// slot, here from prefix expansion) routes to the same merge-join; same
// join-vs-generic discipline as the fixed test. Duplicate positions inside
// a slot (same-position synonyms) cannot occur on this corpus; that case is
// pinned by the merged-stream fuzz oracle.
TEST_P(SlopGatherTestCase, pair_join_equivalence_variadic) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::PayloadedJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  struct Slot {
    bool prefix;
    std::string_view term;
  };
  struct Spec {
    std::string_view ctx;
    Slot first;
    Slot second;
    size_t gap_offs;  // extra offset before the second term (0 == none)
    irs::PosAttr::value_t slop;
  };
  const Spec specs[] = {
    {"qui* fox s1", {true, "qui"}, {false, "fox"}, 0, 1},
    {"qui* moved s3", {true, "qui"}, {false, "moved"}, 0, 3},
    {"fox qui* s3 (reversal)", {false, "fox"}, {true, "qui"}, 0, 3},
    {"qui* __ moved s1 (gap)", {true, "qui"}, {false, "moved"}, 1, 1},
  };

  for (const auto& s : specs) {
    irs::ByPhrase q;
    *q.mutable_field_id() = kField;
    auto& opts = *q.mutable_options();
    if (s.first.prefix) {
      opts.push_back<irs::ByPrefixOptions>().term = Term(s.first.term);
    } else {
      opts.push_back<irs::ByTermOptions>().term = Term(s.first.term);
    }
    if (s.second.prefix) {
      opts.push_back<irs::ByPrefixOptions>(s.gap_offs).term =
        Term(s.second.term);
    } else {
      opts.push_back<irs::ByTermOptions>(s.gap_offs).term = Term(s.second.term);
    }
    opts.set_slop(s.slop);

    tests::PreparedFilter prepared{q, rdr};

    const auto join = CollectDocs(prepared);
    std::vector<irs::doc_id_t> legacy;
    {
      PairJoinGuard pj;
      legacy = CollectDocs(prepared);
    }
    ASSERT_EQ(legacy, join)
      << "variadic pair join diverged from gather: " << s.ctx;
    ASSERT_FALSE(join.empty()) << "expected matches for: " << s.ctx;
  }
}

// Offsets path through the variadic join: per-match offsets must be
// identical to the generic gather path's. Exact comparison is safe: with no
// same-position tokens in the corpus no slot holds duplicate positions, the
// one case where the two paths may legitimately source offsets from
// different equal-position terms.
TEST_P(SlopGatherTestCase, pair_join_equivalence_offsets_variadic) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::PayloadedJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  irs::ByPhrase q;
  *q.mutable_field_id() = kField;
  q.mutable_options()->push_back<irs::ByPrefixOptions>().term = Term("qui");
  q.mutable_options()->push_back<irs::ByTermOptions>().term = Term("fox");
  q.mutable_options()->set_slop(1);

  tests::PreparedFilter prepared{q, rdr};

  const auto join = CollectOffsets<irs::VariadicPhraseQuery>(prepared, rdr);
  std::vector<OffsetMatch> legacy;
  {
    PairJoinGuard pj;
    legacy = CollectOffsets<irs::VariadicPhraseQuery>(prepared, rdr);
  }
  ASSERT_FALSE(join.empty());
  ASSERT_EQ(legacy, join);
}

// Per-doc solo dispatch in the variadic join: a slot whose current document
// holds exactly one live sub-iterator feeds JoinPair raw, others go through
// the merged stream. The sequential corpus never puts two live subs in BOTH
// slots of one document, so the merged x merged branch (and the dispatch
// counting itself) is pinned here on an inline corpus. No same-position
// tokens, so exact offsets comparison is safe (see
// pair_join_equivalence_offsets_variadic).
TEST_P(SlopGatherTestCase, pair_join_solo_dispatch_equivalence) {
  // qui* -> {quick, quilt}, fo* -> {fox, forward}; the per-doc live sub
  // counts walk all four dispatch branches across the documents.
  static constexpr char kData[] =
    R"([{"name":"MM","phrase":"quick quilt fox forward"},)"
    R"({"name":"RR","phrase":"quick fox"},)"
    R"({"name":"MR","phrase":"quick quilt fox"},)"
    R"({"name":"RM","phrase":"quick fox forward"}])";
  {
    tests::JsonDocGenerator gen(kData, &tests::PayloadedJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  for (const irs::PosAttr::value_t slop : {1u, 3u}) {
    irs::ByPhrase q;
    *q.mutable_field_id() = kField;
    q.mutable_options()->push_back<irs::ByPrefixOptions>().term = Term("qui");
    q.mutable_options()->push_back<irs::ByPrefixOptions>().term = Term("fo");
    q.mutable_options()->set_slop(slop);

    tests::PreparedFilter prepared{q, rdr};
    const auto ctx = "qui* fo* s" + std::to_string(slop);

    const auto join = CollectDocs(prepared);
    std::vector<irs::doc_id_t> legacy;
    {
      PairJoinGuard pj;
      legacy = CollectDocs(prepared);
    }
    ASSERT_EQ(legacy, join) << "solo dispatch diverged from gather: " << ctx;
    ASSERT_FALSE(join.empty()) << "expected matches for: " << ctx;

    const auto join_offs =
      CollectOffsets<irs::VariadicPhraseQuery>(prepared, rdr);
    std::vector<OffsetMatch> legacy_offs;
    {
      PairJoinGuard pj;
      legacy_offs = CollectOffsets<irs::VariadicPhraseQuery>(prepared, rdr);
    }
    ASSERT_EQ(legacy_offs, join_offs) << ctx;
    ASSERT_FALSE(join_offs.empty()) << ctx;
  }
}

// Multi-block postings: every corpus above keeps a term's positions within
// a single 128-entry block, so the bulk ReadAll refill and its backlog Skip
// never run in CI (benches are the only consumers). This corpus gives the
// dense term hundreds of positions per document - several position blocks -
// plus a document the conjunction skips, so the pending-position catch-up
// crosses blocks too. All decode paths must agree: join vs gather for the
// pair, and bulk vs scalar offset gather for n == 3.
TEST_P(SlopGatherTestCase, multi_block_postings) {
  const auto repeat = [](std::string_view tok, size_t n) {
    std::string s;
    for (size_t i = 0; i != n; ++i) {
      s += tok;
      s += ' ';
    }
    return s;
  };
  // aaa: 281 + 150 + 131 = 562 positions in the segment, crossing block
  // boundaries inside every document; D2 holds no bbb/xxx, so the phrase
  // conjunction skips it and the next document starts with a backlog.
  const std::string json =
    std::string{R"([{"name":"D1","phrase":"xxx bbb )"} + repeat("aaa", 140) +
    "bbb " + repeat("aaa", 140) + R"(xxx bbb aaa"},{"name":"D2","phrase":")" +
    repeat("aaa", 150) + R"("},{"name":"D3","phrase":"xxx bbb )" +
    repeat("aaa", 130) + R"(xxx bbb aaa"}])";
  {
    tests::JsonDocGenerator gen(json.c_str(),
                                &tests::PayloadedJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  const auto make = [&](std::vector<std::string_view> terms,
                        irs::PosAttr::value_t slop) {
    irs::ByPhrase q;
    *q.mutable_field_id() = kField;
    for (const auto t : terms) {
      q.mutable_options()->push_back<irs::ByTermOptions>().term = Term(t);
    }
    q.mutable_options()->set_slop(slop);
    return tests::PreparedFilter{q, rdr};
  };

  // Pair: production join vs the generic gather path.
  {
    auto prepared = make({"bbb", "aaa"}, 1);
    const auto join = CollectDocs(prepared);
    ASSERT_EQ(2u, join.size());  // D1 and D3
    {
      PairJoinGuard pj;
      ASSERT_EQ(join, CollectDocs(prepared));
    }
  }

  // Triple: bulk ReadAll with refills, positions and offsets.
  {
    auto prepared = make({"xxx", "bbb", "aaa"}, 1);
    const auto read_all = CollectDocs(prepared);
    ASSERT_EQ(2u, read_all.size());  // D1 and D3

    // Offsets: bulk three-array ReadAll vs its scalar loop.
    const auto bulk = CollectOffsets<irs::FixedPhraseQuery>(prepared, rdr);
    ASSERT_FALSE(bulk.empty());
    {
      OffsBulkScalarGuard scalar;
      ASSERT_EQ(bulk, CollectOffsets<irs::FixedPhraseQuery>(prepared, rdr));
    }
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(slop_gather_test, SlopGatherTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         SlopGatherTestCase::to_string);

#endif  // SDB_DEV

// SlopOverlapMatcher: n >= 3 same-position matching, driving
// detail::slop::Run directly with synthetic per-slot position lists and
// term-group ids. Encodes the empirically-verified Elasticsearch n >= 3 spec
// for "foo qux" indexed with synonym foo,bar (postings foo@0, bar@0, qux@1).
// "foo bar qux": no match at slop 0, one match at slop >= 1 (distinct terms
// may share position 0, but that costs 1, so it needs slop). "foo foo qux":
// no match at any slop (one foo occurrence can't fill both foo slots). Group
// ids mark same-term: {0,1,2} for foo/bar/qux, {0,0,2} when foo repeats. Slot
// positions: foo {0}, bar {0}, qux {1} (the repeated foo slot reads foo's
// postings too).

TEST(SlopOverlapMatcher, n3_distinct_terms_share_position) {
  spm::MatchScratch scratch;
  const std::vector<std::vector<irs::PosAttr::value_t>> slot_pos = {
    {0}, {0}, {1}};
  const std::vector<irs::PosAttr::value_t> expected_steps = {1, 1};
  const std::vector<uint32_t> groups = {0, 1, 2};  // foo, bar, qux

  // slop 0: same position is not adjacency -> no match.
  {
    auto r = spm::Run(slot_pos, /*slop=*/0, expected_steps, scratch,
                      /*early_exit=*/false, groups);
    EXPECT_FALSE(r.any);
  }
  // slop >= 1: foo@0 -> bar@0 (delta 0, cost 1) -> qux@1 (delta 1, cost 0) = 1.
  for (const irs::PosAttr::value_t slop : {1u, 2u, 5u}) {
    auto r = spm::Run(slot_pos, slop, expected_steps, scratch,
                      /*early_exit=*/false, groups);
    EXPECT_TRUE(r.any) << "slop=" << slop;
    EXPECT_EQ(1u, r.freq) << "slop=" << slop;
    EXPECT_EQ(1u, r.best_distance) << "slop=" << slop;
  }
}

TEST(SlopOverlapMatcher, n3_repeated_term_never_matches) {
  spm::MatchScratch scratch;
  const std::vector<std::vector<irs::PosAttr::value_t>> slot_pos = {
    {0}, {0}, {1}};
  const std::vector<irs::PosAttr::value_t> expected_steps = {1, 1};
  const std::vector<uint32_t> groups = {0, 0, 2};  // foo, foo, qux

  // The single foo occurrence (pos 0) cannot fill both foo slots, at any slop.
  for (const irs::PosAttr::value_t slop : {0u, 1u, 5u}) {
    auto r = spm::Run(slot_pos, slop, expected_steps, scratch,
                      /*early_exit=*/false, groups);
    EXPECT_FALSE(r.any) << "slop=" << slop;
  }
}

// Wide slot variant: the same group-aware uniqueness, but the third slot
// spans 130 positions, so the DFS prunes over a much larger window volume.

TEST(SlopOverlapMatcher, n3_wide_slot_distinct_terms_share_position) {
  spm::MatchScratch scratch;
  std::vector<irs::PosAttr::value_t> qux;
  for (irs::PosAttr::value_t p = 1; p <= 130; ++p) {
    qux.push_back(p);
  }
  const std::vector<std::vector<irs::PosAttr::value_t>> slot_pos = {
    {0}, {0}, std::move(qux)};
  const std::vector<irs::PosAttr::value_t> expected_steps = {1, 1};
  const std::vector<uint32_t> groups = {0, 1, 2};  // foo, bar, qux

  auto r = spm::Run(slot_pos, /*slop=*/200, expected_steps, scratch,
                    /*early_exit=*/false, groups);
  EXPECT_TRUE(r.any);
  EXPECT_EQ(1u, r.best_distance);
}

TEST(SlopOverlapMatcher, n3_wide_slot_repeated_term_never_matches) {
  spm::MatchScratch scratch;
  std::vector<irs::PosAttr::value_t> qux;
  for (irs::PosAttr::value_t p = 1; p <= 130; ++p) {
    qux.push_back(p);
  }
  const std::vector<std::vector<irs::PosAttr::value_t>> slot_pos = {
    {0}, {0}, std::move(qux)};
  const std::vector<irs::PosAttr::value_t> expected_steps = {1, 1};
  const std::vector<uint32_t> groups = {0, 0, 2};  // foo, foo, qux

  auto r = spm::Run(slot_pos, /*slop=*/200, expected_steps, scratch,
                    /*early_exit=*/false, groups);
  EXPECT_FALSE(r.any);
}

// Guard: empty groups (direct-caller opt-out) keep strict uniqueness:
// two slots cannot share a position.

TEST(SlopOverlapMatcher, n3_empty_groups_enforces_position_uniqueness) {
  spm::MatchScratch scratch;
  const std::vector<std::vector<irs::PosAttr::value_t>> slot_pos = {
    {0}, {0}, {1}};
  const std::vector<irs::PosAttr::value_t> expected_steps = {1, 1};

  // Default empty groups: the foo@0/bar@0 collision is dropped at any slop.

  for (const irs::PosAttr::value_t slop : {0u, 1u, 5u}) {
    auto r = spm::Run(slot_pos, slop, expected_steps, scratch,
                      /*early_exit=*/false);
    EXPECT_FALSE(r.any) << "slop=" << slop;
  }
}
