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

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"

namespace {

using Mode = tests::PreparedFilter::CollectMode;

void AnalyzedFieldFactory(tests::Document& doc, const std::string& name,
                          const tests::JsonDocGenerator::JsonValue& data) {
  if (data.is_string()) {
    doc.indexed.push_back(std::make_shared<tests::TextField<std::string>>(
      name + "_anl", std::string{data.str}));
  }
}

irs::bytes_view Bytes(std::string_view v) {
  return irs::ViewCast<irs::byte_type>(v);
}

void FillTerm(irs::ByTerm& q, std::string_view field, std::string_view term) {
  *q.mutable_field() = field;
  q.mutable_options()->term = Bytes(term);
}

irs::ByTerm MakeTerm(std::string_view field, std::string_view term) {
  irs::ByTerm q;
  FillTerm(q, field, term);
  return q;
}

void FillPrefix(irs::ByPrefix& q, std::string_view field, std::string_view term,
                size_t scored_terms_limit) {
  *q.mutable_field() = field;
  q.mutable_options()->term = Bytes(term);
  q.mutable_options()->scored_terms_limit = scored_terms_limit;
}

irs::ByTerms MakeTerms(
  std::string_view field,
  const std::vector<std::pair<std::string_view, irs::score_t>>& terms,
  size_t min_match) {
  irs::ByTerms q;
  *q.mutable_field() = field;
  q.mutable_options()->min_match = min_match;
  for (const auto& [term, boost] : terms) {
    q.mutable_options()->terms.emplace(Bytes(term), boost);
  }
  return q;
}

irs::ByPrefix MakePrefix(std::string_view field, std::string_view term,
                         size_t scored_terms_limit) {
  irs::ByPrefix q;
  *q.mutable_field() = field;
  q.mutable_options()->term = Bytes(term);
  q.mutable_options()->scored_terms_limit = scored_terms_limit;
  return q;
}

irs::ByRange MakeRange(std::string_view field, std::string_view min,
                       std::string_view max) {
  irs::ByRange q;
  *q.mutable_field() = field;
  auto& range = q.mutable_options()->range;
  range.min = Bytes(min);
  range.min_type = irs::BoundType::Inclusive;
  range.max = Bytes(max);
  range.max_type = irs::BoundType::Inclusive;
  return q;
}

void AssertStatsEqual(const irs::StatsBuffer& lhs,
                      const irs::StatsBuffer& rhs) {
  ASSERT_EQ(lhs.HasScorer(), rhs.HasScorer());

  const auto& l = lhs.GetAllStats();
  const auto& r = rhs.GetAllStats();
  ASSERT_EQ(l.size(), r.size());
  for (size_t i = 0, n = l.size(); i < n; ++i) {
    ASSERT_EQ(l[i], r[i]);
  }

  ASSERT_EQ(lhs.ChildCount(), rhs.ChildCount());
  for (size_t i = 0, n = lhs.ChildCount(); i < n; ++i) {
    AssertStatsEqual(lhs.Child(i), rhs.Child(i));
  }
}

using ScoredDocs = std::vector<std::pair<irs::doc_id_t, irs::score_t>>;

void CollectSegment(const tests::PreparedFilter& prepared, size_t i,
                    const irs::SubReader& sub, ScoredDocs& out,
                    irs::CostAttr::Type& cost) {
  auto docs = prepared.Execute(i);
  ASSERT_NE(nullptr, docs);
  cost = irs::CostAttr::extract(*docs);

  const auto* scorer = prepared.Scorer();
  irs::ScoreFunction score;
  if (scorer != nullptr) {
    score = docs->PrepareScore({
      .scorer = scorer,
      .segment = &sub,
    });
  }

  while (docs->next()) {
    irs::score_t value = 0;
    if (scorer != nullptr) {
      docs->FetchScoreArgs(0);
      score.Score(&value, 1);
    }
    out.emplace_back(docs->value(), value);
  }
}

void AssertMergeConsistent(const irs::Filter& filter,
                           const irs::IndexReader& index,
                           const irs::Scorer* scorer) {
  tests::PreparedFilter single{
    filter, index, scorer, irs::IResourceManager::gNoop, nullptr, Mode::Single};
  tests::PreparedFilter merged{
    filter, index, scorer, irs::IResourceManager::gNoop, nullptr, Mode::Merge};

  ASSERT_EQ(single.size(), merged.size());
  ASSERT_EQ(index.size(), single.size());
  ASSERT_NO_FATAL_FAILURE(AssertStatsEqual(single.Stats(), merged.Stats()));

  for (size_t i = 0; const auto& sub : index) {
    ScoredDocs single_docs;
    ScoredDocs merged_docs;
    irs::CostAttr::Type single_cost = 0;
    irs::CostAttr::Type merged_cost = 0;

    ASSERT_NO_FATAL_FAILURE(
      CollectSegment(single, i, sub, single_docs, single_cost));
    ASSERT_NO_FATAL_FAILURE(
      CollectSegment(merged, i, sub, merged_docs, merged_cost));

    ASSERT_EQ(single_docs, merged_docs);
    ASSERT_EQ(single_cost, merged_cost);
    ++i;
  }
}

class MergeConsistencyTestCase : public tests::FilterTestCaseBase {
 protected:
  void BuildIndex() {
    auto writer = open_writer(irs::kOmCreate);
    {
      tests::JsonDocGenerator gen{resource("AdventureWorks2014.json"),
                                  &tests::GenericJsonFieldFactory};
      add_segment(*writer, gen);
    }
    {
      tests::JsonDocGenerator gen{resource("AdventureWorks2014Edges.json"),
                                  &tests::GenericJsonFieldFactory};
      add_segment(*writer, gen);
    }
    {
      tests::JsonDocGenerator gen{resource("Northwnd.json"),
                                  &tests::GenericJsonFieldFactory};
      add_segment(*writer, gen);
    }
    {
      tests::JsonDocGenerator gen{resource("NorthwndEdges.json"),
                                  &tests::GenericJsonFieldFactory};
      add_segment(*writer, gen);
    }
  }

  void CheckAllScorers(const irs::Filter& filter,
                       const irs::IndexReader& index) {
    {
      SCOPED_TRACE("no scorer");
      ASSERT_NO_FATAL_FAILURE(AssertMergeConsistent(filter, index, nullptr));
    }
    {
      SCOPED_TRACE("frequency scorer");
      tests::sort::FrequencySort scorer;
      ASSERT_NO_FATAL_FAILURE(AssertMergeConsistent(filter, index, &scorer));
    }
  }
};

TEST_P(MergeConsistencyTestCase, term) {
  BuildIndex();
  auto rdr = open_reader();
  ASSERT_EQ(4, rdr.size());

  CheckAllScorers(MakeTerm("Fields", "BusinessEntityID"), rdr);
  CheckAllScorers(MakeTerm("Fields", "StartDate"), rdr);
  CheckAllScorers(MakeTerm("Fields", "MissingTermXyz"), rdr);
}

TEST_P(MergeConsistencyTestCase, terms) {
  BuildIndex();
  auto rdr = open_reader();
  ASSERT_EQ(4, rdr.size());

  CheckAllScorers(
    MakeTerms("Fields", {{"BusinessEntityID", 1.f}, {"StartDate", 1.f}}, 1),
    rdr);
  CheckAllScorers(
    MakeTerms("Fields", {{"BusinessEntityID", 1.f}, {"StartDate", 1.f}}, 2),
    rdr);
}

TEST_P(MergeConsistencyTestCase, prefix) {
  BuildIndex();
  auto rdr = open_reader();
  ASSERT_EQ(4, rdr.size());

  CheckAllScorers(MakePrefix("Fields", "B", 1024), rdr);

  // limited scored terms exercises the global top-K merge path
  CheckAllScorers(MakePrefix("Fields", "", 1), rdr);
  CheckAllScorers(MakePrefix("Fields", "", 4), rdr);
}

TEST_P(MergeConsistencyTestCase, range) {
  BuildIndex();
  auto rdr = open_reader();
  ASSERT_EQ(4, rdr.size());

  CheckAllScorers(MakeRange("Fields", "A", "Z"), rdr);
}

TEST_P(MergeConsistencyTestCase, boolean) {
  BuildIndex();
  auto rdr = open_reader();
  ASSERT_EQ(4, rdr.size());

  {
    irs::Or root;
    FillTerm(root.add<irs::ByTerm>(), "Fields", "BusinessEntityID");
    FillTerm(root.add<irs::ByTerm>(), "Fields", "StartDate");
    CheckAllScorers(root, rdr);
  }

  {
    irs::And root;
    FillTerm(root.add<irs::ByTerm>(), "Fields", "BusinessEntityID");
    FillPrefix(root.add<irs::ByPrefix>(), "Fields", "S", 4);
    CheckAllScorers(root, rdr);
  }

  {
    irs::Or root;
    FillTerm(root.add<irs::ByTerm>(), "Fields", "BusinessEntityID");
    auto& sub = root.add<irs::And>();
    FillTerm(sub.add<irs::ByTerm>(), "Fields", "StartDate");
    FillPrefix(sub.add<irs::ByPrefix>(), "Fields", "B", 8);
    CheckAllScorers(root, rdr);
  }
}

TEST_P(MergeConsistencyTestCase, all) {
  BuildIndex();
  auto rdr = open_reader();
  ASSERT_EQ(4, rdr.size());

  irs::All filter;
  CheckAllScorers(filter, rdr);
}

TEST_P(MergeConsistencyTestCase, ngram_similarity) {
  for (size_t i = 0; i < 2; ++i) {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen, i == 0 ? irs::kOmCreate : irs::kOmAppend);
  }

  auto rdr = open_reader();
  ASSERT_EQ(2, rdr.size());

  // threshold < 1 with a scorer selects NGramCollector
  irs::ByNGramSimilarity filter;
  *filter.mutable_field() = "field";
  filter.mutable_options()->threshold = 0.5f;
  for (auto ngram : {"at", "tl", "as", "ow"}) {
    filter.mutable_options()->ngrams.emplace_back(Bytes(ngram));
  }

  CheckAllScorers(filter, rdr);
}

TEST_P(MergeConsistencyTestCase, phrase) {
  for (size_t i = 0; i < 2; ++i) {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &AnalyzedFieldFactory);
    add_segment(gen, i == 0 ? irs::kOmCreate : irs::kOmAppend,
                irs::tests::DefaultWriterOptions());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(2, rdr.size());

  // fixed phrase -> TermsCollector
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term = Bytes("quick");
    q.mutable_options()->push_back<irs::ByTermOptions>().term = Bytes("brown");
    CheckAllScorers(q, rdr);
  }

  // variadic phrase -> VariadicTermsCollector
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term = Bytes("quick");
    q.mutable_options()->push_back<irs::ByPrefixOptions>().term = Bytes("bro");
    CheckAllScorers(q, rdr);
  }
}

TEST_P(MergeConsistencyTestCase, edit_distance) {
  for (size_t i = 0; i < 2; ++i) {
    tests::JsonDocGenerator gen(resource("levenshtein_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen, i == 0 ? irs::kOmCreate : irs::kOmAppend);
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(2, rdr.size());

  // scored, limited terms -> ScoredTermsCollector
  irs::ByEditDistance filter;
  *filter.mutable_field() = "title";
  filter.mutable_options()->term = Bytes("aa");
  filter.mutable_options()->max_distance = 2;
  filter.mutable_options()->max_terms = 1024;

  CheckAllScorers(filter, rdr);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(merge_consistency_test, MergeConsistencyTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         MergeConsistencyTestCase::to_string);

}  // namespace
