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

#include <algorithm>
#include <vector>

#include "basics/down_cast.h"
#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_set.hpp"

namespace {

using Mode = tests::PreparedFilter::CollectMode;

void AnalyzedFieldFactory(tests::Document& doc, const std::string& name,
                          const tests::JsonDocGenerator::JsonValue& data) {
  if (data.is_string()) {
    const auto anl_name = name + "_anl";
    auto field = std::make_shared<tests::TextField<std::string>>(
      anl_name, std::string{data.str});
    field->id = tests::FieldIdForRuntime(anl_name);
    doc.indexed.push_back(std::move(field));
  }
}

irs::bytes_view Bytes(std::string_view v) {
  return irs::ViewCast<irs::byte_type>(v);
}

void FillTerm(irs::ByTerm& q, std::string_view field, std::string_view term) {
  *q.mutable_field_id() = tests::FieldIdFor(field);
  q.mutable_options()->term = Bytes(term);
}

irs::ByTerm MakeTerm(std::string_view field, std::string_view term) {
  irs::ByTerm q;
  FillTerm(q, field, term);
  return q;
}

void FillPrefix(irs::ByPrefix& q, std::string_view field, std::string_view term,
                size_t scored_terms_limit) {
  *q.mutable_field_id() = tests::FieldIdFor(field);
  q.mutable_options()->term = Bytes(term);
  q.mutable_options()->scored_terms_limit = scored_terms_limit;
}

irs::TermClause MakeTermClause(std::string_view field, std::string_view term) {
  return {.field = tests::FieldIdFor(field), .term = irs::bstring{Bytes(term)}};
}

irs::Filter::ptr MakePrefixPtr(std::string_view field, std::string_view term,
                               size_t scored_terms_limit) {
  auto q = std::make_unique<irs::ByPrefix>();
  FillPrefix(*q, field, term, scored_terms_limit);
  return q;
}

irs::BooleanFilter MakeTerms(
  std::string_view field,
  const std::vector<std::pair<std::string_view, irs::score_t>>& terms,
  size_t min_match) {
  irs::BooleanFilter q;
  for (const auto& [term, boost] : terms) {
    q.Add(irs::TermClause{.field = tests::FieldIdFor(field),
                          .term = irs::bstring{Bytes(term)},
                          .boost = boost},
          irs::Occur::Should);
  }
  q.SetMinShouldMatch(static_cast<uint32_t>(min_match));
  return q;
}

irs::ByPrefix MakePrefix(std::string_view field, std::string_view term,
                         size_t scored_terms_limit) {
  irs::ByPrefix q;
  *q.mutable_field_id() = tests::FieldIdFor(field);
  q.mutable_options()->term = Bytes(term);
  q.mutable_options()->scored_terms_limit = scored_terms_limit;
  return q;
}

irs::ByRange MakeRange(std::string_view field, std::string_view min,
                       std::string_view max) {
  irs::ByRange q;
  *q.mutable_field_id() = tests::FieldIdFor(field);
  auto& range = q.mutable_options()->range;
  range.min = Bytes(min);
  range.min_type = irs::BoundType::Inclusive;
  range.max = Bytes(max);
  range.max_type = irs::BoundType::Inclusive;
  return q;
}

struct StatsEntry {
  const irs::Scorer* scorer;
  bool scored;
  irs::bstring bytes;

  bool operator==(const StatsEntry&) const = default;
};

using StatsDump = std::vector<StatsEntry>;

void DumpRecord(const irs::search::StatsRecord& record, StatsDump& out) {
  StatsEntry entry{.scorer = record.scorer, .scored = record.stats != nullptr};
  if (entry.scored) {
    ASSERT_NE(nullptr, record.scorer);
    entry.bytes.assign(record.stats,
                       record.stats + record.scorer->stats_size());
  }
  out.emplace_back(std::move(entry));
}

void DumpQuery(const irs::QueryBuilder& query, StatsDump& out) {
  ASSERT_NO_FATAL_FAILURE(DumpRecord(query.Stats(), out));
  switch (query.Kind()) {
    case irs::QueryKind::Boolean: {
      const auto& boolean = sdb::basics::downCast<irs::BooleanQuery>(query);
      for (const auto occur : irs::kAllOccur) {
        const auto& bucket = boolean.Bucket(occur);
        for (const auto& clause : bucket.postings) {
          ASSERT_NO_FATAL_FAILURE(DumpRecord(clause.stats, out));
        }
        for (const auto& clause : bucket.all_docs) {
          ASSERT_NO_FATAL_FAILURE(DumpRecord(clause.stats, out));
        }
        for (const auto& child : bucket.filters) {
          ASSERT_NE(nullptr, child);
          ASSERT_NO_FATAL_FAILURE(DumpQuery(*child, out));
        }
      }
      break;
    }
    case irs::QueryKind::Terms: {
      const auto& multi = sdb::basics::downCast<irs::MultiTermQuery>(query);
      const auto* const scorer = multi.Stats().scorer;
      for (const auto& entry : multi.State().Terms()) {
        ASSERT_NO_FATAL_FAILURE(
          DumpRecord({.stats = entry.stats, .scorer = scorer}, out));
      }
      break;
    }
    default:
      break;
  }
}

StatsDump DumpStats(const tests::PreparedFilter& prepared) {
  StatsDump out;
  for (size_t i = 0, n = prepared.size(); i != n; ++i) {
    const auto* query = prepared.Query(i);
    if (query == nullptr || irs::QueryBuilder::IsEmpty(*query)) {
      out.emplace_back();
      continue;
    }
    DumpQuery(*query, out);
  }
  return out;
}

using ScoredDocs = std::vector<std::pair<irs::doc_id_t, irs::score_t>>;

void CollectSegment(const tests::PreparedFilter& prepared, size_t i,
                    ScoredDocs& out, uint64_t& cost) {
  cost = prepared.Estimate(i);

  if (prepared.Scorer() != nullptr) {
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(i, fetcher);
    ASSERT_NE(nullptr, docs);
    auto score = docs->PrepareScore();

    while (!irs::doc_limits::eof(docs->Advance())) {
      docs->FetchScoreArgs(0);
      fetcher.Fetch(docs->Value());
      irs::score_t value = 0;
      score.Score(&value, 1);
      out.emplace_back(docs->Value(), value);
    }
  } else {
    auto docs = prepared.Execute(i);
    ASSERT_NE(nullptr, docs);

    while (!irs::doc_limits::eof(docs->Advance())) {
      out.emplace_back(docs->Value(), 0.f);
    }
  }
}

void AssertSameAsSingle(const tests::PreparedFilter& single,
                        const tests::PreparedFilter& other,
                        const irs::IndexReader& index) {
  ASSERT_EQ(single.size(), other.size());
  ASSERT_EQ(index.size(), single.size());
  ASSERT_EQ(DumpStats(single), DumpStats(other));

  for (size_t i = 0; [[maybe_unused]] const auto& sub : index) {
    ScoredDocs single_docs;
    ScoredDocs other_docs;
    uint64_t single_cost = 0;
    uint64_t other_cost = 0;

    ASSERT_NO_FATAL_FAILURE(
      CollectSegment(single, i, single_docs, single_cost));
    ASSERT_NO_FATAL_FAILURE(CollectSegment(other, i, other_docs, other_cost));

    ASSERT_EQ(single_docs, other_docs);
    ASSERT_EQ(single_cost, other_cost);
    ++i;
  }
}

void AssertMergeConsistent(const irs::Filter& filter,
                           const irs::IndexReader& index,
                           const irs::Scorer* scorer) {
  tests::PreparedFilter single{
    filter, index, scorer, irs::IResourceManager::gNoop, nullptr, Mode::Single};
  tests::PreparedFilter paired{filter,  index,
                               scorer,  irs::IResourceManager::gNoop,
                               nullptr, Mode::PairThreads};
  tests::PreparedFilter per_segment{filter,  index,
                                    scorer,  irs::IResourceManager::gNoop,
                                    nullptr, Mode::PerSegment};

  {
    SCOPED_TRACE("two counter blocks");
    ASSERT_NO_FATAL_FAILURE(AssertSameAsSingle(single, paired, index));
  }
  {
    SCOPED_TRACE("one counter block per segment");
    ASSERT_NO_FATAL_FAILURE(AssertSameAsSingle(single, per_segment, index));
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
    {
      SCOPED_TRACE("tfidf scorer");
      irs::TFIDF scorer;
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
    irs::BooleanFilter root;
    root.Add(MakeTermClause("Fields", "BusinessEntityID"), irs::Occur::Should);
    root.Add(MakeTermClause("Fields", "StartDate"), irs::Occur::Should);
    root.SetMinShouldMatch(1);
    CheckAllScorers(root, rdr);
  }

  {
    irs::BooleanFilter root;
    root.Add(MakeTermClause("Fields", "BusinessEntityID"), irs::Occur::Must);
    root.Add(MakePrefixPtr("Fields", "S", 4), irs::Occur::Must);
    CheckAllScorers(root, rdr);
  }

  {
    auto sub = std::make_unique<irs::BooleanFilter>();
    sub->Add(MakeTermClause("Fields", "StartDate"), irs::Occur::Must);
    sub->Add(MakePrefixPtr("Fields", "B", 8), irs::Occur::Must);

    irs::BooleanFilter root;
    root.Add(MakeTermClause("Fields", "BusinessEntityID"), irs::Occur::Should);
    root.Add(std::move(sub), irs::Occur::Should);
    root.SetMinShouldMatch(1);
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
  *filter.mutable_field_id() = tests::FieldIdFor("field");
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
    *q.mutable_field_id() = tests::FieldIdFor("phrase_anl");
    q.mutable_options()->push_back<irs::ByTermOptions>().term = Bytes("quick");
    q.mutable_options()->push_back<irs::ByTermOptions>().term = Bytes("brown");
    CheckAllScorers(q, rdr);
  }

  // variadic phrase -> VariadicTermsCollector
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = tests::FieldIdFor("phrase_anl");
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

  for (const size_t max_terms : {size_t{1024}, size_t{2}}) {
    irs::ByEditDistance filter;
    *filter.mutable_field_id() = tests::FieldIdFor("title");
    filter.mutable_options()->term = Bytes("aa");
    filter.mutable_options()->max_distance = 2;
    filter.mutable_options()->max_terms = max_terms;

    auto lowered = tests::Optimized(std::move(filter));
    CheckAllScorers(*lowered, rdr);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(merge_consistency_test, MergeConsistencyTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         MergeConsistencyTestCase::to_string);

}  // namespace
