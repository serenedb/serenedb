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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <iresearch/search/explain/explanation.hpp>
#include <iresearch/search/explain/plan.hpp>
#include <iresearch/search/explain/stats.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/levenshtein_filter.hpp>
#include <iresearch/search/filters/ngram_similarity_filter.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/search/scorers/constant_score.hpp>
#include <iresearch/search/scorers/dfi.hpp>
#include <iresearch/search/scorers/idf.hpp>
#include <iresearch/search/scorers/indri_dirichlet.hpp>
#include <iresearch/search/scorers/lm_dirichlet.hpp>
#include <iresearch/search/scorers/lm_jelinek_mercer.hpp>
#include <iresearch/search/scorers/raw_boost.hpp>
#include <iresearch/search/scorers/raw_dl.hpp>
#include <iresearch/search/scorers/raw_tf.hpp>
#include <iresearch/search/scorers/tfidf.hpp>
#include <iresearch/search/scorers/unscored.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "filter_test_case_base.hpp"
#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;
using irs::explain::Role;

inline constexpr irs::field_id kBodyId = 1;
inline constexpr irs::field_id kTagId = 2;

struct Row {
  size_t segment = 0;
  irs::doc_id_t doc = 0;
  irs::score_t score = 0;
};

irs::bstring Bytes(std::string_view text) {
  return irs::bstring{irs::ViewCast<irs::byte_type>(text)};
}

irs::Filter::ptr Term(std::string_view term, irs::field_id field = kBodyId) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = field;
  filter->mutable_options()->term = Bytes(term);
  return filter;
}

std::unique_ptr<irs::BooleanFilter> AnyOf(
  std::initializer_list<std::string_view> terms) {
  auto filter = std::make_unique<irs::BooleanFilter>();
  for (const auto term : terms) {
    filter->Add(irs::TermClause{.field = kBodyId, .term = Bytes(term)},
                irs::Occur::Should);
  }
  filter->SetMinShouldMatch(1);
  return filter;
}

std::unique_ptr<irs::ByPhrase> Phrase(
  std::initializer_list<std::string_view> terms) {
  auto phrase = std::make_unique<irs::ByPhrase>();
  *phrase->mutable_field_id() = kBodyId;
  for (const auto term : terms) {
    phrase->mutable_options()->push_back<irs::ByTermOptions>().term =
      Bytes(term);
  }
  return phrase;
}

class UndescribedFilter final : public irs::Filter {
 public:
  explicit UndescribedFilter(irs::Filter::ptr inner)
    : _inner{std::move(inner)} {}

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final {
    return _inner->PrepareSegment(segment, ctx);
  }

  irs::TypeInfo::type_id type() const noexcept final { return _inner->type(); }

 protected:
  irs::PrepareCollector::ptr MakeCollectorImpl(const irs::Scorer* scorer,
                                               irs::StatsArena& stats,
                                               uint32_t threads) const final {
    return _inner->MakeCollector(*scorer, stats, threads);
  }

 private:
  irs::Filter::ptr _inner;
};

irs::score_t ValueOf(const irs::explain::Node& node) {
  return std::get<irs::score_t>(node.value);
}

uint64_t CountOf(const irs::explain::Node& node) {
  return std::get<uint64_t>(node.value);
}

const irs::explain::Node* Child(const irs::explain::Node& node,
                                std::string_view key) {
  for (const auto& child : node.details) {
    if (child.key == key) {
      return &child;
    }
  }
  return nullptr;
}

class ExplainTest : public IndexTestBase {
 protected:
  void BuildFixture() {
    const std::vector<std::vector<std::pair<std::string, std::string>>>
      segments{
        {
          {"quick brown fox jumps over the lazy dog", "animal"},
          {"the fox", "animal"},
        },
        {
          {"quick quick fox fox fox", "speed"},
          {"lazy cat sleeps", "animal"},
        },
        {
          {"brown fox brown dog", "color"},
          {"a quick red fox and a quick brown fox", "speed"},
        },
      };
    auto writer = open_writer(tests::kOmCreate, irs::IndexWriterOptions{});
    ASSERT_NE(nullptr, writer);
    for (const auto& segment : segments) {
      for (const auto& [body, tag] : segment) {
        tests::Document doc;
        auto text = std::make_shared<tests::TextField<std::string>>(
          "body", body, false, irs::IndexFeatures::Norm);
        text->id = kBodyId;
        doc.insert(text, true, false);
        auto keyword = std::make_shared<tests::StringField>("tag", tag);
        keyword->id = kTagId;
        doc.insert(keyword, true, false);
        ASSERT_TRUE(
          tests::Insert(*writer, doc.indexed.begin(), doc.indexed.end()));
      }
      writer->RefreshCommit();
    }
  }

  std::vector<Row> Execute(const tests::PreparedFilter& prepared) {
    std::vector<Row> rows;
    for (size_t segment = 0; segment != prepared.size(); ++segment) {
      irs::ColumnArgsFetcher fetcher;
      auto docs = prepared.ExecuteScored(segment, fetcher);
      EXPECT_NE(nullptr, docs);
      auto score = docs->PrepareScore();
      while (!irs::doc_limits::eof(docs->Next())) {
        docs->FetchScoreArgs(0);
        fetcher.Fetch(docs->Value());
        irs::score_t value{};
        score.Score(&value, 1);
        rows.push_back({segment, docs->Value(), value});
      }
    }
    return rows;
  }

  const std::vector<irs::explain::Node>& Explain(const irs::Filter& filter,
                                                 const irs::Scorer& scorer,
                                                 float tolerance) {
    auto& run = *_runs.emplace_back(std::make_unique<ExplainRun>());
    run.index = open_reader();
    const auto& prepared =
      run.prepared.emplace(filter, run.index, &scorer, run.counter);
    filter.ExplainStats(prepared.Collector(), run.stats);
    const auto rows = Execute(prepared);
    EXPECT_FALSE(rows.empty());
    for (size_t segment = 0; segment != prepared.size(); ++segment) {
      const auto* query = prepared.Query(segment);
      if (!query) {
        continue;
      }
      auto& plan = *run.plans.emplace_back(
        irs::explain::MakePlan(*query, scorer, run.stats, filter));
      for (const auto& row : rows) {
        if (row.segment != segment) {
          continue;
        }
        auto node = plan.Explain(row.doc, std::nullopt);
        if (tolerance == 0) {
          EXPECT_EQ(row.score, ValueOf(node)) << node.expression;
        } else {
          EXPECT_NEAR(row.score, ValueOf(node), tolerance) << node.expression;
        }
        run.rows.push_back(std::move(node));
      }
    }
    return run.rows;
  }

 private:
  struct ExplainRun {
    irs::CompositeReader index;
    MaxMemoryCounter counter;
    std::optional<tests::PreparedFilter> prepared;
    irs::explain::StatsIndex stats;
    std::vector<std::unique_ptr<irs::explain::Explainer>> plans;
    std::vector<irs::explain::Node> rows;
  };

  std::vector<std::unique_ptr<ExplainRun>> _runs;
};

TEST_P(ExplainTest, bm25_term_breaks_down_into_idf_and_tf) {
  BuildFixture();
  irs::BM25 bm25;
  const auto filter = Term("fox");
  const auto rows = Explain(*filter, bm25, 0);
  ASSERT_EQ(5u, rows.size());
  for (const auto& row : rows) {
    EXPECT_EQ(Role::Weight, row.role);
    EXPECT_EQ(kBodyId, row.subject.field);
    ASSERT_EQ(1u, row.subject.terms.size());
    EXPECT_EQ(Bytes("fox"), row.subject.terms.front());
    EXPECT_EQ(bm25.ToString(), row.scorer);
    EXPECT_EQ("boost * idf * tf", row.expression);

    const auto* idf = Child(row, "idf");
    ASSERT_NE(nullptr, idf);
    EXPECT_EQ(irs::Bm25Idf(6, 5), ValueOf(*idf));
    ASSERT_NE(nullptr, Child(*idf, "df"));
    EXPECT_EQ(5u, CountOf(*Child(*idf, "df")));
    EXPECT_EQ(6u, CountOf(*Child(*idf, "N")));

    const auto* tf = Child(row, "tf");
    ASSERT_NE(nullptr, tf);
    for (const auto key : {"freq", "k1", "b", "dl", "avgdl"}) {
      EXPECT_NE(nullptr, Child(*tf, key)) << key;
    }
    EXPECT_NEAR(ValueOf(row), ValueOf(*idf) * ValueOf(*tf), 1e-6f);
  }
}

TEST_P(ExplainTest, boolean_lists_matched_clauses_and_adds_them_up) {
  BuildFixture();
  irs::BM25 bm25;
  {
    const auto filter = AnyOf({"quick", "dog", "cat"});
    for (const auto& row : Explain(*filter, bm25, 1e-5f)) {
      EXPECT_EQ(Role::Sum, row.role);
      irs::score_t total = 0;
      for (const auto& clause : row.details) {
        EXPECT_EQ(Role::Weight, clause.role);
        total += ValueOf(clause);
      }
      EXPECT_NEAR(ValueOf(row), total, 1e-5f);
    }
  }
  {
    auto filter = AnyOf({"quick", "fox"});
    filter->SetMergeType(irs::ScoreMergeType::Max);
    for (const auto& row : Explain(*filter, bm25, 1e-5f)) {
      EXPECT_EQ(Role::Max, row.role);
    }
  }
  {
    irs::BooleanFilter filter;
    filter.Add(irs::TermClause{.field = kBodyId, .term = Bytes("fox")},
               irs::Occur::Must);
    filter.Add(irs::TermClause{.field = kBodyId, .term = Bytes("quick")},
               irs::Occur::Must);
    filter.Add(irs::TermClause{.field = kBodyId, .term = Bytes("dog")},
               irs::Occur::MustNot);
    const auto rows = Explain(filter, bm25, 1e-5f);
    ASSERT_EQ(2u, rows.size());
    for (const auto& row : rows) {
      EXPECT_EQ(2u, row.details.size());
    }
  }
}

TEST_P(ExplainTest, phrase_sums_the_idf_of_its_terms) {
  BuildFixture();
  irs::BM25 bm25;
  const auto filter = Phrase({"quick", "fox"});
  const auto rows = Explain(*filter, bm25, 0);
  ASSERT_FALSE(rows.empty());
  for (const auto& row : rows) {
    EXPECT_EQ(Role::Weight, row.role);
    EXPECT_EQ(2u, row.subject.phrase.size());
    const auto* idf = Child(row, "idf");
    ASSERT_NE(nullptr, idf);
    EXPECT_EQ(2u, idf->details.size());
  }

  auto sloppy = Phrase({"quick", "fox"});
  sloppy->mutable_options()->set_slop(2);
  for (const auto& row : Explain(*sloppy, bm25, 0)) {
    const auto* tf = Child(row, "tf");
    ASSERT_NE(nullptr, tf);
    EXPECT_NE(nullptr, Child(*tf, "freq"));
  }

  auto alternatives = Phrase({"quick"});
  auto& set =
    alternatives->mutable_options()->push_back<irs::TermSetOptions>().terms;
  set.emplace(Bytes("fox"));
  set.emplace(Bytes("zebra"));
  const auto matched = Explain(*alternatives, bm25, 0);
  ASSERT_FALSE(matched.empty());
  for (const auto& row : matched) {
    const auto* idf = Child(row, "idf");
    ASSERT_NE(nullptr, idf);
    EXPECT_EQ(2u, idf->details.size());
  }
}

TEST_P(ExplainTest, prefix_names_each_expanded_term) {
  BuildFixture();
  irs::BM25 bm25;
  const auto expanded = [&](const irs::ByPrefix& filter, Role merge) {
    const auto rows = Explain(filter, bm25, 1e-5f);
    EXPECT_EQ(1u, rows.size());
    std::vector<irs::bstring> terms;
    for (const auto& row : rows) {
      EXPECT_EQ(merge, row.role);
      for (const auto& leaf : row.details) {
        EXPECT_EQ(kBodyId, leaf.subject.field);
        EXPECT_EQ(1u, leaf.subject.terms.size());
        absl::c_copy(leaf.subject.terms, std::back_inserter(terms));
      }
    }
    absl::c_sort(terms);
    return terms;
  };
  const std::vector<irs::bstring> expected{Bytes("a"), Bytes("and")};

  irs::ByPrefix constant;
  *constant.mutable_field_id() = kBodyId;
  constant.mutable_options()->term = Bytes("a");
  EXPECT_EQ(expected, expanded(constant, Role::Max));

  irs::ByPrefix scored;
  *scored.mutable_field_id() = kBodyId;
  scored.mutable_options()->term = Bytes("a");
  scored.SetScorer(&bm25);
  EXPECT_EQ(expected, expanded(scored, Role::Max));

  scored.mutable_options()->merge = irs::ScoreMergeType::Sum;
  EXPECT_EQ(expected, expanded(scored, Role::Sum));
}

TEST_P(ExplainTest, an_unscored_clause_names_its_terms) {
  BuildFixture();
  irs::BM25 bm25;
  irs::Unscored unscored;
  irs::BooleanFilter filter;
  filter.Add(irs::TermClause{.field = kBodyId, .term = Bytes("cat")},
             irs::Occur::Should);
  auto lazy = Term("lazy");
  lazy->SetScorer(&unscored);
  filter.Add(std::move(lazy), irs::Occur::Should);
  auto phrase = Phrase({"brown", "dog"});
  phrase->SetScorer(&unscored);
  filter.Add(std::move(phrase), irs::Occur::Should);
  filter.SetMinShouldMatch(1);
  std::vector<std::string> named;
  const auto collect = [&](this const auto& self,
                           const irs::explain::Node& node) -> void {
    if (node.role != Role::Match) {
      for (const auto& child : node.details) {
        self(child);
      }
      return;
    }
    EXPECT_EQ(kBodyId, node.subject.field);
    std::string label;
    const auto append = [&](const irs::bstring& term) {
      absl::StrAppend(&label, label.empty() ? "" : " ",
                      irs::ViewCast<char>(irs::bytes_view{term}));
    };
    for (const auto& position : node.subject.phrase) {
      for (const auto& term : position.terms) {
        append(term);
      }
    }
    for (const auto& term : node.subject.terms) {
      append(term);
    }
    named.push_back(
      node.subject.phrase.empty() ? label : absl::StrCat("\"", label, "\""));
  };
  for (const auto& row : Explain(filter, bm25, 1e-5f)) {
    collect(row);
  }
  absl::c_sort(named);
  named.erase(std::unique(named.begin(), named.end()), named.end());
  EXPECT_EQ((std::vector<std::string>{"\"brown dog\"", "lazy"}), named);
}

TEST_P(ExplainTest, a_fuzzy_match_names_the_term_it_matched) {
  BuildFixture();
  irs::BM25 bm25;
  irs::ByEditDistanceOptions options;
  options.term = Bytes("quik");
  options.max_distance = 1;
  const auto filter =
    irs::LowerLevenshtein(kBodyId, irs::DictRef{}, options, irs::kNoBoost);
  ASSERT_NE(nullptr, filter);
  const auto rows = Explain(*filter, bm25, 1e-5f);
  ASSERT_EQ(3u, rows.size());
  for (const auto& row : rows) {
    const auto& leaf = row.role == Role::Sum ? row.details.front() : row;
    ASSERT_EQ(1u, leaf.subject.terms.size());
    EXPECT_EQ(Bytes("quick"), leaf.subject.terms.front());
    const auto* idf = Child(leaf, "idf");
    ASSERT_NE(nullptr, idf);
    const auto* df = Child(*idf, "df");
    ASSERT_NE(nullptr, df);
    EXPECT_NE(std::string::npos, df->note.find("matched terms"));
  }
}

TEST_P(ExplainTest, every_scorer_matches_execution) {
  BuildFixture();
  std::vector<irs::Scorer::ptr> scorers;
  scorers.push_back(std::make_unique<irs::BM25>());
  scorers.push_back(std::make_unique<irs::BM25>(1.2f, 0.f));
  scorers.push_back(std::make_unique<irs::BM25>(1.2f, 1.f));
  scorers.push_back(std::make_unique<irs::BM25>(0.f, 0.75f));
  scorers.push_back(std::make_unique<irs::TFIDF>(false));
  scorers.push_back(std::make_unique<irs::TFIDF>(true));
  scorers.push_back(std::make_unique<irs::LMJelinekMercer>());
  scorers.push_back(std::make_unique<irs::LMDirichlet>());
  scorers.push_back(std::make_unique<irs::IndriDirichlet>());
  scorers.push_back(std::make_unique<irs::DFI>(irs::DFIMeasure::Standardized));
  scorers.push_back(std::make_unique<irs::DFI>(irs::DFIMeasure::Saturated));
  scorers.push_back(std::make_unique<irs::DFI>(irs::DFIMeasure::ChiSquared));
  scorers.push_back(std::make_unique<irs::IDF>());
  scorers.push_back(std::make_unique<irs::RawTF>());
  scorers.push_back(std::make_unique<irs::RawDL>());
  scorers.push_back(std::make_unique<irs::RawBoost>());
  scorers.push_back(std::make_unique<irs::ConstantScore>(2.f));
  for (const auto& scorer : scorers) {
    SCOPED_TRACE(scorer->ToString());
    const auto term = Term("fox");
    for (const auto& row : Explain(*term, *scorer, 0)) {
      EXPECT_EQ(Role::Weight, row.role);
      EXPECT_EQ(scorer->ToString(), row.scorer);
    }
    const auto any = AnyOf({"quick", "lazy", "brown"});
    Explain(*any, *scorer, 1e-5f);
    const auto phrase = Phrase({"brown", "fox"});
    Explain(*phrase, *scorer, 0);
  }
}

TEST_P(ExplainTest, a_branch_keeps_its_own_scorer_or_none) {
  BuildFixture();
  irs::BM25 bm25;
  irs::RawBoost raw_boost;
  irs::Unscored unscored;
  irs::BooleanFilter filter;
  filter.Add(irs::TermClause{.field = kBodyId, .term = Bytes("fox")},
             irs::Occur::Should);
  auto dog = Term("dog");
  dog->SetScorer(&raw_boost);
  filter.Add(std::move(dog), irs::Occur::Should);
  auto lazy = Term("lazy");
  lazy->SetScorer(&unscored);
  filter.Add(std::move(lazy), irs::Occur::Should);
  filter.SetMinShouldMatch(1);
  bool saw_match = false;
  bool saw_raw_boost = false;
  for (const auto& row : Explain(filter, bm25, 1e-5f)) {
    for (const auto& clause :
         row.role == Role::Sum ? row.details : std::vector{row}) {
      if (clause.role == Role::Match) {
        saw_match = true;
        EXPECT_EQ(0.f, ValueOf(clause));
      } else if (clause.scorer.find("raw_boost") != std::string::npos) {
        saw_raw_boost = true;
      }
    }
  }
  EXPECT_TRUE(saw_match);
  EXPECT_TRUE(saw_raw_boost);
}

TEST_P(ExplainTest, a_scorer_without_a_breakdown_still_explains_its_value) {
  BuildFixture();
  tests::sort::Boost boost;
  const auto filter = Term("fox");
  const auto rows = Explain(*filter, boost, 0);
  ASSERT_EQ(5u, rows.size());
  for (const auto& row : rows) {
    EXPECT_EQ(Role::Weight, row.role);
    EXPECT_EQ(boost.ToString(), row.scorer);
    EXPECT_TRUE(row.expression.empty());
    EXPECT_TRUE(row.details.empty());
  }
}

TEST_P(ExplainTest, a_filter_without_statistics_still_explains_its_value) {
  BuildFixture();
  irs::BM25 bm25;
  for (const auto& row : Explain(UndescribedFilter{Term("fox")}, bm25, 0)) {
    const auto* idf = Child(row, "idf");
    ASSERT_NE(nullptr, idf);
    EXPECT_TRUE(idf->details.empty());
    const auto* tf = Child(row, "tf");
    ASSERT_NE(nullptr, tf);
    const auto* avgdl = Child(*tf, "avgdl");
    ASSERT_NE(nullptr, avgdl);
    EXPECT_TRUE(avgdl->details.empty());
  }
  irs::LMDirichlet lm;
  for (const auto& row : Explain(UndescribedFilter{Term("fox")}, lm, 0)) {
    const auto* p = Child(row, "p");
    ASSERT_NE(nullptr, p);
    EXPECT_TRUE(p->details.empty());
  }
  for (const auto& row :
       Explain(UndescribedFilter{Phrase({"brown", "fox"})}, lm, 0)) {
    EXPECT_EQ("sum", row.expression);
    EXPECT_TRUE(row.details.empty());
  }
  for (const auto& row :
       Explain(UndescribedFilter{std::make_unique<irs::All>()}, bm25, 0)) {
    EXPECT_FALSE(irs::field_limits::valid(row.subject.field));
  }
}

TEST_P(ExplainTest, a_single_ngram_explains_as_its_term) {
  BuildFixture();
  irs::BM25 bm25;
  irs::ByNGramSimilarity filter;
  *filter.mutable_field_id() = kBodyId;
  filter.mutable_options()->ngrams.push_back(Bytes("fox"));
  const auto rows = Explain(filter, bm25, 0);
  ASSERT_EQ(5u, rows.size());
  for (const auto& row : rows) {
    EXPECT_EQ(kBodyId, row.subject.field);
    ASSERT_EQ(1u, row.subject.terms.size());
    EXPECT_EQ(Bytes("fox"), row.subject.terms.front());
    EXPECT_NE(nullptr, Child(row, "idf"));
  }
}

TEST_P(ExplainTest, a_column_without_frequency_counts_each_match_once) {
  BuildFixture();
  irs::BM25 bm25;
  const auto filter = Term("animal", kTagId);
  const auto rows = Explain(*filter, bm25, 0);
  ASSERT_EQ(3u, rows.size());
  for (const auto& row : rows) {
    EXPECT_EQ(kTagId, row.subject.field);
  }
}

TEST_P(ExplainTest,
       a_match_without_frequency_says_whether_it_scores_its_boost) {
  BuildFixture();
  const irs::BM25 bm25{irs::BM25::K(), irs::BM25::B(), true};
  irs::All all;
  for (const auto& row : Explain(all, bm25, 0)) {
    EXPECT_EQ("the match has no term frequency, so the clause scores its boost",
              row.note);
  }
  all.SetBoost(0.f);
  for (const auto& row : Explain(all, bm25, 0)) {
    EXPECT_EQ("the match has no term frequency, so the clause scores 0",
              row.note);
  }
}

TEST_P(ExplainTest, rows_in_any_order_are_explained) {
  BuildFixture();
  irs::BM25 bm25;
  const auto filter = AnyOf({"quick", "fox"});
  auto index = open_reader();
  MaxMemoryCounter counter;
  tests::PreparedFilter prepared{*filter, index, &bm25, counter};
  irs::explain::StatsIndex stats;
  filter->ExplainStats(prepared.Collector(), stats);
  const auto rows = Execute(prepared);
  for (size_t segment = 0; segment != prepared.size(); ++segment) {
    const auto plan =
      irs::explain::MakePlan(*prepared.Query(segment), bm25, stats, *filter);
    for (auto it = rows.rbegin(); it != rows.rend(); ++it) {
      if (it->segment != segment) {
        continue;
      }
      EXPECT_NEAR(it->score, ValueOf(plan->Explain(it->doc, std::nullopt)),
                  1e-5f);
      EXPECT_EQ(it->score, ValueOf(plan->Explain(it->doc, it->score)));
    }
  }
}

static constexpr auto kTestDirs = tests::kDirParams;

INSTANTIATE_TEST_SUITE_P(explain_test, ExplainTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs)),
                         ExplainTest::to_string);

}  // namespace
