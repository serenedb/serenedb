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
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <atomic>
#include <iresearch/analysis/text/term_view.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/utils/string.hpp>
#include <limits>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "phrase_families.hpp"
#include "phrase_reference.hpp"
#include "tests_shared.hpp"

namespace {

constexpr irs::field_id kField = 3;
constexpr irs::field_id kNormBase = 16;

irs::bytes_view Bytes(std::string_view text) {
  return irs::ViewCast<irs::byte_type>(text);
}

class SpacedWords final : public irs::analysis::TypedTokenizer<SpacedWords> {
 public:
  irs::TokenTraits Traits() const noexcept final { return {}; }

  static constexpr std::string_view type_name() noexcept {
    return "test_open_gap_index_words";
  }

  template<irs::TokenLayout L>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view data{raw.GetData(), raw.GetSize()};
    for (const auto word : absl::StrSplit(data, ' ', absl::SkipEmpty())) {
      sink.Emit<L>(irs::MakeTermView(word));
    }
    return true;
  }
};

class PositionalIndex : public tests::FamilyIndex {
 public:
  PositionalIndex(std::span<const std::string> docs,
                  const irs::Scorer& scorer) {
    auto options = irs::tests::DefaultWriterOptions();
    options.reader_options.scorer = &scorer;
    options.norm_column_id = [](irs::field_id id) { return id + kNormBase; };
    auto writer =
      tests::IndexWriter::Make(_dir, tests::kOmCreate, std::move(options));
    EXPECT_NE(nullptr, writer);
    SpacedWords words;
    tests::AnalyzedField field{.analyzer = &words,
                               .id = kField,
                               .features = irs::IndexFeatures::Freq |
                                           irs::IndexFeatures::Pos |
                                           irs::IndexFeatures::Norm};
    auto ctx = writer->GetBatch();
    for (const auto& text : docs) {
      field.value = text;
      auto doc = ctx.Insert();
      EXPECT_TRUE(tests::InsertField(doc, field));
    }
    ctx.Commit();
    writer->RefreshCommit();
    Open();
  }
};

struct Top {
  std::vector<irs::score_t> scores;
  uint64_t reached = 0;
};

Top TopOf(const PositionalIndex& index, const irs::Filter& filter,
          const irs::Scorer& scorer, bool prune) {
  MaxMemoryCounter counter;
  tests::PreparedFilter prepared{filter, index.Reader(), &scorer, counter};
  std::vector<irs::ScoreDoc> hits(tests::kTop);
  std::atomic<irs::score_t> threshold{
    std::numeric_limits<irs::score_t>::lowest()};
  irs::LoserScoreCollector collector{threshold, hits};
  for (size_t i = 0; i != prepared.size(); ++i) {
    const auto* query = prepared.Query(i);
    if (!query || irs::QueryBuilder::IsEmpty(*query)) {
      continue;
    }
    collector.SetSegment(static_cast<uint32_t>(i));
    irs::ColumnArgsFetcher fetcher;
    auto top = query->PlanTop({.scorer = scorer,
                               .fetcher = fetcher,
                               .prune = prune,
                               .k = static_cast<uint32_t>(tests::kTop)});
    top->Run(irs::doc_limits::min(), irs::doc_limits::eof(), collector);
  }
  Top out{.reached = collector.TotalMatches()};
  for (size_t j = 0; j != collector.AcceptedCount(); ++j) {
    out.scores.push_back(hits[j].score);
  }
  absl::c_sort(out.scores, std::greater<>{});
  return out;
}

irs::Filter::ptr Lowered(irs::ByPhraseOptions options) {
  irs::ByPhrase phrase;
  *phrase.mutable_field_id() = kField;
  *phrase.mutable_options() = std::move(options);
  irs::Filter::ptr root = std::make_unique<irs::ByPhrase>(std::move(phrase));
  irs::Optimize(root);
  return root;
}

struct Slot {
  std::string_view word;
  irs::PosAttr::value_t offs_min;
};

irs::ByPhraseOptions OpenPhrase(std::span<const Slot> slots) {
  irs::ByPhraseOptions out;
  for (const auto& slot : slots) {
    if (out.empty()) {
      out.push_back<irs::ByTermOptions>(0, 0).term = Bytes(slot.word);
    } else {
      out
        .push_back<irs::ByTermOptions>(slot.offs_min, irs::pos_limits::kOpenGap)
        .term = Bytes(slot.word);
    }
  }
  return out;
}

uint64_t ReferenceMatches(std::span<const std::string> docs,
                          std::span<const Slot> slots) {
  std::vector<tests::phrase_ref::Gap> gaps;
  for (const auto& slot : slots) {
    if (gaps.empty()) {
      gaps.push_back({0, 0});
    } else {
      gaps.push_back({slot.offs_min, irs::pos_limits::kOpenGap});
    }
  }
  uint64_t matched = 0;
  for (const auto& doc : docs) {
    std::vector<std::vector<uint32_t>> positions(slots.size());
    uint32_t pos = irs::pos_limits::min();
    for (const auto word : absl::StrSplit(doc, ' ', absl::SkipEmpty())) {
      for (size_t i = 0; i != slots.size(); ++i) {
        if (slots[i].word == word) {
          positions[i].push_back(pos);
        }
      }
      ++pos;
    }
    matched += tests::phrase_ref::Enumerate(positions, gaps).matched ? 1 : 0;
  }
  return matched;
}

std::vector<std::string> Corpus(size_t count) {
  constexpr std::string_view kFrequent[] = {"a", "b", "c", "d", "e"};
  std::mt19937 rng{20261022};
  std::vector<std::string> docs(count);
  for (size_t i = 0; i != docs.size(); ++i) {
    if (i < 8) {
      docs[i] = "a b r a b r a b r a b r a b r";
      continue;
    }
    std::vector<std::string_view> words(20 + rng() % 30);
    for (auto& word : words) {
      word = kFrequent[rng() % std::size(kFrequent)];
    }
    if (rng() % 33 == 0) {
      words[rng() % words.size()] = "r";
    }
    docs[i] = absl::StrJoin(words, " ");
  }
  return docs;
}

}  // namespace

TEST(PhraseOpenGapIndexTest, matches_agree_with_the_reference) {
  const irs::BM25 scorer;
  const auto docs = Corpus(2000);
  const PositionalIndex index{docs, scorer};
  const std::vector<Slot> cases[] = {
    {{"a", 0}, {"r", 1}},           {{"r", 0}, {"a", 1}},
    {{"a", 0}, {"b", 1}, {"r", 1}}, {{"a", 0}, {"c", 2}, {"r", 1}},
    {{"a", 0}, {"a", 1}, {"r", 0}}, {{"a", 0}, {"b", 1}, {"c", 1}, {"d", 30}},
  };
  for (const auto& slots : cases) {
    SCOPED_TRACE(slots.size());
    const auto filter = Lowered(OpenPhrase(slots));
    const auto top = TopOf(index, *filter, scorer, false);
    EXPECT_EQ(ReferenceMatches(docs, slots), top.reached);
    EXPECT_FALSE(top.scores.empty());
  }
}
