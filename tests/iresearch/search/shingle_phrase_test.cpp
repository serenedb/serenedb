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
#include <absl/strings/str_split.h>

#include <atomic>
#include <bit>
#include <functional>
#include <iresearch/analysis/shingle_tokenizer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/count/root.hpp>
#include <iresearch/search/detail/phrase_verify.hpp>
#include <iresearch/search/detail/window.hpp>
#include <iresearch/search/docs/root.hpp>
#include <iresearch/search/fill/node.hpp>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/search/filters/levenshtein_filter.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/range_filter.hpp>
#include <iresearch/search/filters/shingle_phrase.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/filters/wildcard_filter.hpp>
#include <iresearch/search/hits/root.hpp>
#include <iresearch/search/probe/node.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/search/top/root.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/string.hpp>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <variant>
#include <vector>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

using irs::analysis::ShingleTokenizer;

class WhitespaceTokenizer final
  : public irs::analysis::TypedTokenizer<WhitespaceTokenizer> {
 public:
  irs::TokenTraits Traits() const noexcept final { return {}; }

  static constexpr std::string_view type_name() noexcept {
    return "test_whitespace";
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

std::unique_ptr<ShingleTokenizer> MakeShingles(
  uint32_t min, uint32_t max, bool unigrams = true,
  std::vector<std::string_view> frequent = {}) {
  ShingleTokenizer::Options options{
    .min_shingle_size = min,
    .max_shingle_size = max,
    .output_unigrams = unigrams,
  };
  for (const auto word : frequent) {
    options.frequent_words.emplace_back(irs::ViewCast<irs::byte_type>(word));
  }
  return std::make_unique<ShingleTokenizer>(
    std::make_unique<WhitespaceTokenizer>(), std::move(options));
}

irs::ByPhraseOptions Phrase(std::string_view text) {
  irs::ByPhraseOptions phrase;
  for (const auto word : absl::StrSplit(text, ' ', absl::SkipEmpty())) {
    phrase.push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(word);
  }
  return phrase;
}

std::string Text(irs::bytes_view term) {
  return std::string{irs::ViewCast<char>(term)};
}

std::vector<std::string> Terms(const irs::ByPhraseOptions& phrase) {
  std::vector<std::string> out;
  for (const auto& info : phrase) {
    if (const auto* prefix = std::get_if<irs::ByPrefixOptions>(&info.part)) {
      out.push_back(Text(prefix->term) + "*");
    } else {
      out.push_back(Text(std::get<irs::ByTermOptions>(info.part).term));
    }
  }
  return out;
}

void PushTerm(irs::ByPhraseOptions& phrase, std::string_view word,
              irs::PosAttr::value_t offs_min, irs::PosAttr::value_t offs_max) {
  phrase.push_back<irs::ByTermOptions>(offs_min, offs_max).term =
    irs::ViewCast<irs::byte_type>(word);
}

void PushPrefix(irs::ByPhraseOptions& phrase, std::string_view prefix,
                irs::PosAttr::value_t offs_min,
                irs::PosAttr::value_t offs_max) {
  phrase.push_back<irs::ByPrefixOptions>(offs_min, offs_max).term =
    irs::ViewCast<irs::byte_type>(prefix);
}

std::vector<uint32_t> Offsets(const irs::ByPhraseOptions& phrase) {
  std::vector<uint32_t> out;
  for (const auto& info : phrase) {
    out.push_back(info.offs_max);
  }
  return out;
}

inline constexpr irs::field_id kStoreId = 1;
inline constexpr irs::field_id kShingleId = 2;
inline constexpr irs::field_id kPlainId = 3;
inline constexpr irs::field_id kPositionalId = 4;

irs::StoredText Stored() {
  return {.column = kStoreId,
          .tokenizer = [] { return std::make_unique<WhitespaceTokenizer>(); }};
}

const irs::ByPhraseOptions& PhraseOf(
  const std::optional<irs::ShinglePhrasePlan>& plan) {
  return std::get<irs::ByPhraseOptions>(*plan);
}

std::string TermOf(const std::optional<irs::ShinglePhrasePlan>& plan) {
  return Text(std::get<irs::bstring>(*plan));
}

std::optional<irs::ShinglePhrasePlan> Plan(const ShingleTokenizer& shingles,
                                           std::string_view text,
                                           bool positional) {
  const auto stored = Stored();
  return irs::PlanShinglePhrase(shingles, Phrase(text), positional,
                                positional ? nullptr : &stored);
}

struct Field {
  irs::field_id Id() const { return id; }

  irs::analysis::Tokenizer& GetTokens() const { return *analyzer; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept { return features; }

  bool Write(irs::DataOutput& out) const {
    out.WriteData(reinterpret_cast<const irs::byte_type*>(value.data()),
                  value.size());
    return true;
  }

  irs::analysis::Tokenizer* analyzer{};
  std::string_view value;
  irs::field_id id{};
  irs::IndexFeatures features = irs::IndexFeatures::Freq;
};

struct Families {
  uint64_t count = 0;
  std::vector<irs::doc_id_t> docs;
  std::vector<irs::doc_id_t> fill;
  std::vector<irs::doc_id_t> probe;
  std::map<irs::doc_id_t, irs::score_t> hits;
  std::map<irs::doc_id_t, irs::score_t> fill_scores;
  std::map<irs::doc_id_t, irs::score_t> probe_scores;
  std::vector<irs::score_t> top;
  uint64_t top_total = 0;
};

void ExpectScores(const std::map<irs::doc_id_t, irs::score_t>& expected,
                  const std::map<irs::doc_id_t, irs::score_t>& actual) {
  ASSERT_EQ(expected.size(), actual.size());
  for (const auto& [doc, score] : expected) {
    ASSERT_TRUE(actual.contains(doc)) << doc;
    EXPECT_FLOAT_EQ(score, actual.at(doc)) << doc;
  }
}

void ExpectFamilies(const Families& expected, const Families& actual) {
  EXPECT_EQ(expected.count, actual.count);
  EXPECT_EQ(expected.docs, actual.docs);
  EXPECT_EQ(expected.docs, actual.fill);
  EXPECT_EQ(expected.docs, actual.probe);
  {
    SCOPED_TRACE("hits");
    ExpectScores(expected.hits, actual.hits);
  }
  {
    SCOPED_TRACE("fill");
    ExpectScores(expected.hits, actual.fill_scores);
  }
  {
    SCOPED_TRACE("probe");
    ExpectScores(expected.hits, actual.probe_scores);
  }
  EXPECT_EQ(expected.top_total, actual.top_total);
  ASSERT_EQ(expected.top.size(), actual.top.size());
  for (size_t i = 0; i != expected.top.size(); ++i) {
    EXPECT_FLOAT_EQ(expected.top[i], actual.top[i]) << i;
  }
}

class Index {
 public:
  Index(std::span<const std::string_view> docs, ShingleTokenizer& shingles,
        irs::IndexFeatures shingle_features) {
    auto writer = irs::IndexWriter::Make(_dir, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    EXPECT_NE(nullptr, writer);
    WhitespaceTokenizer plain;
    WhitespaceTokenizer positional;
    Field shingle_field{
      .analyzer = &shingles, .id = kShingleId, .features = shingle_features};
    Field plain_field{.analyzer = &plain, .id = kPlainId};
    Field positional_field{
      .analyzer = &positional,
      .id = kPositionalId,
      .features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
    auto ctx = writer->GetBatch();
    for (const auto text : docs) {
      shingle_field.value = text;
      plain_field.value = text;
      positional_field.value = text;
      auto doc = ctx.Insert();
      EXPECT_TRUE(tests::InsertField(doc, shingle_field));
      EXPECT_TRUE(tests::InsertField(doc, plain_field));
      EXPECT_TRUE(tests::InsertField(doc, positional_field));
      auto* cs = doc.GetColWriter();
      EXPECT_NE(nullptr, cs);
      irs::tests::StoreFieldAt(*cs, kStoreId, doc.DocId(), plain_field);
    }
    ctx.Commit();
    writer->RefreshCommit();
    _reader = irs::DirectoryReader{_dir, irs::tests::DefaultReaderOptions()};
  }

  const irs::DirectoryReader& Reader() const noexcept { return _reader; }

  std::vector<irs::doc_id_t> Docs(const irs::Filter& filter) const {
    tests::PreparedFilter prepared{filter, *_reader};
    std::vector<irs::doc_id_t> out;
    for (size_t i = 0; i != prepared.size(); ++i) {
      auto docs = prepared.Execute(i);
      while (!irs::doc_limits::eof(docs->Next())) {
        out.push_back(docs->Value() - irs::doc_limits::min());
      }
    }
    return out;
  }

  std::map<irs::doc_id_t, irs::score_t> Scores(
    const irs::Filter& filter) const {
    irs::BM25 scorer;
    MaxMemoryCounter counter;
    tests::PreparedFilter prepared{filter, *_reader, &scorer, counter};
    std::map<irs::doc_id_t, irs::score_t> out;
    for (size_t i = 0; i != prepared.size(); ++i) {
      irs::ColumnArgsFetcher fetcher;
      auto docs = prepared.ExecuteScored(i, fetcher);
      auto score = docs->PrepareScore();
      while (!irs::doc_limits::eof(docs->Next())) {
        docs->FetchScoreArgs(0);
        fetcher.Fetch(docs->Value());
        irs::score_t value{};
        score.Score(&value, 1);
        out.emplace(docs->Value() - irs::doc_limits::min(), value);
      }
    }
    return out;
  }

  Families Run(const irs::Filter& filter) const {
    Families out;
    {
      tests::PreparedFilter prepared{filter, *_reader};
      for (size_t i = 0; i != prepared.size(); ++i) {
        const auto* query = prepared.Query(i);
        if (!query || irs::QueryBuilder::IsEmpty(*query)) {
          continue;
        }
        const auto end = End(i);
        out.count += query->PlanCount({})->Run(irs::doc_limits::min(), end);

        auto docs = query->PlanDocs({});
        std::vector<irs::doc_id_t> buf(irs::detail::kWindowDocs +
                                       irs::doc_limits::kDocsSlack);
        auto fill = query->PlanFill({}, irs::ScoreMergeType::Noop);
        std::vector<uint64_t> mask(irs::detail::kWindowWords);
        for (auto min = irs::doc_limits::min(); min < end;
             min += irs::detail::kWindowDocs) {
          const auto max = std::min(min + irs::detail::kWindowDocs, end);
          const auto n = docs->Run(min, max, buf.data());
          for (uint32_t j = 0; j != n; ++j) {
            out.docs.push_back(buf[j] - irs::doc_limits::min());
          }
          absl::c_fill(mask, 0);
          fill->FillOr(min, max, mask.data());
          ForEachBit(mask, min, [&](irs::doc_id_t doc) {
            out.fill.push_back(doc - irs::doc_limits::min());
          });
        }

        auto probe = query->PlanProbe({}, end - irs::doc_limits::min());
        for (auto doc = irs::doc_limits::min(); doc < end; ++doc) {
          if (probe->Probe(doc) == doc) {
            out.probe.push_back(doc - irs::doc_limits::min());
          }
        }
      }
    }

    irs::BM25 scorer;
    MaxMemoryCounter counter;
    tests::PreparedFilter prepared{filter, *_reader, &scorer, counter};
    for (size_t i = 0; i != prepared.size(); ++i) {
      const auto* query = prepared.Query(i);
      if (!query || irs::QueryBuilder::IsEmpty(*query)) {
        continue;
      }
      const auto end = End(i);
      {
        irs::ColumnArgsFetcher fetcher;
        auto hits = query->PlanScored({.scorer = scorer, .fetcher = fetcher});
        std::vector<irs::doc_id_t> docs(irs::detail::kWindowDocs +
                                        irs::doc_limits::kDocsSlack);
        std::vector<irs::score_t> scores(irs::detail::kWindowDocs +
                                         irs::doc_limits::kScoresSlack);
        for (auto min = irs::doc_limits::min(); min < end;
             min += irs::detail::kWindowDocs) {
          const auto max = std::min(min + irs::detail::kWindowDocs, end);
          const auto n = hits->Run(min, max, docs.data(), scores.data());
          for (uint32_t j = 0; j != n; ++j) {
            out.hits.emplace(docs[j] - irs::doc_limits::min(), scores[j]);
          }
        }
      }
      {
        irs::ColumnArgsFetcher fetcher;
        auto fill = query->PlanFill({.scorer = &scorer, .fetcher = &fetcher},
                                    irs::ScoreMergeType::Sum);
        std::vector<uint64_t> mask(irs::detail::kWindowWords);
        std::vector<irs::score_t> scores(irs::detail::kWindowDocs);
        for (auto min = irs::doc_limits::min(); min < end;
             min += irs::detail::kWindowDocs) {
          const auto max = std::min(min + irs::detail::kWindowDocs, end);
          absl::c_fill(mask, 0);
          absl::c_fill(scores, 0.f);
          fill->Fill(min, max, mask.data(), scores.data());
          ForEachBit(mask, min, [&](irs::doc_id_t doc) {
            out.fill_scores.emplace(doc - irs::doc_limits::min(),
                                    scores[doc - min]);
          });
        }
      }
      {
        irs::ColumnArgsFetcher fetcher;
        auto probe = query->PlanProbe({.scorer = &scorer, .fetcher = &fetcher},
                                      end - irs::doc_limits::min());
        auto score = probe->PrepareScore();
        for (auto doc = irs::doc_limits::min(); doc < end; ++doc) {
          if (probe->Probe(doc) != doc) {
            continue;
          }
          probe->FetchScoreArgs(0);
          fetcher.Fetch(doc);
          irs::score_t value{};
          score.Score(&value, 1);
          out.probe_scores.emplace(doc - irs::doc_limits::min(), value);
        }
      }
      {
        constexpr size_t kTop = 5;
        irs::ColumnArgsFetcher fetcher;
        auto top = query->PlanTop(
          {.scorer = scorer, .fetcher = fetcher, .prune = false, .k = kTop});
        std::vector<irs::ScoreDoc> hits(kTop);
        std::atomic<irs::score_t> threshold{
          std::numeric_limits<irs::score_t>::lowest()};
        irs::LoserScoreCollector collector{threshold, hits};
        top->Run(irs::doc_limits::min(), end, collector);
        out.top_total += collector.TotalMatches();
        for (size_t j = 0; j != collector.AcceptedCount(); ++j) {
          out.top.push_back(hits[j].score);
        }
      }
    }
    absl::c_sort(out.top, std::greater<>{});
    return out;
  }

 private:
  irs::doc_id_t End(size_t segment) const {
    return static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                      (*_reader)[segment].docs_count());
  }

  template<typename Visit>
  static void ForEachBit(std::span<const uint64_t> mask, irs::doc_id_t base,
                         Visit&& visit) {
    for (size_t w = 0; w != mask.size(); ++w) {
      for (auto bits = mask[w]; bits; bits = irs::PopBit(bits)) {
        visit(base +
              static_cast<irs::doc_id_t>(w * 64 + std::countr_zero(bits)));
      }
    }
  }

  irs::MemoryDirectory _dir;
  irs::DirectoryReader _reader;
};

irs::Filter::ptr ToFilter(std::optional<irs::ShinglePhrasePlan>&& plan) {
  if (!plan) {
    return nullptr;
  }
  if (auto* term = std::get_if<irs::bstring>(&*plan)) {
    auto filter = std::make_unique<irs::ByTerm>();
    *filter->mutable_field_id() = kShingleId;
    filter->mutable_options()->term = std::move(*term);
    return filter;
  }
  auto filter = std::make_unique<irs::ByPhrase>();
  *filter->mutable_field_id() = kShingleId;
  *filter->mutable_options() = std::get<irs::ByPhraseOptions>(std::move(*plan));
  return filter;
}

irs::ByPhrase PlainPhrase(irs::field_id field, std::string_view text,
                          bool verified) {
  irs::ByPhrase filter;
  *filter.mutable_field_id() = field;
  *filter.mutable_options() = Phrase(text);
  if (verified) {
    filter.mutable_options()->set_verifier(
      std::make_shared<irs::PhraseVerifier>(Stored()));
  }
  return filter;
}

irs::ByPhrase PrefixPhrase(std::string_view prefix, std::string_view word,
                           std::string_view separator) {
  irs::ByPhrase filter;
  *filter.mutable_field_id() = kShingleId;
  auto& options = *filter.mutable_options();
  options.push_back<irs::ByPrefixOptions>().term =
    irs::ViewCast<irs::byte_type>(prefix);
  options.push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(word);
  options.set_word_separator(irs::ViewCast<irs::byte_type>(separator));
  return filter;
}

}  // namespace

TEST(ShinglePhrasePlanTest, exact_terms) {
  const auto shingles = MakeShingles(2, 2);
  auto plan = Plan(*shingles, "quick brown", false);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ("quick brown", TermOf(plan));

  plan = Plan(*shingles, "quick", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ("quick", TermOf(plan));

  const auto no_unigrams = MakeShingles(2, 2, false);
  EXPECT_FALSE(Plan(*no_unigrams, "quick", true).has_value());
}

TEST(ShinglePhrasePlanTest, positional_cover_takes_overlapping_tail) {
  const auto shingles = MakeShingles(2, 2);
  auto plan = Plan(*shingles, "quick brown fox", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(nullptr, PhraseOf(plan).verifier());
  EXPECT_EQ((std::vector<std::string>{"quick brown", "brown fox"}),
            Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 1}), Offsets(PhraseOf(plan)));

  plan = Plan(*shingles, "a b c d e", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"a b", "c d", "d e"}),
            Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 2, 1}), Offsets(PhraseOf(plan)));

  const auto wide = MakeShingles(2, 3);
  plan = Plan(*wide, "a b c d", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"a b c", "b c d"}),
            Terms(PhraseOf(plan)));
}

TEST(ShinglePhrasePlanTest, positional_cover_keeps_repeated_terms) {
  const auto shingles = MakeShingles(2, 2);
  const auto plan = Plan(*shingles, "the cat the cat", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"the cat", "the cat"}),
            Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 2}), Offsets(PhraseOf(plan)));
}

TEST(ShinglePhrasePlanTest, verify_cover_dedups_windows) {
  const auto shingles = MakeShingles(2, 2);
  auto plan = Plan(*shingles, "the cat the cat", false);
  ASSERT_TRUE(plan.has_value());
  ASSERT_NE(nullptr, PhraseOf(plan).verifier());
  EXPECT_EQ((std::vector<std::string>{"cat the", "the cat"}),
            Terms(PhraseOf(plan)));

  plan = Plan(*shingles, "a b c d e", false);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"a b", "b c", "c d", "d e"}),
            Terms(PhraseOf(plan)));

  const auto wide = MakeShingles(2, 3);
  plan = Plan(*wide, "a b c d e", false);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"a b c", "b c d", "c d e"}),
            Terms(PhraseOf(plan)));
}

TEST(ShinglePhrasePlanTest, frequent_words_limit_wide_windows) {
  const auto shingles = MakeShingles(2, 3, true, {"the"});
  auto plan = Plan(*shingles, "the quick brown", false);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ("the quick brown", TermOf(plan));

  plan = Plan(*shingles, "quick brown fox", false);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"brown fox", "quick brown"}),
            Terms(PhraseOf(plan)));

  plan = Plan(*shingles, "the quick brown fox", false);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"brown fox", "the quick brown"}),
            Terms(PhraseOf(plan)));
}

TEST(ShinglePhrasePlanTest, gaps_split_runs) {
  const auto shingles = MakeShingles(2, 2);
  auto phrase = Phrase("a b");
  phrase.push_back<irs::ByTermOptions>(1).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"c"});
  phrase.push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"d"});
  const auto plan = irs::PlanShinglePhrase(*shingles, phrase, true, nullptr);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"a b", "c d"}), Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 3}), Offsets(PhraseOf(plan)));
}

TEST(ShinglePhrasePlanTest, partial_cover_keeps_other_parts) {
  const auto shingles = MakeShingles(2, 2);

  auto trailing = Phrase("quick brown fox");
  PushPrefix(trailing, "ju", 1, 1);
  auto plan = irs::PlanShinglePhrase(*shingles, trailing, true, nullptr);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"quick brown", "brown fox", "ju*"}),
            Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 1, 2}), Offsets(PhraseOf(plan)));
  EXPECT_EQ(" ", Text(PhraseOf(plan).word_separator()));

  irs::ByPhraseOptions leading;
  PushPrefix(leading, "qu", 0, 0);
  PushTerm(leading, "brown", 1, 1);
  PushTerm(leading, "fox", 1, 1);
  plan = irs::PlanShinglePhrase(*shingles, leading, true, nullptr);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"qu*", "brown fox"}),
            Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 1}), Offsets(PhraseOf(plan)));

  auto interval = Phrase("quick brown");
  PushTerm(interval, "lazy", 2, 4);
  PushTerm(interval, "dog", 1, 1);
  plan = irs::PlanShinglePhrase(*shingles, interval, true, nullptr);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"quick brown", "lazy dog"}),
            Terms(PhraseOf(plan)));
  const auto& lazy_dog = *std::next(PhraseOf(plan).begin());
  EXPECT_EQ(3U, lazy_dog.offs_min);
  EXPECT_EQ(5U, lazy_dog.offs_max);
  EXPECT_TRUE(PhraseOf(plan).word_separator().empty());
}

TEST(ShinglePhrasePlanTest, partial_cover_needs_words_for_patterns) {
  auto phrase = Phrase("quick brown");
  PushPrefix(phrase, "fo", 1, 1);

  const auto bare = MakeShingles(2, 2, false);
  EXPECT_FALSE(
    irs::PlanShinglePhrase(*bare, phrase, true, nullptr).has_value());

  const ShingleTokenizer joined{std::make_unique<WhitespaceTokenizer>(),
                                {
                                  .min_shingle_size = 2,
                                  .max_shingle_size = 2,
                                  .token_separator = {},
                                }};
  EXPECT_FALSE(
    irs::PlanShinglePhrase(joined, phrase, true, nullptr).has_value());
}

TEST(ShinglePhrasePlanTest, declines_without_shingle_gain) {
  const auto shingles = MakeShingles(2, 2);
  const auto plans = [&](const irs::ByPhraseOptions& phrase) {
    return irs::PlanShinglePhrase(*shingles, phrase, true, nullptr).has_value();
  };
  const auto brown = irs::ViewCast<irs::byte_type>(std::string_view{"brown"});

  auto sloppy = Phrase("quick brown");
  sloppy.set_slop(1);
  EXPECT_FALSE(plans(sloppy));

  auto interval = Phrase("quick");
  interval.push_back<irs::ByTermOptions>(1, 2).term = brown;
  EXPECT_FALSE(plans(interval));

  auto stacked = Phrase("quick");
  stacked.push_back<irs::ByTermOptions>(0, 0).term = brown;
  EXPECT_FALSE(plans(stacked));

  auto alternatives = Phrase("quick");
  alternatives.push_back<irs::TermSetOptions>().terms.emplace(brown);
  EXPECT_FALSE(plans(alternatives));
}

TEST(ShinglePhrasePlanTest, needs_a_source_to_verify) {
  const auto shingles = MakeShingles(2, 2);
  EXPECT_FALSE(
    irs::PlanShinglePhrase(*shingles, Phrase("quick brown fox"), false, nullptr)
      .has_value());
}

TEST(ShinglePhraseIndexTest, verified_and_positional_covers_match) {
  static constexpr std::string_view kDocs[] = {
    "quick brown fox jumps", "quick brown cat brown fox", "the quick brown fox",
    "brown fox quick",       "fox brown quick brown fox", "lazy dog",
  };
  for (const bool positional : {false, true}) {
    SCOPED_TRACE(positional);
    auto shingles = MakeShingles(2, 2);
    const Index index{kDocs, *shingles,
                      positional
                        ? irs::IndexFeatures::Freq | irs::IndexFeatures::Pos
                        : irs::IndexFeatures::Freq};
    const auto docs = [&](std::string_view text) {
      auto filter = ToFilter(Plan(*shingles, text, positional));
      EXPECT_NE(nullptr, filter);
      return filter ? index.Docs(*filter) : std::vector<irs::doc_id_t>{};
    };
    EXPECT_EQ((std::vector<irs::doc_id_t>{0, 2, 4}), docs("quick brown fox"));
    EXPECT_EQ((std::vector<irs::doc_id_t>{0, 1, 2, 3, 4}), docs("brown fox"));
    EXPECT_EQ((std::vector<irs::doc_id_t>{3}), docs("brown fox quick"));
    EXPECT_EQ((std::vector<irs::doc_id_t>{1}),
              docs("quick brown cat brown fox"));
    EXPECT_EQ((std::vector<irs::doc_id_t>{}), docs("fox jumps quick"));
    EXPECT_EQ((std::vector<irs::doc_id_t>{0}), docs("brown fox jumps"));
  }
}

TEST(ShinglePhraseIndexTest, pattern_parts_skip_shingles) {
  static constexpr std::string_view kDocs[] = {"quick brown fox",
                                               "quick bread"};
  auto shingles = MakeShingles(2, 2);
  const Index index{kDocs, *shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
  EXPECT_EQ((std::vector<irs::doc_id_t>{0}),
            index.Docs(PrefixPhrase("quick b", "brown", "")));
  EXPECT_EQ((std::vector<irs::doc_id_t>{}),
            index.Docs(PrefixPhrase("quick b", "brown", " ")));
  EXPECT_EQ((std::vector<irs::doc_id_t>{0}),
            index.Docs(PrefixPhrase("qu", "brown", " ")));
}

TEST(ShinglePhraseIndexTest, pattern_parts_find_words_after_shingles) {
  static constexpr std::string_view kDocs[] = {"fox den", "foxes den",
                                               "fox run", "fo den"};
  auto shingles = MakeShingles(2, 2);
  const Index index{kDocs, *shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
  EXPECT_EQ((std::vector<irs::doc_id_t>{0, 1, 3}),
            index.Docs(PrefixPhrase("fo", "den", " ")));
  EXPECT_EQ((std::vector<irs::doc_id_t>{0, 1}),
            index.Docs(PrefixPhrase("fox", "den", " ")));
  EXPECT_EQ((std::vector<irs::doc_id_t>{}),
            index.Docs(PrefixPhrase("fox d", "den", " ")));
}

TEST(ShinglePhraseIndexTest, partial_cover_agrees_with_positions) {
  static constexpr std::string_view kWords[] = {"a", "b", "c", "d", "e"};
  std::mt19937 rng{7};
  std::vector<std::string> texts;
  for (size_t i = 0; i != 300; ++i) {
    std::string text;
    const auto length = 3 + rng() % 10;
    for (size_t j = 0; j != length; ++j) {
      absl::StrAppend(&text, j == 0 ? "" : " ", kWords[rng() % 5]);
    }
    texts.push_back(std::move(text));
  }
  std::vector<std::string_view> docs{texts.begin(), texts.end()};
  auto shingles = MakeShingles(2, 3);
  const Index index{docs, *shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};

  for (size_t i = 0; i != 300; ++i) {
    irs::ByPhraseOptions phrase;
    const auto slots = 2 + rng() % 4;
    for (size_t j = 0; j != slots; ++j) {
      irs::PosAttr::value_t offs_min = j == 0 ? 0 : 1 + rng() % 2;
      irs::PosAttr::value_t offs_max = offs_min + (rng() % 5 == 0 ? 1 : 0);
      const auto word = kWords[rng() % 5];
      if (rng() % 4 == 0) {
        PushPrefix(phrase, word, offs_min, offs_max);
      } else {
        PushTerm(phrase, word, offs_min, offs_max);
      }
    }
    irs::ByPhrase positional;
    *positional.mutable_field_id() = kPositionalId;
    *positional.mutable_options() = phrase;

    auto plan = irs::PlanShinglePhrase(*shingles, phrase, true, nullptr);
    auto filter = ToFilter(std::move(plan));
    if (!filter) {
      auto fallback = std::make_unique<irs::ByPhrase>();
      *fallback->mutable_field_id() = kShingleId;
      *fallback->mutable_options() = phrase;
      fallback->mutable_options()->set_word_separator(
        irs::ViewCast<irs::byte_type>(std::string_view{" "}));
      filter = std::move(fallback);
    }
    SCOPED_TRACE(i);
    EXPECT_EQ(index.Docs(positional), index.Docs(*filter));
  }
}

TEST(ShinglePhraseIndexTest, verified_phrase_scores_by_phrase_frequency) {
  static constexpr std::string_view kDocs[] = {
    "quick brown fox and quick brown fox",
    "quick brown fox and some other words",
    "quick brown cat brown fox and words",
  };
  auto shingles = MakeShingles(2, 2);
  const Index index{kDocs, *shingles, irs::IndexFeatures::Freq};
  auto filter = ToFilter(Plan(*shingles, "quick brown fox", false));
  ASSERT_NE(nullptr, filter);
  const auto scores = index.Scores(*filter);
  ASSERT_EQ(2U, scores.size());
  EXPECT_GT(scores.at(0), scores.at(1));
}

TEST(ShinglePhraseIndexTest, verified_phrase_agrees_with_positions) {
  static constexpr std::string_view kWords[] = {"a", "b", "c", "d", "e"};
  std::mt19937 rng{42};
  std::vector<std::string> texts;
  for (size_t i = 0; i != 300; ++i) {
    std::string text;
    const auto length = 3 + rng() % 10;
    for (size_t j = 0; j != length; ++j) {
      absl::StrAppend(&text, j == 0 ? "" : " ", kWords[rng() % 5]);
    }
    texts.push_back(std::move(text));
  }
  std::vector<std::string_view> docs{texts.begin(), texts.end()};
  auto shingles = MakeShingles(2, 3);
  const Index index{docs, *shingles, irs::IndexFeatures::Freq};

  for (size_t i = 0; i != 200; ++i) {
    std::string phrase;
    const auto length = 2 + rng() % 4;
    for (size_t j = 0; j != length; ++j) {
      absl::StrAppend(&phrase, j == 0 ? "" : " ", kWords[rng() % 5]);
    }
    SCOPED_TRACE(phrase);
    const auto positional = PlainPhrase(kPositionalId, phrase, false);
    const auto verified = PlainPhrase(kPlainId, phrase, true);
    EXPECT_EQ(index.Docs(positional), index.Docs(verified));
    const auto expected = index.Scores(positional);
    const auto actual = index.Scores(verified);
    ASSERT_EQ(expected.size(), actual.size());
    for (const auto& [doc, score] : expected) {
      EXPECT_FLOAT_EQ(score, actual.at(doc));
    }
    auto shingle_filter = ToFilter(Plan(*shingles, phrase, false));
    ASSERT_NE(nullptr, shingle_filter);
    EXPECT_EQ(index.Docs(positional), index.Docs(*shingle_filter));
  }
}

TEST(ShinglePhraseIndexTest, verified_phrase_runs_in_every_family) {
  static constexpr std::string_view kWords[] = {"a", "b", "c", "d", "e"};
  std::mt19937 rng{11};
  std::vector<std::string> texts;
  for (size_t i = 0; i != 600; ++i) {
    std::string text;
    const auto length = 3 + rng() % 12;
    for (size_t j = 0; j != length; ++j) {
      absl::StrAppend(&text, j == 0 ? "" : " ", kWords[rng() % 5]);
    }
    texts.push_back(std::move(text));
  }
  std::vector<std::string_view> docs{texts.begin(), texts.end()};
  auto shingles = MakeShingles(2, 3);
  const Index index{docs, *shingles, irs::IndexFeatures::Freq};

  for (size_t i = 0; i != 120; ++i) {
    irs::ByPhraseOptions phrase;
    const auto slots = 2 + rng() % 3;
    const bool sloppy = i % 4 == 3;
    for (size_t j = 0; j != slots; ++j) {
      irs::PosAttr::value_t offs_min =
        j == 0 ? 0 : 1 + (sloppy ? 0 : rng() % 2);
      irs::PosAttr::value_t offs_max =
        offs_min + (!sloppy && rng() % 5 == 0 ? 1 : 0);
      const auto word = kWords[rng() % 5];
      switch (rng() % 6) {
        case 0:
          PushPrefix(phrase, word, offs_min, offs_max);
          break;
        case 1: {
          auto& set = phrase.push_back<irs::TermSetOptions>(offs_min, offs_max);
          set.terms.emplace(irs::ViewCast<irs::byte_type>(word));
          set.terms.emplace(irs::ViewCast<irs::byte_type>(kWords[rng() % 5]));
        } break;
        default:
          PushTerm(phrase, word, offs_min, offs_max);
      }
    }
    if (sloppy) {
      phrase.set_slop(1 + rng() % 2);
    }
    std::string shape = absl::StrCat("slop=", phrase.slop());
    for (const auto& info : phrase) {
      absl::StrAppend(&shape, " [", info.offs_min, ",", info.offs_max, "]");
      if (const auto* term = std::get_if<irs::ByTermOptions>(&info.part)) {
        absl::StrAppend(&shape, Text(term->term));
      } else if (const auto* prefix =
                   std::get_if<irs::ByPrefixOptions>(&info.part)) {
        absl::StrAppend(&shape, Text(prefix->term), "*");
      } else {
        absl::StrAppend(&shape, "{");
        for (const auto& term :
             std::get<irs::TermSetOptions>(info.part).terms) {
          absl::StrAppend(&shape, Text(term), ",");
        }
        absl::StrAppend(&shape, "}");
      }
    }
    SCOPED_TRACE(shape);

    irs::ByPhrase positional;
    *positional.mutable_field_id() = kPositionalId;
    *positional.mutable_options() = phrase;
    irs::ByPhrase verified;
    *verified.mutable_field_id() = kPlainId;
    *verified.mutable_options() = phrase;
    verified.mutable_options()->set_verifier(
      std::make_shared<irs::PhraseVerifier>(Stored()));

    const auto expected = index.Run(positional);
    EXPECT_EQ(index.Docs(positional), expected.docs);
    ExpectFamilies(expected, index.Run(verified));
  }

  for (const auto text : {"a b c", "a b c d", "c a b a", "e e e"}) {
    SCOPED_TRACE(text);
    auto filter = ToFilter(Plan(*shingles, text, false));
    ASSERT_NE(nullptr, filter);
    const auto expected = index.Run(PlainPhrase(kPositionalId, text, false));
    const auto actual = index.Run(*filter);
    EXPECT_EQ(expected.count, actual.count);
    EXPECT_EQ(expected.docs, actual.docs);
    EXPECT_EQ(expected.docs, actual.fill);
    EXPECT_EQ(expected.docs, actual.probe);
    EXPECT_EQ(expected.top_total, actual.top_total);
  }
}

TEST(ShinglePhraseFilterTest, equality_covers_separator_and_verifier) {
  const auto base = Phrase("quick brown");
  auto separated = base;
  separated.set_word_separator(
    irs::ViewCast<irs::byte_type>(std::string_view{" "}));
  EXPECT_NE(base, separated);
  auto other = base;
  other.set_word_separator(
    irs::ViewCast<irs::byte_type>(std::string_view{"_"}));
  EXPECT_NE(separated, other);

  const auto verifier = std::make_shared<irs::PhraseVerifier>(Stored());
  auto verified = base;
  verified.set_verifier(verifier);
  EXPECT_NE(base, verified);
  auto twin = base;
  twin.set_verifier(std::make_shared<irs::PhraseVerifier>(Stored()));
  EXPECT_EQ(verified, twin);
  auto shared = base;
  shared.set_verifier(verifier);
  EXPECT_EQ(verified, shared);

  auto elsewhere = base;
  auto text = Stored();
  text.column = kPlainId;
  elsewhere.set_verifier(std::make_shared<irs::PhraseVerifier>(text));
  EXPECT_NE(verified, elsewhere);
  auto spec = base;
  spec.set_verifier(
    std::make_shared<irs::PhraseVerifier>(Stored(), Phrase("quick brown fox")));
  EXPECT_NE(verified, spec);

  verified.clear();
  separated.clear();
  EXPECT_EQ(irs::ByPhraseOptions{}, verified);
  EXPECT_EQ(irs::ByPhraseOptions{}, separated);
}

TEST(ShinglePhraseFilterTest, simplify_keeps_one_slot_phrases_that_filter) {
  const auto lower = [](irs::ByPhraseOptions options) {
    auto filter = std::make_unique<irs::ByPhrase>();
    *filter->mutable_field_id() = kShingleId;
    *filter->mutable_options() = std::move(options);
    irs::Filter::ptr root = std::move(filter);
    irs::Optimize(root);
    return root;
  };
  const auto is_phrase = [](const irs::Filter::ptr& filter) {
    return filter->type() == irs::Type<irs::ByPhrase>::id();
  };

  EXPECT_FALSE(is_phrase(lower(Phrase("quick"))));

  auto verified = Phrase("quick");
  verified.set_verifier(std::make_shared<irs::PhraseVerifier>(Stored()));
  EXPECT_TRUE(is_phrase(lower(verified)));

  irs::ByPhraseOptions prefix;
  PushPrefix(prefix, "qu", 0, 0);
  EXPECT_EQ(irs::Type<irs::ByPrefix>::id(), lower(prefix)->type());
  prefix.set_word_separator(
    irs::ViewCast<irs::byte_type>(std::string_view{" "}));
  EXPECT_TRUE(is_phrase(lower(prefix)));
}

TEST(ShinglePhraseIndexTest, one_slot_pattern_skips_shingles) {
  static constexpr std::string_view kDocs[] = {"quick brown", "quiet fox",
                                               "lazy quick"};
  auto shingles = MakeShingles(2, 2);
  const Index index{kDocs, *shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
  const auto docs = [&](std::string_view prefix) {
    auto phrase = std::make_unique<irs::ByPhrase>();
    *phrase->mutable_field_id() = kShingleId;
    PushPrefix(*phrase->mutable_options(), prefix, 0, 0);
    phrase->mutable_options()->set_word_separator(
      irs::ViewCast<irs::byte_type>(std::string_view{" "}));
    irs::Filter::ptr filter = std::move(phrase);
    irs::Optimize(filter);
    const auto out = index.Docs(*filter);
    EXPECT_EQ(out.size(), index.Run(*filter).count);
    return out;
  };
  EXPECT_EQ((std::vector<irs::doc_id_t>{}), docs("quick "));
  EXPECT_EQ((std::vector<irs::doc_id_t>{0, 1, 2}), docs("qu"));
  EXPECT_EQ((std::vector<irs::doc_id_t>{0}), docs("br"));
}

TEST(ShinglePhraseIndexTest, verified_phrase_without_stored_column) {
  static constexpr std::string_view kDocs[] = {"quick brown fox"};
  auto shingles = MakeShingles(2, 2);
  const Index index{kDocs, *shingles, irs::IndexFeatures::Freq};
  irs::ByPhrase filter;
  *filter.mutable_field_id() = kPlainId;
  *filter.mutable_options() = Phrase("quick brown");
  auto text = Stored();
  text.column = 99;
  filter.mutable_options()->set_verifier(
    std::make_shared<irs::PhraseVerifier>(text));
  EXPECT_EQ((std::vector<irs::doc_id_t>{}), index.Docs(filter));
  EXPECT_EQ(0U, index.Run(filter).count);
  EXPECT_TRUE(index.Run(filter).hits.empty());
}

TEST(ShinglePhraseIndexTest, every_pattern_kind_skips_shingles) {
  static constexpr std::string_view kWords[] = {"ab", "ac", "bc", "bd", "cd"};
  std::mt19937 rng{23};
  std::vector<std::string> texts;
  for (size_t i = 0; i != 300; ++i) {
    std::string text;
    const auto length = 2 + rng() % 8;
    for (size_t j = 0; j != length; ++j) {
      absl::StrAppend(&text, j == 0 ? "" : " ", kWords[rng() % 5]);
    }
    texts.push_back(std::move(text));
  }
  std::vector<std::string_view> docs{texts.begin(), texts.end()};
  auto shingles = MakeShingles(2, 3);
  const Index index{docs, *shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
  const auto bytes = [](std::string_view text) {
    return irs::bstring{irs::ViewCast<irs::byte_type>(text)};
  };
  const auto run = [&](irs::field_id field, const irs::ByPhraseOptions& phrase,
                       bool separated) {
    auto filter = std::make_unique<irs::ByPhrase>();
    *filter->mutable_field_id() = field;
    *filter->mutable_options() = phrase;
    if (separated) {
      filter->mutable_options()->set_word_separator(bytes(" "));
    }
    irs::Filter::ptr root = std::move(filter);
    irs::Optimize(root);
    return index.Docs(*root);
  };

  size_t matched = 0;
  size_t unseparated = 0;
  for (size_t i = 0; i != 300; ++i) {
    irs::ByPhraseOptions phrase;
    std::string shape;
    const auto slots = 1 + rng() % 3;
    for (size_t j = 0; j != slots; ++j) {
      const irs::PosAttr::value_t offs = j == 0 ? 0 : 1;
      const auto word = kWords[rng() % 5];
      switch (rng() % 5) {
        case 0: {
          const auto pattern = rng() % 2 ? absl::StrCat("%", word.substr(1))
                                         : absl::StrCat(word.substr(0, 1), "%");
          phrase.push_back<irs::ByWildcardOptions>(offs, offs) =
            irs::ByWildcardOptions{bytes(pattern)};
          absl::StrAppend(&shape, " like:", pattern);
        } break;
        case 1:
        case 2: {
          auto& fuzzy =
            phrase.push_back<irs::ByEditDistanceOptions>(offs, offs);
          fuzzy.term = bytes(word);
          fuzzy.max_distance = 2;
          fuzzy.max_terms = rng() % 2 ? 0 : 3;
          absl::StrAppend(&shape, " fuzzy:", word, "/", fuzzy.max_terms);
        } break;
        case 3: {
          auto& range = phrase.push_back<irs::ByRangeOptions>(offs, offs);
          range.range.min = bytes(word.substr(0, 1));
          range.range.max = bytes(absl::StrCat(word.substr(0, 1), "z"));
          range.range.min_type = irs::BoundType::Inclusive;
          range.range.max_type = irs::BoundType::Inclusive;
          absl::StrAppend(&shape, " range:", word.substr(0, 1));
        } break;
        default:
          PushTerm(phrase, word, offs, offs);
          absl::StrAppend(&shape, " ", word);
      }
    }
    SCOPED_TRACE(shape);
    const auto expected = run(kPositionalId, phrase, false);
    matched += !expected.empty();
    unseparated += expected != run(kShingleId, phrase, false);
    EXPECT_EQ(expected, run(kShingleId, phrase, true));
  }
  EXPECT_GT(matched, 100U);
  EXPECT_GT(unseparated, 10U);
}
