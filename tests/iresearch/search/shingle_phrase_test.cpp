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
#include <absl/strings/numbers.h>
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
#include <type_traits>
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

class PositionedTokenizer final
  : public irs::analysis::TypedTokenizer<PositionedTokenizer> {
 public:
  irs::TokenTraits Traits() const noexcept final {
    return {.explicit_pos = true};
  }

  static constexpr std::string_view type_name() noexcept {
    return "test_positioned";
  }

  template<irs::TokenLayout L>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view data{raw.GetData(), raw.GetSize()};
    for (const auto token : absl::StrSplit(data, ' ', absl::SkipEmpty())) {
      const auto at = token.rfind('@');
      uint32_t pos = 0;
      if (at == std::string_view::npos ||
          !absl::SimpleAtoi(token.substr(at + 1), &pos)) {
        return false;
      }
      sink.Emit<L>(irs::MakeTermView(token.substr(0, at)), pos);
    }
    return true;
  }
};

std::unique_ptr<ShingleTokenizer> MakeShingles(
  uint32_t min, uint32_t max, bool unigrams = true,
  std::vector<std::string_view> frequent = {},
  std::string_view separator = " ") {
  ShingleTokenizer::Options options{
    .min_shingle_size = min,
    .max_shingle_size = max,
    .output_unigrams = unigrams,
    .token_separator = irs::bstring{irs::ViewCast<irs::byte_type>(separator)},
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

std::string RandomText(std::mt19937& rng,
                       std::span<const std::string_view> words,
                       size_t min_length, size_t spread) {
  std::string text;
  const auto length = min_length + rng() % spread;
  for (size_t j = 0; j != length; ++j) {
    absl::StrAppend(&text, j == 0 ? "" : " ", words[rng() % words.size()]);
  }
  return text;
}

std::vector<std::string> RandomTexts(std::mt19937& rng,
                                     std::span<const std::string_view> words,
                                     size_t count, size_t min_length,
                                     size_t spread) {
  std::vector<std::string> texts;
  texts.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    texts.push_back(RandomText(rng, words, min_length, spread));
  }
  return texts;
}

inline constexpr irs::field_id kShingleId = 2;
inline constexpr irs::field_id kPlainId = 3;
inline constexpr irs::field_id kPositionalId = 4;
inline constexpr size_t kTop = 5;

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
  return irs::PlanShinglePhrase(shingles, Phrase(text), positional);
}

struct Field {
  irs::field_id Id() const { return id; }

  irs::analysis::Tokenizer& GetTokens() const { return *analyzer; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept { return features; }

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

void ExpectConsistent(const Families& families) {
  EXPECT_EQ(families.docs.size(), families.count);
  EXPECT_EQ(families.docs, families.fill);
  EXPECT_EQ(families.docs, families.probe);
  std::vector<irs::doc_id_t> scored;
  std::vector<irs::score_t> best;
  for (const auto& [doc, score] : families.hits) {
    scored.push_back(doc);
    best.push_back(score);
  }
  EXPECT_EQ(families.docs, scored);
  {
    SCOPED_TRACE("fill");
    ExpectScores(families.hits, families.fill_scores);
  }
  {
    SCOPED_TRACE("probe");
    ExpectScores(families.hits, families.probe_scores);
  }
  EXPECT_EQ(families.count, families.top_total);
  absl::c_sort(best, std::greater<>{});
  best.resize(std::min(best.size(), kTop));
  ASSERT_EQ(best.size(), families.top.size());
  for (size_t i = 0; i != best.size(); ++i) {
    EXPECT_FLOAT_EQ(best[i], families.top[i]) << i;
  }
}

class Index {
 public:
  template<typename Words = WhitespaceTokenizer>
  Index(std::span<const std::string_view> docs, ShingleTokenizer& shingles,
        irs::IndexFeatures shingle_features, std::type_identity<Words> = {}) {
    auto writer = irs::IndexWriter::Make(_dir, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    EXPECT_NE(nullptr, writer);
    Words plain;
    Words positional;
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
    }
    ctx.Commit();
    writer->RefreshCommit();
    _reader = irs::DirectoryReader{_dir, irs::tests::DefaultReaderOptions()};
    EXPECT_EQ(1U, _reader->size());
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

  std::vector<irs::doc_id_t> PhraseDocs(irs::field_id field,
                                        const irs::ByPhraseOptions& phrase,
                                        std::string_view separator = {}) const {
    auto filter = std::make_unique<irs::ByPhrase>();
    *filter->mutable_field_id() = field;
    *filter->mutable_options() = phrase;
    filter->mutable_options()->set_word_separator(
      irs::ViewCast<irs::byte_type>(separator));
    irs::Filter::ptr root = std::move(filter);
    irs::Optimize(root);
    return Docs(*root);
  }

  std::map<irs::doc_id_t, irs::score_t> Scores(
    const irs::Filter& filter) const {
    return ScoresBy(irs::BM25{}, filter);
  }

  std::map<irs::doc_id_t, irs::score_t> Freqs(const irs::Filter& filter) const {
    return ScoresBy(tests::sort::FrequencyScore{}, filter);
  }

  std::map<irs::doc_id_t, irs::score_t> ScoresBy(
    const irs::Scorer& scorer, const irs::Filter& filter) const {
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

irs::ByPhrase PlainPhrase(irs::field_id field, std::string_view text) {
  irs::ByPhrase filter;
  *filter.mutable_field_id() = field;
  *filter.mutable_options() = Phrase(text);
  return filter;
}

irs::Filter::ptr ShingleFilter(const ShingleTokenizer& shingles,
                               const irs::ByPhraseOptions& phrase) {
  if (auto filter = ToFilter(irs::PlanShinglePhrase(shingles, phrase, true))) {
    return filter;
  }
  auto fallback = std::make_unique<irs::ByPhrase>();
  *fallback->mutable_field_id() = kShingleId;
  *fallback->mutable_options() = phrase;
  fallback->mutable_options()->set_word_separator(
    irs::ViewCast<irs::byte_type>(std::string_view{" "}));
  return fallback;
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

TEST(ShinglePhrasePlanTest, several_windows_need_positions) {
  const auto shingles = MakeShingles(2, 2);
  EXPECT_FALSE(Plan(*shingles, "the cat the cat", false).has_value());
  auto plan = Plan(*shingles, "the cat the cat", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"the cat", "the cat"}),
            Terms(PhraseOf(plan)));

  EXPECT_FALSE(Plan(*shingles, "a b c d e", false).has_value());
  plan = Plan(*shingles, "a b c d e", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"a b", "c d", "d e"}),
            Terms(PhraseOf(plan)));

  const auto wide = MakeShingles(2, 3);
  EXPECT_FALSE(Plan(*wide, "a b c d e", false).has_value());
  plan = Plan(*wide, "a b c d e", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"a b c", "d e"}), Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 3}), Offsets(PhraseOf(plan)));
}

TEST(ShinglePhrasePlanTest, frequent_words_limit_wide_windows) {
  const auto shingles = MakeShingles(2, 3, true, {"the"});
  auto plan = Plan(*shingles, "the quick brown", false);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ("the quick brown", TermOf(plan));

  EXPECT_FALSE(Plan(*shingles, "quick brown fox", false).has_value());
  plan = Plan(*shingles, "quick brown fox", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"quick brown", "brown fox"}),
            Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 1}), Offsets(PhraseOf(plan)));

  plan = Plan(*shingles, "the quick brown fox", true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"the quick brown", "brown fox"}),
            Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 2}), Offsets(PhraseOf(plan)));
}

TEST(ShinglePhrasePlanTest, gaps_split_runs) {
  const auto shingles = MakeShingles(2, 2);
  auto phrase = Phrase("a b");
  phrase.push_back<irs::ByTermOptions>(1).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"c"});
  phrase.push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"d"});
  const auto plan = irs::PlanShinglePhrase(*shingles, phrase, true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"a b", "c d"}), Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 3}), Offsets(PhraseOf(plan)));
}

TEST(ShinglePhrasePlanTest, partial_cover_keeps_other_parts) {
  const auto shingles = MakeShingles(2, 2);

  auto trailing = Phrase("quick brown fox");
  PushPrefix(trailing, "ju", 1, 1);
  auto plan = irs::PlanShinglePhrase(*shingles, trailing, true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"quick brown", "brown fox", "ju*"}),
            Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 1, 2}), Offsets(PhraseOf(plan)));
  EXPECT_EQ(" ", Text(PhraseOf(plan).word_separator()));

  irs::ByPhraseOptions leading;
  PushPrefix(leading, "qu", 0, 0);
  PushTerm(leading, "brown", 1, 1);
  PushTerm(leading, "fox", 1, 1);
  plan = irs::PlanShinglePhrase(*shingles, leading, true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"qu*", "brown fox"}),
            Terms(PhraseOf(plan)));
  EXPECT_EQ((std::vector<uint32_t>{0, 1}), Offsets(PhraseOf(plan)));

  auto interval = Phrase("quick brown");
  PushTerm(interval, "lazy", 2, 4);
  PushTerm(interval, "dog", 1, 1);
  plan = irs::PlanShinglePhrase(*shingles, interval, true);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ((std::vector<std::string>{"quick brown", "lazy dog"}),
            Terms(PhraseOf(plan)));
  const auto& lazy_dog = *std::next(PhraseOf(plan).begin());
  EXPECT_EQ(3U, lazy_dog.offs_min);
  EXPECT_EQ(5U, lazy_dog.offs_max);
  EXPECT_EQ(" ", Text(PhraseOf(plan).word_separator()));
}

TEST(ShinglePhrasePlanTest, partial_cover_needs_words_for_patterns) {
  auto phrase = Phrase("quick brown");
  PushPrefix(phrase, "fo", 1, 1);

  const auto bare = MakeShingles(2, 2, false);
  EXPECT_FALSE(irs::PlanShinglePhrase(*bare, phrase, true).has_value());

  const ShingleTokenizer joined{std::make_unique<WhitespaceTokenizer>(),
                                {
                                  .min_shingle_size = 2,
                                  .max_shingle_size = 2,
                                  .token_separator = {},
                                }};
  EXPECT_FALSE(irs::PlanShinglePhrase(joined, phrase, true).has_value());
}

TEST(ShinglePhrasePlanTest, declines_without_shingle_gain) {
  const auto shingles = MakeShingles(2, 2);
  const auto plans = [&](const irs::ByPhraseOptions& phrase) {
    return irs::PlanShinglePhrase(*shingles, phrase, true).has_value();
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

TEST(ShinglePhrasePlanTest, max_gram_decides_without_positions) {
  const auto phrase = Phrase("quick brown fox");
  const auto narrow = MakeShingles(2, 2);
  EXPECT_FALSE(irs::PlanShinglePhrase(*narrow, phrase, false).has_value());
  EXPECT_TRUE(irs::PlanShinglePhrase(*narrow, phrase, true).has_value());

  const auto wide = MakeShingles(2, 3);
  const auto plan = irs::PlanShinglePhrase(*wide, phrase, false);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ("quick brown fox", TermOf(plan));
}

TEST(ShinglePhraseIndexTest, covers_match_with_and_without_positions) {
  using Docs = std::optional<std::vector<irs::doc_id_t>>;
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
    const auto docs = [&](std::string_view text) -> Docs {
      auto filter = ToFilter(Plan(*shingles, text, positional));
      if (!filter) {
        return std::nullopt;
      }
      return index.Docs(*filter);
    };
    const auto covered = [&](std::vector<irs::doc_id_t> expected) -> Docs {
      if (!positional) {
        return std::nullopt;
      }
      return expected;
    };
    EXPECT_EQ(covered({0, 2, 4}), docs("quick brown fox"));
    EXPECT_EQ((Docs{{0, 1, 2, 3, 4}}), docs("brown fox"));
    EXPECT_EQ(covered({3}), docs("brown fox quick"));
    EXPECT_EQ(covered({1}), docs("quick brown cat brown fox"));
    EXPECT_EQ(covered({}), docs("fox jumps quick"));
    EXPECT_EQ(covered({0}), docs("brown fox jumps"));
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
  const auto texts = RandomTexts(rng, kWords, 300, 3, 10);
  const std::vector<std::string_view> docs{texts.begin(), texts.end()};
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

    SCOPED_TRACE(i);
    EXPECT_EQ(index.Docs(positional),
              index.Docs(*ShingleFilter(*shingles, phrase)));
  }
}

TEST(ShinglePhraseIndexTest, cover_scores_by_phrase_frequency) {
  static constexpr std::string_view kDocs[] = {
    "quick brown fox and quick brown fox",
    "quick brown fox and some other words",
    "quick brown cat brown fox and words",
  };
  auto shingles = MakeShingles(2, 2);
  const Index index{kDocs, *shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
  auto filter = ToFilter(Plan(*shingles, "quick brown fox", true));
  ASSERT_NE(nullptr, filter);
  const auto scores = index.Scores(*filter);
  ASSERT_EQ(2U, scores.size());
  EXPECT_GT(scores.at(0), scores.at(1));
}

TEST(ShinglePhraseIndexTest, cover_agrees_with_positions) {
  static constexpr std::string_view kWords[] = {"a", "b", "c", "d", "e"};
  std::mt19937 rng{42};
  const auto texts = RandomTexts(rng, kWords, 300, 3, 10);
  const std::vector<std::string_view> docs{texts.begin(), texts.end()};
  auto shingles = MakeShingles(2, 3);
  const Index index{docs, *shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};

  for (size_t i = 0; i != 200; ++i) {
    const auto phrase = RandomText(rng, kWords, 2, 4);
    SCOPED_TRACE(phrase);
    const auto positional = PlainPhrase(kPositionalId, phrase);
    auto shingle_filter = ToFilter(Plan(*shingles, phrase, true));
    ASSERT_NE(nullptr, shingle_filter);
    EXPECT_EQ(index.Docs(positional), index.Docs(*shingle_filter));
    EXPECT_EQ(index.Freqs(positional), index.Freqs(*shingle_filter));
  }
}

TEST(ShinglePhraseIndexTest, covers_run_in_every_family) {
  static constexpr std::string_view kWords[] = {"a", "b", "c", "d", "e"};
  std::mt19937 rng{11};
  const auto texts = RandomTexts(rng, kWords, 600, 3, 12);
  const std::vector<std::string_view> docs{texts.begin(), texts.end()};
  auto shingles = MakeShingles(2, 3);
  const Index index{docs, *shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};

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

    const auto expected = index.Run(positional);
    EXPECT_EQ(index.Docs(positional), expected.docs);
    {
      SCOPED_TRACE("positions");
      ExpectConsistent(expected);
    }
    const auto actual = index.Run(*ShingleFilter(*shingles, phrase));
    EXPECT_EQ(expected.docs, actual.docs);
    {
      SCOPED_TRACE("shingles");
      ExpectConsistent(actual);
    }
  }

  for (const auto text : {"a b c", "a b c d", "c a b a", "e e e"}) {
    SCOPED_TRACE(text);
    auto filter = ToFilter(Plan(*shingles, text, true));
    ASSERT_NE(nullptr, filter);
    const auto expected = index.Run(PlainPhrase(kPositionalId, text));
    const auto actual = index.Run(*filter);
    EXPECT_EQ(expected.docs, actual.docs);
    ExpectConsistent(actual);
  }
}

TEST(ShinglePhraseFilterTest, equality_covers_separator) {
  const auto base = Phrase("quick brown");
  auto separated = base;
  separated.set_word_separator(
    irs::ViewCast<irs::byte_type>(std::string_view{" "}));
  EXPECT_NE(base, separated);
  auto twin = base;
  twin.set_word_separator(irs::ViewCast<irs::byte_type>(std::string_view{" "}));
  EXPECT_EQ(separated, twin);
  auto other = base;
  other.set_word_separator(
    irs::ViewCast<irs::byte_type>(std::string_view{"_"}));
  EXPECT_NE(separated, other);

  separated.clear();
  EXPECT_EQ(irs::ByPhraseOptions{}, separated);
}

TEST(ShinglePhraseFilterTest, lowered_patterns_reject_shingles) {
  const auto lowered = [](std::string_view separator) {
    irs::ByPhraseOptions phrase;
    PushTerm(phrase, "quick", 0, 0);
    phrase.push_back<irs::ByWildcardOptions>() = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view{"%ox"})};
    phrase.set_word_separator(irs::ViewCast<irs::byte_type>(separator));
    phrase.LowerParts();
    return std::get<irs::AutomatonOptions>(std::next(phrase.begin())->part)
      .source->Predicate();
  };
  const auto accepts = [](const irs::TermPredicate& predicate,
                          std::string_view term) {
    return predicate.Accepts(irs::ViewCast<irs::byte_type>(term));
  };

  const auto space = lowered(" ");
  EXPECT_TRUE(accepts(*space, "fox"));
  EXPECT_FALSE(accepts(*space, "brown fox"));
  EXPECT_TRUE(accepts(*space, "brown_fox"));

  const auto dot = lowered("\xC2\xB7");
  EXPECT_TRUE(accepts(*dot, "box"));
  EXPECT_FALSE(accepts(*dot,
                       "brown\xC2\xB7"
                       "fox"));

  const auto none = lowered("");
  EXPECT_TRUE(accepts(*none, "brown fox"));

  const auto wide = lowered("--");
  EXPECT_TRUE(accepts(*wide, "brown--fox"));

  irs::ByPhraseOptions regexp;
  PushTerm(regexp, "quick", 0, 0);
  regexp.push_back<irs::ByRegexpOptions>() = irs::ByRegexpOptions{
    irs::ViewCast<irs::byte_type>(std::string_view{".*ox"})};
  regexp.set_word_separator(
    irs::ViewCast<irs::byte_type>(std::string_view{" "}));
  regexp.LowerParts();
  const auto words =
    std::get<irs::AutomatonOptions>(std::next(regexp.begin())->part)
      .source->Predicate();
  EXPECT_TRUE(accepts(*words, "fox"));
  EXPECT_FALSE(accepts(*words, "brown fox"));
}

TEST(ShinglePhraseFilterTest, lowering_is_idempotent) {
  const auto bytes = [](std::string_view text) {
    return irs::ViewCast<irs::byte_type>(text);
  };
  for (const std::string_view separator : {"", " ", "\xC2\xB7", "--"}) {
    SCOPED_TRACE(separator);
    irs::ByPhraseOptions phrase;
    PushTerm(phrase, "quick", 0, 0);
    phrase.push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{bytes("%ox")};
    phrase.push_back<irs::ByRegexpOptions>() =
      irs::ByRegexpOptions{bytes("f.x")};
    phrase.set_word_separator(bytes(separator));
    EXPECT_TRUE(phrase.LowerParts());
    EXPECT_FALSE(phrase.LowerParts());

    irs::ByPhraseOptions lowered;
    PushTerm(lowered, "quick", 0, 0);
    lowered.push_back<irs::AutomatonOptions>() =
      irs::AutomatonOptions{bytes("b.*n"), irs::PatternKind::RegexpPerl};
    lowered.set_word_separator(bytes(separator));
    EXPECT_EQ(separator.size() == 1 || separator == "\xC2\xB7",
              lowered.LowerParts());
    EXPECT_FALSE(lowered.LowerParts());
  }
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

TEST(ShinglePhraseIndexTest, phrase_without_positions_matches_nothing) {
  static constexpr std::string_view kDocs[] = {"quick brown fox"};
  auto shingles = MakeShingles(2, 2);
  const Index index{kDocs, *shingles, irs::IndexFeatures::Freq};
  irs::ByPhrase plain = PlainPhrase(kPlainId, "quick brown");
  irs::ByPhrase cover;
  *cover.mutable_field_id() = kShingleId;
  PushTerm(*cover.mutable_options(), "quick brown", 0, 0);
  PushTerm(*cover.mutable_options(), "brown fox", 1, 1);
  for (const auto* filter : {&plain, &cover}) {
    EXPECT_EQ((std::vector<irs::doc_id_t>{}), index.Docs(*filter));
    const auto families = index.Run(*filter);
    EXPECT_EQ(0U, families.count);
    EXPECT_TRUE(families.hits.empty());
    EXPECT_EQ(0U, families.top_total);
  }
  EXPECT_EQ((std::vector<irs::doc_id_t>{0}),
            index.Docs(PlainPhrase(kPositionalId, "quick brown")));
}

TEST(ShinglePhraseIndexTest, every_pattern_kind_skips_shingles) {
  static constexpr std::string_view kWords[] = {"ab", "ac", "bc", "bd", "cd"};
  const auto bytes = [](std::string_view text) {
    return irs::bstring{irs::ViewCast<irs::byte_type>(text)};
  };
  for (const std::string_view separator : {" ", "\xC2\xB7", "--"}) {
    SCOPED_TRACE(separator);
    std::mt19937 rng{23};
    const auto texts = RandomTexts(rng, kWords, 300, 2, 8);
    const std::vector<std::string_view> docs{texts.begin(), texts.end()};
    auto shingles = MakeShingles(2, 3, true, {}, separator);
    const Index index{docs, *shingles,
                      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
    size_t matched = 0;
    size_t unseparated = 0;
    for (size_t i = 0; i != 300; ++i) {
      irs::ByPhraseOptions phrase;
      std::string shape;
      const auto slots = 1 + rng() % 3;
      for (size_t j = 0; j != slots; ++j) {
        const irs::PosAttr::value_t offs = j == 0 ? 0 : 1;
        const auto word = kWords[rng() % 5];
        switch (rng() % 6) {
          case 0: {
            const auto pattern = rng() % 2
                                   ? absl::StrCat("%", word.substr(1))
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
          case 4: {
            std::string pattern;
            switch (rng() % 3) {
              case 0:
                pattern = absl::StrCat(".*", word.substr(1));
                break;
              case 1:
                pattern = absl::StrCat(word.substr(0, 1), ".*");
                break;
              default:
                pattern = absl::StrCat(word.substr(0, 1), ".*", word.substr(1));
            }
            phrase.push_back<irs::ByRegexpOptions>(offs, offs) =
              irs::ByRegexpOptions{bytes(pattern)};
            absl::StrAppend(&shape, " regexp:", pattern);
          } break;
          default:
            PushTerm(phrase, word, offs, offs);
            absl::StrAppend(&shape, " ", word);
        }
      }
      SCOPED_TRACE(shape);
      const auto expected = index.PhraseDocs(kPositionalId, phrase);
      matched += !expected.empty();
      unseparated += expected != index.PhraseDocs(kShingleId, phrase);
      EXPECT_EQ(expected, index.PhraseDocs(kShingleId, phrase, separator));
    }
    EXPECT_GT(matched, 100U);
    EXPECT_GT(unseparated, 10U);
  }
}

TEST(ShinglePhraseIndexTest, range_parts_respect_every_bound_type) {
  static constexpr std::string_view kWords[] = {"ab", "ac", "bc", "bd", "cd"};
  static constexpr irs::BoundType kTypes[] = {irs::BoundType::Unbounded,
                                              irs::BoundType::Inclusive,
                                              irs::BoundType::Exclusive};
  std::mt19937 rng{31};
  const auto texts = RandomTexts(rng, kWords, 200, 2, 6);
  const std::vector<std::string_view> docs{texts.begin(), texts.end()};
  auto shingles = MakeShingles(2, 3);
  const Index index{docs, *shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
  size_t unseparated = 0;
  for (const auto min_type : kTypes) {
    for (const auto max_type : kTypes) {
      for (const bool leading : {false, true}) {
        irs::ByPhraseOptions phrase;
        const auto push_range = [&](irs::PosAttr::value_t offs) {
          auto& range = phrase.push_back<irs::ByRangeOptions>(offs, offs).range;
          range.min = irs::ViewCast<irs::byte_type>(std::string_view{"ac"});
          range.max = irs::ViewCast<irs::byte_type>(std::string_view{"bd"});
          range.min_type = min_type;
          range.max_type = max_type;
        };
        if (leading) {
          push_range(0);
          PushTerm(phrase, "cd", 1, 1);
        } else {
          PushTerm(phrase, "ab", 0, 0);
          push_range(1);
        }
        SCOPED_TRACE(absl::StrCat(static_cast<int>(min_type), " ",
                                  static_cast<int>(max_type), " ", leading));
        const auto expected = index.PhraseDocs(kPositionalId, phrase);
        EXPECT_FALSE(expected.empty());
        unseparated += expected != index.PhraseDocs(kShingleId, phrase);
        EXPECT_EQ(expected, index.PhraseDocs(kShingleId, phrase, " "));
      }
    }
  }
  EXPECT_GT(unseparated, 0U);
}

TEST(ShinglePhraseIndexTest, stacked_base_tokens_match_like_positions) {
  static constexpr std::string_view kDocs[] = {
    "x@1 b@2 c@2 y@3", "x@1 c@2 b@2 y@3", "x@1 b@2 y@3", "x@1 c@2 y@3"};
  ShingleTokenizer shingles{std::make_unique<PositionedTokenizer>(),
                            {.min_shingle_size = 2, .max_shingle_size = 2}};
  const Index index{kDocs, shingles,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos,
                    std::type_identity<PositionedTokenizer>{}};
  for (const auto text : {"x b", "x c", "b y", "c y", "x b y", "x c y"}) {
    SCOPED_TRACE(text);
    const auto expected = index.Docs(PlainPhrase(kPositionalId, text));
    EXPECT_FALSE(expected.empty());
    EXPECT_EQ(expected, index.Docs(*ShingleFilter(shingles, Phrase(text))));
  }
}
