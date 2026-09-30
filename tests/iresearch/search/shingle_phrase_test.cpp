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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <iresearch/analysis/shingle_tokenizer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/detail/phrase_verify.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/shingle_phrase.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/string.hpp>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

using irs::analysis::ShingleTokenizer;
using Kind = irs::ShinglePhrasePlan::Kind;

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

irs::ShinglePhrasePlan Plan(const ShingleTokenizer& shingles,
                            std::string_view text, bool positional) {
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

 private:
  irs::MemoryDirectory _dir;
  irs::DirectoryReader _reader;
};

irs::Filter::ptr ToFilter(irs::ShinglePhrasePlan&& plan) {
  switch (plan.kind) {
    case Kind::None:
      return nullptr;
    case Kind::Term: {
      auto filter = std::make_unique<irs::ByTerm>();
      *filter->mutable_field_id() = kShingleId;
      filter->mutable_options()->term = std::move(plan.term);
      return filter;
    }
    case Kind::Phrase: {
      auto filter = std::make_unique<irs::ByPhrase>();
      *filter->mutable_field_id() = kShingleId;
      *filter->mutable_options() = std::move(plan.phrase);
      return filter;
    }
  }
  return nullptr;
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
  ASSERT_EQ(Kind::Term, plan.kind);
  EXPECT_EQ("quick brown", Text(plan.term));

  plan = Plan(*shingles, "quick", true);
  ASSERT_EQ(Kind::Term, plan.kind);
  EXPECT_EQ("quick", Text(plan.term));

  const auto no_unigrams = MakeShingles(2, 2, false);
  EXPECT_EQ(Kind::None, Plan(*no_unigrams, "quick", true).kind);
}

TEST(ShinglePhrasePlanTest, positional_cover_takes_overlapping_tail) {
  const auto shingles = MakeShingles(2, 2);
  auto plan = Plan(*shingles, "quick brown fox", true);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ(nullptr, plan.phrase.verifier());
  EXPECT_EQ((std::vector<std::string>{"quick brown", "brown fox"}),
            Terms(plan.phrase));
  EXPECT_EQ((std::vector<uint32_t>{0, 1}), Offsets(plan.phrase));

  plan = Plan(*shingles, "a b c d e", true);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"a b", "c d", "d e"}),
            Terms(plan.phrase));
  EXPECT_EQ((std::vector<uint32_t>{0, 2, 1}), Offsets(plan.phrase));

  const auto wide = MakeShingles(2, 3);
  plan = Plan(*wide, "a b c d", true);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"a b c", "b c d"}), Terms(plan.phrase));
}

TEST(ShinglePhrasePlanTest, positional_cover_keeps_repeated_terms) {
  const auto shingles = MakeShingles(2, 2);
  const auto plan = Plan(*shingles, "the cat the cat", true);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"the cat", "the cat"}),
            Terms(plan.phrase));
  EXPECT_EQ((std::vector<uint32_t>{0, 2}), Offsets(plan.phrase));
}

TEST(ShinglePhrasePlanTest, verify_cover_dedups_windows) {
  const auto shingles = MakeShingles(2, 2);
  auto plan = Plan(*shingles, "the cat the cat", false);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  ASSERT_NE(nullptr, plan.phrase.verifier());
  EXPECT_EQ((std::vector<std::string>{"cat the", "the cat"}),
            Terms(plan.phrase));

  plan = Plan(*shingles, "a b c d e", false);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"a b", "b c", "c d", "d e"}),
            Terms(plan.phrase));

  const auto wide = MakeShingles(2, 3);
  plan = Plan(*wide, "a b c d e", false);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"a b c", "b c d", "c d e"}),
            Terms(plan.phrase));
}

TEST(ShinglePhrasePlanTest, frequent_words_limit_wide_windows) {
  const auto shingles = MakeShingles(2, 3, true, {"the"});
  auto plan = Plan(*shingles, "the quick brown", false);
  ASSERT_EQ(Kind::Term, plan.kind);
  EXPECT_EQ("the quick brown", Text(plan.term));

  plan = Plan(*shingles, "quick brown fox", false);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"brown fox", "quick brown"}),
            Terms(plan.phrase));

  plan = Plan(*shingles, "the quick brown fox", false);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"brown fox", "the quick brown"}),
            Terms(plan.phrase));
}

TEST(ShinglePhrasePlanTest, gaps_split_runs) {
  const auto shingles = MakeShingles(2, 2);
  auto phrase = Phrase("a b");
  phrase.push_back<irs::ByTermOptions>(1).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"c"});
  phrase.push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"d"});
  const auto plan = irs::PlanShinglePhrase(*shingles, phrase, true, nullptr);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"a b", "c d"}), Terms(plan.phrase));
  EXPECT_EQ((std::vector<uint32_t>{0, 3}), Offsets(plan.phrase));
}

TEST(ShinglePhrasePlanTest, partial_cover_keeps_other_parts) {
  const auto shingles = MakeShingles(2, 2);

  auto trailing = Phrase("quick brown fox");
  PushPrefix(trailing, "ju", 1, 1);
  auto plan = irs::PlanShinglePhrase(*shingles, trailing, true, nullptr);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"quick brown", "brown fox", "ju*"}),
            Terms(plan.phrase));
  EXPECT_EQ((std::vector<uint32_t>{0, 1, 2}), Offsets(plan.phrase));
  EXPECT_EQ(" ", Text(plan.phrase.word_separator()));

  irs::ByPhraseOptions leading;
  PushPrefix(leading, "qu", 0, 0);
  PushTerm(leading, "brown", 1, 1);
  PushTerm(leading, "fox", 1, 1);
  plan = irs::PlanShinglePhrase(*shingles, leading, true, nullptr);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"qu*", "brown fox"}), Terms(plan.phrase));
  EXPECT_EQ((std::vector<uint32_t>{0, 1}), Offsets(plan.phrase));

  auto interval = Phrase("quick brown");
  PushTerm(interval, "lazy", 2, 4);
  PushTerm(interval, "dog", 1, 1);
  plan = irs::PlanShinglePhrase(*shingles, interval, true, nullptr);
  ASSERT_EQ(Kind::Phrase, plan.kind);
  EXPECT_EQ((std::vector<std::string>{"quick brown", "lazy dog"}),
            Terms(plan.phrase));
  const auto& lazy_dog = *std::next(plan.phrase.begin());
  EXPECT_EQ(3U, lazy_dog.offs_min);
  EXPECT_EQ(5U, lazy_dog.offs_max);
  EXPECT_TRUE(plan.phrase.word_separator().empty());
}

TEST(ShinglePhrasePlanTest, partial_cover_needs_words_for_patterns) {
  auto phrase = Phrase("quick brown");
  PushPrefix(phrase, "fo", 1, 1);

  const auto bare = MakeShingles(2, 2, false);
  EXPECT_EQ(Kind::None,
            irs::PlanShinglePhrase(*bare, phrase, true, nullptr).kind);

  const ShingleTokenizer joined{std::make_unique<WhitespaceTokenizer>(),
                                {
                                  .min_shingle_size = 2,
                                  .max_shingle_size = 2,
                                  .token_separator = {},
                                }};
  EXPECT_EQ(Kind::None,
            irs::PlanShinglePhrase(joined, phrase, true, nullptr).kind);
}

TEST(ShinglePhrasePlanTest, declines_without_shingle_gain) {
  const auto shingles = MakeShingles(2, 2);
  const auto plan = [&](const irs::ByPhraseOptions& phrase) {
    return irs::PlanShinglePhrase(*shingles, phrase, true, nullptr).kind;
  };
  const auto brown = irs::ViewCast<irs::byte_type>(std::string_view{"brown"});

  auto sloppy = Phrase("quick brown");
  sloppy.set_slop(1);
  EXPECT_EQ(Kind::None, plan(sloppy));

  auto interval = Phrase("quick");
  interval.push_back<irs::ByTermOptions>(1, 2).term = brown;
  EXPECT_EQ(Kind::None, plan(interval));

  auto stacked = Phrase("quick");
  stacked.push_back<irs::ByTermOptions>(0, 0).term = brown;
  EXPECT_EQ(Kind::None, plan(stacked));

  auto alternatives = Phrase("quick");
  alternatives.push_back<irs::TermSetOptions>().terms.emplace(brown);
  EXPECT_EQ(Kind::None, plan(alternatives));
}

TEST(ShinglePhrasePlanTest, needs_a_source_to_verify) {
  const auto shingles = MakeShingles(2, 2);
  EXPECT_EQ(Kind::None, irs::PlanShinglePhrase(
                          *shingles, Phrase("quick brown fox"), false, nullptr)
                          .kind);
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
