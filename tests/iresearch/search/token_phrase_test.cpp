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
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <atomic>
#include <bit>
#include <iresearch/analysis/text/term_view.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/formats/empty_term_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/count/root.hpp>
#include <iresearch/search/detail/phrase_slop_matcher.hpp>
#include <iresearch/search/detail/token_phrase.hpp>
#include <iresearch/search/detail/window.hpp>
#include <iresearch/search/docs/root.hpp>
#include <iresearch/search/fill/node.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/hits/root.hpp>
#include <iresearch/search/probe/node.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/search/top/root.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/string.hpp>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

irs::bytes_view Bytes(std::string_view text) {
  return irs::ViewCast<irs::byte_type>(text);
}

class DenseWords final : public irs::analysis::TypedTokenizer<DenseWords> {
 public:
  irs::TokenTraits Traits() const noexcept final { return {}; }

  static constexpr std::string_view type_name() noexcept {
    return "test_dense_words";
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

class StackedWords final : public irs::analysis::TypedTokenizer<StackedWords> {
 public:
  irs::TokenTraits Traits() const noexcept final {
    return {.explicit_pos = true};
  }

  static constexpr std::string_view type_name() noexcept {
    return "test_stacked_words";
  }

  template<irs::TokenLayout L>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view data{raw.GetData(), raw.GetSize()};
    uint32_t pos = irs::pos_limits::min();
    for (const auto word : absl::StrSplit(data, ' ', absl::SkipEmpty())) {
      if (word != "~") {
        for (const auto alt : absl::StrSplit(word, '|')) {
          sink.Emit<L>(irs::MakeTermView(alt), pos);
        }
      }
      ++pos;
    }
    return true;
  }
};

constexpr std::optional<irs::PhraseMatch> kMatches[] = {
  std::nullopt,
  irs::PhraseMatch::Anchor,
  irs::PhraseMatch::Automaton,
  irs::PhraseMatch::Positions,
};

std::string MatchName(std::optional<irs::PhraseMatch> match) {
  if (!match) {
    return "auto";
  }
  switch (*match) {
    case irs::PhraseMatch::Anchor:
      return "anchor";
    case irs::PhraseMatch::Automaton:
      return "automaton";
    case irs::PhraseMatch::Positions:
      return "positions";
  }
  return "?";
}

irs::ByPhraseOptions Phrase(std::string_view text) {
  irs::ByPhraseOptions phrase;
  for (const auto word : absl::StrSplit(text, ' ', absl::SkipEmpty())) {
    phrase.push_back<irs::ByTermOptions>().term = Bytes(word);
  }
  return phrase;
}

void PushTerm(irs::ByPhraseOptions& phrase, std::string_view word,
              irs::PosAttr::value_t offs_min, irs::PosAttr::value_t offs_max) {
  phrase.push_back<irs::ByTermOptions>(offs_min, offs_max).term = Bytes(word);
}

std::string Repeat(std::string_view word, size_t n) {
  std::string out;
  for (size_t i = 0; i != n; ++i) {
    absl::StrAppend(&out, i == 0 ? "" : " ", word);
  }
  return out;
}

struct Outcome {
  bool matched = false;
  uint32_t freq = 0;
  irs::score_t scale = irs::kNoBoost;
  bool restarted = false;
};

template<typename Words>
Outcome Check(const irs::ByPhraseOptions& phrase, std::string_view text,
              bool count, std::optional<irs::PhraseMatch> match,
              std::span<const std::vector<irs::bstring>> expanded) {
  const irs::EmptyTermReader reader{0};
  const irs::TokenPhraseMatcher matcher{phrase, expanded, reader, match};
  Words tokenizer;
  irs::ValueAnalyzer analyzer;
  irs::TokenPhraseSink sink{matcher, tokenizer.Traits()};
  const duckdb::string_t value{text.data(), static_cast<uint32_t>(text.size())};
  Outcome out;
  sink.Begin(count);
  EXPECT_TRUE(analyzer.Analyze(tokenizer, value, sink));
  if (sink.Restart()) {
    out.restarted = true;
    EXPECT_TRUE(analyzer.Analyze(tokenizer, value, sink));
  }
  irs::PhraseVerdict verdict;
  out.matched = sink.End(verdict);
  out.freq = verdict.freq;
  out.scale = verdict.scale;
  return out;
}

template<typename Words = DenseWords>
Outcome Verify(const irs::ByPhraseOptions& phrase, std::string_view text,
               std::span<const std::vector<irs::bstring>> expanded = {}) {
  std::optional<Outcome> expected;
  for (const auto match : kMatches) {
    SCOPED_TRACE(MatchName(match));
    const auto counted = Check<Words>(phrase, text, true, match, expanded);
    const auto found = Check<Words>(phrase, text, false, match, expanded);
    EXPECT_EQ(counted.matched, found.matched);
    EXPECT_EQ(found.matched ? 1U : 0U, found.freq);
    EXPECT_EQ(counted.matched, counted.freq != 0);
    if (!expected) {
      expected = counted;
      continue;
    }
    EXPECT_EQ(expected->matched, counted.matched);
    EXPECT_EQ(expected->freq, counted.freq);
    EXPECT_FLOAT_EQ(expected->scale, counted.scale);
  }
  return *expected;
}

template<typename Words = DenseWords>
bool Matches(const irs::ByPhraseOptions& phrase, std::string_view text) {
  return Verify<Words>(phrase, text).matched;
}

}  // namespace

TEST(TokenPhraseMatcherTest, adjacent_terms) {
  const auto phrase = Phrase("quick brown fox");
  EXPECT_TRUE(Matches(phrase, "the quick brown fox jumps"));
  EXPECT_FALSE(Matches(phrase, "quick brown dog fox"));
  EXPECT_FALSE(Matches(phrase, "fox brown quick"));
  EXPECT_FALSE(Matches(phrase, "quick brown"));
  EXPECT_FALSE(Matches(phrase, ""));
}

TEST(TokenPhraseMatcherTest, backtracks_over_repeated_tokens) {
  EXPECT_TRUE(Matches(Phrase("a a b"), "a a a b"));
  EXPECT_TRUE(Matches(Phrase("a b a b c"), "a b a b a b c"));
  EXPECT_TRUE(Matches(Phrase("x x y"), "x x x x y"));
  EXPECT_TRUE(Matches(Phrase("the the the"), "the the the quick"));
  EXPECT_FALSE(Matches(Phrase("the the the the"), "the the the quick"));
}

TEST(TokenPhraseMatcherTest, counts_every_start) {
  EXPECT_EQ(2U, Verify(Phrase("a a"), "a a a").freq);
  EXPECT_EQ(2U, Verify(Phrase("quick fox"), "quick fox and a quick fox").freq);
  EXPECT_EQ(1U, Verify(Phrase("quick fox"), "quick fox").freq);
}

TEST(TokenPhraseMatcherTest, exact_gap) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "quick", 0, 0);
  PushTerm(phrase, "fox", 2, 2);
  EXPECT_TRUE(Matches(phrase, "quick brown fox"));
  EXPECT_FALSE(Matches(phrase, "quick fox"));
  EXPECT_FALSE(Matches(phrase, "quick brown red fox"));
  EXPECT_EQ(2U, Verify(phrase, "quick a fox quick b fox").freq);
}

TEST(TokenPhraseMatcherTest, interval_gap) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "fox", 0, 0);
  PushTerm(phrase, "dog", 1, 3);
  EXPECT_TRUE(Matches(phrase, "fox dog"));
  EXPECT_TRUE(Matches(phrase, "fox lazy dog"));
  EXPECT_TRUE(Matches(phrase, "fox jumps lazy dog"));
  EXPECT_FALSE(Matches(phrase, "fox jumps over lazy dog"));
  EXPECT_FALSE(Matches(phrase, "dog fox"));
}

TEST(TokenPhraseMatcherTest, interval_gap_counts_every_combination) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "a", 0, 0);
  PushTerm(phrase, "c", 2, 3);
  EXPECT_EQ(2U, Verify(phrase, "a x c c").freq);
  EXPECT_EQ(3U, Verify(phrase, "a a c c").freq);
  PushTerm(phrase, "d", 1, 2);
  EXPECT_EQ(3U, Verify(phrase, "a x c c d d").freq);
  EXPECT_EQ(0U, Verify(phrase, "a x c x x d").freq);
}

TEST(TokenPhraseMatcherTest, anchor_on_any_word) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "a", 0, 0);
  PushTerm(phrase, "b", 1, 2);
  PushTerm(phrase, "c", 1, 1);
  PushTerm(phrase, "d", 1, 3);
  for (const auto* text : {"a b c d", "a x b c x x d", "a b c x x d",
                           "x a x b c d d d", "a b b c d", "a a b c d d"}) {
    SCOPED_TRACE(text);
    const auto out = Verify(phrase, text);
    EXPECT_TRUE(out.matched);
  }
  EXPECT_FALSE(Matches(phrase, "a b x c d"));
  EXPECT_FALSE(Matches(phrase, "a b c x x x d"));
  EXPECT_EQ(4U, Verify(phrase, "a a b c d d").freq);
}

TEST(TokenPhraseMatcherTest, stacked_document_tokens) {
  EXPECT_TRUE(Matches<StackedWords>(Phrase("red car"), "a red car|automobile"));
  EXPECT_TRUE(
    Matches<StackedWords>(Phrase("red automobile"), "a red car|automobile"));
  EXPECT_FALSE(Matches<StackedWords>(Phrase("red car"), "red ~ car"));
  EXPECT_TRUE(Matches<StackedWords>(Phrase("car fast"), "car|auto fast|quick"));
  EXPECT_FALSE(
    Matches<StackedWords>(Phrase("car auto"), "car|auto fast|quick"));
}

TEST(TokenPhraseMatcherTest, stacked_duplicates_count_once) {
  EXPECT_EQ(1U,
            Verify<StackedWords>(Phrase("red car"), "red|red car|car").freq);
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "red", 0, 0);
  auto& set = phrase.push_back<irs::TermSetOptions>();
  set.terms.emplace(Bytes("car"));
  set.terms.emplace(Bytes("automobile"));
  EXPECT_EQ(1U, Verify<StackedWords>(phrase, "red car|automobile").freq);
  EXPECT_EQ(2U,
            Verify<StackedWords>(phrase, "red car|automobile red car").freq);
}

TEST(TokenPhraseMatcherTest, gaps_in_explicit_positions) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "quick", 0, 0);
  PushTerm(phrase, "fox", 2, 2);
  EXPECT_TRUE(Matches<StackedWords>(phrase, "quick ~ fox"));
  EXPECT_FALSE(Matches<StackedWords>(phrase, "quick fox"));
  EXPECT_FALSE(Matches<StackedWords>(Phrase("quick fox"), "quick ~ fox"));
}

TEST(TokenPhraseMatcherTest, term_set_slot) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "red", 0, 0);
  auto& set = phrase.push_back<irs::TermSetOptions>();
  set.terms.emplace(Bytes("car"));
  set.terms.emplace(Bytes("automobile"));
  EXPECT_TRUE(Matches(phrase, "a red automobile"));
  EXPECT_TRUE(Matches(phrase, "a red car"));
  EXPECT_FALSE(Matches(phrase, "a red bike"));
}

TEST(TokenPhraseMatcherTest, expansion_slot_accepts_expanded_terms) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "quick", 0, 0);
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  const std::vector<std::vector<irs::bstring>> expanded{
    {}, {irs::bstring{Bytes("brown")}, irs::bstring{Bytes("brick")}}};
  EXPECT_TRUE(Verify(phrase, "the quick brown fox", expanded).matched);
  EXPECT_TRUE(Verify(phrase, "the quick brick wall", expanded).matched);
  EXPECT_FALSE(Verify(phrase, "the quick bread", expanded).matched);
}

TEST(TokenPhraseMatcherTest, no_words_runs_without_anchor) {
  irs::ByPhraseOptions phrase;
  auto& first = phrase.push_back<irs::TermSetOptions>();
  first.terms.emplace(Bytes("quick"));
  first.terms.emplace(Bytes("fast"));
  phrase.push_back<irs::ByPrefixOptions>(1, 2).term = Bytes("br");
  const std::vector<std::vector<irs::bstring>> expanded{
    {}, {irs::bstring{Bytes("brown")}}};
  EXPECT_EQ(2U, Verify(phrase, "quick brown fast x brown", expanded).freq);
  EXPECT_FALSE(Verify(phrase, "quick x x brown", expanded).matched);
}

TEST(TokenPhraseMatcherTest, wide_layouts_check_positions) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "a", 0, 0);
  PushTerm(phrase, "b", 1, 40);
  PushTerm(phrase, "c", 1, 40);
  const auto far =
    absl::StrCat("a ", Repeat("x", 38), " b ", Repeat("x", 30), " c");
  EXPECT_TRUE(Verify(phrase, far).matched);
  EXPECT_FALSE(
    Verify(phrase, absl::StrCat("a ", Repeat("x", 40), " b c")).matched);
}

TEST(TokenPhraseMatcherTest, slop) {
  auto phrase = Phrase("quick fox");
  phrase.set_slop(1);
  EXPECT_TRUE(Matches(phrase, "quick brown fox"));
  EXPECT_FALSE(Matches(phrase, "fox quick"));
  phrase.set_slop(2);
  EXPECT_TRUE(Matches(phrase, "fox quick"));
  EXPECT_FALSE(Matches(phrase, "quick a b c fox"));

  auto triple = Phrase("quick brown fox");
  triple.set_slop(2);
  EXPECT_TRUE(Matches(triple, "quick fox brown"));
}

TEST(TokenPhraseMatcherTest, slop_agrees_with_engine_sweep) {
  auto phrase = Phrase("a b a");
  phrase.set_slop(2);
  const auto out = Verify(phrase, "a x b a b a");

  const std::vector<std::vector<irs::PosAttr::value_t>> slots{
    {1, 4, 6}, {3, 5}, {1, 4, 6}};
  irs::detail::slop::MatchScratch scratch;
  const auto expected =
    irs::detail::slop::Run(slots, 2, {1, 1}, scratch, false, {0, 1, 0});
  ASSERT_TRUE(expected.any);
  EXPECT_TRUE(out.matched);
  EXPECT_EQ(expected.freq, out.freq);
  EXPECT_FLOAT_EQ(static_cast<irs::score_t>(expected.weight /
                                            static_cast<double>(expected.freq)),
                  out.scale);
}

TEST(TokenPhraseMatcherTest, matches_across_token_batches) {
  constexpr size_t kBatch = irs::TokenBatch::kCapacity;
  const auto text = absl::StrCat(Repeat("x", kBatch - 2), " quick brown fox ",
                                 Repeat("x", kBatch - 3), " quick brown fox ",
                                 Repeat("x", kBatch), " quick brown");
  EXPECT_EQ(2U, Verify(Phrase("quick brown fox"), text).freq);
  EXPECT_EQ(2U, Verify<StackedWords>(Phrase("quick brown fox"), text).freq);
  auto gapped = Phrase("quick");
  PushTerm(gapped, "fox", 2, 2);
  EXPECT_EQ(2U, Verify(gapped, text).freq);
  auto sloppy = Phrase("brown quick");
  sloppy.set_slop(2);
  EXPECT_TRUE(Verify(sloppy, text).matched);
  EXPECT_FALSE(Verify(Phrase("fox quick"), text).matched);
  EXPECT_TRUE(
    Verify(Phrase("brown fox x x"), absl::StrCat(text, " fox")).matched);
}

TEST(TokenPhraseMatcherTest, anchor_left_context_spans_batches) {
  constexpr size_t kBatch = irs::TokenBatch::kCapacity;
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "a", 0, 0);
  PushTerm(phrase, "b", 1, 5);
  PushTerm(phrase, "zebra", 1, 5);
  for (size_t shift = 0; shift != 12; ++shift) {
    SCOPED_TRACE(shift);
    const auto text = absl::StrCat(Repeat("x", kBatch - 6 + shift),
                                   " a x b x x zebra ", Repeat("x", 10));
    EXPECT_EQ(1U, Verify(phrase, text).freq);
  }
}

TEST(TokenPhraseMatcherTest, dense_positions_continue_across_batches) {
  constexpr auto kBatch =
    static_cast<irs::PosAttr::value_t>(irs::TokenBatch::kCapacity);
  irs::ByPhraseOptions gapped;
  PushTerm(gapped, "a", 0, 0);
  PushTerm(gapped, "b", kBatch, kBatch);
  EXPECT_TRUE(
    Verify(gapped, absl::StrCat("a ", Repeat("x", kBatch - 1), " b")).matched);
  EXPECT_FALSE(
    Verify(gapped, absl::StrCat("a ", Repeat("x", kBatch), " b")).matched);
}

TEST(TokenPhraseMatcherTest, adversarial_repetition_restarts) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "the", 0, 0);
  PushTerm(phrase, "the", 1, 11);
  PushTerm(phrase, "the", 1, 11);
  const auto text = Repeat("the", 1000);
  const auto out = Verify(phrase, text);
  EXPECT_TRUE(out.matched);
  uint64_t expected = 0;
  for (uint64_t p = 1; p <= 1000; ++p) {
    for (uint64_t q = p + 1; q <= std::min<uint64_t>(p + 11, 1000); ++q) {
      expected += std::min<uint64_t>(q + 11, 1000) - q;
    }
  }
  EXPECT_EQ(expected, out.freq);
  EXPECT_TRUE(
    Check<DenseWords>(phrase, text, true, irs::PhraseMatch::Anchor, {})
      .restarted);
}

TEST(TokenPhraseMatcherTest, empty_value_matches_nothing) {
  const auto out = Verify(Phrase("a b"), "");
  EXPECT_FALSE(out.matched);
  EXPECT_EQ(0U, out.freq);
}

TEST(TokenPhraseMatcherTest, routes) {
  const irs::EmptyTermReader reader{0};
  const auto mode = [&](const irs::ByPhraseOptions& phrase,
                        std::optional<irs::PhraseMatch> match = {}) {
    return irs::TokenPhraseMatcher{phrase, {}, reader, match}.Primary();
  };
  EXPECT_EQ(irs::PhraseMatch::Anchor, mode(Phrase("a b")));
  EXPECT_EQ(irs::PhraseMatch::Automaton,
            mode(Phrase("a b"), irs::PhraseMatch::Automaton));
  EXPECT_EQ(irs::PhraseMatch::Positions,
            mode(Phrase("a b"), irs::PhraseMatch::Positions));
  auto sloppy = Phrase("a b");
  sloppy.set_slop(1);
  EXPECT_EQ(irs::PhraseMatch::Positions, mode(sloppy));
  EXPECT_EQ(irs::PhraseMatch::Positions,
            mode(sloppy, irs::PhraseMatch::Automaton));
  irs::ByPhraseOptions sets;
  sets.push_back<irs::TermSetOptions>().terms.emplace(Bytes("a"));
  sets.push_back<irs::TermSetOptions>().terms.emplace(Bytes("b"));
  EXPECT_EQ(irs::PhraseMatch::Automaton, mode(sets));
  irs::ByPhraseOptions wide;
  PushTerm(wide, "a", 0, 0);
  PushTerm(wide, "b", 1, 70);
  EXPECT_EQ(irs::PhraseMatch::Anchor, mode(wide));
  const irs::TokenPhraseMatcher matcher{wide, {}, reader};
  EXPECT_EQ(irs::PhraseMatch::Positions, matcher.Fallback());
}

TEST(TokenPhraseMatcherTest, standalone_parts) {
  using irs::TokenPhraseMatcher;
  EXPECT_TRUE(TokenPhraseMatcher::Standalone(Phrase("a b")));
  auto sloppy = Phrase("a b");
  sloppy.set_slop(2);
  EXPECT_TRUE(TokenPhraseMatcher::Standalone(sloppy));

  auto prefix = Phrase("a");
  prefix.push_back<irs::ByPrefixOptions>().term = Bytes("b");
  EXPECT_TRUE(TokenPhraseMatcher::Standalone(prefix));
  prefix.set_slop(1);
  EXPECT_FALSE(TokenPhraseMatcher::Standalone(prefix));

  auto like = Phrase("a");
  like.push_back<irs::ByWildcardOptions>().term = Bytes("%b");
  EXPECT_FALSE(TokenPhraseMatcher::Standalone(like));
  EXPECT_TRUE(like.LowerParts());
  EXPECT_TRUE(TokenPhraseMatcher::Standalone(like));

  for (const size_t max_terms : {0, 3}) {
    SCOPED_TRACE(max_terms);
    auto fuzzy = Phrase("a");
    auto& part = fuzzy.push_back<irs::ByEditDistanceOptions>();
    part.term = Bytes("bob");
    part.max_distance = 1;
    part.max_terms = max_terms;
    EXPECT_TRUE(fuzzy.LowerParts());
    EXPECT_EQ(max_terms == 0, TokenPhraseMatcher::Standalone(fuzzy));
  }
}

TEST(TokenPhraseMatcherTest, standalone_patterns_skip_shingles) {
  auto phrase = Phrase("quick");
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  const irs::EmptyTermReader reader{0};
  const irs::TokenPhraseMatcher matcher{
    phrase, Bytes("_"),
    [&](irs::bytes_view term) { return reader.Lookup(term); }};
  DenseWords tokenizer;
  irs::ValueAnalyzer analyzer;
  irs::TokenPhraseSink sink{matcher, tokenizer.Traits()};
  const auto check = [&](std::string_view text) {
    const duckdb::string_t value{text.data(),
                                 static_cast<uint32_t>(text.size())};
    irs::PhraseVerdict verdict;
    return irs::CheckValues(sink, analyzer, tokenizer, {&value, 1}, true,
                            verdict);
  };
  EXPECT_TRUE(check("the quick brown fox"));
  EXPECT_FALSE(check("the quick brown_fox"));
  EXPECT_FALSE(check("the quick dread"));
}

namespace {

inline constexpr irs::field_id kStoreId = 1;
inline constexpr irs::field_id kPlainId = 2;
inline constexpr irs::field_id kPositionalId = 3;
inline constexpr irs::field_id kDecoyId = 4;
inline constexpr size_t kTop = 5;

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

template<typename Words>
std::shared_ptr<const irs::PhraseTokens> Tokens(
  std::optional<irs::PhraseMatch> match, irs::field_id column = kStoreId) {
  auto tokens = std::make_shared<irs::PhraseTokens>();
  tokens->text = {.columns = {column}, .types = {duckdb::LogicalType::VARCHAR}};
  tokens->tokenizer = [] { return std::make_shared<Words>(); };
  tokens->match = match;
  return tokens;
}

class Index {
 public:
  template<typename Words>
  Index(std::span<const std::string> docs, std::type_identity<Words>,
        size_t segment_docs = std::numeric_limits<size_t>::max()) {
    auto writer = irs::IndexWriter::Make(_dir, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    EXPECT_NE(nullptr, writer);
    Words plain;
    Words positional;
    Field plain_field{.analyzer = &plain, .id = kPlainId};
    Field positional_field{
      .analyzer = &positional,
      .id = kPositionalId,
      .features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
    for (size_t begin = 0; begin < docs.size(); begin += segment_docs) {
      auto ctx = writer->GetBatch();
      const auto end = std::min(docs.size(), begin + segment_docs);
      for (auto i = begin; i != end; ++i) {
        plain_field.value = docs[i];
        positional_field.value = docs[i];
        auto doc = ctx.Insert();
        EXPECT_TRUE(tests::InsertField(doc, plain_field));
        EXPECT_TRUE(tests::InsertField(doc, positional_field));
        irs::tests::StoreFieldAt(*doc.GetColWriter(), kStoreId, doc.DocId(),
                                 plain_field);
        std::vector<std::string_view> words =
          absl::StrSplit(docs[i], ' ', absl::SkipEmpty());
        absl::c_reverse(words);
        const auto decoy = absl::StrJoin(words, " ");
        const Field decoy_field{.value = decoy, .id = kDecoyId};
        irs::tests::StoreFieldAt(*doc.GetColWriter(), kDecoyId, doc.DocId(),
                                 decoy_field);
      }
      ctx.Commit();
      writer->RefreshCommit();
    }
    _reader = irs::DirectoryReader{_dir, irs::tests::DefaultReaderOptions()};
  }

  const irs::IndexReader& Reader() const noexcept { return _reader; }

  std::vector<irs::doc_id_t> Docs(const irs::Filter& filter) const {
    tests::PreparedFilter prepared{filter, *_reader};
    std::vector<irs::doc_id_t> out;
    for (size_t i = 0; i != prepared.size(); ++i) {
      auto docs = prepared.Execute(i);
      while (!irs::doc_limits::eof(docs->Next())) {
        out.push_back(Global(i, docs->Value()));
      }
    }
    return out;
  }

  std::map<irs::doc_id_t, irs::score_t> Freqs(const irs::Filter& filter) const {
    tests::sort::FrequencyScore scorer;
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
        out.emplace(Global(i, docs->Value()), value);
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
            out.docs.push_back(Global(i, buf[j]));
          }
          absl::c_fill(mask, 0);
          fill->FillOr(min, max, mask.data());
          ForEachBit(mask, min, [&](irs::doc_id_t doc) {
            out.fill.push_back(Global(i, doc));
          });
        }

        auto probe = query->PlanProbe({}, end - irs::doc_limits::min());
        for (auto doc = irs::doc_limits::min(); doc < end; ++doc) {
          if (probe->Probe(doc) == doc) {
            out.probe.push_back(Global(i, doc));
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
            out.hits.emplace(Global(i, docs[j]), scores[j]);
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
            out.fill_scores.emplace(Global(i, doc), scores[doc - min]);
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
          out.probe_scores.emplace(Global(i, doc), value);
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

  irs::doc_id_t Global(size_t segment, irs::doc_id_t doc) const {
    irs::doc_id_t base = 0;
    for (size_t i = 0; i != segment; ++i) {
      base += static_cast<irs::doc_id_t>((*_reader)[i].docs_count());
    }
    return base + doc - irs::doc_limits::min();
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

irs::ByPhrase PhraseOn(irs::field_id field, irs::ByPhraseOptions options,
                       std::shared_ptr<const irs::PhraseTokens> tokens = {}) {
  irs::ByPhrase filter;
  *filter.mutable_field_id() = field;
  *filter.mutable_options() = std::move(options);
  filter.mutable_options()->set_tokens(std::move(tokens));
  return filter;
}

irs::Filter::ptr Lowered(irs::ByPhrase phrase) {
  irs::Filter::ptr root = std::make_unique<irs::ByPhrase>(std::move(phrase));
  irs::Optimize(root);
  return root;
}

irs::Filter::ptr LoweredAnd(irs::ByPhrase phrase, std::string_view term) {
  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(std::make_unique<irs::ByPhrase>(std::move(phrase)),
            irs::Occur::Must);
  root->Add(
    irs::TermClause{.field = kPlainId, .term = irs::bstring{Bytes(term)}},
    irs::Occur::Must);
  irs::Filter::ptr out = std::move(root);
  irs::Optimize(out);
  return out;
}

void Defer(irs::Filter& filter) {
  auto& options =
    *irs::utils::downCast<irs::ByPhrase>(filter).mutable_options();
  auto tokens = std::make_shared<irs::PhraseTokens>(*options.tokens());
  tokens->deferred = true;
  options.set_tokens(std::move(tokens));
}

irs::PostingMeta Summed(const irs::IndexReader& reader, irs::field_id field,
                        irs::bytes_view term) {
  irs::PostingMeta out;
  for (const auto& segment : reader) {
    if (const auto* terms = segment.field(field)) {
      const auto meta = terms->Lookup(term);
      out.docs_count += meta.docs_count;
      out.freq += meta.freq;
    }
  }
  return out;
}

std::string RandomText(std::mt19937& rng,
                       std::span<const std::string_view> words, size_t length) {
  std::string text;
  for (size_t j = 0; j != length; ++j) {
    absl::StrAppend(&text, j == 0 ? "" : " ", words[rng() % words.size()]);
  }
  return text;
}

irs::ByPhraseOptions RandomPhrase(std::mt19937& rng,
                                  std::span<const std::string_view> words) {
  irs::ByPhraseOptions phrase;
  const auto size = 2 + rng() % 3;
  const bool sloppy = rng() % 5 == 0;
  for (size_t k = 0; k != size; ++k) {
    irs::PosAttr::value_t offs_min = k == 0 ? 0 : 1 + rng() % 2;
    irs::PosAttr::value_t offs_max = offs_min;
    if (k != 0 && !sloppy && rng() % 3 == 0) {
      offs_max += rng() % 4;
    }
    switch (rng() % 6) {
      case 0: {
        auto& set =
          phrase.push_back<irs::TermSetOptions>(offs_min, offs_max).terms;
        set.emplace(Bytes(words[rng() % words.size()]));
        set.emplace(Bytes(words[rng() % words.size()]));
      } break;
      case 1:
        phrase.push_back<irs::ByPrefixOptions>(offs_min, offs_max).term =
          Bytes(words[rng() % words.size()].substr(0, 1));
        break;
      default:
        PushTerm(phrase, words[rng() % words.size()], offs_min, offs_max);
        break;
    }
  }
  if (sloppy) {
    phrase.set_slop(1 + rng() % 3);
  }
  return phrase;
}

std::string Describe(const irs::ByPhraseOptions& phrase) {
  std::string out;
  for (const auto& info : phrase) {
    absl::StrAppend(&out, "[", info.offs_min, ",", info.offs_max, "]");
    if (const auto* term = std::get_if<irs::ByTermOptions>(&info.part)) {
      absl::StrAppend(&out, irs::ViewCast<char>(irs::bytes_view{term->term}));
    } else if (const auto* prefix =
                 std::get_if<irs::ByPrefixOptions>(&info.part)) {
      absl::StrAppend(&out, irs::ViewCast<char>(irs::bytes_view{prefix->term}),
                      "*");
    } else if (const auto* set = std::get_if<irs::TermSetOptions>(&info.part)) {
      absl::StrAppend(&out, "{");
      for (const auto& term : set->terms) {
        absl::StrAppend(&out, irs::ViewCast<char>(irs::bytes_view{term}), ",");
      }
      absl::StrAppend(&out, "}");
    } else if (const auto* like =
                 std::get_if<irs::ByWildcardOptions>(&info.part)) {
      absl::StrAppend(
        &out, "like:", irs::ViewCast<char>(irs::bytes_view{like->term}));
    } else if (const auto* regex =
                 std::get_if<irs::ByRegexpOptions>(&info.part)) {
      absl::StrAppend(
        &out, "regex:", irs::ViewCast<char>(irs::bytes_view{regex->pattern}));
    } else if (const auto* fuzzy =
                 std::get_if<irs::ByEditDistanceOptions>(&info.part)) {
      absl::StrAppend(
        &out, "fuzzy:", irs::ViewCast<char>(irs::bytes_view{fuzzy->term}), "~",
        fuzzy->max_distance);
    } else if (const auto* range =
                 std::get_if<irs::ByRangeOptions>(&info.part)) {
      absl::StrAppend(
        &out, "range:", irs::ViewCast<char>(irs::bytes_view{range->range.min}),
        "..", irs::ViewCast<char>(irs::bytes_view{range->range.max}));
    }
    absl::StrAppend(&out, " ");
  }
  absl::StrAppend(&out, "slop=", phrase.slop());
  return out;
}

template<typename Words>
void ExpectLikePositions(const Index& index,
                         const irs::ByPhraseOptions& phrase) {
  SCOPED_TRACE(Describe(phrase));
  const auto positional = Lowered(PhraseOn(kPositionalId, phrase));
  const auto expected = index.Run(*positional);
  const auto freqs = index.Freqs(*positional);
  for (const auto match : kMatches) {
    SCOPED_TRACE(MatchName(match));
    const auto checked =
      Lowered(PhraseOn(kPlainId, phrase, Tokens<Words>(match)));
    ExpectFamilies(expected, index.Run(*checked));
    ExpectScores(freqs, index.Freqs(*checked));
  }
}

irs::ByPhraseOptions RandomPatternPhrase(
  std::mt19937& rng, std::span<const std::string_view> words) {
  irs::ByPhraseOptions phrase;
  const auto size = 1 + rng() % 3;
  for (size_t k = 0; k != size; ++k) {
    const irs::PosAttr::value_t offs_min = k == 0 ? 0 : 1 + rng() % 2;
    irs::PosAttr::value_t offs_max = offs_min;
    if (k != 0 && rng() % 3 == 0) {
      offs_max += rng() % 3;
    }
    const auto word = words[rng() % words.size()];
    switch (rng() % 8) {
      case 0:
        phrase.push_back<irs::ByWildcardOptions>(offs_min, offs_max).term =
          Bytes(absl::StrCat("%", word.substr(word.size() - 3)));
        break;
      case 1:
        phrase.push_back<irs::ByRegexpOptions>(offs_min, offs_max).pattern =
          Bytes(absl::StrCat(".", word.substr(1, 2), ".*"));
        break;
      case 2: {
        auto& fuzzy =
          phrase.push_back<irs::ByEditDistanceOptions>(offs_min, offs_max);
        fuzzy.term = Bytes(word);
        fuzzy.max_distance = 1;
      } break;
      case 3: {
        auto& range =
          phrase.push_back<irs::ByRangeOptions>(offs_min, offs_max).range;
        range.min = Bytes("b");
        range.min_type = irs::BoundType::Inclusive;
        range.max = Bytes("f");
        range.max_type = irs::BoundType::Exclusive;
      } break;
      case 4:
        phrase.push_back<irs::ByPrefixOptions>(offs_min, offs_max).term =
          Bytes(word.substr(0, 2));
        break;
      case 5: {
        auto& set =
          phrase.push_back<irs::TermSetOptions>(offs_min, offs_max).terms;
        set.emplace(Bytes(word));
        set.emplace(Bytes(words[rng() % words.size()]));
      } break;
      default:
        PushTerm(phrase, word, offs_min, offs_max);
        break;
    }
  }
  return phrase;
}

template<typename Words>
void ExpectDeferredLikeInline(const Index& index,
                              std::span<const std::string> docs,
                              const irs::ByPhraseOptions& phrase) {
  SCOPED_TRACE(Describe(phrase));
  for (const auto match : kMatches) {
    SCOPED_TRACE(MatchName(match));
    const auto expected =
      index.Docs(*Lowered(PhraseOn(kPlainId, phrase, Tokens<Words>(match))));
    auto deferred = Lowered(PhraseOn(kPlainId, phrase, Tokens<Words>(match)));
    ASSERT_EQ(irs::Type<irs::ByPhrase>::id(), deferred->type());
    Defer(*deferred);
    const auto& options =
      irs::utils::downCast<irs::ByPhrase>(*deferred).options();
    ASSERT_TRUE(irs::TokenPhraseMatcher::Standalone(options));
    const irs::TokenPhraseMatcher matcher{options, options.word_separator(),
                                          [&](irs::bytes_view term) {
                                            return Summed(index.Reader(),
                                                          kPlainId, term);
                                          },
                                          match};
    Words tokenizer;
    irs::ValueAnalyzer analyzer;
    irs::TokenPhraseSink sink{matcher, tokenizer.Traits()};
    std::vector<irs::doc_id_t> actual;
    for (const auto doc : index.Docs(*deferred)) {
      const auto& text = docs[doc];
      const duckdb::string_t value{text.data(),
                                   static_cast<uint32_t>(text.size())};
      irs::PhraseVerdict verdict;
      if (irs::CheckValues(sink, analyzer, tokenizer, {&value, 1}, false,
                           verdict)) {
        actual.push_back(doc);
      }
    }
    EXPECT_EQ(expected, actual);
  }
}

}  // namespace

TEST(TokenPhraseIndexTest, agrees_with_positions) {
  constexpr std::string_view kWords[] = {"the", "quick", "brown", "fox",
                                         "the", "the",   "dog",   "a"};
  std::mt19937 rng{42};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 300; ++i) {
    docs.push_back(RandomText(rng, kWords, 1 + rng() % 30));
  }
  const Index index{docs, std::type_identity<DenseWords>{}, 128};
  for (size_t i = 0; i != 60; ++i) {
    const auto phrase = RandomPhrase(rng, kWords);
    SCOPED_TRACE(i);
    ExpectLikePositions<DenseWords>(index, phrase);
  }
}

TEST(TokenPhraseIndexTest, stacked_tokens_agree_with_positions) {
  constexpr std::string_view kWords[] = {"red", "car|auto", "car", "auto",
                                         "~",   "red|car",  "big", "red"};
  std::mt19937 rng{7};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 200; ++i) {
    docs.push_back(RandomText(rng, kWords, 1 + rng() % 20));
  }
  const Index index{docs, std::type_identity<StackedWords>{}};
  constexpr std::string_view kTerms[] = {"red", "car", "auto", "big"};
  for (size_t i = 0; i != 40; ++i) {
    const auto phrase = RandomPhrase(rng, kTerms);
    SCOPED_TRACE(i);
    ExpectLikePositions<StackedWords>(index, phrase);
  }
}

TEST(TokenPhraseIndexTest, stacked_set_slots_count_each_position_once) {
  const std::vector<std::string> docs{"car|auto auto", "auto car|auto",
                                      "car|auto x auto", "auto|car auto|car",
                                      "car|auto ~ auto car|auto ~ auto"};
  const Index index{docs, std::type_identity<StackedWords>{}};
  for (const irs::PosAttr::value_t gap : {1, 3}) {
    SCOPED_TRACE(gap);
    irs::ByPhraseOptions phrase;
    auto& set = phrase.push_back<irs::TermSetOptions>().terms;
    set.emplace(Bytes("auto"));
    set.emplace(Bytes("car"));
    PushTerm(phrase, "auto", 1, gap);
    const std::map<irs::doc_id_t, irs::score_t> expected =
      gap == 1
        ? std::map<irs::doc_id_t, irs::score_t>{{0, 1}, {1, 1}, {3, 1}, {4, 1}}
        : std::map<irs::doc_id_t, irs::score_t>{
            {0, 1}, {1, 1}, {2, 1}, {3, 1}, {4, 5}};
    ExpectScores(expected,
                 index.Freqs(*Lowered(PhraseOn(kPositionalId, phrase))));
    ExpectLikePositions<StackedWords>(index, phrase);
  }
}

TEST(TokenPhraseIndexTest, long_documents_agree_with_positions) {
  constexpr std::string_view kWords[] = {"x", "x", "x", "x", "a", "b", "c"};
  std::mt19937 rng{11};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 20; ++i) {
    docs.push_back(RandomText(rng, kWords, 1000 + rng() % 3000));
  }
  const Index index{docs, std::type_identity<DenseWords>{}};
  for (const auto* text : {"a b", "a b c", "c a", "a x b"}) {
    SCOPED_TRACE(text);
    ExpectLikePositions<DenseWords>(index, Phrase(text));
  }
  irs::ByPhraseOptions interval;
  PushTerm(interval, "a", 0, 0);
  PushTerm(interval, "b", 1, 6);
  PushTerm(interval, "c", 1, 6);
  ExpectLikePositions<DenseWords>(index, interval);
}

TEST(TokenPhraseIndexTest, conjunctions_seek_into_checked_batches) {
  constexpr std::string_view kWords[] = {"the", "quick", "brown", "fox",
                                         "x",   "x",     "dog",   "a"};
  std::mt19937 rng{5};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 4000; ++i) {
    auto text = RandomText(rng, kWords, 1 + rng() % 12);
    if (rng() % 9 == 0) {
      absl::StrAppend(&text, " rare");
    }
    docs.push_back(std::move(text));
  }
  const Index index{docs, std::type_identity<DenseWords>{}, 1500};
  for (const auto* text :
       {"quick brown", "x x", "the quick", "brown fox dog"}) {
    SCOPED_TRACE(text);
    const auto expected =
      index.Run(*LoweredAnd(PhraseOn(kPositionalId, Phrase(text)), "rare"));
    for (const auto match : kMatches) {
      SCOPED_TRACE(MatchName(match));
      ExpectFamilies(
        expected, index.Run(*LoweredAnd(
                    PhraseOn(kPlainId, Phrase(text), Tokens<DenseWords>(match)),
                    "rare")));
    }
    ExpectLikePositions<DenseWords>(index, Phrase(text));
  }
}

TEST(TokenPhraseIndexTest, deferred_check_agrees_with_inline) {
  constexpr std::string_view kWords[] = {"quick", "quack", "brown", "brawn",
                                         "fox",   "box",   "the",   "dog"};
  std::mt19937 rng{3};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 300; ++i) {
    docs.push_back(RandomText(rng, kWords, 1 + rng() % 20));
  }
  const Index index{docs, std::type_identity<DenseWords>{}, 64};
  for (size_t i = 0; i != 80; ++i) {
    const auto phrase = RandomPatternPhrase(rng, kWords);
    SCOPED_TRACE(i);
    ExpectDeferredLikeInline<DenseWords>(index, docs, phrase);
  }
}

TEST(TokenPhraseIndexTest, deferred_check_on_stacked_tokens) {
  constexpr std::string_view kWords[] = {"red", "car|auto", "car", "auto",
                                         "~",   "red|car",  "big", "red"};
  std::mt19937 rng{9};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 200; ++i) {
    docs.push_back(RandomText(rng, kWords, 1 + rng() % 20));
  }
  const Index index{docs, std::type_identity<StackedWords>{}, 64};
  constexpr std::string_view kTerms[] = {"red", "car", "auto", "big"};
  for (size_t i = 0; i != 40; ++i) {
    const auto phrase = RandomPatternPhrase(rng, kTerms);
    SCOPED_TRACE(i);
    ExpectDeferredLikeInline<StackedWords>(index, docs, phrase);
  }
}

TEST(TokenPhraseIndexTest, expression_over_stored_columns) {
  class SecondColumn final : public irs::TextExpression {
   public:
    duckdb::Vector& Evaluate(duckdb::DataChunk& columns) final {
      return columns.data[1];
    }
  };
  constexpr std::string_view kWords[] = {"the", "quick", "brown", "fox",
                                         "the", "dog",   "a"};
  std::mt19937 rng{13};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 300; ++i) {
    docs.push_back(RandomText(rng, kWords, 1 + rng() % 20));
  }
  const Index index{docs, std::type_identity<DenseWords>{}, 64};
  for (const auto* text : {"quick brown", "the fox", "brown fox dog"}) {
    SCOPED_TRACE(text);
    const auto expected =
      index.Run(*Lowered(PhraseOn(kPositionalId, Phrase(text))));
    for (const auto match : kMatches) {
      SCOPED_TRACE(MatchName(match));
      auto tokens =
        std::make_shared<irs::PhraseTokens>(*Tokens<DenseWords>(match));
      tokens->text = {
        .columns = {kDecoyId, kStoreId},
        .types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
        .expression = [] { return std::make_unique<SecondColumn>(); }};
      ExpectFamilies(
        expected,
        index.Run(*Lowered(PhraseOn(kPlainId, Phrase(text), tokens))));
    }
  }
}

TEST(TokenPhraseIndexTest, without_stored_column_matches_nothing) {
  const std::vector<std::string> docs{"quick brown fox", "quick brown"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  const auto filter =
    PhraseOn(kPlainId, Phrase("quick brown"),
             Tokens<DenseWords>(std::nullopt, irs::field_id{42}));
  EXPECT_TRUE(index.Docs(filter).empty());
  const auto checked =
    PhraseOn(kPlainId, Phrase("quick brown"), Tokens<DenseWords>(std::nullopt));
  EXPECT_EQ((std::vector<irs::doc_id_t>{0, 1}), index.Docs(checked));
  EXPECT_TRUE(index.Docs(PhraseOn(kPlainId, Phrase("quick brown"))).empty());
}

TEST(TokenPhraseFilterTest, equality_covers_tokens) {
  const auto base = Phrase("quick brown");
  const auto tokens = Tokens<DenseWords>(std::nullopt);
  auto checked = base;
  checked.set_tokens(tokens);
  EXPECT_NE(base, checked);

  auto twin = base;
  twin.set_tokens(Tokens<DenseWords>(std::nullopt));
  EXPECT_EQ(checked, twin);

  auto elsewhere = base;
  elsewhere.set_tokens(Tokens<DenseWords>(std::nullopt, irs::field_id{42}));
  EXPECT_NE(checked, elsewhere);

  auto forced = base;
  forced.set_tokens(Tokens<DenseWords>(irs::PhraseMatch::Automaton));
  EXPECT_NE(checked, forced);

  auto spec = std::make_shared<irs::PhraseTokens>(*tokens);
  spec->spec = Phrase("quick brown fox");
  auto words = base;
  words.set_tokens(std::move(spec));
  EXPECT_NE(checked, words);

  checked.clear();
  EXPECT_FALSE(checked.tokens());
  EXPECT_EQ(irs::ByPhraseOptions{}, checked);
}

TEST(TokenPhraseFilterTest, simplify_keeps_one_slot_phrases_with_tokens) {
  const auto checked =
    Lowered(PhraseOn(kPlainId, Phrase("quick"), Tokens<DenseWords>({})));
  EXPECT_EQ(irs::Type<irs::ByPhrase>::id(), checked->type());
  const auto plain = Lowered(PhraseOn(kPlainId, Phrase("quick")));
  EXPECT_NE(irs::Type<irs::ByPhrase>::id(), plain->type());
}

TEST(TokenPhraseFilterTest, lowering_reaches_the_checked_words) {
  auto tokens =
    std::make_shared<irs::PhraseTokens>(*Tokens<DenseWords>(std::nullopt));
  irs::ByPhraseOptions words;
  PushTerm(words, "quick", 0, 0);
  words.push_back<irs::ByWildcardOptions>().term = Bytes("br%");
  tokens->spec = words;
  irs::ByPhraseOptions cover;
  PushTerm(cover, "quick", 0, 0);
  cover.push_back<irs::ByWildcardOptions>().term = Bytes("br%");
  cover.set_tokens(tokens);
  EXPECT_TRUE(cover.LowerParts());
  ASSERT_TRUE(cover.tokens());
  ASSERT_TRUE(cover.tokens()->spec);
  const auto& lowered = *cover.tokens()->spec;
  ASSERT_EQ(2U, lowered.size());
  EXPECT_TRUE(
    std::holds_alternative<irs::ByPrefixOptions>((lowered.begin() + 1)->part));
  EXPECT_FALSE(cover.LowerParts());
  EXPECT_TRUE(tokens->spec);
  EXPECT_TRUE(std::holds_alternative<irs::ByWildcardOptions>(
    (tokens->spec->begin() + 1)->part));
}
