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

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstring>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/analysis/text/term_view.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/detail/phrase_slop_matcher.hpp>
#include <iresearch/search/detail/table_filter.hpp>
#include <iresearch/search/detail/token_phrase.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/deferral.hpp>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/search/scorers/constant_score.hpp>
#include <iresearch/search/scorers/unscored.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/string.hpp>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "phrase_families.hpp"
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

class RejectingWords final
  : public irs::analysis::TypedTokenizer<RejectingWords> {
 public:
  irs::TokenTraits Traits() const noexcept final { return {}; }

  static constexpr std::string_view type_name() noexcept {
    return "test_rejecting_words";
  }

  template<irs::TokenLayout L>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view data{raw.GetData(), raw.GetSize()};
    for (const auto word : absl::StrSplit(data, ' ', absl::SkipEmpty())) {
      sink.Emit<L>(irs::MakeTermView(word));
    }
    return !data.ends_with('!');
  }
};

class ProgressWords final
  : public irs::analysis::TypedTokenizer<ProgressWords> {
 public:
  irs::TokenTraits Traits() const noexcept final { return {}; }

  static constexpr std::string_view type_name() noexcept {
    return "test_progress_words";
  }

  bool Scan(const duckdb::string_t& value, irs::ScanSink& sink,
            irs::BlockTraits) final {
    const std::string_view data{value.GetData(), value.GetSize()};
    const auto report = sink.Progress();
    for (const auto word : absl::StrSplit(data, ' ', absl::SkipEmpty())) {
      sink.Emit<irs::TokenLayout::TermsPos>(irs::MakeTermView(word));
      if (!report()) {
        break;
      }
    }
    return true;
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

enum class PhraseMatch : uint8_t {
  Anchor,
  Automaton,
  Positions,
};

constexpr std::optional<PhraseMatch> kMatches[] = {
  std::nullopt,
  PhraseMatch::Automaton,
  PhraseMatch::Positions,
};

std::string MatchName(std::optional<PhraseMatch> match) {
  if (!match) {
    return "auto";
  }
  switch (*match) {
    case PhraseMatch::Anchor:
      return "anchor";
    case PhraseMatch::Automaton:
      return "automaton";
    case PhraseMatch::Positions:
      return "positions";
  }
  return "?";
}

void Force(irs::CompiledPhrase& compiled, std::optional<PhraseMatch> match) {
  if (match == PhraseMatch::Positions) {
    compiled.anchor.reset();
    compiled.automaton.reset();
  } else if (match == PhraseMatch::Automaton && compiled.automaton) {
    compiled.anchor.reset();
  }
}

PhraseMatch RouteOf(const irs::CompiledPhrase& compiled) {
  if (compiled.anchor) {
    return PhraseMatch::Anchor;
  }
  return compiled.automaton ? PhraseMatch::Automaton : PhraseMatch::Positions;
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

void PushShingle(irs::ByPhraseOptions& phrase, std::string_view term,
                 std::initializer_list<std::string_view> words,
                 irs::PosAttr::value_t offs_min,
                 irs::PosAttr::value_t offs_max) {
  auto& shingle = phrase.push_back<irs::ShingleOptions>(offs_min, offs_max);
  shingle.term = Bytes(term);
  for (const auto word : words) {
    shingle.words.emplace_back(Bytes(word));
  }
}

irs::ByPhraseOptions QuickBrownFoxShingles() {
  irs::ByPhraseOptions phrase;
  PushShingle(phrase, "quick brown", {"quick", "brown"}, 0, 0);
  PushShingle(phrase, "brown fox", {"fox"}, 1, 1);
  return phrase;
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
};

template<typename Words>
irs::ByPhraseOptions::TokenizerFactory TextOf() {
  return [] { return std::make_shared<Words>(); };
}

bool CheckText(irs::PhraseCheck& check, std::string_view text,
               irs::PhraseVerdict& verdict,
               const duckdb::LogicalType& type = duckdb::LogicalType::VARCHAR) {
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), {type});
  chunk.SetChildCardinality(1);
  duckdb::FlatVector::GetDataMutable<duckdb::string_t>(chunk.data[0])[0] = {
    text.data(), static_cast<uint32_t>(text.size())};
  check.Bind(chunk.data[0], 1);
  verdict = check.Check(0);
  return verdict.freq != 0;
}

template<typename Words>
Outcome Check(const irs::ByPhraseOptions& phrase, std::string_view text,
              bool count, std::optional<PhraseMatch> match,
              std::span<const std::vector<irs::bstring>> expanded) {
  irs::CompiledPhrase compiled{phrase, {expanded.begin(), expanded.end()}, {}};
  Force(compiled, match);
  irs::PhraseCheck check{compiled, TextOf<Words>(), count};
  irs::PhraseVerdict verdict;
  Outcome out;
  out.matched = CheckText(check, text, verdict);
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

TEST(PhraseCheckTest, adjacent_terms) {
  const auto phrase = Phrase("quick brown fox");
  EXPECT_TRUE(Matches(phrase, "the quick brown fox jumps"));
  EXPECT_FALSE(Matches(phrase, "quick brown dog fox"));
  EXPECT_FALSE(Matches(phrase, "fox brown quick"));
  EXPECT_FALSE(Matches(phrase, "quick brown"));
  EXPECT_FALSE(Matches(phrase, ""));
}

TEST(PhraseCheckTest, backtracks_over_repeated_tokens) {
  EXPECT_TRUE(Matches(Phrase("a a b"), "a a a b"));
  EXPECT_TRUE(Matches(Phrase("a b a b c"), "a b a b a b c"));
  EXPECT_TRUE(Matches(Phrase("x x y"), "x x x x y"));
  EXPECT_TRUE(Matches(Phrase("the the the"), "the the the quick"));
  EXPECT_FALSE(Matches(Phrase("the the the the"), "the the the quick"));
}

TEST(PhraseCheckTest, counts_every_start) {
  EXPECT_EQ(2U, Verify(Phrase("a a"), "a a a").freq);
  EXPECT_EQ(2U, Verify(Phrase("quick fox"), "quick fox and a quick fox").freq);
  EXPECT_EQ(1U, Verify(Phrase("quick fox"), "quick fox").freq);
}

TEST(PhraseCheckTest, exact_gap) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "quick", 0, 0);
  PushTerm(phrase, "fox", 2, 2);
  EXPECT_TRUE(Matches(phrase, "quick brown fox"));
  EXPECT_FALSE(Matches(phrase, "quick fox"));
  EXPECT_FALSE(Matches(phrase, "quick brown red fox"));
  EXPECT_EQ(2U, Verify(phrase, "quick a fox quick b fox").freq);
}

TEST(PhraseCheckTest, interval_gap) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "fox", 0, 0);
  PushTerm(phrase, "dog", 1, 3);
  EXPECT_TRUE(Matches(phrase, "fox dog"));
  EXPECT_TRUE(Matches(phrase, "fox lazy dog"));
  EXPECT_TRUE(Matches(phrase, "fox jumps lazy dog"));
  EXPECT_FALSE(Matches(phrase, "fox jumps over lazy dog"));
  EXPECT_FALSE(Matches(phrase, "dog fox"));
}

TEST(PhraseCheckTest, interval_gap_counts_every_combination) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "a", 0, 0);
  PushTerm(phrase, "c", 2, 3);
  EXPECT_EQ(2U, Verify(phrase, "a x c c").freq);
  EXPECT_EQ(3U, Verify(phrase, "a a c c").freq);
  PushTerm(phrase, "d", 1, 2);
  EXPECT_EQ(3U, Verify(phrase, "a x c c d d").freq);
  EXPECT_EQ(0U, Verify(phrase, "a x c x x d").freq);
}

TEST(PhraseCheckTest, anchor_on_any_word) {
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

TEST(PhraseCheckTest, stacked_document_tokens) {
  EXPECT_TRUE(Matches<StackedWords>(Phrase("red car"), "a red car|automobile"));
  EXPECT_TRUE(
    Matches<StackedWords>(Phrase("red automobile"), "a red car|automobile"));
  EXPECT_FALSE(Matches<StackedWords>(Phrase("red car"), "red ~ car"));
  EXPECT_TRUE(Matches<StackedWords>(Phrase("car fast"), "car|auto fast|quick"));
  EXPECT_FALSE(
    Matches<StackedWords>(Phrase("car auto"), "car|auto fast|quick"));
}

TEST(PhraseCheckTest, stacked_duplicates_count_once) {
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

TEST(PhraseCheckTest, gaps_in_explicit_positions) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "quick", 0, 0);
  PushTerm(phrase, "fox", 2, 2);
  EXPECT_TRUE(Matches<StackedWords>(phrase, "quick ~ fox"));
  EXPECT_FALSE(Matches<StackedWords>(phrase, "quick fox"));
  EXPECT_FALSE(Matches<StackedWords>(Phrase("quick fox"), "quick ~ fox"));
}

TEST(PhraseCheckTest, term_set_slot) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "red", 0, 0);
  auto& set = phrase.push_back<irs::TermSetOptions>();
  set.terms.emplace(Bytes("car"));
  set.terms.emplace(Bytes("automobile"));
  EXPECT_TRUE(Matches(phrase, "a red automobile"));
  EXPECT_TRUE(Matches(phrase, "a red car"));
  EXPECT_FALSE(Matches(phrase, "a red bike"));
}

TEST(PhraseCheckTest, expansion_slot_accepts_expanded_terms) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "quick", 0, 0);
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  const std::vector<std::vector<irs::bstring>> expanded{
    {}, {irs::bstring{Bytes("brown")}, irs::bstring{Bytes("brick")}}};
  EXPECT_TRUE(Verify(phrase, "the quick brown fox", expanded).matched);
  EXPECT_TRUE(Verify(phrase, "the quick brick wall", expanded).matched);
  EXPECT_FALSE(Verify(phrase, "the quick bread", expanded).matched);
}

TEST(PhraseCheckTest, no_words_runs_without_anchor) {
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

TEST(PhraseCheckTest, wide_layouts_check_positions) {
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

TEST(PhraseCheckTest, slop) {
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

TEST(PhraseCheckTest, slop_agrees_with_engine_sweep) {
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

TEST(PhraseCheckTest, matches_across_token_batches) {
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

TEST(PhraseCheckTest, anchor_left_context_spans_batches) {
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

TEST(PhraseCheckTest, dense_positions_continue_across_batches) {
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

TEST(PhraseCheckTest, adversarial_repetition_restarts) {
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
}

TEST(PhraseCheckTest, empty_value_matches_nothing) {
  const auto out = Verify(Phrase("a b"), "");
  EXPECT_FALSE(out.matched);
  EXPECT_EQ(0U, out.freq);
}

TEST(PhraseCheckTest, routes) {
  const auto mode = [&](const irs::ByPhraseOptions& phrase,
                        std::optional<PhraseMatch> match = {}) {
    irs::CompiledPhrase compiled{phrase, {}, {}};
    Force(compiled, match);
    return RouteOf(compiled);
  };
  EXPECT_EQ(PhraseMatch::Anchor, mode(Phrase("a b")));
  EXPECT_EQ(PhraseMatch::Automaton,
            mode(Phrase("a b"), PhraseMatch::Automaton));
  EXPECT_EQ(PhraseMatch::Positions,
            mode(Phrase("a b"), PhraseMatch::Positions));
  auto sloppy = Phrase("a b");
  sloppy.set_slop(1);
  EXPECT_EQ(PhraseMatch::Positions, mode(sloppy));
  EXPECT_EQ(PhraseMatch::Positions, mode(sloppy, PhraseMatch::Automaton));
  irs::ByPhraseOptions sets;
  sets.push_back<irs::TermSetOptions>().terms.emplace(Bytes("a"));
  sets.push_back<irs::TermSetOptions>().terms.emplace(Bytes("b"));
  EXPECT_EQ(PhraseMatch::Automaton, mode(sets));
  irs::ByPhraseOptions wide;
  PushTerm(wide, "a", 0, 0);
  PushTerm(wide, "b", 1, 70);
  EXPECT_EQ(PhraseMatch::Anchor, mode(wide));
  const irs::CompiledPhrase compiled{wide, {}, {}};
  EXPECT_FALSE(compiled.automaton);
}

TEST(PhraseCheckTest, anchor_is_the_rarest_word) {
  const auto phrase = Phrase("a b c");
  const irs::PhraseWordStat stats[] = {{.docs = 10, .freq = 50},
                                       {.docs = 10, .freq = 12},
                                       {.docs = 4, .freq = 40}};
  const irs::CompiledPhrase compiled{phrase, {}, stats};
  ASSERT_TRUE(compiled.anchor);
  EXPECT_EQ(1U, compiled.anchor->slot);
  auto sloppy = phrase;
  sloppy.set_slop(1);
  EXPECT_EQ(PhraseMatch::Positions,
            RouteOf(irs::CompiledPhrase{sloppy, {}, stats}));
  irs::ByPhraseOptions stacked;
  PushTerm(stacked, "a", 0, 0);
  PushTerm(stacked, "b", 0, 0);
  EXPECT_EQ(PhraseMatch::Positions,
            RouteOf(irs::CompiledPhrase{stacked, {}, stats}));
}

TEST(PhraseCheckTest, standalone_parts) {
  using irs::CompiledPhrase;
  EXPECT_TRUE(irs::IsStandalone(Phrase("a b")));
  auto sloppy = Phrase("a b");
  sloppy.set_slop(2);
  EXPECT_TRUE(irs::IsStandalone(sloppy));

  auto prefix = Phrase("a");
  prefix.push_back<irs::ByPrefixOptions>().term = Bytes("b");
  EXPECT_TRUE(irs::IsStandalone(prefix));
  prefix.set_slop(1);
  EXPECT_TRUE(irs::IsStandalone(prefix));

  auto like = Phrase("a");
  like.push_back<irs::ByWildcardOptions>().term = Bytes("%b");
  EXPECT_FALSE(irs::IsStandalone(like));
  EXPECT_TRUE(like.LowerParts());
  EXPECT_TRUE(irs::IsStandalone(like));

  for (const size_t max_terms : {0, 3}) {
    SCOPED_TRACE(max_terms);
    auto fuzzy = Phrase("a");
    auto& part = fuzzy.push_back<irs::ByEditDistanceOptions>();
    part.term = Bytes("bob");
    part.max_distance = 1;
    part.max_terms = max_terms;
    EXPECT_TRUE(fuzzy.LowerParts());
    EXPECT_EQ(max_terms == 0, irs::IsStandalone(fuzzy));
  }
}

TEST(PhraseCheckTest, stacked_parts_check_positions) {
  irs::ByPhraseOptions phrase;
  PushTerm(phrase, "red", 0, 0);
  PushTerm(phrase, "car", 0, 0);
  const irs::CompiledPhrase compiled{phrase, {}, {}};
  EXPECT_EQ(PhraseMatch::Positions, RouteOf(compiled));
  EXPECT_EQ(1U, Verify<StackedWords>(phrase, "a red|car b").freq);
  EXPECT_FALSE(Matches<StackedWords>(phrase, "a red car"));
}

TEST(PhraseCheckTest, shingle_parts_check_the_words_they_add) {
  const auto phrase = QuickBrownFoxShingles();
  EXPECT_TRUE(irs::IsStandalone(phrase));
  const irs::CompiledPhrase compiled{phrase, {}, {}};
  EXPECT_EQ(PhraseMatch::Anchor, RouteOf(compiled));
  ASSERT_EQ(3U, compiled.slots.size());
  for (uint32_t slot = 1; slot != 3; ++slot) {
    EXPECT_EQ(1U, compiled.slots[slot].offs_min);
    EXPECT_EQ(1U, compiled.slots[slot].offs_max);
  }
  const auto words = Phrase("quick brown fox");
  for (const std::string_view text :
       {"the quick brown fox jumps", "quick brown dog fox", "fox brown quick",
        "quick brown fox quick brown fox", "quick brown brown fox"}) {
    SCOPED_TRACE(text);
    const auto expected = Verify(words, text);
    const auto shingled = Verify(phrase, text);
    EXPECT_EQ(expected.matched, shingled.matched);
    EXPECT_EQ(expected.freq, shingled.freq);
  }
}

TEST(PhraseCheckTest, standalone_fuzzy_part_needs_a_source) {
  auto fuzzy = Phrase("a");
  fuzzy.push_back<irs::LevenshteinAutomatonOptions>();
  EXPECT_FALSE(irs::IsStandalone(fuzzy));
}

TEST(PhraseCheckTest, expansion_slot_without_expanded_terms_matches_nothing) {
  auto phrase = Phrase("quick");
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  const std::vector<std::vector<irs::bstring>> expanded{{}, {}};
  EXPECT_FALSE(Verify(phrase, "the quick brown fox", expanded).matched);
}

TEST(PhraseCheckTest, pattern_without_expansion_checks_its_predicate) {
  auto phrase = Phrase("quick");
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  EXPECT_TRUE(Verify(phrase, "the quick brown fox").matched);
  EXPECT_FALSE(Verify(phrase, "the quick dread").matched);
}

TEST(PhraseCheckTest, duplicate_expanded_terms_list_their_slot_once) {
  auto phrase = Phrase("quick");
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  const std::vector<std::vector<irs::bstring>> expanded{
    {}, {irs::bstring{Bytes("brown")}, irs::bstring{Bytes("brown")}}};
  const irs::CompiledPhrase compiled{phrase, expanded, {}};
  ASSERT_EQ(2U, compiled.accept.Size());
  compiled.accept.ForEachMapped(
    [](const auto& entry) { EXPECT_EQ(1U, entry.slots.size()); });
  EXPECT_EQ(2U, Verify(phrase, "quick brown x quick brown", expanded).freq);
}

TEST(PhraseCheckTest, sloppy_patterns_skip_separated_words) {
  auto phrase = Phrase("quick brown_fox");
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  phrase.set_slop(1);
  phrase.set_word_separator(Bytes("_"));
  const irs::CompiledPhrase compiled{phrase, {}, {}};
  EXPECT_EQ(PhraseMatch::Positions, RouteOf(compiled));
  irs::PhraseCheck phrase_check{compiled, TextOf<DenseWords>(), true};
  const auto check = [&](std::string_view text) {
    irs::PhraseVerdict verdict;
    return CheckText(phrase_check, text, verdict);
  };
  EXPECT_TRUE(check("quick brown_fox brick"));
  EXPECT_FALSE(check("quick brown_fox brown_fox"));
}

TEST(PhraseCheckTest, anchor_carries_long_terms_across_batches) {
  constexpr size_t kBatch = irs::TokenBatch::kCapacity;
  const auto phrase = Phrase("internationalization standardization");
  for (size_t shift = 0; shift != 4; ++shift) {
    SCOPED_TRACE(shift);
    const auto text = absl::StrCat(Repeat("x", kBatch - 2 + shift),
                                   " internationalization standardization");
    EXPECT_EQ(1U, Verify(phrase, text).freq);
  }
}

TEST(PhraseCheckTest, json_values_check_the_text_of_strings) {
  const irs::CompiledPhrase compiled{Phrase("quick brown"), {}, {}};
  irs::PhraseCheck check{compiled, TextOf<DenseWords>(), true};
  irs::PhraseVerdict verdict;
  const auto json = duckdb::LogicalType::JSON();
  EXPECT_TRUE(CheckText(check, R"("quick brown")", verdict, json));
  EXPECT_TRUE(CheckText(check, R"("the quick brown fox")", verdict, json));
  EXPECT_TRUE(CheckText(check, " \"quick brown\"\n", verdict, json));
  EXPECT_TRUE(CheckText(check, "\"quick\\u0020brown\"", verdict, json));
  EXPECT_FALSE(CheckText(check, R"(["quick brown"])", verdict, json));
  EXPECT_FALSE(CheckText(check, "quick brown", verdict, json));
  EXPECT_FALSE(CheckText(check, "42", verdict, json));
  EXPECT_FALSE(CheckText(check, "true", verdict, json));
  EXPECT_FALSE(CheckText(check, "null", verdict, json));
}

TEST(PhraseCheckTest, only_json_typed_values_are_decoded) {
  const irs::CompiledPhrase compiled{Phrase("quick brown"), {}, {}};
  const auto tokens = TextOf<DenseWords>();
  irs::PhraseCheck text{compiled, tokens, true};
  irs::PhraseCheck json{compiled, tokens, true};
  irs::PhraseVerdict verdict;
  for (const std::string_view value :
       {R"("quick brown")", "\"quick\\u0020brown\"", " \"quick brown\" "}) {
    EXPECT_FALSE(CheckText(text, value, verdict)) << value;
    EXPECT_TRUE(CheckText(json, value, verdict, duckdb::LogicalType::JSON()))
      << value;
  }
  EXPECT_TRUE(CheckText(text, "quick brown", verdict));
  EXPECT_FALSE(
    CheckText(json, "quick brown", verdict, duckdb::LogicalType::JSON()));
}

TEST(PhraseCheckTest, json_rows_of_every_kind) {
  constexpr std::string_view kRows[] = {
    R"("a quick brown")",
    "\"quick\\u0020brown\"",
    " \"quick brown\" ",
    R"("brown quick")",
    "42",
    "true",
    "null",
    R"({"t":"quick brown"})",
    R"(["quick brown"])",
    R"("quick brown")",
  };
  constexpr size_t kSqlNull = 9;
  constexpr bool kMatches[] = {true,  true,  true,  false, false,
                               false, false, false, false, false};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(),
                   {duckdb::LogicalType::JSON()});
  chunk.SetChildCardinality(std::size(kRows));
  auto& rows = chunk.data[0];
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(rows);
  for (size_t i = 0; i != std::size(kRows); ++i) {
    data[i] = {kRows[i].data(), static_cast<uint32_t>(kRows[i].size())};
  }
  duckdb::FlatVector::ValidityMutable(rows).SetInvalid(kSqlNull);
  const irs::CompiledPhrase compiled{Phrase("quick brown"), {}, {}};
  irs::PhraseCheck check{compiled, TextOf<DenseWords>(), true};
  check.Bind(rows, std::size(kRows));
  for (size_t i = 0; i != std::size(kRows); ++i) {
    EXPECT_EQ(kMatches[i], check.Check(i).freq != 0) << kRows[i];
  }
}

TEST(PhraseCheckTest, array_rows_skip_null_rows_and_elements) {
  const auto type = duckdb::LogicalType::ARRAY(duckdb::LogicalType::VARCHAR, 2);
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), {type});
  chunk.SetChildCardinality(4);
  auto& rows = chunk.data[0];
  const auto pair = [](duckdb::Value first, duckdb::Value second) {
    return duckdb::Value::ARRAY(duckdb::LogicalType::VARCHAR,
                                {std::move(first), std::move(second)});
  };
  rows.SetValue(0, pair("quick brown fox", "lazy dog"));
  rows.SetValue(1, pair("brown quick", "fox brown"));
  rows.SetValue(
    2, pair(duckdb::Value{duckdb::LogicalType::VARCHAR}, "a quick brown"));
  rows.SetValue(3, duckdb::Value{type});
  const irs::CompiledPhrase compiled{Phrase("quick brown"), {}, {}};
  irs::PhraseCheck check{compiled, TextOf<DenseWords>(), true};
  check.Bind(rows, 4);
  EXPECT_NE(0U, check.Check(0).freq);
  EXPECT_EQ(0U, check.Check(1).freq);
  EXPECT_NE(0U, check.Check(2).freq);
  EXPECT_EQ(0U, check.Check(3).freq);
}

TEST(PhraseCheckTest, progress_tokens_are_fed_once) {
  const auto text = absl::StrCat(Repeat("a b", 15), " x x");
  EXPECT_EQ(15U, Verify<ProgressWords>(Phrase("a b"), text).freq);
}

TEST(PhraseCheckTest, rejected_values_contribute_no_tokens) {
  const auto phrase = Phrase("quick brown");
  EXPECT_TRUE(Matches<RejectingWords>(phrase, "quick brown fox"));
  EXPECT_FALSE(Matches<RejectingWords>(phrase, "quick brown fox !"));
}

namespace {

inline constexpr irs::field_id kStoreId = 1;
inline constexpr irs::field_id kPlainId = 2;
inline constexpr irs::field_id kPositionalId = 3;
inline constexpr irs::field_id kDecoyId = 4;
inline constexpr irs::field_id kNormBase = 16;

void ExpectFamilies(const tests::Families& expected,
                    const tests::Families& actual) {
  EXPECT_EQ(expected.count, actual.count);
  EXPECT_EQ(expected.docs, actual.docs);
  EXPECT_EQ(expected.docs, actual.fill);
  EXPECT_EQ(expected.docs, actual.probe);
  {
    SCOPED_TRACE("hits");
    tests::ExpectScores(expected.hits, actual.hits);
  }
  {
    SCOPED_TRACE("fill");
    tests::ExpectScores(expected.hits, actual.fill_scores);
  }
  {
    SCOPED_TRACE("probe");
    tests::ExpectScores(expected.hits, actual.probe_scores);
  }
  EXPECT_EQ(expected.top_total, actual.top_total);
  ASSERT_EQ(expected.top.size(), actual.top.size());
  for (size_t i = 0; i != expected.top.size(); ++i) {
    EXPECT_FLOAT_EQ(expected.top[i], actual.top[i]) << i;
  }
}

struct StoredText {
  irs::field_id column;
  irs::ByPhraseOptions::TokenizerFactory tokenizer;
};

template<typename Words>
StoredText Tokens(irs::field_id column = kStoreId) {
  return {column, TextOf<Words>()};
}

void SetText(irs::ByPhraseOptions& options, StoredText text) {
  options.set_text(text.column, std::move(text.tokenizer));
}

struct Layout {
  size_t segment_docs = std::numeric_limits<size_t>::max();
  irs::IndexFeatures plain = irs::IndexFeatures::Freq;
  irs::IndexFeatures positional =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  bool decoy = false;
  size_t stored = std::numeric_limits<size_t>::max();
  const irs::Scorer* bounds = nullptr;
};

class Index : public tests::FamilyIndex {
 public:
  template<typename Words>
  Index(std::span<const std::string> docs, std::type_identity<Words>,
        Layout layout = {}) {
    auto options = irs::tests::DefaultWriterOptions();
    options.reader_options.scorer = layout.bounds;
    options.norm_column_id = [](irs::field_id id) { return id + kNormBase; };
    auto writer =
      irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
    EXPECT_NE(nullptr, writer);
    Words plain;
    Words positional;
    tests::AnalyzedField plain_field{
      .analyzer = &plain, .id = kPlainId, .features = layout.plain};
    tests::AnalyzedField positional_field{.analyzer = &positional,
                                          .id = kPositionalId,
                                          .features = layout.positional};
    for (size_t begin = 0; begin < docs.size(); begin += layout.segment_docs) {
      auto ctx = writer->GetBatch();
      const auto end = std::min(docs.size(), begin + layout.segment_docs);
      for (auto i = begin; i != end; ++i) {
        plain_field.value = docs[i];
        positional_field.value = docs[i];
        auto doc = ctx.Insert();
        EXPECT_TRUE(tests::InsertField(doc, plain_field));
        EXPECT_TRUE(tests::InsertField(doc, positional_field));
        if (i < layout.stored) {
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kStoreId, doc.DocId(),
                                   plain_field);
        }
        if (layout.decoy) {
          std::vector<std::string_view> words =
            absl::StrSplit(docs[i], ' ', absl::SkipEmpty());
          absl::c_reverse(words);
          const auto reversed = absl::StrJoin(words, " ");
          const tests::AnalyzedField decoy_field{.value = reversed,
                                                 .id = kDecoyId};
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kDecoyId, doc.DocId(),
                                   decoy_field);
        }
      }
      ctx.Commit();
      writer->RefreshCommit();
    }
    Open();
  }
};

class DeadRange final : public irs::detail::TableFilter {
 public:
  DeadRange(irs::doc_id_t begin, irs::doc_id_t end) noexcept
    : _begin{begin}, _end{end} {}

  irs::doc_id_t Live(irs::doc_id_t doc) final { return Dead(doc) ? _end : doc; }

  bool Foldable() const noexcept final { return true; }

  uint64_t CountAndClear(irs::doc_id_t base, uint64_t* mask,
                         uint32_t words) final {
    const uint64_t live = Narrow(base, mask, mask, nullptr, words);
    std::memset(mask, 0, words * sizeof(uint64_t));
    return live;
  }

  uint32_t Narrow(irs::doc_id_t* docs, irs::score_t* scores, uint32_t n) final {
    uint32_t kept = 0;
    for (uint32_t i = 0; i != n; ++i) {
      if (Dead(docs[i])) {
        continue;
      }
      docs[kept] = docs[i];
      if (scores) {
        scores[kept] = scores[i];
      }
      ++kept;
    }
    return kept;
  }

  uint32_t Narrow(irs::doc_id_t base, const uint64_t* in, uint64_t* out,
                  irs::score_t*, uint32_t words) final {
    uint32_t kept = 0;
    for (uint32_t w = 0; w != words; ++w) {
      auto word = in[w];
      for (auto bits = in[w]; bits != 0; bits &= bits - 1) {
        const auto bit = std::countr_zero(bits);
        if (Dead(base + static_cast<irs::doc_id_t>(w * 64 + bit))) {
          word &= ~(uint64_t{1} << bit);
        } else {
          ++kept;
        }
      }
      out[w] = word;
    }
    return kept;
  }

 private:
  bool Dead(irs::doc_id_t doc) const noexcept {
    return _begin <= doc && doc < _end;
  }

  irs::doc_id_t _begin;
  irs::doc_id_t _end;
};

irs::ByPhrase PhraseOn(irs::field_id field, irs::ByPhraseOptions options,
                       std::optional<StoredText> text = std::nullopt) {
  irs::ByPhrase filter;
  *filter.mutable_field_id() = field;
  *filter.mutable_options() = std::move(options);
  if (text) {
    SetText(*filter.mutable_options(), std::move(*text));
  }
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

irs::Filter::ptr IndexPart(const irs::ByPhrase& phrase, const Index& index,
                           const irs::Scorer* scorer = nullptr) {
  irs::Filter::ptr root = std::make_unique<irs::ByPhrase>(phrase);
  irs::Deferral deferral{.reader = index.Reader()};
  irs::Optimize(root, {.scored = static_cast<bool>(scorer),
                       .scorer = scorer,
                       .deferral = &deferral});
  if (deferral.filters.empty()) {
    EXPECT_EQ(irs::Type<irs::ByPhrase>::id(), root->type());
    return nullptr;
  }
  EXPECT_EQ(1U, deferral.filters.size());
  const auto& deferred = deferral.filters.front();
  EXPECT_EQ(irs::Type<irs::ByPhrase>::id(), deferred.filter->type());
  EXPECT_EQ(phrase.options().text(), deferred.column);
  return root;
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
  const auto checked = Lowered(PhraseOn(kPlainId, phrase, Tokens<Words>()));
  ExpectFamilies(expected, index.Run(*checked));
  tests::ExpectScores(freqs, index.Freqs(*checked));
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
  const auto lowered = Lowered(PhraseOn(kPlainId, phrase, Tokens<Words>()));
  ASSERT_EQ(irs::Type<irs::ByPhrase>::id(), lowered->type());
  const auto& filter = irs::utils::downCast<irs::ByPhrase>(*lowered);
  const auto& options = filter.options();
  ASSERT_TRUE(irs::IsStandalone(options));
  const auto expected = index.Docs(filter);
  const auto conjunction = IndexPart(filter, index);
  ASSERT_NE(nullptr, conjunction);
  const auto candidates = index.Docs(*conjunction);
  ASSERT_LE(candidates.size(), STANDARD_VECTOR_SIZE);
  EXPECT_TRUE(std::ranges::includes(candidates, expected));
  const auto stats = irs::PhraseWordStats(options, index.Reader(), kPlainId);
  for (const auto match : kMatches) {
    SCOPED_TRACE(MatchName(match));
    irs::CompiledPhrase compiled{options, {}, stats};
    Force(compiled, match);
    irs::PhraseCheck check{compiled, options.tokenizer(), false};
    duckdb::DataChunk chunk;
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(),
                     {duckdb::LogicalType::VARCHAR});
    chunk.SetChildCardinality(candidates.size());
    auto* values =
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(chunk.data[0]);
    for (size_t i = 0; i != candidates.size(); ++i) {
      const auto& text = docs[candidates[i]];
      values[i] = {text.data(), static_cast<uint32_t>(text.size())};
    }
    check.Bind(chunk.data[0], candidates.size());
    std::vector<irs::doc_id_t> actual;
    for (size_t i = 0; i != candidates.size(); ++i) {
      if (check.Check(i).freq != 0) {
        actual.push_back(candidates[i]);
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
  const Index index{
    docs, std::type_identity<DenseWords>{}, {.segment_docs = 128}};
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
    tests::ExpectScores(expected,
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

TEST(TokenPhraseIndexTest, early_stopping_scans_agree_with_positions) {
  constexpr std::string_view kWords[] = {"x", "x", "x", "a", "b", "c", "the"};
  std::mt19937 rng{23};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 60; ++i) {
    docs.push_back(RandomText(rng, kWords, 100 + rng() % 1500));
  }
  const Index index{
    docs, std::type_identity<ProgressWords>{}, {.segment_docs = 16}};
  for (size_t i = 0; i != 60; ++i) {
    const auto phrase = RandomPhrase(rng, kWords);
    SCOPED_TRACE(i);
    ExpectLikePositions<ProgressWords>(index, phrase);
  }
  for (const auto* text : {"a b", "a b c", "the a", "c x a"}) {
    SCOPED_TRACE(text);
    ExpectLikePositions<ProgressWords>(index, Phrase(text));
  }
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
  const Index index{
    docs, std::type_identity<DenseWords>{}, {.segment_docs = 1500}};
  for (const auto* text :
       {"quick brown", "x x", "the quick", "brown fox dog"}) {
    SCOPED_TRACE(text);
    const auto expected =
      index.Run(*LoweredAnd(PhraseOn(kPositionalId, Phrase(text)), "rare"));
    ExpectFamilies(
      expected,
      index.Run(*LoweredAnd(
        PhraseOn(kPlainId, Phrase(text), Tokens<DenseWords>()), "rare")));
    ExpectLikePositions<DenseWords>(index, Phrase(text));
  }
}

TEST(TokenPhraseIndexTest, without_frequency_every_family_scores_constant) {
  const std::vector<std::string> docs{"quick brown quick brown rare",
                                      "quick brown rare", "brown quick rare",
                                      "quick brown quick brown", "quick brown"};
  const Index index{docs,
                    std::type_identity<DenseWords>{},
                    {.plain = irs::IndexFeatures::None}};
  const auto phrase = [] {
    return PhraseOn(kPlainId, Phrase("quick brown"), Tokens<DenseWords>());
  };
  const auto expect_constant = [&](const irs::Filter& filter,
                                   std::vector<irs::doc_id_t> expected) {
    const auto families = index.Run(filter);
    EXPECT_EQ(expected, families.docs);
    ASSERT_EQ(expected.size(), families.hits.size());
    const auto score = families.hits.begin()->second;
    for (const auto& [doc, value] : families.hits) {
      EXPECT_FLOAT_EQ(score, value) << doc;
    }
    tests::ExpectScores(families.hits, families.fill_scores);
    tests::ExpectScores(families.hits, families.probe_scores);
  };
  expect_constant(*Lowered(phrase()), {0, 1, 3, 4});
  expect_constant(*LoweredAnd(phrase(), "rare"), {0, 1});
}

TEST(TokenPhraseIndexTest, deferred_check_agrees_with_inline) {
  constexpr std::string_view kWords[] = {"quick", "quack", "brown", "brawn",
                                         "fox",   "box",   "the",   "dog"};
  std::mt19937 rng{3};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 300; ++i) {
    docs.push_back(RandomText(rng, kWords, 1 + rng() % 20));
  }
  const Index index{
    docs, std::type_identity<DenseWords>{}, {.segment_docs = 64}};
  for (size_t i = 0; i != 80; ++i) {
    const auto phrase = RandomPatternPhrase(rng, kWords);
    SCOPED_TRACE(i);
    ExpectDeferredLikeInline<DenseWords>(index, docs, phrase);
  }
}

TEST(TokenPhraseIndexTest, deferred_check_agrees_with_inline_under_slop) {
  constexpr std::string_view kWords[] = {"quick", "quack", "brown", "brawn",
                                         "fox",   "box",   "the",   "dog"};
  for (const uint32_t seed : {5, 11, 17, 23}) {
    SCOPED_TRACE(seed);
    std::mt19937 rng{seed};
    std::vector<std::string> docs;
    for (size_t i = 0; i != 300; ++i) {
      docs.push_back(RandomText(rng, kWords, 1 + rng() % 20));
    }
    const Index index{
      docs, std::type_identity<DenseWords>{}, {.segment_docs = 64}};
    for (size_t i = 0; i != 200; ++i) {
      auto phrase = RandomPatternPhrase(rng, kWords);
      if (absl::c_any_of(phrase, [](const auto& info) {
            return info.offs_min != info.offs_max;
          })) {
        continue;
      }
      phrase.set_slop(1 + rng() % 3);
      SCOPED_TRACE(i);
      ExpectDeferredLikeInline<DenseWords>(index, docs, phrase);
    }
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
  const Index index{
    docs, std::type_identity<StackedWords>{}, {.segment_docs = 64}};
  constexpr std::string_view kTerms[] = {"red", "car", "auto", "big"};
  for (size_t i = 0; i != 40; ++i) {
    const auto phrase = RandomPatternPhrase(rng, kTerms);
    SCOPED_TRACE(i);
    ExpectDeferredLikeInline<StackedWords>(index, docs, phrase);
  }
}

TEST(TokenPhraseFilterTest, parts_conjunction_has_one_filter_per_part) {
  irs::ByPhraseOptions options;
  options.push_back<irs::ByTermOptions>().term = Bytes("quick");
  auto& set = options.push_back<irs::TermSetOptions>();
  set.terms.emplace(Bytes("brown"));
  set.terms.emplace(Bytes("brawn"));
  options.push_back<irs::ByPrefixOptions>().term = Bytes("fo");
  const auto phrase =
    PhraseOn(kPlainId, std::move(options), Tokens<DenseWords>());
  const std::vector<std::string> docs{"quick brown fox"};
  const Index index{docs, std::type_identity<DenseWords>{}};

  const auto conjunction = IndexPart(phrase, index);
  ASSERT_NE(nullptr, conjunction);
  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), conjunction->type());
  const auto& all = irs::utils::downCast<irs::BooleanFilter>(*conjunction);
  EXPECT_TRUE(all.Transparent());
  ASSERT_EQ(1U, all.Terms(irs::Occur::Must).size());
  EXPECT_EQ(kPlainId, all.Terms(irs::Occur::Must).front().field);
  EXPECT_EQ(Bytes("quick"), all.Terms(irs::Occur::Must).front().term);
  const auto filters = all.Filters(irs::Occur::Must);
  ASSERT_EQ(2U, filters.size());

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), filters[0]->type());
  const auto& any = irs::utils::downCast<irs::BooleanFilter>(*filters[0]);
  EXPECT_EQ(2U, any.Terms(irs::Occur::Should).size());
  EXPECT_EQ(1U, any.MinShouldMatch());
  EXPECT_EQ(irs::ScoreMergeType::Sum, any.MergeType());

  ASSERT_EQ(irs::Type<irs::ByPrefix>::id(), filters[1]->type());
  const auto& prefix = irs::utils::downCast<irs::ByPrefix>(*filters[1]);
  EXPECT_EQ(kPlainId, prefix.field_id());
  EXPECT_EQ(Bytes("fo"), prefix.options().term);
}

TEST(TokenPhraseFilterTest, parts_conjunction_keeps_one_clause_per_word) {
  const auto phrase =
    PhraseOn(kPlainId, Phrase("the cat in the hat"), Tokens<DenseWords>());
  const std::vector<std::string> docs{"the cat in the hat"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  const auto conjunction = IndexPart(phrase, index);
  ASSERT_NE(nullptr, conjunction);
  const auto& all = irs::utils::downCast<irs::BooleanFilter>(*conjunction);
  EXPECT_EQ((std::vector<irs::bstring>{
              irs::bstring{Bytes("cat")}, irs::bstring{Bytes("hat")},
              irs::bstring{Bytes("in")}, irs::bstring{Bytes("the")}}),
            std::ranges::to<std::vector>(
              all.Terms(irs::Occur::Must) |
              std::views::transform(&irs::TermClause::term)));
}

TEST(TokenPhraseFilterTest, part_filter_of_every_kind) {
  const auto term = irs::PartFilter(
    kPlainId, irs::ByTermOptions{.term = irs::bstring{Bytes("fox")}}, false);
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), term->type());
  EXPECT_EQ(Bytes("fox"),
            irs::utils::downCast<irs::ByTerm>(*term).options().term);

  irs::TermSetOptions set;
  set.terms.emplace(Bytes("fox"));
  set.terms.emplace(Bytes("box"));
  const auto any = irs::PartFilter(kPlainId, set, true);
  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), any->type());
  const auto& boolean = irs::utils::downCast<irs::BooleanFilter>(*any);
  EXPECT_EQ(2U, boolean.Terms(irs::Occur::Should).size());
  EXPECT_EQ(irs::ScoreMergeType::Max, boolean.MergeType());

  const auto none = irs::PartFilter(kPlainId, irs::TermSetOptions{}, false);
  EXPECT_EQ(irs::Type<irs::Empty>::id(), none->type());

  irs::ByRangeOptions range;
  range.range.min = irs::bstring{Bytes("b")};
  range.range.max = irs::bstring{Bytes("d")};
  const auto between = irs::PartFilter(kPlainId, range, false);
  ASSERT_EQ(irs::Type<irs::ByRange>::id(), between->type());
  EXPECT_EQ(range, irs::utils::downCast<irs::ByRange>(*between).options());
}

TEST(TokenPhraseFilterTest, parts_conjunction_follows_the_scorer) {
  auto phrase = PhraseOn(kPlainId, Phrase("quick brown"), Tokens<DenseWords>());
  phrase.SetBoost(2.f);
  const std::vector<std::string> docs{"quick brown fox"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  const irs::BM25 bm25;
  const irs::ConstantScore constant{3.f};
  const auto& unscored = irs::Unscored::Instance();
  const auto conjunction = [&](const irs::Scorer* scorer) {
    auto filter = IndexPart(phrase, index, scorer);
    EXPECT_TRUE(!filter ||
                filter->type() == irs::Type<irs::BooleanFilter>::id());
    return filter;
  };
  const auto merge = [](const irs::Filter& filter) {
    return irs::utils::downCast<irs::BooleanFilter>(filter).MergeType();
  };

  EXPECT_EQ(nullptr, conjunction(&bm25));

  const auto plain = conjunction(nullptr);
  ASSERT_NE(nullptr, plain);
  EXPECT_TRUE(irs::utils::downCast<irs::BooleanFilter>(*plain).Transparent());

  const auto by_query = conjunction(&constant);
  ASSERT_NE(nullptr, by_query);
  EXPECT_EQ(nullptr, by_query->GetScorer());
  EXPECT_EQ(2.f, by_query->GetBoost());
  EXPECT_EQ(irs::ScoreMergeType::Max, merge(*by_query));

  phrase.SetScorer(&constant);
  const auto own = conjunction(&bm25);
  ASSERT_NE(nullptr, own);
  EXPECT_EQ(&constant, own->GetScorer());
  EXPECT_EQ(irs::ScoreMergeType::Max, merge(*own));

  phrase.SetScorer(&unscored);
  const auto silent = conjunction(&bm25);
  ASSERT_NE(nullptr, silent);
  EXPECT_EQ(&unscored, silent->GetScorer());
  EXPECT_EQ(irs::ScoreMergeType::Sum, merge(*silent));

  phrase.SetScorer(&bm25);
  EXPECT_EQ(nullptr, conjunction(&constant));
}

TEST(TokenPhraseIndexTest, constant_score_conjunction_scores_like_the_phrase) {
  const std::vector<std::string> docs{"quick brown fox",
                                      "quick brawn fox",
                                      "brown quick",
                                      "quick brown brawn",
                                      "quick brown fox quick brawn",
                                      "fox"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  irs::ByPhraseOptions options;
  options.push_back<irs::ByTermOptions>().term = Bytes("quick");
  auto& set = options.push_back<irs::TermSetOptions>();
  set.terms.emplace(Bytes("brown"));
  set.terms.emplace(Bytes("brawn"));
  auto phrase = PhraseOn(kPlainId, std::move(options), Tokens<DenseWords>());
  phrase.SetBoost(2.f);
  const irs::ConstantScore constant{3.f};
  const auto conjunction = IndexPart(phrase, index, &constant);
  ASSERT_NE(nullptr, conjunction);

  const auto expected = index.ScoresBy(constant, phrase);
  const auto actual = index.ScoresBy(constant, *conjunction);
  EXPECT_EQ((std::vector<irs::doc_id_t>{0, 1, 3, 4}),
            std::ranges::to<std::vector>(std::views::keys(expected)));
  for (const auto& [doc, score] : expected) {
    const auto it = actual.find(doc);
    ASSERT_NE(actual.end(), it) << doc;
    EXPECT_FLOAT_EQ(score, it->second) << doc;
    EXPECT_FLOAT_EQ(6.f, it->second) << doc;
  }
}

TEST(TokenPhraseIndexTest, ranked_fuzzy_words_score_like_exact_words) {
  const std::vector<std::string> docs{"quick brown fox", "quick brawn fox",
                                      "brown quick", "fox"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  auto phrase = Phrase("quick");
  auto& fuzzy = phrase.push_back<irs::ByEditDistanceOptions>();
  fuzzy.term = Bytes("brown");
  fuzzy.max_distance = 1;
  const auto positional = index.Run(*Lowered(PhraseOn(kPositionalId, phrase)));
  const auto checked =
    index.Run(*Lowered(PhraseOn(kPlainId, phrase, Tokens<DenseWords>())));
  EXPECT_EQ((std::vector<irs::doc_id_t>{0, 1}), checked.docs);
  EXPECT_EQ(positional.docs, checked.docs);
  tests::ExpectScores(checked.hits, checked.fill_scores);
  tests::ExpectScores(checked.hits, checked.probe_scores);
  ASSERT_EQ(2U, checked.hits.size());
  EXPECT_FLOAT_EQ(checked.hits.at(0), checked.hits.at(1));
  EXPECT_FLOAT_EQ(positional.hits.at(0), checked.hits.at(0));
  EXPECT_LT(positional.hits.at(1), checked.hits.at(1));
}

TEST(TokenPhraseIndexTest, reads_only_the_named_stored_field) {
  constexpr std::string_view kWords[] = {"the", "quick", "brown", "fox",
                                         "the", "dog",   "a"};
  std::mt19937 rng{13};
  std::vector<std::string> docs;
  for (size_t i = 0; i != 300; ++i) {
    docs.push_back(RandomText(rng, kWords, 1 + rng() % 20));
  }
  const Index index{docs,
                    std::type_identity<DenseWords>{},
                    {.segment_docs = 64, .decoy = true}};
  for (const auto* text : {"quick brown", "the fox", "brown fox dog"}) {
    SCOPED_TRACE(text);
    const auto expected =
      index.Run(*Lowered(PhraseOn(kPositionalId, Phrase(text))));
    ExpectFamilies(expected, index.Run(*Lowered(PhraseOn(
                               kPlainId, Phrase(text), Tokens<DenseWords>()))));
    EXPECT_NE(index.Docs(*Lowered(
                PhraseOn(kPlainId, Phrase(text), Tokens<DenseWords>()))),
              index.Docs(*Lowered(PhraseOn(kPlainId, Phrase(text),
                                           Tokens<DenseWords>(kDecoyId)))));
  }
}

TEST(TokenPhraseIndexTest, standalone_patterns_skip_shingles) {
  const std::vector<std::string> docs{"the quick brown fox"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  auto phrase = Phrase("quick");
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  phrase.set_word_separator(Bytes("_"));
  const irs::CompiledPhrase compiled{
    phrase, {}, irs::PhraseWordStats(phrase, index.Reader(), kPlainId)};
  irs::PhraseCheck phrase_check{compiled, TextOf<DenseWords>(), true};
  const auto check = [&](std::string_view text) {
    irs::PhraseVerdict verdict;
    return CheckText(phrase_check, text, verdict);
  };
  EXPECT_TRUE(check("the quick brown fox"));
  EXPECT_FALSE(check("the quick brown_fox"));
  EXPECT_FALSE(check("the quick dread"));
}

TEST(TokenPhraseIndexTest, without_stored_column_matches_nothing) {
  const std::vector<std::string> docs{"quick brown fox", "quick brown"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  const auto filter = PhraseOn(kPlainId, Phrase("quick brown"),
                               Tokens<DenseWords>(irs::field_id{42}));
  EXPECT_TRUE(index.Docs(filter).empty());
  const auto checked =
    PhraseOn(kPlainId, Phrase("quick brown"), Tokens<DenseWords>());
  EXPECT_EQ((std::vector<irs::doc_id_t>{0, 1}), index.Docs(checked));
  EXPECT_TRUE(index.Docs(PhraseOn(kPlainId, Phrase("quick brown"))).empty());
}

TEST(TokenPhraseIndexTest, segments_without_the_field_match_nothing) {
  const std::vector<std::string> docs{
    "", "", "", "quick brown fox", "brown quick", "fox"};
  const Index index{
    docs, std::type_identity<DenseWords>{}, {.segment_docs = 3}};
  ASSERT_TRUE(absl::c_any_of(index.Reader(), [](const irs::SubReader& segment) {
    return !segment.field(kPlainId);
  }));
  ExpectLikePositions<DenseWords>(index, Phrase("quick brown"));
  EXPECT_EQ((std::vector<irs::doc_id_t>{3}),
            index.Docs(
              PhraseOn(kPlainId, Phrase("quick brown"), Tokens<DenseWords>())));
}

TEST(TokenPhraseIndexTest, positions_win_over_tokens) {
  const std::vector<std::string> docs{"quick brown fox", "brown quick",
                                      "quick x brown", "quick brown"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  const auto phrase = Phrase("quick brown");
  const auto expected = index.Run(PhraseOn(kPositionalId, phrase));
  EXPECT_EQ((std::vector<irs::doc_id_t>{0, 3}), expected.docs);
  ExpectFamilies(expected,
                 index.Run(PhraseOn(kPositionalId, phrase,
                                    Tokens<DenseWords>(irs::field_id{42}))));
}

TEST(TokenPhraseIndexTest, word_stats_sum_every_segment) {
  std::vector<std::string> docs(19, "quick brown");
  docs[0].clear();
  docs[1].clear();
  const Index index{
    docs, std::type_identity<DenseWords>{}, {.segment_docs = 2}};
  ASSERT_EQ(10U, index.Reader().size());
  ASSERT_EQ(nullptr, index.Reader()[0].field(kPlainId));
  const auto stats =
    irs::PhraseWordStats(Phrase("quick brown"), index.Reader(), kPlainId);
  ASSERT_EQ(2U, stats.size());
  for (const auto& stat : stats) {
    EXPECT_EQ(17U, stat.docs);
    EXPECT_EQ(17U, stat.freq);
  }
}

TEST(TokenPhraseIndexTest, word_stats_follow_the_shingle_words) {
  const std::vector<std::string> docs{"quick brown fox", "quick fox", "fox"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  const auto stats =
    irs::PhraseWordStats(QuickBrownFoxShingles(), index.Reader(), kPlainId);
  const auto words =
    irs::PhraseWordStats(Phrase("quick brown fox"), index.Reader(), kPlainId);
  ASSERT_EQ(3U, stats.size());
  ASSERT_EQ(words.size(), stats.size());
  for (size_t i = 0; i != stats.size(); ++i) {
    EXPECT_EQ(words[i].docs, stats[i].docs);
    EXPECT_EQ(words[i].freq, stats[i].freq);
  }
  EXPECT_EQ(3U, stats[2].docs);
}

TEST(TokenPhraseIndexTest, seeks_within_and_past_the_checked_batch) {
  const std::vector<std::string> docs{
    "quick brown",   "brown quick", "quick brown", "quick brown",
    "quick x brown", "quick brown", "quick brown", "quick brown"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  const auto filter =
    PhraseOn(kPlainId, Phrase("quick brown"), Tokens<DenseWords>());
  tests::PreparedFilter prepared{filter, index.Reader()};
  ASSERT_EQ(1U, prepared.size());
  const auto cursor = prepared.Execute(0);
  ASSERT_NE(nullptr, cursor);
  constexpr auto kFirst = irs::doc_limits::min();
  EXPECT_EQ(kFirst, cursor->Next());
  EXPECT_EQ(kFirst + 2, cursor->Next());
  EXPECT_EQ(kFirst + 5, cursor->Seek(kFirst + 5));
  EXPECT_EQ(kFirst + 6, cursor->Next());
  EXPECT_TRUE(irs::doc_limits::eof(cursor->Seek(kFirst + 8)));
}

TEST(TokenPhraseIndexTest, rows_past_the_stored_column_never_match) {
  const std::vector<std::string> docs{"quick brown", "the quick brown fox",
                                      "quick brown", "quick brown"};
  const Index index{docs, std::type_identity<DenseWords>{}, {.stored = 2}};
  const auto families = index.Run(
    *Lowered(PhraseOn(kPlainId, Phrase("quick brown"), Tokens<DenseWords>())));
  const std::vector<irs::doc_id_t> expected{0, 1};
  EXPECT_EQ(expected.size(), families.count);
  EXPECT_EQ(expected, families.docs);
  EXPECT_EQ(expected, families.fill);
  EXPECT_EQ(expected, families.probe);
  EXPECT_EQ(expected,
            std::ranges::to<std::vector>(std::views::keys(families.hits)));
  EXPECT_EQ(expected.size(), families.top_total);
}

TEST(TokenPhraseIndexTest, table_filters_narrow_count_and_top) {
  const std::vector<std::string> docs{
    "quick brown",     "brown quick",
    "quick brown fox", "quick brown quick brown",
    "quick x brown",   "quick brown"};
  constexpr auto kFirst = irs::doc_limits::min();
  const auto end = static_cast<irs::doc_id_t>(kFirst + docs.size());
  for (const bool freq : {true, false}) {
    SCOPED_TRACE(freq);
    const Index index{
      docs,
      std::type_identity<DenseWords>{},
      {.plain = freq ? irs::IndexFeatures::Freq : irs::IndexFeatures::None}};
    const auto filter =
      PhraseOn(kPlainId, Phrase("quick brown"), Tokens<DenseWords>());
    irs::BM25 scorer;
    MaxMemoryCounter counter;
    tests::PreparedFilter prepared{filter, index.Reader(), &scorer, counter};
    ASSERT_EQ(1U, prepared.size());
    const auto* query = prepared.Query(0);
    ASSERT_NE(nullptr, query);
    DeadRange table{kFirst, kFirst + 3};
    EXPECT_EQ(2U, query->PlanCount({.table = &table})->Run(kFirst, end));
    irs::ColumnArgsFetcher fetcher;
    auto top = query->PlanTop({.scorer = scorer,
                               .fetcher = fetcher,
                               .table = &table,
                               .prune = false,
                               .k = tests::kTop});
    std::vector<irs::ScoreDoc> hits(tests::kTop);
    std::atomic<irs::score_t> threshold{
      std::numeric_limits<irs::score_t>::lowest()};
    irs::LoserScoreCollector collector{threshold, hits};
    top->Run(kFirst, end, collector);
    EXPECT_EQ(2U, collector.TotalMatches());
    EXPECT_EQ(2U, collector.AcceptedCount());
  }
}

struct Top {
  std::vector<irs::score_t> scores;
  uint64_t reached = 0;
};

Top TopOf(const Index& index, const irs::Filter& filter,
          const irs::Scorer& scorer, bool prune,
          irs::detail::TableFilter* table = nullptr, size_t k = tests::kTop) {
  MaxMemoryCounter counter;
  tests::PreparedFilter prepared{filter, index.Reader(), &scorer, counter};
  std::vector<irs::ScoreDoc> hits(k);
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
                               .table = table,
                               .prune = prune,
                               .k = static_cast<uint32_t>(k)});
    top->Run(irs::doc_limits::min(), irs::doc_limits::eof(), collector);
  }
  Top out{.reached = collector.TotalMatches()};
  for (size_t j = 0; j != collector.AcceptedCount(); ++j) {
    out.scores.push_back(hits[j].score);
  }
  absl::c_sort(out.scores, std::greater<>{});
  return out;
}

void ExpectSameTop(const Top& expected, const Top& actual) {
  ASSERT_EQ(expected.scores.size(), actual.scores.size());
  for (size_t i = 0; i != expected.scores.size(); ++i) {
    EXPECT_FLOAT_EQ(expected.scores[i], actual.scores[i]) << i;
  }
}

Layout Bounded(const irs::Scorer& scorer) {
  return {.segment_docs = 1500,
          .plain = irs::IndexFeatures::Freq | irs::IndexFeatures::Norm,
          .positional = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                        irs::IndexFeatures::Norm,
          .bounds = &scorer};
}

std::vector<std::string> RandomDocs(std::span<const std::string_view> words,
                                    size_t count) {
  std::mt19937 rng{20261013};
  std::vector<std::string> docs(count);
  for (auto& doc : docs) {
    doc = RandomText(rng, words, 1 + rng() % 48);
  }
  return docs;
}

TEST(TokenPhraseIndexTest, pruned_top_prunes_only_anchor_bounded_phrases) {
  constexpr std::string_view kWords[] = {"ab", "ac", "b", "c", "d"};
  const auto docs = RandomDocs(kWords, 4000);
  const irs::BM25 scorer;
  const Index index{docs, std::type_identity<DenseWords>{}, Bounded(scorer)};
  const auto phrase = [](std::initializer_list<std::string_view> words) {
    irs::ByPhraseOptions out;
    for (const auto word : words) {
      if (word.ends_with('*')) {
        out.push_back<irs::ByPrefixOptions>().term =
          Bytes(word.substr(0, word.size() - 1));
      } else {
        out.push_back<irs::ByTermOptions>().term = Bytes(word);
      }
    }
    return out;
  };
  auto skip = phrase({"ab"});
  PushTerm(skip, "c", 2, 2);
  auto set = phrase({"b"});
  auto& terms = set.push_back<irs::TermSetOptions>().terms;
  terms.emplace(Bytes("ab"));
  terms.emplace(Bytes("d"));
  auto gap = phrase({"ab"});
  PushTerm(gap, "b", 1, 3);
  auto sloppy = phrase({"ab", "c"});
  sloppy.set_slop(1);
  const std::pair<irs::ByPhraseOptions, bool> cases[] = {
    {phrase({"ab", "b"}), true}, {phrase({"c", "ab", "b"}), true},
    {phrase({"b", "b"}), true},  {phrase({"b", "a*"}), true},
    {std::move(skip), true},     {std::move(set), true},
    {std::move(gap), false},     {std::move(sloppy), false},
  };
  DeadRange table{irs::doc_limits::min(), irs::doc_limits::min() + 100};
  for (const auto& [options, prunes] : cases) {
    SCOPED_TRACE(Describe(options));
    const auto checked =
      Lowered(PhraseOn(kPlainId, options, Tokens<DenseWords>()));
    const auto positional = Lowered(PhraseOn(kPositionalId, options));
    for (auto* filter : {static_cast<irs::detail::TableFilter*>(nullptr),
                         static_cast<irs::detail::TableFilter*>(&table)}) {
      SCOPED_TRACE(static_cast<bool>(filter));
      const auto full = TopOf(index, *checked, scorer, false, filter);
      const auto pruned = TopOf(index, *checked, scorer, true, filter);
      ASSERT_EQ(tests::kTop, full.scores.size());
      ExpectSameTop(full, pruned);
      ExpectSameTop(full, TopOf(index, *positional, scorer, true, filter));
      if (prunes) {
        EXPECT_LT(pruned.reached, full.reached);
      } else {
        EXPECT_EQ(pruned.reached, full.reached);
      }
    }
  }
}

irs::Filter::ptr Shaped(irs::ByPhrase phrase, irs::field_id field,
                        size_t shape) {
  if (shape == 0) {
    return Lowered(std::move(phrase));
  }
  auto root = std::make_unique<irs::BooleanFilter>();
  const auto occur = shape == 1 ? irs::Occur::Must : irs::Occur::Should;
  root->Add(std::make_unique<irs::ByPhrase>(std::move(phrase)), occur);
  root->Add(irs::TermClause{.field = field, .term = irs::bstring{Bytes("d")}},
            occur);
  if (occur == irs::Occur::Should) {
    root->SetMinShouldMatch(1);
  }
  irs::Filter::ptr out = std::move(root);
  irs::Optimize(out);
  return out;
}

TEST(TokenPhraseIndexTest, pruned_top_matches_full_ranking_on_random_phrases) {
  constexpr std::string_view kWords[] = {"ab", "ac", "b", "c", "d", "e"};
  const auto docs = RandomDocs(kWords, 4000);
  const irs::BM25 scorer;
  const Index index{docs, std::type_identity<DenseWords>{}, Bounded(scorer)};
  DeadRange table{irs::doc_limits::min() + 200, irs::doc_limits::min() + 900};
  constexpr size_t kTops[] = {1, tests::kTop, 50};
  std::mt19937 rng{1013};
  size_t pruned = 0;
  for (size_t i = 0; i != 200; ++i) {
    const auto options = RandomPhrase(rng, kWords);
    const auto shape = i % 3;
    const auto k = kTops[(i / 3) % std::size(kTops)];
    auto* filter = i % 2 == 0 ? nullptr : &table;
    SCOPED_TRACE(absl::StrCat(Describe(options), " shape=", shape, " k=", k,
                              " table=", static_cast<bool>(filter)));
    const auto checked = Shaped(
      PhraseOn(kPlainId, options, Tokens<DenseWords>()), kPlainId, shape);
    const auto positional =
      Shaped(PhraseOn(kPositionalId, options), kPositionalId, shape);
    const auto full = TopOf(index, *checked, scorer, false, filter, k);
    const auto top = TopOf(index, *checked, scorer, true, filter, k);
    ExpectSameTop(full, top);
    ExpectSameTop(full, TopOf(index, *positional, scorer, false, filter, k));
    pruned += top.reached < full.reached;
  }
  EXPECT_LT(10U, pruned);
}

TEST(TokenPhraseIndexTest, pruned_top_keeps_stacked_positions_unpruned) {
  constexpr std::string_view kWords[] = {"a", "b", "a|b", "c", "b|c"};
  const auto docs = RandomDocs(kWords, 3000);
  const irs::BM25 scorer;
  const Index index{docs, std::type_identity<StackedWords>{}, Bounded(scorer)};
  for (const auto& options : {Phrase("a b"), Phrase("b c a")}) {
    SCOPED_TRACE(Describe(options));
    const auto checked =
      Lowered(PhraseOn(kPlainId, options, Tokens<StackedWords>()));
    const auto full = TopOf(index, *checked, scorer, false);
    const auto pruned = TopOf(index, *checked, scorer, true);
    ASSERT_EQ(tests::kTop, full.scores.size());
    ExpectSameTop(full, pruned);
    EXPECT_EQ(pruned.reached, full.reached);
  }
}

TEST(TokenPhraseIndexTest, nested_boosted_conjunction_scores_like_positions) {
  const std::vector<std::string> docs{"quick brown fox", "quick brown",
                                      "brown quick fox", "fox quick brown fox"};
  const Index index{docs, std::type_identity<DenseWords>{}};
  const auto nested = [](irs::field_id field, std::optional<StoredText> text) {
    auto inner = std::make_unique<irs::BooleanFilter>();
    inner->Add(std::make_unique<irs::ByPhrase>(
                 PhraseOn(field, Phrase("quick brown"), std::move(text))),
               irs::Occur::Must);
    inner->Add(
      irs::TermClause{.field = field, .term = irs::bstring{Bytes("fox")}},
      irs::Occur::Must);
    inner->SetBoost(2.f);
    auto root = std::make_unique<irs::BooleanFilter>();
    root->Add(std::move(inner), irs::Occur::Must);
    root->Add(
      irs::TermClause{.field = field, .term = irs::bstring{Bytes("quick")}},
      irs::Occur::Must);
    return root;
  };
  ExpectFamilies(index.Run(*nested(kPositionalId, std::nullopt)),
                 index.Run(*nested(kPlainId, Tokens<DenseWords>())));
}

TEST(TokenPhraseFilterTest, equality_covers_slop) {
  const auto base = Phrase("quick brown");
  auto sloppy = base;
  sloppy.set_slop(1);
  EXPECT_NE(base, sloppy);
  sloppy.set_slop(0);
  EXPECT_EQ(base, sloppy);
}

TEST(TokenPhraseFilterTest, equality_covers_tokens) {
  const auto base = Phrase("quick brown");
  auto checked = base;
  SetText(checked, Tokens<DenseWords>());
  EXPECT_NE(base, checked);

  auto twin = base;
  SetText(twin, Tokens<DenseWords>());
  EXPECT_EQ(checked, twin);

  auto elsewhere = base;
  SetText(elsewhere, Tokens<DenseWords>(irs::field_id{42}));
  EXPECT_NE(checked, elsewhere);

  checked.clear();
  EXPECT_FALSE(irs::field_limits::valid(checked.text()));
  EXPECT_EQ(irs::ByPhraseOptions{}, checked);
}

TEST(TokenPhraseFilterTest, simplify_keeps_one_slot_phrases_with_tokens) {
  const auto checked =
    Lowered(PhraseOn(kPlainId, Phrase("quick"), Tokens<DenseWords>()));
  EXPECT_EQ(irs::Type<irs::ByPhrase>::id(), checked->type());
  const auto plain = Lowered(PhraseOn(kPlainId, Phrase("quick")));
  EXPECT_NE(irs::Type<irs::ByPhrase>::id(), plain->type());
}

TEST(TokenPhraseFilterTest, shingle_parts_compare_terms_and_words) {
  const auto base = QuickBrownFoxShingles();
  EXPECT_EQ(base, QuickBrownFoxShingles());

  irs::ByPhraseOptions overlapping;
  PushShingle(overlapping, "quick brown", {"quick", "brown"}, 0, 0);
  PushShingle(overlapping, "brown fox", {"brown", "fox"}, 1, 1);
  EXPECT_NE(base, overlapping);

  irs::ByPhraseOptions terms;
  PushTerm(terms, "quick brown", 0, 0);
  PushTerm(terms, "brown fox", 1, 1);
  EXPECT_NE(base, terms);

  auto lowered = base;
  EXPECT_FALSE(lowered.LowerParts());
  EXPECT_EQ(base, lowered);
}
