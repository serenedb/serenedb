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

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <iresearch/analysis/text/term_view.hpp>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/detail/phrase_slop_matcher.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/string.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kField = 1;

irs::bytes_view Bytes(std::string_view s) {
  return irs::ViewCast<irs::byte_type>(s);
}

class Doc {
 public:
  explicit Doc(std::string_view text) {
    irs::PosAttr::value_t pos = irs::pos_limits::min();
    for (const auto word : absl::StrSplit(text, ' ', absl::SkipEmpty())) {
      _words.emplace_back(word);
      _positions.push_back(pos++);
    }
  }

  Doc(std::initializer_list<std::pair<std::string_view, uint32_t>> tokens) {
    for (const auto& [word, pos] : tokens) {
      _words.emplace_back(word);
      _positions.push_back(pos);
    }
  }

  bool Dense() const noexcept {
    for (size_t i = 0; i != _positions.size(); ++i) {
      if (_positions[i] != irs::pos_limits::min() + i) {
        return false;
      }
    }
    return true;
  }

  std::vector<irs::PosAttr::value_t> Positions(std::string_view word) const {
    std::vector<irs::PosAttr::value_t> out;
    for (size_t i = 0; i != _words.size(); ++i) {
      if (_words[i] == word) {
        out.push_back(_positions[i]);
      }
    }
    return out;
  }

  std::string Text() const { return absl::StrJoin(_words, " "); }

  std::string Explicit() const {
    std::string out;
    for (size_t i = 0; i != _words.size(); ++i) {
      absl::StrAppend(&out, i == 0 ? "" : " ", _words[i], "@", _positions[i]);
    }
    return out;
  }

 private:
  std::vector<std::string> _words;
  std::vector<irs::PosAttr::value_t> _positions;
};

class WhitespaceTokenizer final
  : public irs::analysis::TypedTokenizer<WhitespaceTokenizer> {
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

class ExplicitTokenizer final
  : public irs::analysis::TypedTokenizer<ExplicitTokenizer> {
 public:
  irs::TokenTraits Traits() const noexcept final {
    return {.explicit_pos = true};
  }

  static constexpr std::string_view type_name() noexcept {
    return "test_explicit_words";
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

struct Field {
  irs::field_id Id() const { return kField; }

  irs::analysis::Tokenizer& GetTokens() const { return *tokenizer; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  }

  irs::analysis::Tokenizer* tokenizer{};
  std::string_view value;
};

struct ScaleScore : public irs::ScorerBase<ScaleScore, tests::sort::StatsT> {
  struct Scorer : public irs::ScoreOperator {
    explicit Scorer(const irs::ScaleBlockAttr* scale) : scale{scale} {}

    template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
    void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
      irs::Merge<MergeType>(res, scale->value, n);
    }

    void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
      ScoreImpl(res, n);
    }
    void ScoreSum(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
    }
    void ScoreMax(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Max>(res, n);
    }

    const irs::ScaleBlockAttr* scale;
  };

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    const auto* scale = irs::get<irs::ScaleBlockAttr>(ctx.doc_attrs);
    if (scale == nullptr) {
      return irs::ScoreFunction::Constant(irs::kNoBoost);
    }
    return irs::ScoreFunction::Make<Scorer>(scale);
  }
};

template<typename ScorerT>
std::vector<irs::score_t> Scores(const irs::Filter& filter,
                                 const irs::IndexReader& reader) {
  ScorerT scorer;
  MaxMemoryCounter counter;
  tests::PreparedFilter prepared{filter, reader, &scorer, counter};
  std::vector<irs::score_t> out;
  for (size_t i = 0; i != prepared.size(); ++i) {
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(i, fetcher);
    auto score = docs->PrepareScore();
    while (!irs::doc_limits::eof(docs->Next())) {
      docs->FetchScoreArgs(0);
      fetcher.Fetch(docs->Value());
      irs::score_t value{};
      score.Score(&value, 1);
      out.push_back(value);
    }
  }
  return out;
}

std::string Filler(size_t n) {
  std::string out;
  for (size_t i = 0; i != n; ++i) {
    absl::StrAppend(&out, "x ");
  }
  return out;
}

irs::ByPhraseOptions Phrase(std::string_view text) {
  irs::ByPhraseOptions phrase;
  for (const auto word : absl::StrSplit(text, ' ', absl::SkipEmpty())) {
    phrase.push_back<irs::ByTermOptions>().term = Bytes(word);
  }
  return phrase;
}

struct Outcome {
  bool matched = false;
  uint32_t freq = 0;
  irs::score_t scale = irs::kNoBoost;
};

bool Boosted(const irs::ByPhraseOptions& phrase) {
  return std::ranges::any_of(phrase, [](const auto& slot) {
    return std::holds_alternative<irs::ByEditDistanceOptions>(slot.part);
  });
}

Outcome Run(const irs::ByPhraseOptions& phrase, const Doc& doc, bool dense) {
  WhitespaceTokenizer whitespace;
  ExplicitTokenizer explicit_positions;
  const auto value = dense ? doc.Text() : doc.Explicit();
  irs::MemoryDirectory dir;
  {
    auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    EXPECT_NE(nullptr, writer);
    Field field{.value = value};
    field.tokenizer = &explicit_positions;
    if (dense) {
      field.tokenizer = &whitespace;
    }
    auto ctx = writer->GetBatch();
    {
      auto inserted = ctx.Insert();
      EXPECT_TRUE(tests::InsertField(inserted, field));
    }
    ctx.Commit();
    writer->RefreshCommit();
  }
  const irs::DirectoryReader reader{dir, irs::tests::DefaultReaderOptions()};

  irs::ByPhrase filter;
  *filter.mutable_field_id() = kField;
  *filter.mutable_options() = phrase;
  filter.mutable_options()->LowerParts();

  Outcome out;
  tests::PreparedFilter prepared{filter, *reader};
  for (size_t i = 0; i != prepared.size(); ++i) {
    auto docs = prepared.Execute(i);
    out.matched |= !irs::doc_limits::eof(docs->Next());
  }
  const auto freqs = Scores<tests::sort::FrequencyScore>(filter, *reader);
  EXPECT_EQ(out.matched, !freqs.empty());
  if (!freqs.empty()) {
    EXPECT_EQ(1U, freqs.size());
    out.freq = static_cast<uint32_t>(freqs.front());
  }
  if (out.matched && (phrase.slop() != 0 || Boosted(phrase))) {
    const auto scales = Scores<ScaleScore>(filter, *reader);
    EXPECT_EQ(1U, scales.size());
    if (!scales.empty()) {
      out.scale = scales.front();
    }
  }
  return out;
}

Outcome Verify(const irs::ByPhraseOptions& phrase, const Doc& doc) {
  const auto out = Run(phrase, doc, false);
  if (doc.Dense()) {
    const auto dense = Run(phrase, doc, true);
    EXPECT_EQ(out.matched, dense.matched);
    EXPECT_EQ(out.freq, dense.freq);
    EXPECT_FLOAT_EQ(out.scale, dense.scale);
  }
  return out;
}

bool Matches(const irs::ByPhraseOptions& phrase, const Doc& doc) {
  return Verify(phrase, doc).matched;
}

struct ReferenceSlot {
  std::vector<std::pair<std::string_view, irs::score_t>> words;
  uint32_t min = 0;
  uint32_t max = 0;
};

Outcome Reference(std::span<const std::string_view> doc,
                  std::span<const ReferenceSlot> slots) {
  std::vector<std::vector<std::pair<uint32_t, irs::score_t>>> hits(
    slots.size());
  for (uint32_t i = 0; i != doc.size(); ++i) {
    for (size_t s = 0; s != slots.size(); ++s) {
      for (const auto& [word, boost] : slots[s].words) {
        if (word == doc[i]) {
          hits[s].emplace_back(irs::pos_limits::min() + i, boost);
        }
      }
    }
  }
  Outcome out{.scale = 0.f};
  const auto walk = [&](this const auto& self, size_t s, uint32_t prev,
                        irs::score_t floor) -> void {
    if (s == slots.size()) {
      ++out.freq;
      out.scale = std::max(out.scale, floor);
      return;
    }
    for (const auto& [pos, boost] : hits[s]) {
      if (s == 0 ||
          (pos >= prev + slots[s].min && pos <= prev + slots[s].max)) {
        self(s + 1, pos, std::min(floor, boost));
      }
    }
  };
  walk(0, 0, irs::kNoBoost);
  out.matched = out.freq != 0;
  if (!out.matched) {
    out.scale = irs::kNoBoost;
  }
  return out;
}

template<typename Options>
Options& Push(irs::ByPhraseOptions& phrase, const ReferenceSlot& slot,
              bool first) {
  return first ? phrase.push_back<Options>()
               : phrase.push_back<Options>(slot.min, slot.max);
}

std::string Describe(std::span<const std::string_view> doc,
                     std::span<const ReferenceSlot> slots) {
  std::string out = absl::StrCat("doc: ", absl::StrJoin(doc, " "), " phrase:");
  for (const auto& slot : slots) {
    absl::StrAppend(&out, " [", slot.min, ",", slot.max, "]");
    for (const auto& [word, boost] : slot.words) {
      absl::StrAppend(&out, word, "/");
    }
  }
  return out;
}

void WidenOneGap(std::span<ReferenceSlot> slots, std::mt19937& rng) {
  if (std::ranges::none_of(slots.subspan(1), [](const ReferenceSlot& slot) {
        return slot.min != slot.max;
      })) {
    slots[std::uniform_int_distribution<size_t>{1, slots.size() - 1}(rng)]
      .max += 1;
  }
}

}  // namespace

TEST(PhrasePositionsTest, adjacent_terms) {
  const auto phrase = Phrase("quick brown fox");
  EXPECT_TRUE(Matches(phrase, Doc{"the quick brown fox jumps"}));
  EXPECT_FALSE(Matches(phrase, Doc{"quick brown dog fox"}));
  EXPECT_FALSE(Matches(phrase, Doc{"fox brown quick"}));
  EXPECT_FALSE(Matches(phrase, Doc{"quick brown"}));
  EXPECT_FALSE(Matches(phrase, Doc{""}));
}

TEST(PhrasePositionsTest, backtracks_over_repeated_tokens) {
  EXPECT_TRUE(Matches(Phrase("a a b"), Doc{"a a a b"}));
  EXPECT_TRUE(Matches(Phrase("a b a b c"), Doc{"a b a b a b c"}));
  EXPECT_TRUE(Matches(Phrase("x x y"), Doc{"x x x x y"}));
  EXPECT_TRUE(Matches(Phrase("the the the"), Doc{"the the the quick"}));
  EXPECT_FALSE(Matches(Phrase("the the the the"), Doc{"the the the quick"}));
}

TEST(PhrasePositionsTest, counts_every_start) {
  EXPECT_EQ(2U, Verify(Phrase("a a"), Doc{"a a a"}).freq);
  EXPECT_EQ(2U,
            Verify(Phrase("quick fox"), Doc{"quick fox and a quick fox"}).freq);
  EXPECT_EQ(1U, Verify(Phrase("quick fox"), Doc{"quick fox"}).freq);
}

TEST(PhrasePositionsTest, exact_gap) {
  irs::ByPhraseOptions phrase;
  phrase.push_back<irs::ByTermOptions>().term = Bytes("quick");
  phrase.push_back<irs::ByTermOptions>(1).term = Bytes("fox");
  EXPECT_TRUE(Matches(phrase, Doc{"quick brown fox"}));
  EXPECT_FALSE(Matches(phrase, Doc{"quick fox"}));
  EXPECT_FALSE(Matches(phrase, Doc{"quick brown red fox"}));
  EXPECT_EQ(2U, Verify(phrase, Doc{"quick a fox quick b fox"}).freq);
}

TEST(PhrasePositionsTest, interval_gap) {
  irs::ByPhraseOptions phrase;
  phrase.push_back<irs::ByTermOptions>().term = Bytes("fox");
  phrase.push_back<irs::ByTermOptions>(1, 3).term = Bytes("dog");
  EXPECT_TRUE(Matches(phrase, Doc{"fox dog"}));
  EXPECT_TRUE(Matches(phrase, Doc{"fox lazy dog"}));
  EXPECT_TRUE(Matches(phrase, Doc{"fox jumps lazy dog"}));
  EXPECT_FALSE(Matches(phrase, Doc{"fox jumps over lazy dog"}));
  EXPECT_FALSE(Matches(phrase, Doc{"dog fox"}));
}

TEST(PhrasePositionsTest, interval_gap_counts_every_combination) {
  irs::ByPhraseOptions phrase;
  phrase.push_back<irs::ByTermOptions>().term = Bytes("a");
  phrase.push_back<irs::ByTermOptions>(2, 3).term = Bytes("c");
  EXPECT_EQ(2U, Verify(phrase, Doc{"a x c c"}).freq);
  EXPECT_EQ(3U, Verify(phrase, Doc{"a a c c"}).freq);
  phrase.push_back<irs::ByTermOptions>(1, 2).term = Bytes("d");
  EXPECT_EQ(3U, Verify(phrase, Doc{"a x c c d d"}).freq);
  EXPECT_EQ(0U, Verify(phrase, Doc{"a x c x x d"}).freq);
}

TEST(PhrasePositionsTest, interval_phrase_agrees_with_reference) {
  constexpr std::string_view kWords[] = {"a", "b", "c", "d", "x"};
  std::mt19937 rng{20261010};
  for (size_t round = 0; round != 500; ++round) {
    std::array<int, std::size(kWords)> weights;
    for (auto& weight : weights) {
      weight = std::uniform_int_distribution{0, 6}(rng);
    }
    weights.back() += 2;
    std::discrete_distribution<size_t> pick{weights.begin(), weights.end()};
    std::vector<std::string_view> doc(
      std::uniform_int_distribution<size_t>{0, 40}(rng));
    for (auto& word : doc) {
      word = kWords[pick(rng)];
    }

    std::vector<ReferenceSlot> slots(
      std::uniform_int_distribution<size_t>{2, 6}(rng));
    std::uniform_int_distribution<size_t> term{0, 3};
    for (size_t s = 0; s != slots.size(); ++s) {
      auto& slot = slots[s];
      if (s != 0) {
        slot.min = std::uniform_int_distribution<uint32_t>{1, 3}(rng);
        slot.max =
          slot.min + std::uniform_int_distribution<uint32_t>{0, 2}(rng);
      }
      slot.words.emplace_back(kWords[term(rng)], irs::kNoBoost);
      if (std::bernoulli_distribution{0.25}(rng)) {
        const auto other = kWords[term(rng)];
        if (other != slot.words.front().first) {
          slot.words.emplace_back(other, irs::kNoBoost);
        }
      }
    }
    WidenOneGap(slots, rng);

    irs::ByPhraseOptions phrase;
    for (size_t s = 0; s != slots.size(); ++s) {
      const auto& slot = slots[s];
      if (slot.words.size() == 1) {
        Push<irs::ByTermOptions>(phrase, slot, s == 0).term =
          Bytes(slot.words.front().first);
      } else {
        auto& set = Push<irs::TermSetOptions>(phrase, slot, s == 0);
        for (const auto& [word, boost] : slot.words) {
          set.terms.emplace(Bytes(word));
        }
      }
    }

    SCOPED_TRACE(Describe(doc, slots));
    const auto expected = Reference(doc, slots);
    const auto out = Verify(phrase, Doc{absl::StrJoin(doc, " ")});
    EXPECT_EQ(expected.matched, out.matched);
    EXPECT_EQ(expected.freq, out.freq);
  }
}

TEST(PhrasePositionsTest, interval_phrase_scale_agrees_with_reference) {
  constexpr std::string_view kWords[] = {"a",   "c",     "golf", "gold",
                                         "gol", "golfs", "x"};
  constexpr std::pair<std::string_view, irs::score_t> kNear[] = {
    {"golf", 1.f}, {"gold", 0.75f}, {"golfs", 0.75f}, {"gol", 1.f - 1.f / 3.f}};
  std::mt19937 rng{20261011};
  for (size_t round = 0; round != 400; ++round) {
    std::array<int, std::size(kWords)> weights;
    for (auto& weight : weights) {
      weight = std::uniform_int_distribution{0, 6}(rng);
    }
    weights.back() += 2;
    std::discrete_distribution<size_t> pick{weights.begin(), weights.end()};
    std::vector<std::string_view> doc(
      std::uniform_int_distribution<size_t>{0, 40}(rng));
    for (auto& word : doc) {
      word = kWords[pick(rng)];
    }

    std::vector<ReferenceSlot> slots(
      std::uniform_int_distribution<size_t>{2, 6}(rng));
    const auto near =
      std::uniform_int_distribution<size_t>{0, slots.size() - 1}(rng);
    std::uniform_int_distribution<size_t> term{0, 1};
    for (size_t s = 0; s != slots.size(); ++s) {
      auto& slot = slots[s];
      if (s != 0) {
        slot.min = std::uniform_int_distribution<uint32_t>{1, 3}(rng);
        slot.max =
          slot.min + std::uniform_int_distribution<uint32_t>{0, 2}(rng);
      }
      if (s == near) {
        slot.words.assign(std::begin(kNear), std::end(kNear));
      } else {
        slot.words.emplace_back(kWords[term(rng)], irs::kNoBoost);
      }
    }
    WidenOneGap(slots, rng);

    irs::ByPhraseOptions phrase;
    for (size_t s = 0; s != slots.size(); ++s) {
      const auto& slot = slots[s];
      if (s == near) {
        auto& edit = Push<irs::ByEditDistanceOptions>(phrase, slot, s == 0);
        edit.term = Bytes("golf");
        edit.max_distance = 1;
      } else {
        Push<irs::ByTermOptions>(phrase, slot, s == 0).term =
          Bytes(slot.words.front().first);
      }
    }

    SCOPED_TRACE(Describe(doc, slots));
    const auto expected = Reference(doc, slots);
    const auto out = Verify(phrase, Doc{absl::StrJoin(doc, " ")});
    EXPECT_EQ(expected.matched, out.matched);
    EXPECT_EQ(expected.freq, out.freq);
    if (expected.matched) {
      EXPECT_FLOAT_EQ(expected.scale, out.scale);
    }
  }
}

TEST(PhrasePositionsTest, stacked_document_tokens) {
  const auto phrase = Phrase("red car");
  const Doc synonyms{{"red", 1}, {"automobile", 2}, {"car", 2}};
  EXPECT_TRUE(Matches(phrase, synonyms));
  EXPECT_TRUE(Matches(Phrase("red automobile"), synonyms));
  const Doc gap{{"red", 1}, {"car", 3}};
  EXPECT_FALSE(Matches(phrase, gap));
}

TEST(PhrasePositionsTest, term_set_slot) {
  irs::ByPhraseOptions phrase;
  phrase.push_back<irs::ByTermOptions>().term = Bytes("red");
  auto& set = phrase.push_back<irs::TermSetOptions>();
  set.terms.emplace(Bytes("car"));
  set.terms.emplace(Bytes("automobile"));
  EXPECT_TRUE(Matches(phrase, Doc{"a red automobile"}));
  EXPECT_TRUE(Matches(phrase, Doc{"a red car"}));
  EXPECT_FALSE(Matches(phrase, Doc{"a red bike"}));
}

TEST(PhrasePositionsTest, expansion_slot_accepts_expanded_terms) {
  irs::ByPhraseOptions phrase;
  phrase.push_back<irs::ByTermOptions>().term = Bytes("quick");
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  EXPECT_TRUE(Matches(phrase, Doc{"the quick brown fox"}));
  EXPECT_TRUE(Matches(phrase, Doc{"the quick bread"}));
  EXPECT_FALSE(Matches(phrase, Doc{"the brown quick"}));

  irs::ByPhraseOptions narrow;
  narrow.push_back<irs::ByTermOptions>().term = Bytes("quick");
  narrow.push_back<irs::ByPrefixOptions>().term = Bytes("bro");
  EXPECT_TRUE(Matches(narrow, Doc{"the quick brown fox"}));
  EXPECT_FALSE(Matches(narrow, Doc{"the quick bread"}));
}

TEST(PhrasePositionsTest, regexp_slot) {
  const auto regexp = [](std::string_view pattern,
                         irs::RegexpSyntax syntax = irs::RegexpSyntax::Perl) {
    irs::ByPhraseOptions phrase;
    phrase.push_back<irs::ByTermOptions>().term = Bytes("quick");
    phrase.push_back<irs::ByRegexpOptions>() =
      irs::ByRegexpOptions{Bytes(pattern), syntax};
    phrase.push_back<irs::ByTermOptions>().term = Bytes("fox");
    return phrase;
  };
  EXPECT_TRUE(Matches(regexp("br.wn"), Doc{"the quick brown fox"}));
  EXPECT_FALSE(Matches(regexp("br.wn"), Doc{"the quick bread fox"}));
  EXPECT_TRUE(Matches(regexp("(brown|red)"), Doc{"quick red fox"}));
  EXPECT_TRUE(Matches(regexp("brown"), Doc{"quick brown fox"}));
  EXPECT_FALSE(Matches(regexp("brown"), Doc{"quick browns fox"}));
  EXPECT_TRUE(Matches(regexp("bro.*"), Doc{"quick browns fox"}));
  EXPECT_TRUE(Matches(regexp("[[:alpha:]]+n", irs::RegexpSyntax::PosixEre),
                      Doc{"quick brown fox"}));
  EXPECT_FALSE(Matches(regexp("[[:alpha:]]+n", irs::RegexpSyntax::PosixEre),
                       Doc{"quick red fox"}));
  EXPECT_EQ(2U, Verify(regexp("[a-z]+"), Doc{"quick a fox quick b fox"}).freq);
  EXPECT_FALSE(Matches(regexp("("), Doc{"quick ( fox"}));
}

TEST(PhrasePositionsTest, regexp_parts_lower_by_shape) {
  const auto lowered = [](std::string_view pattern,
                          irs::RegexpSyntax syntax = irs::RegexpSyntax::Perl) {
    irs::ByPhraseOptions phrase;
    phrase.push_back<irs::ByRegexpOptions>() =
      irs::ByRegexpOptions{Bytes(pattern), syntax};
    phrase.LowerParts();
    return phrase.begin()->part;
  };
  const auto literal = lowered("brown");
  ASSERT_TRUE(std::holds_alternative<irs::ByTermOptions>(literal));
  EXPECT_EQ(Bytes("brown"),
            irs::bytes_view{std::get<irs::ByTermOptions>(literal).term});
  const auto prefix = lowered("bro.*");
  ASSERT_TRUE(std::holds_alternative<irs::ByPrefixOptions>(prefix));
  EXPECT_EQ(Bytes("bro"),
            irs::bytes_view{std::get<irs::ByPrefixOptions>(prefix).term});
  const auto perl = lowered("br.wn");
  ASSERT_TRUE(std::holds_alternative<irs::AutomatonOptions>(perl));
  EXPECT_EQ(irs::PatternKind::RegexpPerl,
            std::get<irs::AutomatonOptions>(perl).kind);
  const auto posix = lowered("br[[:alpha:]]wn", irs::RegexpSyntax::PosixEre);
  ASSERT_TRUE(std::holds_alternative<irs::AutomatonOptions>(posix));
  EXPECT_EQ(irs::PatternKind::RegexpPosixEre,
            std::get<irs::AutomatonOptions>(posix).kind);
}

TEST(PhrasePositionsTest, slop) {
  auto phrase = Phrase("quick fox");
  phrase.set_slop(1);
  EXPECT_TRUE(Matches(phrase, Doc{"quick brown fox"}));
  EXPECT_FALSE(Matches(phrase, Doc{"fox quick"}));
  phrase.set_slop(2);
  EXPECT_TRUE(Matches(phrase, Doc{"fox quick"}));
  EXPECT_FALSE(Matches(phrase, Doc{"quick a b c fox"}));

  auto triple = Phrase("quick brown fox");
  triple.set_slop(2);
  EXPECT_TRUE(Matches(triple, Doc{"quick fox brown"}));
}

TEST(PhrasePositionsTest, slop_agrees_with_engine_sweep) {
  auto phrase = Phrase("a b a");
  phrase.set_slop(2);
  const Doc doc{"a x b a b a"};
  const auto out = Verify(phrase, doc);

  std::vector<std::vector<irs::PosAttr::value_t>> slots(3);
  for (const auto [word, slot] :
       {std::pair{"a", 0}, std::pair{"b", 1}, std::pair{"a", 2}}) {
    slots[slot] = doc.Positions(word);
  }
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

TEST(PhraseTokenBatchTest, matches_across_token_batches) {
  constexpr size_t kBatch = irs::TokenBatch::kCapacity;
  const auto text =
    absl::StrCat(Filler(kBatch - 2), "quick brown fox ", Filler(kBatch - 3),
                 "quick brown fox ", Filler(kBatch), "quick brown");
  const Doc doc{text};
  auto gapped = Phrase("quick");
  gapped.push_back<irs::ByTermOptions>(2, 2).term = Bytes("fox");
  auto sloppy = Phrase("brown quick");
  sloppy.set_slop(2);
  for (const auto& phrase : {Phrase("quick brown fox"), gapped}) {
    const auto out = Verify(phrase, doc);
    EXPECT_TRUE(out.matched);
    EXPECT_EQ(2U, out.freq);
  }
  EXPECT_TRUE(Matches(sloppy, doc));
  EXPECT_FALSE(Matches(Phrase("fox quick"), doc));
  EXPECT_TRUE(
    Matches(Phrase("brown fox x x"), Doc{absl::StrCat(text, " fox")}));
}

TEST(PhraseTokenBatchTest, dense_positions_continue_across_batches) {
  constexpr size_t kBatch = irs::TokenBatch::kCapacity;
  auto gapped = Phrase("a");
  gapped.push_back<irs::ByTermOptions>(kBatch, kBatch).term = Bytes("b");
  EXPECT_TRUE(
    Matches(gapped, Doc{absl::StrCat("a ", Filler(kBatch - 1), "b")}));
  EXPECT_FALSE(Matches(gapped, Doc{absl::StrCat("a ", Filler(kBatch), "b")}));
}

TEST(PhraseTokenBatchTest, explicit_positions_keep_stacked_tokens) {
  const Doc stacked{
    {"the", 1}, {"quick", 2}, {"fast", 3}, {"fox", 3}, {"dog", 4}};
  EXPECT_TRUE(Matches(Phrase("quick fox"), stacked));
  EXPECT_FALSE(Matches(Phrase("fast fox"), stacked));
  EXPECT_TRUE(Matches(Phrase("fast dog"), stacked));
  const auto out = Verify(Phrase("fox dog"), stacked);
  EXPECT_TRUE(out.matched);
  EXPECT_EQ(1U, out.freq);
  EXPECT_FALSE(Matches(Phrase("fox dog"), Doc{"the quick fast|fox dog"}));
}

TEST(PhraseTokenBatchTest, empty_value_matches_nothing) {
  const auto out = Verify(Phrase("a b"), Doc{""});
  EXPECT_FALSE(out.matched);
  EXPECT_EQ(0U, out.freq);
}
