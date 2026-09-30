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

#include <absl/strings/str_split.h>

#include <iresearch/search/detail/phrase_slop_matcher.hpp>
#include <iresearch/search/detail/phrase_verify.hpp>
#include <iresearch/utils/string.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "tests_shared.hpp"

namespace {

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
    Fill();
  }

  Doc(std::initializer_list<std::pair<std::string_view, uint32_t>> tokens) {
    for (const auto& [word, pos] : tokens) {
      _words.emplace_back(word);
      _positions.push_back(pos);
    }
    Fill();
  }

  const irs::PhraseDocTokens& Tokens() const noexcept { return _tokens; }

 private:
  void Fill() {
    for (size_t i = 0; i != _words.size(); ++i) {
      _tokens.Push(Bytes(_words[i]), _positions[i]);
    }
  }

  std::vector<std::string> _words;
  std::vector<irs::PosAttr::value_t> _positions;
  irs::PhraseDocTokens _tokens;
};

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

Outcome Verify(const irs::ByPhraseOptions& phrase, const Doc& doc,
               std::span<const std::vector<irs::bstring>> expanded = {}) {
  const irs::PhraseVerifyKernel kernel{phrase, expanded};
  irs::PhraseVerifyScratch scratch;
  irs::PhraseVerdict verdict;
  Outcome out;
  out.matched = kernel.Match(doc.Tokens(), true, scratch, verdict);
  out.freq = verdict.freq;
  out.scale = verdict.scale;
  irs::PhraseVerdict first;
  EXPECT_EQ(out.matched, kernel.Match(doc.Tokens(), false, scratch, first));
  return out;
}

bool Matches(const irs::ByPhraseOptions& phrase, const Doc& doc) {
  return Verify(phrase, doc).matched;
}

}  // namespace

TEST(PhraseVerifyKernelTest, adjacent_terms) {
  const auto phrase = Phrase("quick brown fox");
  EXPECT_TRUE(Matches(phrase, Doc{"the quick brown fox jumps"}));
  EXPECT_FALSE(Matches(phrase, Doc{"quick brown dog fox"}));
  EXPECT_FALSE(Matches(phrase, Doc{"fox brown quick"}));
  EXPECT_FALSE(Matches(phrase, Doc{"quick brown"}));
  EXPECT_FALSE(Matches(phrase, Doc{""}));
}

TEST(PhraseVerifyKernelTest, backtracks_over_repeated_tokens) {
  EXPECT_TRUE(Matches(Phrase("a a b"), Doc{"a a a b"}));
  EXPECT_TRUE(Matches(Phrase("a b a b c"), Doc{"a b a b a b c"}));
  EXPECT_TRUE(Matches(Phrase("x x y"), Doc{"x x x x y"}));
  EXPECT_TRUE(Matches(Phrase("the the the"), Doc{"the the the quick"}));
  EXPECT_FALSE(Matches(Phrase("the the the the"), Doc{"the the the quick"}));
}

TEST(PhraseVerifyKernelTest, counts_every_start) {
  EXPECT_EQ(2U, Verify(Phrase("a a"), Doc{"a a a"}).freq);
  EXPECT_EQ(2U,
            Verify(Phrase("quick fox"), Doc{"quick fox and a quick fox"}).freq);
  EXPECT_EQ(1U, Verify(Phrase("quick fox"), Doc{"quick fox"}).freq);
}

TEST(PhraseVerifyKernelTest, exact_gap) {
  irs::ByPhraseOptions phrase;
  phrase.push_back<irs::ByTermOptions>().term = Bytes("quick");
  phrase.push_back<irs::ByTermOptions>(1).term = Bytes("fox");
  EXPECT_TRUE(Matches(phrase, Doc{"quick brown fox"}));
  EXPECT_FALSE(Matches(phrase, Doc{"quick fox"}));
  EXPECT_FALSE(Matches(phrase, Doc{"quick brown red fox"}));
  EXPECT_EQ(2U, Verify(phrase, Doc{"quick a fox quick b fox"}).freq);
}

TEST(PhraseVerifyKernelTest, interval_gap) {
  irs::ByPhraseOptions phrase;
  phrase.push_back<irs::ByTermOptions>().term = Bytes("fox");
  phrase.push_back<irs::ByTermOptions>(1, 3).term = Bytes("dog");
  EXPECT_TRUE(Matches(phrase, Doc{"fox dog"}));
  EXPECT_TRUE(Matches(phrase, Doc{"fox lazy dog"}));
  EXPECT_TRUE(Matches(phrase, Doc{"fox jumps lazy dog"}));
  EXPECT_FALSE(Matches(phrase, Doc{"fox jumps over lazy dog"}));
  EXPECT_FALSE(Matches(phrase, Doc{"dog fox"}));
}

TEST(PhraseVerifyKernelTest, stacked_document_tokens) {
  const auto phrase = Phrase("red car");
  const Doc synonyms{{"red", 1}, {"automobile", 2}, {"car", 2}};
  EXPECT_TRUE(Matches(phrase, synonyms));
  EXPECT_TRUE(Matches(Phrase("red automobile"), synonyms));
  const Doc gap{{"red", 1}, {"car", 3}};
  EXPECT_FALSE(Matches(phrase, gap));
}

TEST(PhraseVerifyKernelTest, term_set_slot) {
  irs::ByPhraseOptions phrase;
  phrase.push_back<irs::ByTermOptions>().term = Bytes("red");
  auto& set = phrase.push_back<irs::TermSetOptions>();
  set.terms.emplace(Bytes("car"));
  set.terms.emplace(Bytes("automobile"));
  EXPECT_TRUE(Matches(phrase, Doc{"a red automobile"}));
  EXPECT_TRUE(Matches(phrase, Doc{"a red car"}));
  EXPECT_FALSE(Matches(phrase, Doc{"a red bike"}));
}

TEST(PhraseVerifyKernelTest, expansion_slot_accepts_expanded_terms) {
  irs::ByPhraseOptions phrase;
  phrase.push_back<irs::ByTermOptions>().term = Bytes("quick");
  phrase.push_back<irs::ByPrefixOptions>().term = Bytes("br");
  const std::vector<std::vector<irs::bstring>> expanded{
    {}, {irs::bstring{Bytes("brown")}}};
  EXPECT_TRUE(Verify(phrase, Doc{"the quick brown fox"}, expanded).matched);
  EXPECT_FALSE(Verify(phrase, Doc{"the quick bread"}, expanded).matched);
}

TEST(PhraseVerifyKernelTest, slop) {
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

TEST(PhraseVerifyKernelTest, slop_agrees_with_engine_sweep) {
  auto phrase = Phrase("a b a");
  phrase.set_slop(2);
  const Doc doc{"a x b a b a"};
  const auto out = Verify(phrase, doc);

  std::vector<std::vector<irs::PosAttr::value_t>> slots(3);
  for (const auto [word, slot] :
       {std::pair{"a", 0}, std::pair{"b", 1}, std::pair{"a", 2}}) {
    const auto& tokens = doc.Tokens();
    for (size_t i = 0; i != tokens.terms.size(); ++i) {
      if (tokens.terms[i] == Bytes(word)) {
        slots[slot].push_back(tokens.positions[i]);
      }
    }
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
