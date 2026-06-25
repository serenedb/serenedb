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

#include "iresearch/analysis/shingle_analyzer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"

#include <string>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/string.hpp"
#include "tests_shared.hpp"

namespace {

// Minimal whitespace tokenizer used as a ShingleAnalyzer base so the tests
// drive a known word stream. The term value views into the input passed to
// reset(), which the caller keeps alive for the lifetime of the iteration.
class WhitespaceTokenizer final
  : public irs::analysis::TypedAnalyzer<WhitespaceTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "test_whitespace_tokenizer";
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<irs::TermAttr>::id()) {
      return &_term;
    }
    if (type == irs::Type<irs::IncAttr>::id()) {
      return &_inc;
    }
    return nullptr;
  }

  bool reset(std::string_view data) final {
    _data = data;
    _pos = 0;
    return true;
  }

  bool next() final {
    while (_pos < _data.size() && _data[_pos] == ' ') {
      ++_pos;
    }
    if (_pos >= _data.size()) {
      return false;
    }
    const auto start = _pos;
    while (_pos < _data.size() && _data[_pos] != ' ') {
      ++_pos;
    }
    _term.value = irs::ViewCast<irs::byte_type>(_data.substr(start, _pos - start));
    _inc.value = 1;
    return true;
  }

 private:
  std::string_view _data;
  size_t _pos{0};
  irs::TermAttr _term;
  irs::IncAttr _inc;
};

// Whitespace tokenizer that drops the stopword "the" and reports the gap as a
// position increment > 1 (like Lucene's StopFilter), so the shingle analyzer's
// filler handling can be exercised.
class StopwordTokenizer final
  : public irs::analysis::TypedAnalyzer<StopwordTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "test_stopword_tokenizer";
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<irs::TermAttr>::id()) {
      return &_term;
    }
    if (type == irs::Type<irs::IncAttr>::id()) {
      return &_inc;
    }
    return nullptr;
  }

  bool reset(std::string_view data) final {
    _data = data;
    _pos = 0;
    return true;
  }

  bool next() final {
    uint32_t skipped = 0;
    for (;;) {
      while (_pos < _data.size() && _data[_pos] == ' ') {
        ++_pos;
      }
      if (_pos >= _data.size()) {
        return false;
      }
      const auto start = _pos;
      while (_pos < _data.size() && _data[_pos] != ' ') {
        ++_pos;
      }
      const auto word = _data.substr(start, _pos - start);
      if (word == "the") {
        ++skipped;  // a removed stopword widens the next token's increment
        continue;
      }
      _term.value = irs::ViewCast<irs::byte_type>(word);
      _inc.value = 1 + skipped;
      return true;
    }
  }

 private:
  std::string_view _data;
  size_t _pos{0};
  irs::TermAttr _term;
  irs::IncAttr _inc;
};

irs::analysis::ShingleAnalyzer MakeAnalyzer(uint32_t min, uint32_t max,
                                            bool output_unigrams,
                                            bool output_unigrams_if_no_shingles =
                                              false) {
  return irs::analysis::ShingleAnalyzer{
    std::make_unique<WhitespaceTokenizer>(),
    {
      .min_shingle_size = min,
      .max_shingle_size = max,
      .output_unigrams = output_unigrams,
      .output_unigrams_if_no_shingles = output_unigrams_if_no_shingles,
    }};
}

std::string ToString(irs::bytes_view v) {
  return std::string{irs::ViewCast<char>(v)};
}

// A shingle term: tokens joined by the default 0xFF separator.
std::string Shingle(std::initializer_list<std::string_view> tokens) {
  std::string out;
  for (auto t : tokens) {
    if (!out.empty()) {
      out.push_back('\xFF');
    }
    out.append(t);
  }
  return out;
}

std::vector<std::string> Emit(irs::analysis::Analyzer& analyzer,
                              std::string_view data) {
  EXPECT_TRUE(analyzer.reset(data));
  const auto* term = irs::get<irs::TermAttr>(analyzer);
  EXPECT_NE(nullptr, term);
  std::vector<std::string> out;
  while (analyzer.next()) {
    out.push_back(ToString(term->value));
  }
  return out;
}

// Emit (term, position_increment) pairs. The increment model places each base
// token at exactly one position (first term at a window front inc=1, shingles
// that follow inc=0), which is what makes a positional ByPhrase over the
// shingle terms exact.
using TermInc = std::pair<std::string, uint32_t>;
std::vector<TermInc> EmitWithInc(irs::analysis::ShingleAnalyzer& analyzer,
                                 std::string_view data) {
  EXPECT_TRUE(analyzer.reset(data));
  const auto* term = irs::get<irs::TermAttr>(analyzer);
  const auto* inc = irs::get<irs::IncAttr>(analyzer);
  EXPECT_NE(nullptr, term);
  EXPECT_NE(nullptr, inc);
  std::vector<TermInc> out;
  while (analyzer.next()) {
    out.emplace_back(ToString(term->value), inc->value);
  }
  return out;
}

std::vector<std::string> DecodeStore(irs::analysis::ShingleAnalyzer& analyzer) {
  const auto* store = irs::get<irs::StoreAttr>(analyzer);
  EXPECT_NE(nullptr, store);
  std::vector<std::string> out;
  const auto* p = store->value.data();
  const auto* const end = p + store->value.size();
  while (p != end) {
    irs::bytes_view token;
    p = irs::analysis::ShingleAnalyzer::ReadToken(p, token);
    out.push_back(ToString(token));
  }
  return out;
}

// Round-trip `token` through WriteToken/ReadToken; if `prefix_bytes` is given,
// report the length-prefix width (record size minus token size).
std::string CodecRoundTrip(std::string_view token,
                           size_t* prefix_bytes = nullptr) {
  std::string buf;
  irs::analysis::ShingleAnalyzer::WriteToken(
    irs::ViewCast<irs::byte_type>(token), buf);
  if (prefix_bytes != nullptr) {
    *prefix_bytes = buf.size() - token.size();
  }
  const auto* p = reinterpret_cast<const irs::byte_type*>(buf.data());
  irs::bytes_view decoded;
  const auto* next = irs::analysis::ShingleAnalyzer::ReadToken(p, decoded);
  EXPECT_EQ(next, p + buf.size());  // consumed exactly the whole record
  return ToString(decoded);
}

}  // namespace

TEST(ShingleAnalyzerTest, attributes) {
  auto analyzer = MakeAnalyzer(2, 2, true);
  EXPECT_NE(nullptr, irs::get<irs::TermAttr>(analyzer));
  EXPECT_NE(nullptr, irs::get<irs::IncAttr>(analyzer));
  EXPECT_NE(nullptr, irs::get<irs::StoreAttr>(analyzer));
  EXPECT_EQ(nullptr, irs::get<irs::OffsAttr>(analyzer));
}

TEST(ShingleAnalyzerTest, bigrams_with_unigrams) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/true);
  const std::vector<std::string> expected{
    "quick", Shingle({"quick", "brown"}), "brown", Shingle({"brown", "fox"}),
    "fox",
  };
  EXPECT_EQ(expected, Emit(analyzer, "quick brown fox"));
  EXPECT_EQ((std::vector<std::string>{"quick", "brown", "fox"}),
            DecodeStore(analyzer));
}

TEST(ShingleAnalyzerTest, bigrams_without_unigrams) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/false);
  const std::vector<std::string> expected{
    Shingle({"a", "b"}),
    Shingle({"b", "c"}),
  };
  EXPECT_EQ(expected, Emit(analyzer, "a b c"));
}

TEST(ShingleAnalyzerTest, min2_max3_with_unigrams) {
  auto analyzer = MakeAnalyzer(2, 3, /*output_unigrams=*/true);
  // Matches the Elasticsearch shingle example ordering.
  const std::vector<std::string> expected{
    "a",
    Shingle({"a", "b"}),
    Shingle({"a", "b", "c"}),
    "b",
    Shingle({"b", "c"}),
    Shingle({"b", "c", "d"}),
    "c",
    Shingle({"c", "d"}),
    "d",
  };
  EXPECT_EQ(expected, Emit(analyzer, "a b c d"));
}

TEST(ShingleAnalyzerTest, single_token_emits_unigram) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/true);
  EXPECT_EQ((std::vector<std::string>{"lonely"}), Emit(analyzer, "lonely"));
  EXPECT_EQ((std::vector<std::string>{"lonely"}), DecodeStore(analyzer));
}

TEST(ShingleAnalyzerTest, output_unigrams_if_no_shingles) {
  // Fewer tokens than min: only emit unigrams when explicitly requested.
  auto with = MakeAnalyzer(2, 2, /*output_unigrams=*/false,
                           /*output_unigrams_if_no_shingles=*/true);
  EXPECT_EQ((std::vector<std::string>{"solo"}), Emit(with, "solo"));

  auto without = MakeAnalyzer(2, 2, /*output_unigrams=*/false,
                              /*output_unigrams_if_no_shingles=*/false);
  EXPECT_TRUE(Emit(without, "solo").empty());
}

TEST(ShingleAnalyzerTest, store_tokens_off) {
  // Without token storage the StoreAttr is hidden (the indexer persists
  // nothing), but term emission and the query-side TokenBlob() are unchanged.
  irs::analysis::ShingleAnalyzer analyzer{
    std::make_unique<WhitespaceTokenizer>(),
    {
      .min_shingle_size = 2,
      .max_shingle_size = 2,
      .output_unigrams = true,
      .store_tokens = false,
    }};
  EXPECT_EQ(nullptr, irs::get<irs::StoreAttr>(analyzer));
  const std::vector<std::string> expected{
    "a", Shingle({"a", "b"}), "b", Shingle({"b", "c"}), "c",
  };
  EXPECT_EQ(expected, Emit(analyzer, "a b c"));
  analyzer.DrainTokens("a b c");
  std::vector<std::string> tokens;
  const auto blob = analyzer.TokenBlob();
  const auto* p = blob.data();
  const auto* const end = p + blob.size();
  while (p != end) {
    irs::bytes_view token;
    p = irs::analysis::ShingleAnalyzer::ReadToken(p, token);
    tokens.push_back(ToString(token));
  }
  EXPECT_EQ((std::vector<std::string>{"a", "b", "c"}), tokens);
}

TEST(ShingleAnalyzerTest, empty_input) {
  auto analyzer = MakeAnalyzer(2, 2, true);
  EXPECT_TRUE(Emit(analyzer, "").empty());
}

irs::bstring Bytes(std::string_view s) {
  return irs::bstring{irs::ViewCast<irs::byte_type>(s)};
}

TEST(ShingleAnalyzerTest, frequent_words_escalation) {
  // A frequent-words list escalates width adaptively: min-size shingles stay
  // dense; wider sizes exist only for windows containing a frequent word.
  // Unigrams are always emitted (output_unigrams is forced on).
  irs::analysis::ShingleAnalyzer analyzer{
    std::make_unique<WhitespaceTokenizer>(),
    {
      .min_shingle_size = 2,
      .max_shingle_size = 3,
      .output_unigrams = false,  // overridden because a list is set
      .frequent_words = {Bytes("the"), Bytes("of")},
    }};
  // Frequent-containing window: bigrams dense + the escalated trigram.
  const std::vector<std::string> frequent{
    "the",   Shingle({"the", "quick"}), Shingle({"the", "quick", "brown"}),
    "quick", Shingle({"quick", "brown"}),
    "brown",
  };
  EXPECT_EQ(frequent, Emit(analyzer, "the quick brown"));
  // All-rare window: bigrams stay dense, no trigram is escalated.
  const std::vector<std::string> rare{
    "quick", Shingle({"quick", "brown"}),
    "brown", Shingle({"brown", "fox"}),
    "fox",
  };
  EXPECT_EQ(rare, Emit(analyzer, "quick brown fox"));
}

TEST(ShingleAnalyzerTest, frequent_words_positions) {
  // Escalated and dense terms share their window front's position; skipped
  // trigrams change nothing about the increments.
  irs::analysis::ShingleAnalyzer analyzer{
    std::make_unique<WhitespaceTokenizer>(),
    {
      .min_shingle_size = 2,
      .max_shingle_size = 3,
      .frequent_words = {Bytes("the")},
    }};
  const std::vector<TermInc> expected{
    {"the", 1},
    {Shingle({"the", "quick"}), 0},
    {Shingle({"the", "quick", "brown"}), 0},
    {"quick", 1},
    {Shingle({"quick", "brown"}), 0},
    {"brown", 1},
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "the quick brown"));
}

TEST(ShingleAnalyzerTest, lucene_filler_gap) {
  // The base drops "the"; the resulting gap becomes a filler token, so no
  // shingle bridges the removed stopword.
  irs::analysis::ShingleAnalyzer analyzer{
    std::make_unique<StopwordTokenizer>(),
    {
      .min_shingle_size = 2,
      .max_shingle_size = 2,
      .output_unigrams = true,
    }};
  // A removed token is never baked into a term: no standalone filler (its
  // posting list would span nearly every document) and no filler-bearing
  // shingle (it would overconstrain the gap, which means "any single token
  // here"). The gap survives as a position increment.
  const std::vector<std::string> expected{"quick", "brown"};
  EXPECT_EQ(expected, Emit(analyzer, "quick the brown"));
}

TEST(ShingleAnalyzerTest, lucene_filler_positions) {
  // The filler occupies its own position so a positional phrase is gap-aware.
  irs::analysis::ShingleAnalyzer analyzer{
    std::make_unique<StopwordTokenizer>(),
    {
      .min_shingle_size = 2,
      .max_shingle_size = 2,
      .output_unigrams = true,
    }};
  const std::vector<TermInc> expected{
    {"quick", 1},
    {"brown", 2},  // the removed stopword's position is preserved
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "quick the brown"));

  // Shingling resumes across the gap: spans without a filler still shingle.
  const std::vector<TermInc> longer{
    {"quick", 1},
    {"brown", 2},
    {Shingle({"brown", "fox"}), 0},
    {"fox", 1},
  };
  EXPECT_EQ(longer, EmitWithInc(analyzer, "quick the brown fox"));
}

TEST(ShingleAnalyzerTest, silent_filler_front_carries_increment) {
  // A filler front whose shingles all span the filler emits nothing at all;
  // its position must carry into the next term's increment.
  irs::analysis::ShingleAnalyzer analyzer{
    std::make_unique<StopwordTokenizer>(),
    {
      .min_shingle_size = 2,
      .max_shingle_size = 2,
      .output_unigrams = true,
    }};
  const std::vector<TermInc> expected{
    {"quick", 1},
    {"brown", 2},  // the removed stopword's position is preserved
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "quick the brown"));
}

TEST(ShingleAnalyzerTest, base_analyzer_view) {
  // Non-phrase query builders tokenize against the wrapped base analyzer
  // directly (the unigram view: one token per position); the shingle
  // analyzer itself is configured once from Options and never reconfigured.
  auto analyzer = MakeAnalyzer(2, 3, /*output_unigrams=*/true);
  EXPECT_EQ((std::vector<std::string>{"a", "b", "c", "d"}),
            Emit(analyzer.BaseAnalyzer(), "a b c d"));
  // Driving the base does not disturb the wrapper's own emission.
  const std::vector<std::string> full{
    "a", Shingle({"a", "b"}), Shingle({"a", "b", "c"}),
    "b", Shingle({"b", "c"}), Shingle({"b", "c", "d"}),
    "c", Shingle({"c", "d"}),
    "d",
  };
  EXPECT_EQ(full, Emit(analyzer, "a b c d"));
}

TEST(ShingleAnalyzerTest, drain_tokens_builds_store) {
  irs::analysis::ShingleAnalyzer analyzer{
    std::make_unique<StopwordTokenizer>(),
    {
      .min_shingle_size = 2,
      .max_shingle_size = 2,
      .output_unigrams = true,
    }};
  analyzer.DrainTokens("quick the brown");
  EXPECT_EQ((std::vector<std::string>{"quick", "_", "brown"}),
            DecodeStore(analyzer));
  // The mode toggle used internally must not leak: full emission still works.
  EXPECT_EQ((std::vector<std::string>{"quick", "brown", Shingle({"brown", "fox"}), "fox"}),
            Emit(analyzer, "quick the brown fox"));
}

TEST(ShingleAnalyzerTest, read_token_checked_rejects_corrupt_blobs) {
  using SA = irs::analysis::ShingleAnalyzer;
  std::string buf;
  SA::WriteToken(irs::ViewCast<irs::byte_type>(std::string_view{"quick"}), buf);
  const auto* p = reinterpret_cast<const irs::byte_type*>(buf.data());
  const auto* end = p + buf.size();
  irs::bytes_view token;
  EXPECT_EQ(end, SA::ReadTokenChecked(p, end, token));
  EXPECT_EQ("quick", ToString(token));
  // Truncated payload: length prefix promises more bytes than remain.
  EXPECT_EQ(nullptr, SA::ReadTokenChecked(p, end - 1, token));
  // Empty input.
  EXPECT_EQ(nullptr, SA::ReadTokenChecked(p, p, token));
  // Unused length selector 0b11.
  const irs::byte_type bad[] = {0xC1, 0x00};
  EXPECT_EQ(nullptr, SA::ReadTokenChecked(bad, bad + sizeof bad, token));
  // Truncated multi-byte length prefix.
  const irs::byte_type short_prefix[] = {0x41};  // 2-byte form, 1 byte present
  EXPECT_EQ(nullptr,
            SA::ReadTokenChecked(short_prefix, short_prefix + 1, token));
}

TEST(ShingleAnalyzerTest, positions_bigrams_with_unigrams) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/true);
  // Each base token gets one position: the unigram advances it (inc=1), the
  // shingle starting there shares it (inc=0).
  const std::vector<TermInc> expected{
    {"quick", 1}, {Shingle({"quick", "brown"}), 0},
    {"brown", 1}, {Shingle({"brown", "fox"}), 0},
    {"fox", 1},
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "quick brown fox"));
}

TEST(ShingleAnalyzerTest, positions_min2_max3) {
  auto analyzer = MakeAnalyzer(2, 3, /*output_unigrams=*/true);
  // All shingle sizes that start at a given token share that token's position.
  const std::vector<TermInc> expected{
    {"a", 1}, {Shingle({"a", "b"}), 0}, {Shingle({"a", "b", "c"}), 0},
    {"b", 1}, {Shingle({"b", "c"}), 0}, {Shingle({"b", "c", "d"}), 0},
    {"c", 1}, {Shingle({"c", "d"}), 0},
    {"d", 1},
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "a b c d"));
}

TEST(ShingleAnalyzerTest, positions_without_unigrams) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/false);
  // No unigram precedes the shingle, so the first (only) shingle at each token
  // advances the position; consecutive bigrams land at consecutive positions.
  const std::vector<TermInc> expected{
    {Shingle({"a", "b"}), 1},
    {Shingle({"b", "c"}), 1},
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "a b c"));
}

TEST(ShingleAnalyzerTest, token_codec_round_trip) {
  // One token of each length tier, including the tier boundaries.
  for (size_t len : {size_t{0}, size_t{1}, size_t{63}, size_t{64}, size_t{255},
                     size_t{16383}, size_t{16384}, size_t{70000}}) {
    SCOPED_TRACE(len);
    const std::string token(len, 'x');
    EXPECT_EQ(token, CodecRoundTrip(token));
  }
}

TEST(ShingleAnalyzerTest, token_codec_prefix_width) {
  // Length prefix is 1, 2, or 4 bytes, selected by magnitude.
  size_t width = 0;
  CodecRoundTrip(std::string(0, 'x'), &width);
  EXPECT_EQ(1u, width);
  CodecRoundTrip(std::string(63, 'x'), &width);
  EXPECT_EQ(1u, width);
  CodecRoundTrip(std::string(64, 'x'), &width);
  EXPECT_EQ(2u, width);
  CodecRoundTrip(std::string(16383, 'x'), &width);
  EXPECT_EQ(2u, width);
  CodecRoundTrip(std::string(16384, 'x'), &width);
  EXPECT_EQ(4u, width);
  CodecRoundTrip(std::string(1 << 20, 'x'), &width);
  EXPECT_EQ(4u, width);
}

TEST(ShingleAnalyzerTest, token_codec_binary_safe) {
  // Token bytes may contain 0xFF; the length-prefixed format carries no markers
  // to collide with (unlike the original 0xFF-delimited encoding).
  const std::string token{'\x00', '\xFF', '\xFF', '\x01', '\xFF'};
  EXPECT_EQ(token, CodecRoundTrip(token));
}

TEST(ShingleAnalyzerTest, token_codec_sequence) {
  // A blob of consecutive records decodes back to the original token list,
  // mixing empty, multi-byte-prefix, and 0xFF-laden tokens.
  const std::vector<std::string> tokens{
    "", "a", std::string(64, 'b'), std::string(20000, 'c'),
    std::string{'\xFF', '\xFF'},
  };
  std::string buf;
  for (const auto& t : tokens) {
    irs::analysis::ShingleAnalyzer::WriteToken(
      irs::ViewCast<irs::byte_type>(std::string_view{t}), buf);
  }
  std::vector<std::string> decoded;
  const auto* p = reinterpret_cast<const irs::byte_type*>(buf.data());
  const auto* const end = p + buf.size();
  while (p != end) {
    irs::bytes_view token;
    p = irs::analysis::ShingleAnalyzer::ReadToken(p, token);
    decoded.push_back(ToString(token));
  }
  EXPECT_EQ(tokens, decoded);
}
