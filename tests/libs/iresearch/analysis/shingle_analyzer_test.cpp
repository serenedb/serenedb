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

irs::analysis::ShingleAnalyzer MakeAnalyzer(uint32_t min, uint32_t max,
                                            bool output_unigrams,
                                            bool output_unigrams_if_no_shingles =
                                              false) {
  return irs::analysis::ShingleAnalyzer{{
    .base_analyzer = std::make_unique<WhitespaceTokenizer>(),
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

std::vector<std::string> Emit(irs::analysis::ShingleAnalyzer& analyzer,
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

TEST(ShingleAnalyzerTest, empty_input) {
  auto analyzer = MakeAnalyzer(2, 2, true);
  EXPECT_TRUE(Emit(analyzer, "").empty());
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
