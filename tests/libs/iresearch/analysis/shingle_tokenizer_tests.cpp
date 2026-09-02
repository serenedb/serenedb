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

#include <cctype>
#include <string>
#include <vector>

#include "iresearch/analysis/shingle_tokenizer.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/utils/string.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

using irs::analysis::ShingleTokenizer;

// Whitespace splitter base (dense positions, inc=1). Emits term views into the
// input, so it also exercises the shingle's zero-copy value-view path.
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
    size_t p = 0;
    while (p < data.size()) {
      while (p < data.size() && data[p] == ' ') {
        ++p;
      }
      if (p >= data.size()) {
        break;
      }
      const auto start = p;
      while (p < data.size() && data[p] != ' ') {
        ++p;
      }
      const auto w = data.substr(start, p - start);
      sink.Emit<L>(
        irs::MakeTermView(w.data(), static_cast<uint32_t>(w.size())));
    }
    return true;
  }
};

// Whitespace splitter that drops the stopword "the", reporting the gap as a
// position increment > 1 (non-dense) so filler handling is exercised.
class StopwordTokenizer final
  : public irs::analysis::TypedTokenizer<StopwordTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "test_stopword";
  }
  irs::TokenTraits Traits() const noexcept final {
    return {.explicit_pos = true};
  }
  template<irs::TokenLayout L>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view data{raw.GetData(), raw.GetSize()};
    size_t p = 0;
    uint32_t pos = 0;
    uint32_t skipped = 0;
    while (p < data.size()) {
      while (p < data.size() && data[p] == ' ') {
        ++p;
      }
      if (p >= data.size()) {
        break;
      }
      const auto start = p;
      while (p < data.size() && data[p] != ' ') {
        ++p;
      }
      const auto w = data.substr(start, p - start);
      if (w == "the") {
        ++skipped;
        continue;
      }
      pos += 1 + skipped;
      skipped = 0;
      sink.Emit<L>(irs::MakeTermView(w.data(), static_cast<uint32_t>(w.size())),
                   pos);
    }
    return true;
  }
};

class UpperTokenizer final
  : public irs::analysis::TypedTokenizer<UpperTokenizer> {
 public:
  irs::TokenTraits Traits() const noexcept final { return {}; }

  static constexpr std::string_view type_name() noexcept {
    return "test_upper";
  }
  template<irs::TokenLayout L>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view data{raw.GetData(), raw.GetSize()};
    size_t p = 0;
    while (p < data.size()) {
      while (p < data.size() && data[p] == ' ') {
        ++p;
      }
      if (p >= data.size()) {
        break;
      }
      const auto start = p;
      while (p < data.size() && data[p] != ' ') {
        ++p;
      }
      const auto w = data.substr(start, p - start);
      _buf.assign(w.size(), 0);
      for (size_t i = 0; i < w.size(); ++i) {
        _buf[i] = static_cast<irs::byte_type>(
          std::toupper(static_cast<unsigned char>(w[i])));
      }
      tests::EmitCopy<L>(sink, irs::bytes_view{_buf.data(), _buf.size()});
    }
    return true;
  }

 private:
  irs::bstring _buf;
};

ShingleTokenizer MakeAnalyzer(uint32_t min, uint32_t max, bool output_unigrams,
                              bool output_unigrams_if_no_shingles = false) {
  return ShingleTokenizer{
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

std::vector<std::string> Emit(irs::analysis::Tokenizer& analyzer,
                              std::string_view data) {
  auto terms = tests::AnalyzeTerms(analyzer, data);
  EXPECT_TRUE(terms.has_value());
  return terms.value_or(std::vector<std::string>{});
}

// (term, position increment) pairs. Increments are recovered from the batch
// prefix-sum positions (inc[k] = pos[k] - pos[k-1], pos[-1] = 0): the first
// term at a window front advances the position, co-located shingles share it.
using TermInc = std::pair<std::string, uint32_t>;
std::vector<TermInc> EmitWithInc(irs::analysis::Tokenizer& analyzer,
                                 std::string_view data) {
  irs::TokenCollector collector{irs::TokenLayout::TermsPos};
  EXPECT_TRUE(irs::AnalyzeValue(
    analyzer, duckdb::string_t{data.data(), static_cast<uint32_t>(data.size())},
    collector));
  std::vector<TermInc> out;
  uint32_t prev = 0;
  for (const auto& t : collector.tokens) {
    out.emplace_back(ToString(t.term), t.pos - prev);
    prev = t.pos;
  }
  return out;
}

std::vector<std::string> DecodeStore(irs::bytes_view blob) {
  std::vector<std::string> out;
  const auto* p = blob.data();
  const auto* const end = p + blob.size();
  while (p != end) {
    irs::bytes_view token;
    p = ShingleTokenizer::ReadToken(p, token);
    out.push_back(ToString(token));
  }
  return out;
}

std::vector<std::string> StoreOf(irs::analysis::Tokenizer& analyzer,
                                 std::string_view data) {
  irs::TokenCollector collector{irs::TokenLayout::Terms};
  EXPECT_TRUE(irs::AnalyzeValue(
    analyzer, duckdb::string_t{data.data(), static_cast<uint32_t>(data.size())},
    collector));
  return DecodeStore(irs::bytes_view{collector.store});
}

std::string CodecRoundTrip(std::string_view token,
                           size_t* prefix_bytes = nullptr) {
  irs::bstring buf;
  ShingleTokenizer::WriteToken(irs::ViewCast<irs::byte_type>(token), buf);
  if (prefix_bytes != nullptr) {
    *prefix_bytes = buf.size() - token.size();
  }
  const auto* p = buf.data();
  irs::bytes_view decoded;
  const auto* next = ShingleTokenizer::ReadToken(p, decoded);
  EXPECT_EQ(next, p + buf.size());
  return ToString(decoded);
}

irs::bstring Bytes(std::string_view s) {
  return irs::bstring{irs::ViewCast<irs::byte_type>(s)};
}

class EmitThenFailTokenizer final
  : public irs::analysis::TypedTokenizer<EmitThenFailTokenizer> {
 public:
  irs::TokenTraits Traits() const noexcept final { return {}; }

  static constexpr std::string_view type_name() noexcept {
    return "test_emit_then_fail";
  }
  template<irs::TokenLayout L>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view data{raw.GetData(), raw.GetSize()};
    if (data.starts_with("poison")) {
      tests::EmitCopy<L>(sink, irs::ViewCast<irs::byte_type>(data));
      return false;
    }
    size_t p = 0;
    while (p < data.size()) {
      while (p < data.size() && data[p] == ' ') {
        ++p;
      }
      if (p >= data.size()) {
        break;
      }
      const auto start = p;
      while (p < data.size() && data[p] != ' ') {
        ++p;
      }
      tests::EmitCopy<L>(
        sink, irs::ViewCast<irs::byte_type>(data.substr(start, p - start)));
    }
    return true;
  }
};

}  // namespace

TEST(ShingleTokenizerTest, traits) {
  {
    auto analyzer = MakeAnalyzer(2, 2, true);
    const auto traits = analyzer.Traits();
    EXPECT_TRUE(traits.explicit_pos);
    EXPECT_FALSE(traits.offsets);
    EXPECT_TRUE(traits.store);
  }
  {
    auto analyzer = MakeAnalyzer(2, 2, false);
    EXPECT_FALSE(analyzer.Traits().explicit_pos);
  }
  {
    auto analyzer = MakeAnalyzer(2, 2, false, true);
    EXPECT_FALSE(analyzer.Traits().explicit_pos);
  }
  {
    auto analyzer = MakeAnalyzer(2, 3, false);
    EXPECT_TRUE(analyzer.Traits().explicit_pos);
  }
}

TEST(ShingleTokenizerTest, bigrams_with_unigrams) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/true);
  const std::vector<std::string> expected{
    "quick", Shingle({"quick", "brown"}), "brown", Shingle({"brown", "fox"}),
    "fox",
  };
  EXPECT_EQ(expected, Emit(analyzer, "quick brown fox"));
  EXPECT_EQ((std::vector<std::string>{"quick", "brown", "fox"}),
            StoreOf(analyzer, "quick brown fox"));
}

TEST(ShingleTokenizerTest, bigrams_without_unigrams) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/false);
  const std::vector<std::string> expected{
    Shingle({"a", "b"}),
    Shingle({"b", "c"}),
  };
  EXPECT_EQ(expected, Emit(analyzer, "a b c"));
}

TEST(ShingleTokenizerTest, min2_max3_with_unigrams) {
  auto analyzer = MakeAnalyzer(2, 3, /*output_unigrams=*/true);
  const std::vector<std::string> expected{
    "a", Shingle({"a", "b"}), Shingle({"a", "b", "c"}),
    "b", Shingle({"b", "c"}), Shingle({"b", "c", "d"}),
    "c", Shingle({"c", "d"}), "d",
  };
  EXPECT_EQ(expected, Emit(analyzer, "a b c d"));
}

TEST(ShingleTokenizerTest, single_token_emits_unigram) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/true);
  EXPECT_EQ((std::vector<std::string>{"lonely"}), Emit(analyzer, "lonely"));
  EXPECT_EQ((std::vector<std::string>{"lonely"}), StoreOf(analyzer, "lonely"));
}

TEST(ShingleTokenizerTest, output_unigrams_if_no_shingles) {
  auto with = MakeAnalyzer(2, 2, /*output_unigrams=*/false,
                           /*output_unigrams_if_no_shingles=*/true);
  EXPECT_EQ((std::vector<std::string>{"solo"}), Emit(with, "solo"));

  auto without = MakeAnalyzer(2, 2, /*output_unigrams=*/false,
                              /*output_unigrams_if_no_shingles=*/false);
  EXPECT_TRUE(Emit(without, "solo").empty());
}

TEST(ShingleTokenizerTest, store_tokens_off) {
  ShingleTokenizer analyzer{std::make_unique<WhitespaceTokenizer>(),
                            {
                              .min_shingle_size = 2,
                              .max_shingle_size = 2,
                              .output_unigrams = true,
                              .store_tokens = false,
                            }};
  EXPECT_FALSE(analyzer.Traits().store);
  const std::vector<std::string> expected{
    "a", Shingle({"a", "b"}), "b", Shingle({"b", "c"}), "c",
  };
  EXPECT_EQ(expected, Emit(analyzer, "a b c"));
  // No blob is delivered when token storage is off.
  EXPECT_TRUE(StoreOf(analyzer, "a b c").empty());
}

TEST(ShingleTokenizerTest, empty_input) {
  auto analyzer = MakeAnalyzer(2, 2, true);
  EXPECT_TRUE(Emit(analyzer, "").empty());
}

TEST(ShingleTokenizerTest, frequent_words_escalation) {
  ShingleTokenizer analyzer{std::make_unique<WhitespaceTokenizer>(),
                            {
                              .min_shingle_size = 2,
                              .max_shingle_size = 3,
                              .output_unigrams = false,
                              .frequent_words = {Bytes("the"), Bytes("of")},
                            }};
  const std::vector<std::string> frequent{
    "the",   Shingle({"the", "quick"}),   Shingle({"the", "quick", "brown"}),
    "quick", Shingle({"quick", "brown"}), "brown",
  };
  EXPECT_EQ(frequent, Emit(analyzer, "the quick brown"));
  const std::vector<std::string> rare{
    "quick", Shingle({"quick", "brown"}), "brown", Shingle({"brown", "fox"}),
    "fox",
  };
  EXPECT_EQ(rare, Emit(analyzer, "quick brown fox"));
}

TEST(ShingleTokenizerTest, frequent_words_positions) {
  ShingleTokenizer analyzer{std::make_unique<WhitespaceTokenizer>(),
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

TEST(ShingleTokenizerTest, lucene_filler_gap) {
  ShingleTokenizer analyzer{std::make_unique<StopwordTokenizer>(),
                            {
                              .min_shingle_size = 2,
                              .max_shingle_size = 2,
                              .output_unigrams = true,
                            }};
  const std::vector<std::string> expected{"quick", "brown"};
  EXPECT_EQ(expected, Emit(analyzer, "quick the brown"));
}

TEST(ShingleTokenizerTest, lucene_filler_positions) {
  ShingleTokenizer analyzer{std::make_unique<StopwordTokenizer>(),
                            {
                              .min_shingle_size = 2,
                              .max_shingle_size = 2,
                              .output_unigrams = true,
                            }};
  const std::vector<TermInc> expected{
    {"quick", 1},
    {"brown", 2},
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "quick the brown"));

  const std::vector<TermInc> longer{
    {"quick", 1},
    {"brown", 2},
    {Shingle({"brown", "fox"}), 0},
    {"fox", 1},
  };
  EXPECT_EQ(longer, EmitWithInc(analyzer, "quick the brown fox"));
}

TEST(ShingleTokenizerTest, filler_appears_in_store) {
  ShingleTokenizer analyzer{std::make_unique<StopwordTokenizer>(),
                            {
                              .min_shingle_size = 2,
                              .max_shingle_size = 2,
                              .output_unigrams = true,
                            }};
  EXPECT_EQ((std::vector<std::string>{"quick", "_", "brown"}),
            StoreOf(analyzer, "quick the brown"));
  EXPECT_EQ((std::vector<std::string>{"quick", "brown",
                                      Shingle({"brown", "fox"}), "fox"}),
            Emit(analyzer, "quick the brown fox"));
}

TEST(ShingleTokenizerTest, positions_bigrams_with_unigrams) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/true);
  const std::vector<TermInc> expected{
    {"quick", 1}, {Shingle({"quick", "brown"}), 0},
    {"brown", 1}, {Shingle({"brown", "fox"}), 0},
    {"fox", 1},
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "quick brown fox"));
}

TEST(ShingleTokenizerTest, positions_min2_max3) {
  auto analyzer = MakeAnalyzer(2, 3, /*output_unigrams=*/true);
  const std::vector<TermInc> expected{
    {"a", 1}, {Shingle({"a", "b"}), 0}, {Shingle({"a", "b", "c"}), 0},
    {"b", 1}, {Shingle({"b", "c"}), 0}, {Shingle({"b", "c", "d"}), 0},
    {"c", 1}, {Shingle({"c", "d"}), 0}, {"d", 1},
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "a b c d"));
}

TEST(ShingleTokenizerTest, positions_without_unigrams) {
  auto analyzer = MakeAnalyzer(2, 2, /*output_unigrams=*/false);
  const std::vector<TermInc> expected{
    {Shingle({"a", "b"}), 1},
    {Shingle({"b", "c"}), 1},
  };
  EXPECT_EQ(expected, EmitWithInc(analyzer, "a b c"));
}

TEST(ShingleTokenizerTest, generated_base_tokens_are_copied) {
  ShingleTokenizer analyzer{std::make_unique<UpperTokenizer>(),
                            {
                              .min_shingle_size = 2,
                              .max_shingle_size = 2,
                              .output_unigrams = true,
                            }};
  const std::string a = "alphabetlongword";  // > 12 bytes -> non-inline
  const std::string b = "betatokenlongword";
  const auto input = a + " " + b;
  std::string A = a;
  std::string B = b;
  for (auto& c : A) {
    c = static_cast<char>(std::toupper(c));
  }
  for (auto& c : B) {
    c = static_cast<char>(std::toupper(c));
  }
  const std::vector<std::string> expected{A, Shingle({A, B}), B};
  EXPECT_EQ(expected, Emit(analyzer, input));
  EXPECT_EQ((std::vector<std::string>{A, B}), StoreOf(analyzer, input));
}

TEST(ShingleTokenizerTest, column_fill_matches_per_value) {
  auto analyzer = MakeAnalyzer(2, 3, /*output_unigrams=*/true);
  const std::vector<std::string> values{"quick brown fox", "a b c d", "lonely",
                                        "", "one two"};

  std::vector<std::vector<std::string>> expected;
  for (const auto& v : values) {
    expected.push_back(Emit(analyzer, v));
  }

  std::vector<duckdb::string_t> vals;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
  }
  std::vector<std::vector<std::string>> got(values.size());
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    uint32_t tok = 0;
    for (const auto& run : runs) {
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        got[run.doc - 1].emplace_back(t.GetData(), t.GetSize());
      }
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPos, collect};
  tests::FillColumn(analyzer, vals, 1, sink.writer, sink.layout);
  sink.writer.Finish();
  EXPECT_EQ(expected, got);
}

TEST(ShingleTokenizerTest, column_fill_wave_split_matches_per_value) {
  auto analyzer = MakeAnalyzer(2, 3, /*output_unigrams=*/true);
  auto per_value = MakeAnalyzer(2, 3, /*output_unigrams=*/true);
  ASSERT_TRUE(analyzer.Traits().explicit_pos);

  const std::string value = "w1 w2 w3 w4";
  const auto incs = EmitWithInc(per_value, value);
  ASSERT_EQ(9, incs.size());
  std::vector<TermInc> expected_one;
  uint32_t pos = 0;
  for (const auto& [term, inc] : incs) {
    pos += inc;
    expected_one.emplace_back(term, pos);
  }

  constexpr size_t kDocs = 240;
  ASSERT_GT(kDocs * 9, 2 * irs::TokenBatch::kCapacity);

  std::vector<duckdb::string_t> vals;
  for (size_t i = 0; i < kDocs; ++i) {
    vals.emplace_back(value.data(), static_cast<uint32_t>(value.size()));
  }
  std::vector<std::vector<TermInc>> got(kDocs);
  bool saw_tail_open = false;
  const auto collect = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
    saw_tail_open |= runs.tail_open;
    uint32_t tok = 0;
    for (const auto& run : runs) {
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        got[run.doc - 1].emplace_back(std::string{t.GetData(), t.GetSize()},
                                      batch.pos[tok]);
      }
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPos, collect};
  tests::FillColumn(analyzer, vals, 1, sink.writer, sink.layout);
  sink.writer.Finish();

  ASSERT_TRUE(saw_tail_open);
  for (size_t v = 0; v < kDocs; ++v) {
    SCOPED_TRACE(testing::Message() << "doc=" << v + 1);
    ASSERT_EQ(expected_one, got[v]);
  }
}

TEST(ShingleTokenizerTest, read_token_checked_rejects_corrupt_blobs) {
  using ST = ShingleTokenizer;
  irs::bstring buf;
  ST::WriteToken(irs::ViewCast<irs::byte_type>(std::string_view{"quick"}), buf);
  const auto* p = buf.data();
  const auto* end = p + buf.size();
  irs::bytes_view token;
  EXPECT_EQ(end, ST::ReadTokenChecked(p, end, token));
  EXPECT_EQ("quick", ToString(token));
  EXPECT_EQ(nullptr, ST::ReadTokenChecked(p, end - 1, token));
  EXPECT_EQ(nullptr, ST::ReadTokenChecked(p, p, token));
  const irs::byte_type bad[] = {0xC1, 0x00};
  EXPECT_EQ(nullptr, ST::ReadTokenChecked(bad, bad + sizeof bad, token));
  const irs::byte_type short_prefix[] = {0x41};
  EXPECT_EQ(nullptr,
            ST::ReadTokenChecked(short_prefix, short_prefix + 1, token));
}

TEST(ShingleTokenizerTest, token_codec_round_trip) {
  for (size_t len : {size_t{0}, size_t{1}, size_t{63}, size_t{64}, size_t{255},
                     size_t{16383}, size_t{16384}, size_t{70000}}) {
    SCOPED_TRACE(len);
    const std::string token(len, 'x');
    EXPECT_EQ(token, CodecRoundTrip(token));
  }
}

TEST(ShingleTokenizerTest, token_codec_prefix_width) {
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

TEST(ShingleTokenizerTest, token_codec_binary_safe) {
  const std::string token{'\x00', '\xFF', '\xFF', '\x01', '\xFF'};
  EXPECT_EQ(token, CodecRoundTrip(token));
}

TEST(ShingleTokenizerTest, token_codec_sequence) {
  const std::vector<std::string> tokens{
    "",
    "a",
    std::string(64, 'b'),
    std::string(20000, 'c'),
    std::string{'\xFF', '\xFF'},
  };
  irs::bstring buf;
  for (const auto& t : tokens) {
    ShingleTokenizer::WriteToken(
      irs::ViewCast<irs::byte_type>(std::string_view{t}), buf);
  }
  EXPECT_EQ(tokens, DecodeStore(irs::bytes_view{buf}));
}

TEST(ShingleTokenizerTest, failed_base_fill_does_not_leak_into_next_value) {
  ShingleTokenizer analyzer{std::make_unique<EmitThenFailTokenizer>(),
                            {
                              .min_shingle_size = 2,
                              .max_shingle_size = 2,
                              .output_unigrams = true,
                            }};
  EXPECT_FALSE(tests::AnalyzeTerms(analyzer, "poison").has_value());
  const std::vector<std::string> expected{
    "w1", Shingle({"w1", "w2"}), "w2", Shingle({"w2", "w3"}), "w3",
  };
  EXPECT_EQ(expected, Emit(analyzer, "w1 w2 w3"));
}

TEST(ShingleTokenizerTest, memory_usage_accounts_scratch) {
  auto analyzer = MakeAnalyzer(2, 2, true);
  const auto before = analyzer.MemoryUsage();
  ASSERT_FALSE(Emit(analyzer, "quick brown fox").empty());
  EXPECT_GE(analyzer.MemoryUsage(), sizeof(irs::AccumulatorSink));
  EXPECT_GT(analyzer.MemoryUsage(), before);
}
