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

#include <functional>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/icu_text_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/split_by_non_alpha_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/stopwords_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "test_resources.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

using irs::analysis::CreateTokenizer;
using irs::analysis::DelimitedTokenizer;
using irs::analysis::IcuTextTokenizer;
using irs::analysis::NGramTokenizer;
using irs::analysis::NormalizingTokenizer;
using irs::analysis::PipelineTokenizer;
using irs::analysis::SplitByNonAlphaTokenizer;
using irs::analysis::StemmingTokenizer;
using irs::analysis::StopwordsTokenizer;
using irs::analysis::TextTokenizer;
using irs::analysis::Tokenizer;
using irs::analysis::TokenizerConfig;

struct Token {
  std::string term;
  uint32_t pos;

  bool operator==(const Token&) const = default;
};

class Collect final : public irs::TokenConsumer {
 public:
  static constexpr irs::TokenLayout kLayout = irs::TokenLayout::TermsPos;

  Collect(size_t limit, bool positions)
    : _limit{limit}, _positions{positions} {}

  void Prepare(duckdb::string_t) noexcept {}
  void Discard() noexcept { _taken = 0; }

  void Consume(irs::TokenBatch& batch, irs::DocRuns) final {
    Take(batch);
    _taken = 0;
  }

  bool OnProgress(irs::TokenBatch& batch) final {
    Take(batch);
    ++progress_calls;
    return tokens.size() < _limit;
  }

  std::vector<Token> tokens;
  size_t progress_calls = 0;

 private:
  void Take(const irs::TokenBatch& batch) {
    for (auto i = _taken; i != batch.count; ++i) {
      tokens.emplace_back(
        std::string{batch.terms[i].GetData(), batch.terms[i].GetSize()},
        _positions ? batch.pos[i] : 0);
    }
    _taken = batch.count;
  }

  size_t _limit;
  bool _positions;
  uint32_t _taken = 0;
};

class FailOnMarker final : public irs::analysis::TypedTokenizer<FailOnMarker> {
 public:
  irs::TokenTraits Traits() const noexcept final { return {}; }

  template<irs::TokenLayout Layout>
  bool DoFill(const duckdb::string_t& raw, irs::TokenSink& sink) {
    if (std::string_view{raw.GetData(), raw.GetSize()} == "FAIL") {
      return false;
    }
    sink.Emit<Layout>(raw);
    return true;
  }
};

struct Run {
  std::optional<std::vector<Token>> tokens;
  size_t progress_calls = 0;
};

Run Fill(Tokenizer& tokenizer, std::string_view value) {
  irs::ValueAnalyzer analyzer;
  Collect out{std::numeric_limits<size_t>::max(),
              tokenizer.Traits().explicit_pos};
  if (!analyzer.Analyze(tokenizer, tests::ToStringT(value), out)) {
    return {};
  }
  return {std::move(out.tokens), out.progress_calls};
}

Run Scan(Tokenizer& tokenizer, std::string_view value, size_t limit) {
  irs::ScanAnalyzer analyzer;
  Collect out{limit, tokenizer.Traits().explicit_pos};
  if (!analyzer.Scan(tokenizer, tests::ToStringT(value), out)) {
    return {};
  }
  return {std::move(out.tokens), out.progress_calls};
}

struct Fixture {
  std::string name;
  std::function<Tokenizer::ptr()> make;
  bool reports;
};

constexpr irs::Case kCases[] = {irs::Case::None, irs::Case::Lower,
                                irs::Case::Upper};

constexpr TextTokenizer::Options::Accept kAccepts[] = {
  TextTokenizer::Options::Accept::Any, TextTokenizer::Options::Accept::Graphic,
  TextTokenizer::Options::Accept::AlphaNumeric,
  TextTokenizer::Options::Accept::Alpha};

std::string Name(std::string_view kind, std::initializer_list<int> options) {
  std::string name{kind};
  for (const auto option : options) {
    name += '/';
    name += std::to_string(option);
  }
  return name;
}

TokenizerConfig Split(SplitByNonAlphaTokenizer::Options::Chars chars,
                      irs::Case convert) {
  return {
    SplitByNonAlphaTokenizer::Options{.case_convert = convert, .chars = chars}};
}

TokenizerConfig Text(TextTokenizer::Options::Separate separate,
                     TextTokenizer::Options::Accept accept, irs::Case convert) {
  return {TextTokenizer::Options{
    .separate = separate, .accept = accept, .convert = convert}};
}

TokenizerConfig Icu(IcuTextTokenizer::Options::Separate separate,
                    IcuTextTokenizer::Options::Accept accept,
                    const char* locale) {
  return {IcuTextTokenizer::Options{
    .separate = separate,
    .accept = accept,
    .locale = duckdb::text::Locale::FromName(locale)}};
}

Tokenizer::ptr Pipeline(std::vector<TokenizerConfig> stages) {
  PipelineTokenizer::Options opts;
  for (auto& stage : stages) {
    opts.children.emplace_back(
      std::make_unique<TokenizerConfig>(std::move(stage)));
  }
  return CreateTokenizer(TokenizerConfig{std::move(opts)}, tests::Cache());
}

std::vector<TokenizerConfig> Filters(int set) {
  const auto en = duckdb::text::Locale::FromName("en");
  std::vector<TokenizerConfig> filters;
  if (set == 1 || set == 3) {
    filters.emplace_back(
      NormalizingTokenizer::Options{.locale = en, .accent = false});
  }
  if (set == 2 || set == 3) {
    filters.emplace_back(StopwordsTokenizer::Options{.mask = {"the", "of"}});
  }
  if (set == 0 || set == 3) {
    filters.emplace_back(StemmingTokenizer::Options{.locale = en});
  }
  return filters;
}

std::vector<Fixture> Fixtures() {
  using Chars = SplitByNonAlphaTokenizer::Options::Chars;
  using Separate = TextTokenizer::Options::Separate;
  using IcuSeparate = IcuTextTokenizer::Options::Separate;
  constexpr Chars kChars[] = {Chars::Ascii, Chars::AsciiBytes, Chars::Alnum,
                              Chars::Letters, Chars::Whitespace};
  constexpr Separate kSeparates[] = {Separate::None,      Separate::Word,
                                     Separate::Sentence,  Separate::Line,
                                     Separate::Paragraph, Separate::Grapheme};
  constexpr const char* kLocales[] = {"en", "en_US_POSIX"};

  std::vector<Fixture> out;
  for (const auto chars : kChars) {
    for (const auto convert : kCases) {
      out.emplace_back(
        Name("split", {static_cast<int>(chars), static_cast<int>(convert)}),
        [=] { return CreateTokenizer(Split(chars, convert), tests::Cache()); },
        true);
    }
  }
  for (const auto separate : kSeparates) {
    for (const auto accept : kAccepts) {
      for (const auto convert : kCases) {
        out.emplace_back(
          Name("text", {static_cast<int>(separate), static_cast<int>(accept),
                        static_cast<int>(convert)}),
          [=] {
            return CreateTokenizer(Text(separate, accept, convert),
                                   tests::Cache());
          },
          separate == Separate::Word || separate == Separate::Grapheme);
      }
    }
  }
  for (const auto separate : {IcuSeparate::Word, IcuSeparate::Sentence}) {
    for (const auto accept : kAccepts) {
      for (const auto* locale : kLocales) {
        out.emplace_back(
          Name("icu", {static_cast<int>(separate), static_cast<int>(accept),
                       locale == kLocales[0] ? 0 : 1}),
          [=] {
            return CreateTokenizer(Icu(separate, accept, locale),
                                   tests::Cache());
          },
          true);
      }
    }
  }
  const std::function<TokenizerConfig()> heads[] = {
    [] { return Split(Chars::Ascii, irs::Case::Lower); },
    [] { return Split(Chars::Whitespace, irs::Case::None); },
    [] { return Split(Chars::Letters, irs::Case::Lower); },
    [] { return Split(Chars::Alnum, irs::Case::None); },
    [] {
      return Text(Separate::Word, TextTokenizer::Options::Accept::AlphaNumeric,
                  irs::Case::Lower);
    },
    [] {
      return Text(Separate::Grapheme, TextTokenizer::Options::Accept::Any,
                  irs::Case::None);
    },
    [] {
      return Icu(IcuSeparate::Word,
                 TextTokenizer::Options::Accept::AlphaNumeric, "en");
    },
    [] {
      return Icu(IcuSeparate::Word, TextTokenizer::Options::Accept::Any,
                 "en_US_POSIX");
    },
  };
  for (size_t head = 0; head != std::size(heads); ++head) {
    for (int set = 0; set != 4; ++set) {
      out.emplace_back(
        Name("pipeline", {static_cast<int>(head), set}),
        [make = heads[head], set] {
          auto stages = Filters(set);
          stages.insert(stages.begin(), make());
          return Pipeline(std::move(stages));
        },
        true);
    }
    out.emplace_back(
      Name("ngram_pipeline", {static_cast<int>(head)}),
      [make = heads[head]] {
        std::vector<TokenizerConfig> stages;
        stages.emplace_back(make());
        stages.emplace_back(NGramTokenizer::Options{
          .min_gram = 2, .max_gram = 3, .preserve_original = false});
        return Pipeline(std::move(stages));
      },
      false);
    out.emplace_back(
      Name("expander_pipeline", {static_cast<int>(head)}),
      [make = heads[head]] {
        std::vector<TokenizerConfig> stages;
        stages.emplace_back(make());
        stages.emplace_back(NGramTokenizer::Options{
          .min_gram = 2, .max_gram = 3, .preserve_original = false});
        auto filters = Filters(1);
        stages.insert(stages.end(), std::make_move_iterator(filters.begin()),
                      std::make_move_iterator(filters.end()));
        return Pipeline(std::move(stages));
      },
      false);
  }
  out.emplace_back(
    "sentence_pipeline",
    [] {
      auto stages = Filters(0);
      stages.insert(stages.begin(),
                    Text(Separate::Sentence,
                         TextTokenizer::Options::Accept::Any, irs::Case::None));
      return Pipeline(std::move(stages));
    },
    false);
  out.emplace_back(
    "delimited", [] { return DelimitedTokenizer::Make({.delimiter = " "}); },
    false);
  return out;
}

std::vector<std::string> Corpus() {
  std::mt19937 rng{20261009};
  const std::string_view words[] = {"the",
                                    "of",
                                    "United",
                                    "STATES",
                                    "a",
                                    "x1",
                                    "42",
                                    "naïve",
                                    "Straße",
                                    "über",
                                    "can't",
                                    "e-mail",
                                    "ab12cd",
                                    "Mixed",
                                    "verylongwordthatcrossesblockboundaries",
                                    "z",
                                    "日本語",
                                    "テキスト",
                                    "中文",
                                    "\xF0\x9F\x98\x80"};
  const std::string_view gaps[] = {
    " ", ", ", "  \t", ".",        "!? ",          "\n",    " \xE2\x80\x94 ",
    "/", "__", "\r\n", "\xC2\xA0", "\xE3\x80\x82", ". The "};
  std::vector<std::string> out{"",    " ",    "word", "two words",
                               ",,,", "\t\n", "日本", "a.b. c"};
  for (size_t n : {3, 31, 32, 33, 63, 64, 65, 200, 1500, 3000}) {
    for (int round = 0; round != 3; ++round) {
      std::string value;
      for (size_t i = 0; i != n; ++i) {
        value += words[rng() % std::size(words)];
        value += gaps[rng() % std::size(gaps)];
      }
      out.emplace_back(std::move(value));
    }
  }
  const std::string_view ascii_words[] = {
    "the",
    "of",
    "United",
    "STATES",
    "a",
    "x1",
    "42",
    "can't",
    "e-mail",
    "ab12cd",
    "Mixed",
    "z",
    "verylongwordthatcrossesblockboundaries"};
  const std::string_view ascii_gaps[] = {" ", ", ", "  \t", ".", "!? ", "\n"};
  for (size_t n : {200, 1500}) {
    std::string value;
    for (size_t i = 0; i != n; ++i) {
      value += ascii_words[rng() % std::size(ascii_words)];
      value += ascii_gaps[rng() % std::size(ascii_gaps)];
    }
    out.emplace_back(std::move(value));
  }
  return out;
}

std::string Repeat(std::string_view piece, size_t times) {
  std::string out;
  out.reserve(piece.size() * times);
  for (size_t i = 0; i != times; ++i) {
    out += piece;
  }
  return out;
}

TEST(scan_sink_test, scan_matches_fill) {
  const auto corpus = Corpus();
  for (const auto& fixture : Fixtures()) {
    SCOPED_TRACE(fixture.name);
    const auto tokenizer = fixture.make();
    for (const auto& value : corpus) {
      const auto expected = Fill(*tokenizer, value);
      const auto got =
        Scan(*tokenizer, value, std::numeric_limits<size_t>::max());
      EXPECT_EQ(expected.tokens, got.tokens) << value;
    }
  }
}

TEST(scan_sink_test, stopped_scan_is_prefix) {
  const auto corpus = Corpus();
  for (const auto& fixture : Fixtures()) {
    SCOPED_TRACE(fixture.name);
    const auto tokenizer = fixture.make();
    for (const auto& value : corpus) {
      const auto expected = Fill(*tokenizer, value);
      ASSERT_TRUE(expected.tokens) << value;
      const auto& all = *expected.tokens;
      for (const size_t limit : {1, 5, 40, 300}) {
        const auto got = Scan(*tokenizer, value, limit);
        ASSERT_TRUE(got.tokens) << value;
        const auto& part = *got.tokens;
        ASSERT_LE(part.size(), all.size()) << value;
        EXPECT_TRUE(std::equal(part.begin(), part.end(), all.begin())) << value;
        if (all.size() >= limit) {
          EXPECT_GE(part.size(), limit) << value;
        }
        if (!fixture.reports) {
          EXPECT_EQ(part.size(), all.size()) << value;
          EXPECT_EQ(got.progress_calls, 0U) << value;
        }
      }
    }
  }
}

TEST(scan_sink_test, scan_stops_early) {
  const std::string values[] = {Repeat("Word. ", 5000),
                                Repeat("W\xC3\xB6rd \xC3\xBC"
                                       "ber. ",
                                       2500)};
  for (const auto& fixture : Fixtures()) {
    SCOPED_TRACE(fixture.name);
    const auto tokenizer = fixture.make();
    for (const auto& value : values) {
      const auto got = Scan(*tokenizer, value, 10);
      ASSERT_TRUE(got.tokens);
      if (fixture.reports) {
        EXPECT_EQ(got.progress_calls, 1U) << value.substr(0, 16);
        EXPECT_GE(got.tokens->size(), 10U) << value.substr(0, 16);
        EXPECT_LT(got.tokens->size(), 200U) << value.substr(0, 16);
      } else {
        EXPECT_EQ(got.progress_calls, 0U) << value.substr(0, 16);
      }
    }
  }
}

TEST(scan_sink_test, cjk_scan_stops_early) {
  using Separate = TextTokenizer::Options::Separate;
  using IcuSeparate = IcuTextTokenizer::Options::Separate;
  const auto value = Repeat("日本語のテキストです。", 2000);
  const std::function<TokenizerConfig()> configs[] = {
    [] {
      return Text(Separate::Word, TextTokenizer::Options::Accept::AlphaNumeric,
                  irs::Case::None);
    },
    [] {
      return Text(Separate::Grapheme, TextTokenizer::Options::Accept::Any,
                  irs::Case::None);
    },
    [] {
      return Icu(IcuSeparate::Word,
                 TextTokenizer::Options::Accept::AlphaNumeric, "ja");
    },
    [] {
      return Split(SplitByNonAlphaTokenizer::Options::Chars::Letters,
                   irs::Case::None);
    },
  };
  for (const auto& config : configs) {
    const auto tokenizer = CreateTokenizer(config(), tests::Cache());
    const auto expected = Fill(*tokenizer, value);
    ASSERT_TRUE(expected.tokens);
    ASSERT_GE(expected.tokens->size(), 200U);
    const auto got = Scan(*tokenizer, value, 10);
    ASSERT_TRUE(got.tokens);
    EXPECT_EQ(got.progress_calls, 1U);
    EXPECT_GE(got.tokens->size(), 10U);
    EXPECT_LT(got.tokens->size(), 200U);
    EXPECT_TRUE(std::equal(got.tokens->begin(), got.tokens->end(),
                           expected.tokens->begin()));
  }
}

TEST(scan_sink_test, failed_scan_rejects_the_value) {
  std::vector<Tokenizer::ptr> stages;
  stages.emplace_back(std::make_unique<FailOnMarker>());
  stages.emplace_back(CreateTokenizer(
    TokenizerConfig{NormalizingTokenizer::Options{
      .locale = duckdb::text::Locale::FromName("en"), .accent = false}},
    tests::Cache()));
  const auto pipeline = std::make_unique<PipelineTokenizer>(std::move(stages));
  FailOnMarker front;
  for (Tokenizer* tokenizer : {static_cast<Tokenizer*>(pipeline.get()),
                               static_cast<Tokenizer*>(&front)}) {
    EXPECT_FALSE(Scan(*tokenizer, "FAIL", 1).tokens);
    const auto got = Scan(*tokenizer, "Word", 1);
    ASSERT_TRUE(got.tokens);
    EXPECT_EQ(*got.tokens, *Fill(*tokenizer, "Word").tokens);
    EXPECT_FALSE(Scan(*tokenizer, "FAIL", 1).tokens);
  }
}

TEST(scan_sink_test, scan_rebinds_a_sink_bound_for_another_fill) {
  auto stages = Filters(1);
  stages.insert(
    stages.begin(),
    Split(SplitByNonAlphaTokenizer::Options::Chars::Ascii, irs::Case::Lower));
  const auto pipeline = Pipeline(std::move(stages));
  const std::string value =
    Repeat("Quick Brown Foxes jumped over lazy dogs. ", 40);
  const auto filled = Fill(*pipeline, value);
  ASSERT_TRUE(filled.tokens);
  std::vector<std::string> expected;
  for (const auto& token : *filled.tokens) {
    expected.emplace_back(token.term);
  }

  std::vector<std::string> terms;
  tests::FnTokenSink sink{
    irs::TokenLayout::TermsPos, [&](irs::TokenBatch& batch, irs::DocRuns) {
      for (uint32_t i = 0; i != batch.count; ++i) {
        terms.emplace_back(batch.terms[i].GetData(), batch.terms[i].GetSize());
      }
    }};
  irs::ScanSink writer;
  writer.Bind(sink, nullptr);
  const auto scan = [&] {
    terms.clear();
    writer.BeginScan();
    EXPECT_TRUE(pipeline->Scan(tests::ToStringT(value), writer, {}));
    writer.Finish();
    return terms;
  };

  ASSERT_TRUE(pipeline->Fill(tests::ToStringT(value), writer,
                             {.layout = irs::TokenLayout::Terms}));
  writer.Finish();
  EXPECT_EQ(scan(), expected);

  const duckdb::string_t column[] = {tests::ToStringT(value)};
  tests::FillColumn(*pipeline, column, irs::doc_limits::min(), writer,
                    irs::TokenLayout::TermsPos);
  writer.Finish();
  EXPECT_EQ(scan(), expected);

  EXPECT_EQ(scan(), expected);
}

}  // namespace
