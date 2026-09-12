////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <unicode/locid.h>

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "test_resources.hpp"
#include "tests_config.hpp"
#include "token_sink_asserts.hpp"

namespace {

constexpr size_t kNoMax = std::numeric_limits<uint32_t>::max();

struct TextOpts {
  std::string locale = "en_US.UTF-8";
  irs::Case case_convert = irs::Case::Lower;
  std::vector<std::string> stopwords;
  std::string stopwords_path;
  bool accent = false;
  bool stemming = true;
  bool edge = false;
  size_t min_gram = 1;
  size_t max_gram = kNoMax;
  bool preserve_original = false;
};

irs::analysis::Tokenizer::ptr MakeText(TextOpts opts = {}) {
  using namespace irs::analysis;
  const auto locale = icu::Locale::createFromName(opts.locale.c_str());
  const std::string lang = locale.getLanguage();
  const bool locale_case =
    lang == "tr" || lang == "az" || lang == "lt" || lang == "el";
  PipelineTokenizer::Options pipeline;
  const auto add = [&](TokenizerConfig cfg) {
    pipeline.children.push_back(
      std::make_unique<TokenizerConfig>(std::move(cfg)));
  };
  add({TextTokenizer::Options{.convert = locale_case ? irs::Case::None
                                                     : opts.case_convert}});
  if (!opts.accent || locale_case) {
    add({NormalizingTokenizer::Options{
      .locale = locale,
      .case_convert = locale_case ? opts.case_convert : irs::Case::None,
      .accent = opts.accent}});
  }
  if (!opts.stopwords.empty() || !opts.stopwords_path.empty()) {
    add({StopwordsTokenizer::Options{
      .mask = std::move(opts.stopwords),
      .stopwords_path = std::move(opts.stopwords_path)}});
  }
  if (opts.stemming) {
    add({StemmingTokenizer::Options{.locale = locale}});
  }
  if (opts.edge) {
    add({NGramTokenizer::Options{
      .min_gram = opts.min_gram,
      .max_gram = opts.max_gram,
      .preserve_original = opts.preserve_original,
      .stream_bytes_type = NGramTokenizer::InputType::UTF8,
      .ngram_mode = NGramTokenizer::NGramMode::Prefix}});
  }
  return CreateTokenizer(TokenizerConfig{std::move(pipeline)}, tests::Cache());
}

const std::string kStopwordsDir = IRS_TEST_RESOURCE_DIR "/en";
const std::string kStopwordsFile =
  IRS_TEST_RESOURCE_DIR "/en/text_analyzer_stopwords.txt";

}  // namespace
namespace tests {

class TextAnalyzerParserTestSuite : public ::testing::Test {};

}  // namespace tests

using namespace tests;
using namespace irs::analysis;

TEST_F(TextAnalyzerParserTestSuite, consts) {
  static_assert("split_text" ==
                irs::Type<irs::analysis::TextTokenizer>::name());
}

TEST_F(TextAnalyzerParserTestSuite, test_nbsp_whitespace) {
  auto stream = MakeText({.locale = "C.UTF-8"});
  ASSERT_NE(nullptr, stream);

  auto tokens = tests::Analyze(*stream, "1,24 prosenttia");
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected{{"1,24", 1, 0, 4},
                                                   {"prosenttia", 2, 5, 15}};
  ASSERT_EQ(expected, *tokens);
}

TEST_F(TextAnalyzerParserTestSuite, invalid_utf8_offsets_stay_within_value) {
  auto stream = MakeText();

  const std::string data =
    "a\xFF\xFF"
    "b c\xFF"
    "d";
  const auto tokens = tests::Analyze(*stream, data);
  ASSERT_TRUE(tokens.has_value());
  ASSERT_FALSE(tokens->empty());
  for (const auto& token : *tokens) {
    SCOPED_TRACE(testing::Message() << "term=" << token.term);
    ASSERT_LE(token.offs_start, token.offs_end);
    ASSERT_LE(token.offs_end, data.size());
  }
}

TEST_F(TextAnalyzerParserTestSuite, repeated_fills_hit_the_stem_cache) {
  auto stream = MakeText();

  const std::string data = "running runners jumps jumped easily";
  const auto first = tests::Analyze(*stream, data);
  ASSERT_TRUE(first.has_value());
  ASSERT_FALSE(first->empty());
  const auto after_first = stream->MemoryUsage();
  EXPECT_GT(after_first, 0u);
  for (int i = 0; i < 3; ++i) {
    const auto again = tests::Analyze(*stream, data);
    ASSERT_TRUE(again.has_value());
    ASSERT_EQ(*first, *again);
  }
  EXPECT_EQ(after_first, stream->MemoryUsage());
}

TEST_F(TextAnalyzerParserTestSuite, test_text_analyzer) {
  {
    auto stream = MakeText();
    ASSERT_NE(nullptr, stream);
    auto tokens = tests::Analyze(
      *stream,
      " A  hErd of   quIck brown  foXes ran    and Jumped over  a     "
      "runninG dog");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected{
      {"a", 1, 1, 2},       {"herd", 2, 4, 8},    {"of", 3, 9, 11},
      {"quick", 4, 14, 19}, {"brown", 5, 20, 25}, {"fox", 6, 27, 32},
      {"ran", 7, 33, 36},   {"and", 8, 40, 43},   {"jump", 9, 44, 50},
      {"over", 10, 51, 55}, {"a", 11, 57, 58},    {"run", 12, 63, 70},
      {"dog", 13, 71, 74}};
    ASSERT_EQ(expected, *tokens);
  }

  {
    auto stream = MakeText({.case_convert = irs::Case::Lower});
    auto tokens = tests::AnalyzeTerms(*stream, "A qUiCk brOwn FoX");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"a", "quick", "brown", "fox"};
    ASSERT_EQ(expected, *tokens);
  }

  {
    auto stream = MakeText({.case_convert = irs::Case::Upper});
    auto tokens = tests::AnalyzeTerms(*stream, "A qUiCk brOwn FoX");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"A", "QUICK", "BROWN", "FOX"};
    ASSERT_EQ(expected, *tokens);
  }

  {
    auto stream = MakeText({.case_convert = irs::Case::None});
    auto tokens = tests::AnalyzeTerms(*stream, "A qUiCk brOwn FoX");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"A", "qUiCk", "brOwn", "FoX"};
    ASSERT_EQ(expected, *tokens);
  }

  {
    auto stream = MakeText({.stopwords = {"a", "of", "and"}});
    auto tokens = tests::Analyze(*stream, " A thing of some KIND and ANoTher ");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"thing", "some", "kind", "anoth"};
    ASSERT_EQ(expected.size(), tokens->size());
    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_EQ(expected[i], (*tokens)[i].term);
      ASSERT_EQ(i + 1, (*tokens)[i].pos);
    }
  }

  {
    constexpr std::u8string_view kData(
      u8"по вечерам "
      u8"ежик ходил "
      u8"к медвежонк"
      u8"у считать з"
      u8"везды");
    auto stream = MakeText({.locale = "ru_RU.UTF-16"});
    auto tokens = tests::Analyze(*stream, irs::ViewCast<char>(kData));
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected{
      {"\xD0\xBF\xD0\xBE", 1, 0, 4},
      {"\xD0\xB2\xD0\xB5\xD1\x87\xD0\xB5\xD1\x80", 2, 5, 19},
      {"\xD0\xB5\xD0\xB6\xD0\xB8\xD0\xBA", 3, 20, 28},
      {"\xD1\x85\xD0\xBE\xD0\xB4", 4, 29, 39},
      {"\xD0\xBA", 5, 40, 42},
      {"\xD0\xBC\xD0\xB5\xD0\xB4\xD0\xB2\xD0\xB5\xD0\xB6\xD0\xBE"
       "\xD0\xBD\xD0\xBA",
       6, 43, 63},
      {"\xD1\x81\xD1\x87\xD0\xB8\xD1\x82\xD0\xB0", 7, 64, 78},
      {"\xD0\xB7\xD0\xB2\xD0\xB5\xD0\xB7\xD0\xB4", 8, 79, 91}};
    ASSERT_EQ(expected, *tokens);
  }

  {
    constexpr std::u8string_view kData(
      u8"\U0000043f\U0000043e\U00000020\U00000432\U00000435\U00000447"
      u8"\U00000435\U00000440\U00000430\U0000043c\U00000020\U00000435"
      u8"\U00000436\U00000438\U0000043a");
    auto stream = MakeText({.locale = "en_US.utf32"});
    auto tokens = tests::Analyze(*stream, irs::ViewCast<char>(kData));
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected{
      {"\xD0\xBF\xD0\xBE", 1, 0, 4},
      {"\xD0\xB2\xD0\xB5\xD1\x87\xD0\xB5\xD1\x80\xD0\xB0\xD0\xBC", 2, 5, 19},
      {"\xD0\xB5\xD0\xB6\xD0\xB8\xD0\xBA", 3, 20, 28}};
    ASSERT_EQ(expected, *tokens);
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_fail_load_stopwords) {
  ASSERT_ANY_THROW(MakeText({.stopwords_path = "invalid stopwords path"}));
}

TEST_F(TextAnalyzerParserTestSuite, test_load_stopwords) {
  const std::vector<tests::AnalyzerToken> expected{{"e", 1, 2, 3},
                                                   {"u", 2, 8, 9}};
  {
    auto stream = MakeText({.stopwords_path = kStopwordsDir});
    ASSERT_NE(nullptr, stream);
    auto tokens = tests::Analyze(*stream, "A E I O U");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(expected, *tokens);
  }
  {
    auto stream = MakeText({.stopwords_path = kStopwordsFile});
    ASSERT_NE(nullptr, stream);
    auto tokens = tests::Analyze(*stream, "A E I O U");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(expected, *tokens);
  }
  {
    auto stream =
      MakeText({.stopwords = {"e"}, .stopwords_path = kStopwordsFile});
    ASSERT_NE(nullptr, stream);
    auto tokens = tests::Analyze(*stream, "A E I O U");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> merged{{"u", 1, 8, 9}};
    ASSERT_EQ(merged, *tokens);
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_load_no_stopwords) {
  const std::vector<tests::AnalyzerToken> expected{{"a", 1, 0, 1},
                                                   {"e", 2, 2, 3},
                                                   {"i", 3, 4, 5},
                                                   {"o", 4, 6, 7},
                                                   {"u", 5, 8, 9}};
  for (auto& stream : {MakeText(), MakeText({.stopwords_path = ""})}) {
    ASSERT_NE(nullptr, stream);
    auto tokens = tests::Analyze(*stream, "A E I O U");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(expected, *tokens);
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_text_ngrams) {
  const auto terms = [](TextOpts opts, std::string_view data) {
    auto stream = MakeText(std::move(opts));
    EXPECT_NE(nullptr, stream);
    const auto tokens = tests::AnalyzeTerms(*stream, data);
    EXPECT_TRUE(tokens.has_value());
    return *tokens;
  };
  const std::string data = " A  hErd of   quIck ";
  const std::string short_data = " A  hErd of";

  {
    const std::vector<std::string> expected{"he", "her", "of", "qu", "qui"};
    ASSERT_EQ(
      expected,
      terms({.stopwords = {"a"}, .edge = true, .min_gram = 2, .max_gram = 3},
            data));
  }

  {
    const std::vector<std::string> expected{"h",  "he", "her", "o",
                                            "of", "q",  "qu",  "qui"};
    auto stream = MakeText(
      {.stopwords = {"a"}, .edge = true, .min_gram = 0, .max_gram = 3});
    ASSERT_NE(nullptr, stream);
    const auto tokens = tests::AnalyzeTerms(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(expected, *tokens);
    const auto again = tests::AnalyzeTerms(*stream, data);
    ASSERT_TRUE(again.has_value());
    ASSERT_EQ(expected, *again);
  }

  {
    const std::vector<std::string> expected{"he", "her", "herd", "of",
                                            "qu", "qui", "quick"};
    ASSERT_EQ(expected, terms({.stopwords = {"a"},
                               .edge = true,
                               .min_gram = 2,
                               .max_gram = 3,
                               .preserve_original = true},
                              data));
  }

  {
    const std::vector<std::string> expected{"her", "qui"};
    ASSERT_EQ(
      expected,
      terms({.stopwords = {"a"}, .edge = true, .min_gram = 3, .max_gram = 3},
            data));
  }

  {
    const std::vector<std::string> expected{"herd", "quic"};
    ASSERT_EQ(
      expected,
      terms({.stopwords = {"a"}, .edge = true, .min_gram = 4, .max_gram = 3},
            data));
  }

  {
    const std::vector<std::string> expected{"herd", "of", "quic", "quick"};
    ASSERT_EQ(expected, terms({.stopwords = {"a"},
                               .edge = true,
                               .min_gram = 4,
                               .max_gram = 3,
                               .preserve_original = true},
                              data));
  }

  {
    const std::vector<std::string> expected{"h", "o", "q"};
    ASSERT_EQ(
      expected,
      terms({.stopwords = {"a"}, .edge = true, .min_gram = 0, .max_gram = 0},
            data));
  }

  {
    const std::vector<std::string> expected{"h", "o"};
    ASSERT_EQ(expected, terms({.stopwords = {"a"}, .edge = true, .max_gram = 1},
                              short_data));
  }

  {
    const std::vector<std::string> expected{"h", "herd", "o", "of"};
    ASSERT_EQ(expected, terms({.stopwords = {"a"},
                               .edge = true,
                               .max_gram = 1,
                               .preserve_original = true},
                              short_data));
  }

  {
    const std::vector<std::string> expected{"h",    "he", "her",
                                            "herd", "o",  "of"};
    ASSERT_EQ(expected, terms({.stopwords = {"a"}, .edge = true, .min_gram = 1},
                              short_data));
    ASSERT_EQ(expected, terms({.stopwords = {"a"}, .edge = true}, short_data));
    ASSERT_EQ(
      expected,
      terms({.stopwords = {"a"}, .edge = true, .preserve_original = true},
            short_data));
  }

  {
    const std::string cyrillic =
      "\xD0\x9F\xD0\xBE\x20\xD0\xB2\xD0\xB5\xD1\x87\xD0\xB5\xD1\x80\xD0\xB0"
      "\xD0\xBC\x20\xD0\xBA\x20\xD0\x9C\xD0\xB5\xD0\xB4\xD0\xB2\xD0\xB5\xD0"
      "\xB6\xD0\xBE\xD0\xBD\xD0\xBA\xD1\x83";
    auto stream = MakeText({.locale = "ru_RU.UTF-8",
                            .stopwords = {"\xD0\xBA"},
                            .edge = true,
                            .min_gram = 1,
                            .max_gram = 2});
    ASSERT_NE(nullptr, stream);
    const auto tokens = tests::Analyze(*stream, cyrillic);
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected{
      {"\xD0\xBF", 1, 0, 2},   {"\xD0\xBF\xD0\xBE", 1, 0, 4},
      {"\xD0\xB2", 2, 5, 7},   {"\xD0\xB2\xD0\xB5", 2, 5, 9},
      {"\xD0\xBC", 3, 23, 25}, {"\xD0\xBC\xD0\xB5", 3, 23, 27}};
    ASSERT_EQ(expected, *tokens);
  }
}

namespace {

struct TextTok {
  std::string term;
  uint32_t pos;
  uint32_t offs_start;
  uint32_t offs_end;
};

std::vector<TextTok> PullText(irs::analysis::Tokenizer& stream,
                              std::string_view data) {
  std::vector<TextTok> out;
  const auto tokens = tests::Analyze(stream, data);
  if (!tokens) {
    return out;
  }
  for (auto& tok : *tokens) {
    out.push_back({std::move(tok.term), tok.pos, tok.offs_start, tok.offs_end});
  }
  return out;
}

}  // namespace

TEST(text_tokenizer_batch, native_fills_match_pull) {
  const std::vector<std::string> values = {"The Quick Brown Fox", "",
                                           "running runner runs", "a",
                                           "Ma\xc3\xb1"
                                           "ana caf\xc3\xa9 na\xc3\xaf"
                                           "ve"};

  auto run_case = [&](const TextOpts& base) {
    auto pull_stream = MakeText(base);
    auto fill_stream = MakeText(base);
    ASSERT_NE(nullptr, pull_stream);
    ASSERT_NE(nullptr, fill_stream);

    for (const auto& v : values) {
      SCOPED_TRACE(v);
      const auto pulled = PullText(*pull_stream, v);

      std::vector<TextTok> filled;
      const auto collect = [&](irs::TokenBatch& batch,
                               std::span<const irs::DocRun> /*runs*/) {
        for (uint32_t i = 0; i < batch.count; ++i) {
          const auto& t = batch.terms[i];
          filled.push_back({std::string{t.GetData(), t.GetSize()}, batch.pos[i],
                            batch.offs_start[i], batch.offs_end[i]});
        }
      };
      tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
      fill_stream->Fill(v, sink.writer, {sink.layout});
      sink.writer.Finish();

      ASSERT_EQ(pulled.size(), filled.size());
      for (size_t i = 0; i < pulled.size(); ++i) {
        SCOPED_TRACE(i);
        ASSERT_EQ(pulled[i].term, filled[i].term);
        ASSERT_EQ(pulled[i].pos, filled[i].pos);
        ASSERT_EQ(pulled[i].offs_start, filled[i].offs_start);
        ASSERT_EQ(pulled[i].offs_end, filled[i].offs_end);
      }
    }
  };

  run_case(TextOpts{});
  run_case(TextOpts{.accent = true, .stemming = false});
  run_case(TextOpts{.stemming = false,
                    .edge = true,
                    .min_gram = 2,
                    .max_gram = 3,
                    .preserve_original = true});
}

TEST(text_tokenizer_batch, column_fill_matches_pull) {
  auto pull_stream = MakeText();
  auto fill_stream = MakeText();
  ASSERT_NE(nullptr, pull_stream);
  ASSERT_NE(nullptr, fill_stream);

  const std::vector<std::string> raw = {"The Quick Brown Fox", "",
                                        "running jumps", "lazy Dog"};
  std::vector<duckdb::string_t> values;
  for (size_t i = 0; i < raw.size(); ++i) {
    values.emplace_back(raw[i].data(), static_cast<uint32_t>(raw[i].size()));
  }

  constexpr irs::doc_id_t kFirstDoc = 100;
  std::map<irs::doc_id_t, std::vector<TextTok>> filled;
  irs::doc_id_t open_doc = irs::doc_limits::invalid();
  uint32_t dense_pos = 0;
  const auto collect = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
    uint32_t base = 0;
    for (const auto& run : runs) {
      if (run.doc != open_doc) {
        open_doc = run.doc;
        dense_pos = 0;
      }
      auto& out = filled[run.doc];
      for (uint32_t i = base; i < base + run.ntokens; ++i) {
        const auto& t = batch.terms[i];
        out.push_back({std::string{t.GetData(), t.GetSize()}, ++dense_pos,
                       batch.offs_start[i], batch.offs_end[i]});
      }
      base += run.ntokens;
    }
    ASSERT_EQ(batch.count, base);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
  tests::FillColumn(*fill_stream, values, kFirstDoc, sink.writer, sink.layout);
  sink.writer.Finish();

  for (size_t v = 0; v < raw.size(); ++v) {
    SCOPED_TRACE(raw[v]);
    const auto pulled = PullText(*pull_stream, raw[v]);
    const auto it = filled.find(kFirstDoc + static_cast<irs::doc_id_t>(v));
    const auto& got = it == filled.end() ? std::vector<TextTok>{} : it->second;
    ASSERT_EQ(pulled.size(), got.size());
    for (size_t i = 0; i < pulled.size(); ++i) {
      SCOPED_TRACE(i);
      ASSERT_EQ(pulled[i].term, got[i].term);
      ASSERT_EQ(pulled[i].pos, got[i].pos);
      ASSERT_EQ(pulled[i].offs_start, got[i].offs_start);
      ASSERT_EQ(pulled[i].offs_end, got[i].offs_end);
    }
  }
}

namespace {

std::vector<tests::AnalyzerToken> TextAnalyze(const TextOpts& opts,
                                              std::string_view value) {
  auto stream = MakeText(opts);
  auto tokens = tests::Analyze(*stream, value);
  EXPECT_TRUE(tokens.has_value());
  return std::move(*tokens);
}

void AssertTextAsciiMatchesUnicode(const TextOpts& opts,
                                   std::string_view value) {
  auto stream = MakeText(opts);
  tests::AssertAsciiMatchesUnicode(*stream, value);
}

}  // namespace

TEST(TextTokenizerAsciiFastPath, word_mode_goldens) {
  const std::vector<std::string> values = {
    "The Quick BROWN foxes are Running easily",
    "don't stop believing",
    "3.14 and 1,234 numbers",
    "connection connections connected",
    "a_b snake_case mixed",
    "  spaces\tand\r\nnewlines  ",
    "M.I.T. e.g. i.e.",
    "",
    "x",
    "!!!",
    "STOP the And of THE road",
  };
  for (const auto cc : {irs::Case::None, irs::Case::Lower, irs::Case::Upper}) {
    for (const bool stemming : {true, false}) {
      for (const bool accent : {true, false}) {
        TextOpts opts{.case_convert = cc,
                      .stopwords = {"the", "and", "stop"},
                      .accent = accent,
                      .stemming = stemming};
        for (const auto& v : values) {
          SCOPED_TRACE(testing::Message()
                       << "case=" << int(cc) << " stem=" << stemming
                       << " accent=" << accent << " value=\"" << v << "\"");
          AssertTextAsciiMatchesUnicode(opts, v);
        }
      }
    }
  }
}

TEST(TextTokenizerAsciiFastPath, ngram_mode_matches) {
  const std::vector<std::string> values = {"quick brown foxes", "don't", "ab",
                                           "abcdefgh 123456", ""};
  for (const auto& [mn, mx, preserve] :
       std::vector<std::tuple<size_t, size_t, bool>>{
         {2, 3, false}, {2, 3, true}, {1, kNoMax, false}, {3, 3, true}}) {
    TextOpts opts{.stopwords = {"the"},
                  .edge = true,
                  .min_gram = mn,
                  .max_gram = mx,
                  .preserve_original = preserve};
    for (const auto& v : values) {
      SCOPED_TRACE(testing::Message()
                   << "min=" << mn << " max=" << mx << " preserve=" << preserve
                   << " value=\"" << v << "\"");
      AssertTextAsciiMatchesUnicode(opts, v);
    }
  }
}

TEST(TextTokenizerAsciiFastPath, property_oracle_random_ascii) {
  constexpr std::string_view kCharset =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    "'.,:;_\"!?-()[] \t\r\n"
    "the running foxes  ..''";
  uint64_t seed = 0x7e47;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (const bool stemming : {true, false}) {
    for (const bool ngram : {false, true}) {
      TextOpts opts{.stopwords = {"the", "a"}, .stemming = stemming};
      if (ngram) {
        opts.edge = true;
        opts.min_gram = 2;
        opts.max_gram = 4;
      }
      for (size_t iter = 0; iter < 200; ++iter) {
        std::string v;
        const size_t len = next() % 120;
        for (size_t i = 0; i < len; ++i) {
          v += kCharset[next() % kCharset.size()];
        }
        SCOPED_TRACE(testing::Message()
                     << "stem=" << stemming << " ngram=" << ngram
                     << " iter=" << iter << " value=\"" << v << "\"");
        AssertTextAsciiMatchesUnicode(opts, v);
      }
    }
  }
}

TEST(TextTokenizerAsciiFastPath, non_ascii_and_turkish_stay_unicode) {
  TextOpts opts{};
  AssertTextAsciiMatchesUnicode(opts, "caf\xc3\xa9 running dogs");
  TextOpts tr{.locale = "tr_TR.UTF-8", .stemming = false};
  const auto tokens = TextAnalyze(tr, "III");
  ASSERT_EQ(1, tokens.size());
  ASSERT_EQ("\xc4\xb1\xc4\xb1\xc4\xb1", tokens[0].term);
}
