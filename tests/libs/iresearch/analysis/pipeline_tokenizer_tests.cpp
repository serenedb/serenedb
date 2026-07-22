////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/collation_tokenizer.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/pipeline_tokenizer.hpp"
#include "iresearch/analysis/solr_synonyms_tokenizer.hpp"
#include "iresearch/analysis/stemming_tokenizer.hpp"
#include "iresearch/analysis/stopwords_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/analysis/wordnet_synonyms_tokenizer.hpp"
#include "pg/sql_exception.h"
#include "tests_config.hpp"
#include "token_sink_utils.hpp"

namespace {

irs::analysis::Tokenizer::ptr MakeDelimiter(std::string_view delim) {
  return irs::analysis::DelimitedTokenizer::Make(
    irs::analysis::DelimitedTokenizer::Options{.delimiter =
                                                 std::string(delim)});
}

irs::analysis::Tokenizer::ptr MakeCollation(std::string_view locale) {
  return irs::analysis::CollationTokenizer::Make(
    irs::analysis::CollationTokenizer::Options{
      .locale = icu::Locale::createFromName(std::string(locale).c_str()),
    });
}

irs::analysis::Tokenizer::ptr MakeNgram(size_t min_gram, size_t max_gram,
                                        bool preserve_original) {
  return irs::analysis::NGramTokenizerBase::Make(
    irs::analysis::NGramTokenizerBase::Options{
      .min_gram = min_gram,
      .max_gram = max_gram,
      .preserve_original = preserve_original,
    });
}

irs::analysis::Tokenizer::ptr MakeNorm(std::string_view locale,
                                       irs::Case case_convert) {
  return irs::analysis::NormalizingTokenizer::Make(
    irs::analysis::NormalizingTokenizer::Options{
      .locale = icu::Locale::createFromName(std::string(locale).c_str()),
      .case_convert = case_convert,
    });
}

irs::analysis::Tokenizer::ptr MakeText(std::string_view locale,
                                       irs::Case case_convert, bool stemming,
                                       std::vector<std::string> stopwords = {},
                                       bool accent = true) {
  irs::analysis::TextTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName(std::string(locale).c_str());
  opts.case_convert = case_convert;
  opts.stemming = stemming;
  opts.accent = accent;
  for (auto& w : stopwords) {
    opts.explicit_stopwords.insert(std::move(w));
  }
  opts.explicit_stopwords_set = true;
  return irs::analysis::TextTokenizer::Make(std::move(opts));
}

class PipelineTestAnalyzer
  : public irs::analysis::TypedTokenizer<PipelineTestAnalyzer>,
    private irs::util::Noncopyable {
 public:
  PipelineTestAnalyzer(bool has_offset, irs::bytes_view /*payload*/)
    : _has_offset{has_offset} {}

  irs::TokenTraits Traits() const noexcept final {
    return {.dense_pos = false, .offsets = _has_offset};
  }

  template<irs::TokenLayout Layout>
  bool DoFill(std::string_view value, irs::TokenEmitter& sink) {
    sink.EmitInterned<Layout>(irs::ViewCast<irs::byte_type>(value), 1, 0,
                              static_cast<uint32_t>(value.size()));
    return true;
  }

 private:
  bool _has_offset;
};

class PipelineTestAnalyzer2
  : public irs::analysis::TypedTokenizer<PipelineTestAnalyzer2>,
    private irs::util::Noncopyable {
 public:
  PipelineTestAnalyzer2(std::vector<std::pair<uint32_t, uint32_t>>&& offsets,
                        std::vector<uint32_t>&& increments,
                        std::vector<bool>&& nexts, std::vector<bool>&& resets,
                        std::vector<irs::bytes_view>&& terms)
    : _offsets(offsets),
      _increments(increments),
      _nexts(nexts),
      _resets(resets),
      _terms(terms) {
    _current_offset = _offsets.begin();
    _current_increment = _increments.begin();
    _current_next = _nexts.begin();
    _current_reset = _resets.begin();
    _current_term = _terms.begin();
  }

  irs::TokenTraits Traits() const noexcept final {
    return {.dense_pos = false};
  }

  template<irs::TokenLayout Layout>
  bool DoFill(std::string_view /*value*/, irs::TokenEmitter& sink) {
    if (_current_reset == _resets.end() || !*(_current_reset++)) {
      return false;
    }
    uint32_t pos = 0;
    while (_current_next != _nexts.end() && *(_current_next++)) {
      uint32_t start = 0;
      uint32_t end = 0;
      if (_current_offset != _offsets.end()) {
        std::tie(start, end) = *(_current_offset++);
      }
      uint32_t inc = 0;
      if (_current_increment != _increments.end()) {
        inc = *(_current_increment++);
      }
      irs::bytes_view term;
      if (_current_term != _terms.end()) {
        term = *(_current_term++);
      }
      pos += inc;
      sink.EmitInterned<Layout>(term, pos, start, end);
    }
    return true;
  }

 private:
  std::vector<std::pair<uint32_t, uint32_t>> _offsets;
  std::vector<std::pair<uint32_t, uint32_t>>::const_iterator _current_offset;
  std::vector<uint32_t> _increments;
  std::vector<uint32_t>::const_iterator _current_increment;
  std::vector<bool> _nexts;
  std::vector<bool>::const_iterator _current_next;
  std::vector<bool> _resets;
  std::vector<bool>::const_iterator _current_reset;
  std::vector<irs::bytes_view> _terms;
  std::vector<irs::bytes_view>::const_iterator _current_term;
};

struct AnalyzerToken {
  std::string_view value;
  size_t start;
  size_t end;
  uint32_t pos;
};

using AnalyzerTokens = std::vector<AnalyzerToken>;

void AssertPipeline(irs::analysis::Tokenizer* pipe, const std::string& data,
                    const AnalyzerTokens& expected_tokens) {
  SCOPED_TRACE(data);
  ASSERT_TRUE(pipe->Traits().offsets);
  const auto tokens = tests::Analyze(*pipe, data);
  ASSERT_TRUE(tokens.has_value());
  auto expected_token = expected_tokens.begin();
  for (const auto& tok : *tokens) {
    SCOPED_TRACE(testing::Message("Term:") << tok.term);
    ASSERT_NE(expected_token, expected_tokens.end());
    ASSERT_EQ(expected_token->value, tok.term);
    ASSERT_EQ(expected_token->start, tok.offs_start);
    ASSERT_EQ(expected_token->end, tok.offs_end);
    ASSERT_EQ(expected_token->pos + 1, tok.pos);
    ++expected_token;
  }
  ASSERT_EQ(expected_token, expected_tokens.end());
}

void AssertPipelineMembers(
  irs::analysis::PipelineTokenizer& pipe,
  const std::vector<irs::TypeInfo::type_id>& expected) {
  size_t i{0};
  auto visitor = [&expected, &i](const irs::analysis::Tokenizer& a) {
    EXPECT_LT(i, expected.size());
    if (i >= expected.size()) {
      return false;  // save ourselves from crash
    }
    EXPECT_EQ(a.type(), expected[i++]);
    return true;
  };
  ASSERT_TRUE(pipe.visit_members(visitor));
  ASSERT_EQ(i, expected.size());
}

}  // namespace

TEST(pipeline_token_stream_test, consts) {
  static_assert("pipeline" ==
                irs::Type<irs::analysis::PipelineTokenizer>::name());
}

TEST(pipeline_token_stream_test, empty_pipeline) {
  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));

  std::string data = "quick broWn,, FOX  jumps,  over lazy dog";
  ASSERT_FALSE(tests::Analyze(pipe, data).has_value());
}

TEST(pipeline_token_stream_test, incompatible_types_rejected) {
  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  pipeline_options.emplace_back(MakeCollation("en_US.UTF-8"));
  pipeline_options.emplace_back(MakeDelimiter(" "));

  ASSERT_THROW(
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options)),
    sdb::SqlException);
}

TEST(pipeline_token_stream_test, many_tokenizers) {
  auto delimiter = MakeDelimiter(",");
  auto delimiter2 = MakeDelimiter(" ");
  auto text = MakeText("en_US.UTF-8", irs::Case::None, /*stemming=*/false);
  auto ngram = MakeNgram(2, 2, /*preserve_original=*/true);

  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  pipeline_options.emplace_back(std::move(delimiter));
  pipeline_options.emplace_back(std::move(delimiter2));
  pipeline_options.emplace_back(std::move(text));
  pipeline_options.emplace_back(std::move(ngram));

  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
  ASSERT_EQ(irs::Type<irs::analysis::PipelineTokenizer>::id(), pipe.type());

  std::string data = "quick broWn,, FOX  jumps,  over lazy dog";
  const AnalyzerTokens expected{
    {"qu", 0, 2, 0},     {"quick", 0, 5, 0},   {"ui", 1, 3, 1},
    {"ic", 2, 4, 2},     {"ck", 3, 5, 3},      {"br", 6, 8, 4},
    {"broWn", 6, 11, 4}, {"ro", 7, 9, 5},      {"oW", 8, 10, 6},
    {"Wn", 9, 11, 7},    {"FO", 14, 16, 8},    {"FOX", 14, 17, 8},
    {"OX", 15, 17, 9},   {"ju", 19, 21, 10},   {"jumps", 19, 24, 10},
    {"um", 20, 22, 11},  {"mp", 21, 23, 12},   {"ps", 22, 24, 13},
    {"ov", 27, 29, 14},  {"over", 27, 31, 14}, {"ve", 28, 30, 15},
    {"er", 29, 31, 16},  {"la", 32, 34, 17},   {"lazy", 32, 36, 17},
    {"az", 33, 35, 18},  {"zy", 34, 36, 19},   {"do", 37, 39, 20},
    {"dog", 37, 40, 20}, {"og", 38, 40, 21},
  };
  AssertPipeline(&pipe, data, expected);
}

TEST(pipeline_token_stream_test, overlapping_ngrams) {
  auto ngram = MakeNgram(6, 7, /*preserve_original=*/false);
  auto ngram2 = MakeNgram(2, 3, /*preserve_original=*/false);

  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  pipeline_options.emplace_back(std::move(ngram));
  pipeline_options.emplace_back(std::move(ngram2));
  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));

  std::string data = "ABCDEFJH";
  const AnalyzerTokens expected{
    {"AB", 0, 2, 0},   {"ABC", 0, 3, 0},  {"BC", 1, 3, 1},   {"BCD", 1, 4, 1},
    {"CD", 2, 4, 2},   {"CDE", 2, 5, 2},  {"DE", 3, 5, 3},   {"DEF", 3, 6, 3},
    {"EF", 4, 6, 4},   {"AB", 0, 2, 5},   {"ABC", 0, 3, 5},  {"BC", 1, 3, 6},
    {"BCD", 1, 4, 6},  {"CD", 2, 4, 7},   {"CDE", 2, 5, 7},  {"DE", 3, 5, 8},
    {"DEF", 3, 6, 8},  {"EF", 4, 6, 9},   {"EFJ", 4, 7, 9},  {"FJ", 5, 7, 10},
    {"BC", 1, 3, 11},  {"BCD", 1, 4, 11}, {"CD", 2, 4, 12},  {"CDE", 2, 5, 12},
    {"DE", 3, 5, 13},  {"DEF", 3, 6, 13}, {"EF", 4, 6, 14},  {"EFJ", 4, 7, 14},
    {"FJ", 5, 7, 15},  {"BC", 1, 3, 16},  {"BCD", 1, 4, 16}, {"CD", 2, 4, 17},
    {"CDE", 2, 5, 17}, {"DE", 3, 5, 18},  {"DEF", 3, 6, 18}, {"EF", 4, 6, 19},
    {"EFJ", 4, 7, 19}, {"FJ", 5, 7, 20},  {"FJH", 5, 8, 20}, {"JH", 6, 8, 21},
    {"CD", 2, 4, 22},  {"CDE", 2, 5, 22}, {"DE", 3, 5, 23},  {"DEF", 3, 6, 23},
    {"EF", 4, 6, 24},  {"EFJ", 4, 7, 24}, {"FJ", 5, 7, 25},  {"FJH", 5, 8, 25},
    {"JH", 6, 8, 26},
  };
  AssertPipeline(&pipe, data, expected);
}

TEST(pipeline_token_stream_test, case_ngrams) {
  std::string data = "QuIck BroWN FoX";
  const AnalyzerTokens expected{
    {"QUI", 0, 3, 0},    {"UIC", 1, 4, 1},    {"ICK", 2, 5, 2},
    {"CK ", 3, 6, 3},    {"K B", 4, 7, 4},    {" BR", 5, 8, 5},
    {"BRO", 6, 9, 6},    {"ROW", 7, 10, 7},   {"OWN", 8, 11, 8},
    {"WN ", 9, 12, 9},   {"N F", 10, 13, 10}, {" FO", 11, 14, 11},
    {"FOX", 12, 15, 12},
  };
  {
    auto ngram = MakeNgram(3, 3, /*preserve_original=*/false);
    auto norm = MakeNorm("en", irs::Case::Upper);
    std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
    pipeline_options.emplace_back(std::move(ngram));
    pipeline_options.emplace_back(std::move(norm));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
  {
    auto ngram = MakeNgram(3, 3, /*preserve_original=*/false);
    auto norm = MakeNorm("en", irs::Case::Upper);
    std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
    pipeline_options.emplace_back(std::move(norm));
    pipeline_options.emplace_back(std::move(ngram));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
}

TEST(pipeline_token_stream_test, no_tokenizers) {
  std::string data = "QuIck";
  auto norm1 = MakeNorm("en", irs::Case::Upper);
  auto norm2 = MakeNorm("en", irs::Case::Lower);
  const AnalyzerTokens expected{
    {"quick", 0, 5, 0},
  };
  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  pipeline_options.emplace_back(std::move(norm1));
  pipeline_options.emplace_back(std::move(norm2));
  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
  AssertPipeline(&pipe, data, expected);
}

TEST(pipeline_token_stream_test, source_modification_tokenizer) {
  std::string data = "QuIck broWn fox jumps";
  const AnalyzerTokens expected{{"quick", 0, 5, 0},
                                {"brown", 6, 11, 1},
                                {"fox", 12, 15, 2},
                                {"jump", 16, 21, 3}};
  {
    auto text = MakeText("en_US.UTF-8", irs::Case::None, /*stemming=*/true);
    auto norm = MakeNorm("en", irs::Case::Lower);
    std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
    pipeline_options.emplace_back(std::move(text));
    pipeline_options.emplace_back(std::move(norm));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
  {
    auto text = MakeText("en_US.UTF-8", irs::Case::None, /*stemming=*/true);
    auto norm = MakeNorm("en", irs::Case::Lower);
    std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
    pipeline_options.emplace_back(std::move(norm));
    pipeline_options.emplace_back(std::move(text));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
}

TEST(pipeline_token_stream_test, signle_tokenizer) {
  auto text = MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/true);
  std::string data = "QuIck broWn fox jumps";
  const AnalyzerTokens expected{{"quick", 0, 5, 0},
                                {"brown", 6, 11, 1},
                                {"fox", 12, 15, 2},
                                {"jump", 16, 21, 3}};
  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  pipeline_options.emplace_back(std::move(text));
  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
  AssertPipeline(&pipe, data, expected);
}

TEST(pipeline_token_stream_test, signle_non_tokenizer) {
  auto norm = MakeNorm("en", irs::Case::Lower);
  std::string data = "QuIck";
  const AnalyzerTokens expected{{"quick", 0, 5, 0}};
  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  pipeline_options.emplace_back(std::move(norm));
  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
  AssertPipeline(&pipe, data, expected);
}

TEST(pipeline_token_stream_test, hold_position_tokenizer) {
  std::string data = "QuIck";
  const AnalyzerTokens expected{
    {"qu", 0, 2, 0},  {"qui", 0, 3, 0}, {"quick", 0, 5, 0}, {"ui", 1, 3, 1},
    {"uic", 1, 4, 1}, {"ic", 2, 4, 2},  {"ick", 2, 5, 2},   {"ck", 3, 5, 3},
  };
  {
    auto ngram = MakeNgram(2, 3, /*preserve_original=*/true);
    auto norm = MakeNorm("en", irs::Case::Lower);
    std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
    pipeline_options.emplace_back(std::move(ngram));
    pipeline_options.emplace_back(std::move(norm));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
  {
    auto ngram = MakeNgram(2, 3, /*preserve_original=*/true);
    auto norm = MakeNorm("en", irs::Case::Lower);
    std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
    pipeline_options.emplace_back(std::move(norm));
    pipeline_options.emplace_back(std::move(ngram));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
}

TEST(pipeline_token_stream_test, hold_position_tokenizer2) {
  std::string data = "A";
  irs::bytes_view term = irs::ViewCast<irs::byte_type>(std::string_view(data));
  irs::analysis::Tokenizer::ptr tokenizer1;
  {
    std::vector<std::pair<uint32_t, uint32_t>> offsets{{0, 5}, {0, 5}};
    std::vector<uint32_t> increments{1, 0};
    std::vector<bool> nexts{true, true};
    std::vector<bool> resets{true};
    std::vector<irs::bytes_view> terms{term};
    tokenizer1.reset(new PipelineTestAnalyzer2(
      std::move(offsets), std::move(increments), std::move(nexts),
      std::move(resets), std::move(terms)));
  }
  irs::analysis::Tokenizer::ptr tokenizer2;
  {
    std::vector<std::pair<uint32_t, uint32_t>> offsets{
      {0, 5}, {1, 5}, {2, 5}, {2, 5}};
    std::vector<uint32_t> increments{1, 1, 1, 0};
    std::vector<bool> nexts{true, true, false, true, true};
    std::vector<bool> resets{true, true};
    std::vector<irs::bytes_view> terms{term};
    tokenizer2.reset(new PipelineTestAnalyzer2(
      std::move(offsets), std::move(increments), std::move(nexts),
      std::move(resets), std::move(terms)));
  }
  irs::analysis::Tokenizer::ptr tokenizer3;
  {
    std::vector<std::pair<uint32_t, uint32_t>> offsets{{0, 1}, {0, 1}};
    std::vector<uint32_t> increments{1, 1};
    std::vector<bool> nexts{true, false, false, false, true};
    std::vector<bool> resets{true, true, true, true};
    std::vector<irs::bytes_view> terms{term, term};
    tokenizer3.reset(new PipelineTestAnalyzer2(
      std::move(offsets), std::move(increments), std::move(nexts),
      std::move(resets), std::move(terms)));
  }

  const AnalyzerTokens expected{{data, 0, 5, 0}, {data, 2, 3, 1}};
  {
    std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
    pipeline_options.emplace_back(std::move(tokenizer1));
    pipeline_options.emplace_back(std::move(tokenizer2));
    pipeline_options.emplace_back(std::move(tokenizer3));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
}

TEST(pipeline_token_stream_test, test_construct) {
  irs::analysis::PipelineTokenizer::Options opts;
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{
      irs::analysis::DelimitedTokenizer::Options{.delimiter = "A"}}));

  irs::analysis::TextTokenizer::Options text_opts;
  text_opts.locale = icu::Locale::createFromName("en_US.UTF-8");
  text_opts.case_convert = irs::Case::Lower;
  text_opts.accent = false;
  text_opts.stemming = true;
  text_opts.explicit_stopwords.insert("fox");
  text_opts.explicit_stopwords_set = true;
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{std::move(text_opts)}));

  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{irs::analysis::NormalizingTokenizer::Options{
      .locale = icu::Locale::createFromName("en_US.UTF-8"),
      .case_convert = irs::Case::Upper,
    }}));

  auto stream = irs::analysis::PipelineTokenizer::Make(std::move(opts));
  ASSERT_NE(nullptr, stream);
  const AnalyzerTokens expected{
    {"QUICK", 0, 5, 0}, {"BROWN", 6, 11, 1}, {"JUMP", 16, 21, 2}};
  AssertPipeline(stream.get(), "QuickABrownAFOXAjUmps", expected);
}

TEST(pipeline_token_stream_test, empty_pipeline_construct) {
  ASSERT_ANY_THROW(irs::analysis::PipelineTokenizer::Make(
    irs::analysis::PipelineTokenizer::Options{}));
}

// The legacy parser-level rejections (`test_construct_invalid_json`,
// `test_construct_not_object_json`, `test_construct_no_pipeline`,
// `test_construct_not_array_pipeline`, `test_construct_not_pipeline_objects`,
// `test_construct_no_type`, `test_construct_non_string_type`,
// `test_construct_no_properties`) all exercised JSON-parser bookkeeping
// that has no direct analogue against the strongly-typed Options API.
// They collapse to the `empty_pipeline_construct` and
// `test_construct_invalid_child` assertions below.

TEST(pipeline_token_stream_test, test_construct_invalid_child) {
  // Ported from the legacy `test_construct_invalid_analyzer` test, which
  // fed an UNKNOWN type into the JSON parser. With the typed Options
  // API the equivalent is a child whose `Make` returns nullptr -- e.g.
  // a `PatternTokenizer` with an empty pattern.
  irs::analysis::PipelineTokenizer::Options opts;
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{
      irs::analysis::PatternTokenizer::Options{.pattern = ""}}));

  ASSERT_ANY_THROW(irs::analysis::PipelineTokenizer::Make(std::move(opts)));
}

TEST(pipeline_token_stream_test, analyzers_with_payload_offset) {
  irs::byte_type p1[] = {0x1, 0x2, 0x3};
  irs::byte_type p2[] = {0x11, 0x22, 0x33};

  const auto assert_pipe =
    [](bool first_offset, irs::bytes_view first_payload, bool second_offset,
       irs::bytes_view second_payload, bool expected_offsets) {
      std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
      pipeline_options.emplace_back(
        std::make_unique<PipelineTestAnalyzer>(first_offset, first_payload));
      pipeline_options.emplace_back(
        std::make_unique<PipelineTestAnalyzer>(second_offset, second_payload));
      irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
      ASSERT_EQ(expected_offsets, pipe.Traits().offsets);
      const auto tokens =
        tests::Analyze(pipe, "A",
                       expected_offsets ? irs::TokenLayout::TermsPosOffs
                                        : irs::TokenLayout::TermsPos);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(1U, tokens->size());
      ASSERT_EQ("A", (*tokens)[0].term);
      ASSERT_EQ(1U, (*tokens)[0].pos);
      if (expected_offsets) {
        ASSERT_EQ(0U, (*tokens)[0].offs_start);
        ASSERT_EQ(1U, (*tokens)[0].offs_end);
      }
    };

  assert_pipe(true, irs::bytes_view{p1, std::size(p1)}, true, {}, true);
  assert_pipe(true, {}, true, irs::bytes_view{p1, std::size(p1)}, true);
  assert_pipe(true, irs::bytes_view{p1, std::size(p1)}, false,
              irs::bytes_view{p2, std::size(p2)}, false);
  assert_pipe(false, irs::bytes_view{p2, std::size(p2)}, true,
              irs::bytes_view{p1, std::size(p1)}, false);
  assert_pipe(false, irs::bytes_view{p2, std::size(p2)}, false, {}, false);
}

TEST(pipeline_token_stream_test, members_visitor) {
  auto delimiter = MakeDelimiter(",");
  auto text = MakeText("en_US.UTF-8", irs::Case::None, /*stemming=*/false);
  auto ngram = MakeNgram(2, 2, /*preserve_original=*/true);
  auto norm = MakeNorm("en", irs::Case::Upper);

  std::vector<irs::TypeInfo::type_id> expected{delimiter->type(), norm->type()};
  std::vector<irs::TypeInfo::type_id> expected_nested{
    delimiter->type(), norm->type(), text->type(), ngram->type()};
  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  pipeline_options.emplace_back(std::move(delimiter));
  pipeline_options.emplace_back(std::move(norm));
  auto pipe = std::make_unique<irs::analysis::PipelineTokenizer>(
    std::move(pipeline_options));
  AssertPipelineMembers(*pipe, expected);

  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options2;
  pipeline_options2.emplace_back(std::move(text));

  auto pipe2 = std::make_unique<irs::analysis::PipelineTokenizer>(
    std::move(pipeline_options2));

  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options3;
  pipeline_options3.emplace_back(std::move(pipe));
  pipeline_options3.emplace_back(std::move(pipe2));
  pipeline_options3.emplace_back(std::move(ngram));
  irs::analysis::PipelineTokenizer pipe3(std::move(pipeline_options3));
  AssertPipelineMembers(pipe3, expected_nested);
}

namespace {

std::vector<irs::analysis::Tokenizer::ptr> MakePipeSubs() {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(MakeDelimiter(" "));
  subs.push_back(MakeNgram(2, 3, /*preserve_original=*/true));
  return subs;
}

struct PulledTok {
  std::string term;
  uint32_t pos;
  uint32_t offs_start;
  uint32_t offs_end;
};

std::vector<PulledTok> SingleFill(irs::analysis::Tokenizer& stream,
                                  std::string_view data) {
  std::vector<PulledTok> out;
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

TEST(pipeline_token_stream_test, native_fill_matches_pull) {
  auto pull_stream =
    std::make_unique<irs::analysis::PipelineTokenizer>(MakePipeSubs());
  auto fill_stream =
    std::make_unique<irs::analysis::PipelineTokenizer>(MakePipeSubs());

  const std::vector<std::string> values = {"quick brown", "", "a", "the lazy"};
  for (const auto& v : values) {
    SCOPED_TRACE(v);
    const auto pulled = SingleFill(*pull_stream, v);

    std::vector<PulledTok> filled;
    const auto collect = [&](irs::TokenBatch& batch,
                             std::span<const irs::DocRun> /*runs*/) {
      for (uint32_t i = 0; i < batch.count; ++i) {
        const auto& t = batch.terms[i];
        filled.push_back({std::string{t.GetData(), t.GetSize()}, batch.pos[i],
                          batch.offs_start[i], batch.offs_end[i]});
      }
    };
    tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
    fill_stream->Fill(v, sink.writer, sink.layout);
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
}

TEST(pipeline_token_stream_test, column_fill_matches_pull) {
  auto pull_stream =
    std::make_unique<irs::analysis::PipelineTokenizer>(MakePipeSubs());
  auto fill_stream =
    std::make_unique<irs::analysis::PipelineTokenizer>(MakePipeSubs());

  const std::vector<std::string> raw = {"quick brown fox", "", "a b c",
                                        "the lazy dog"};
  std::vector<duckdb::string_t> values;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < raw.size(); ++i) {
    values.emplace_back(raw[i].data(), static_cast<uint32_t>(raw[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(100 + i));
  }

  tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
  fill_stream->Fill(values, docs, sink.writer, sink.layout);
  ASSERT_FALSE(sink.flushed());
  auto& batch = sink.writer.buf;
  const auto runs = sink.writer.Runs();

  ASSERT_EQ(raw.size(), runs.size());
  uint32_t token_idx = 0;
  for (size_t v = 0; v < raw.size(); ++v) {
    SCOPED_TRACE(raw[v]);
    const auto pulled = SingleFill(*pull_stream, raw[v]);
    ASSERT_EQ(docs[v], runs[v].doc);
    ASSERT_EQ(pulled.size(), runs[v].ntokens);
    for (const auto& expected : pulled) {
      const auto& t = batch.terms[token_idx];
      ASSERT_EQ(expected.term, (std::string{t.GetData(), t.GetSize()}));
      ASSERT_EQ(expected.pos, batch.pos[token_idx]);
      ASSERT_EQ(expected.offs_start, batch.offs_start[token_idx]);
      ASSERT_EQ(expected.offs_end, batch.offs_end[token_idx]);
      ++token_idx;
    }
  }
  ASSERT_EQ(batch.count, token_idx);
}

namespace {

irs::analysis::Tokenizer::ptr MakeStopwords(
  std::initializer_list<std::string_view> words) {
  irs::analysis::StopwordsTokenizer::stopwords_set mask;
  for (const auto w : words) {
    mask.emplace(w);
  }
  return irs::analysis::StopwordsTokenizer::Make({.mask = std::move(mask)});
}

std::unique_ptr<irs::analysis::PipelineTokenizer> MakeDropFilterPipe(
  bool force_generic, bool two_filters = false) {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(MakeDelimiter(","));
  if (two_filters) {
    subs.push_back(MakeStopwords({"the"}));
    subs.push_back(MakeStopwords({"and"}));
  } else {
    subs.push_back(MakeStopwords({"the", "and"}));
  }
  auto pipe =
    std::make_unique<irs::analysis::PipelineTokenizer>(std::move(subs));
  pipe->ForceGenericPath(force_generic);
  return pipe;
}

const std::vector<std::string>& DropFilterValues() {
  static const std::vector<std::string> values = {
    "the,quick,the,brown,fox,the",
    "quick,brown",
    "the,and,the",
    "the",
    "",
    "a,,b",
    "and,quick",
    "quick,and",
    "supercalifragilisticexpialidocious,the,anotherverylongtokenvalue",
  };
  return values;
}

std::string LongDropFilterValue() {
  std::string value;
  for (size_t i = 0; i < 3000; ++i) {
    if (i % 3 == 0) {
      value += "the,";
    } else {
      value += "w" + std::to_string(i) + ",";
    }
  }
  value += "tail";
  return value;
}

struct ChainTok {
  std::string term;
  uint32_t pos;
  uint32_t offs_start;
  uint32_t offs_end;

  bool operator==(const ChainTok&) const = default;
};

struct ChainCollected {
  std::vector<ChainTok> tokens;
  std::vector<std::pair<uint32_t, uint32_t>> runs;

  bool operator==(const ChainCollected&) const = default;
};

ChainCollected CollectColumn(irs::analysis::PipelineTokenizer& pipe,
                             std::span<const duckdb::string_t> values,
                             std::span<const irs::doc_id_t> docs) {
  ChainCollected out;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    EXPECT_FALSE(pipe.Traits().dense_pos);
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      out.tokens.push_back({std::string{t.GetData(), t.GetSize()}, batch.pos[i],
                            batch.offs_start[i], batch.offs_end[i]});
    }
    for (const auto& r : runs) {
      out.runs.emplace_back(r.doc, r.ntokens);
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
  pipe.Fill(values, docs, sink.writer, sink.layout);
  sink.writer.Finish();
  return out;
}

}  // namespace

TEST(pipeline_token_stream_test, drop_filter_fast_path_eligibility) {
  {
    std::vector<irs::analysis::Tokenizer::ptr> subs;
    subs.push_back(MakeDelimiter(","));
    subs.push_back(MakeStopwords({"the"}));
    irs::analysis::PipelineTokenizer pipe{std::move(subs)};
    ASSERT_TRUE(pipe.FastPathEligible());
  }
  {
    std::vector<irs::analysis::Tokenizer::ptr> subs;
    subs.push_back(MakeDelimiter(","));
    subs.push_back(MakeStopwords({"the"}));
    subs.push_back(MakeStopwords({"and"}));
    irs::analysis::PipelineTokenizer pipe{std::move(subs)};
    ASSERT_TRUE(pipe.FastPathEligible());
  }
  {
    std::vector<irs::analysis::Tokenizer::ptr> subs;
    subs.push_back(MakeDelimiter(","));
    subs.push_back(MakeNgram(2, 3, false));
    irs::analysis::PipelineTokenizer pipe{std::move(subs)};
    ASSERT_FALSE(pipe.FastPathEligible());
  }
  {
    std::vector<irs::analysis::Tokenizer::ptr> subs;
    subs.push_back(MakeStopwords({"the"}));
    irs::analysis::PipelineTokenizer pipe{std::move(subs)};
    ASSERT_FALSE(pipe.FastPathEligible());
  }
  {
    std::vector<irs::analysis::Tokenizer::ptr> subs;
    subs.push_back(MakeDelimiter(","));
    subs.push_back(MakeStopwords({"the"}));
    subs.push_back(MakeNgram(2, 3, false));
    irs::analysis::PipelineTokenizer pipe{std::move(subs)};
    ASSERT_FALSE(pipe.FastPathEligible());
  }
}

TEST(pipeline_token_stream_test, drop_filter_fast_matches_generic) {
  for (const bool two_filters : {false, true}) {
    auto fast = MakeDropFilterPipe(false, two_filters);
    auto generic = MakeDropFilterPipe(true, two_filters);
    ASSERT_TRUE(fast->FastPathEligible());
    for (const auto& v : DropFilterValues()) {
      SCOPED_TRACE(v);
      const auto expected = tests::Analyze(*generic, v);
      const auto actual = tests::Analyze(*fast, v);
      ASSERT_EQ(expected.has_value(), actual.has_value());
      if (expected) {
        ASSERT_EQ(*expected, *actual);
      }
      const auto expected_terms = tests::AnalyzeTerms(*generic, v);
      const auto actual_terms = tests::AnalyzeTerms(*fast, v);
      ASSERT_EQ(expected_terms.has_value(), actual_terms.has_value());
      if (expected_terms) {
        ASSERT_EQ(*expected_terms, *actual_terms);
      }
    }
  }
}

TEST(pipeline_token_stream_test, drop_filter_fast_matches_generic_long_value) {
  auto fast = MakeDropFilterPipe(false);
  auto generic = MakeDropFilterPipe(true);
  const auto value = LongDropFilterValue();
  const auto expected = tests::Analyze(*generic, value);
  const auto actual = tests::Analyze(*fast, value);
  ASSERT_TRUE(expected.has_value());
  ASSERT_TRUE(actual.has_value());
  ASSERT_GT(expected->size(), irs::TokenBatch::kCapacity);
  ASSERT_EQ(*expected, *actual);
}

namespace {

irs::analysis::Tokenizer::ptr MakeSolrSyn() {
  return irs::analysis::SolrSynonymsTokenizer::Make(
    {.synonyms_text = "quick, fast, speedy\nbig => large\n"});
}

irs::analysis::Tokenizer::ptr MakeWordnetSyn() {
  return irs::analysis::WordnetSynonymsTokenizer::Make(
    {.synonyms_text = "s(100,1,'quick',a,1,0).\n"
                      "s(300,2,'quick',a,1,0).\n"
                      "s(200,1,'brown',a,1,0).\n"});
}

irs::analysis::Tokenizer::ptr MakeKeywordChild() {
  return irs::StringTokenizer::Make({});
}

using SubFactory = irs::analysis::Tokenizer::ptr (*)();

std::unique_ptr<irs::analysis::PipelineTokenizer> MakeChainPipe(
  std::initializer_list<SubFactory> factories, bool force_generic) {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  for (const auto f : factories) {
    subs.push_back(f());
  }
  auto pipe =
    std::make_unique<irs::analysis::PipelineTokenizer>(std::move(subs));
  pipe->ForceGenericPath(force_generic);
  return pipe;
}

irs::analysis::Tokenizer::ptr MakeCommaDelim() { return MakeDelimiter(","); }

irs::analysis::Tokenizer::ptr MakeChainStopwords() {
  return MakeStopwords({"the", "and"});
}

const std::vector<std::string>& ChainValues() {
  static const std::vector<std::string> values = {
    "quick,brown,big",
    "big,big",
    "quick",
    "",
    "zzz,yyy",
    "the,quick,and,big,the",
    "supercalifragilisticexpialidocious,quick",
    "a,,b,quick",
  };
  return values;
}

void AssertChainEquivalence(std::initializer_list<SubFactory> factories,
                            const std::vector<std::string>& values) {
  auto fast = MakeChainPipe(factories, false);
  auto generic = MakeChainPipe(factories, true);
  for (const auto& v : values) {
    SCOPED_TRACE(v);
    const auto expected = tests::Analyze(*generic, v);
    const auto actual = tests::Analyze(*fast, v);
    ASSERT_EQ(expected.has_value(), actual.has_value());
    if (expected) {
      ASSERT_EQ(*expected, *actual);
    }
    const auto expected_terms = tests::AnalyzeTerms(*generic, v);
    const auto actual_terms = tests::AnalyzeTerms(*fast, v);
    ASSERT_EQ(expected_terms.has_value(), actual_terms.has_value());
    if (expected_terms) {
      ASSERT_EQ(*expected_terms, *actual_terms);
    }
  }
}

}  // namespace

TEST(pipeline_token_stream_test, solr_synonyms_generic_semantics_pinned) {
  auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeSolrSyn}, true);
  const auto tokens = tests::Analyze(*pipe, "quick,brown,big");
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected = {
    {"fast", 1, 0, 5},   {"quick", 1, 0, 5},   {"speedy", 1, 0, 5},
    {"brown", 2, 6, 11}, {"large", 3, 12, 15},
  };
  ASSERT_EQ(expected, *tokens);
}

TEST(pipeline_token_stream_test, wordnet_synonyms_generic_semantics_pinned) {
  auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeWordnetSyn}, true);
  const auto tokens = tests::Analyze(*pipe, "quick,zzz,brown");
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected = {
    {"100", 1, 0, 5},
    {"300", 2, 0, 5},
    {"200", 3, 10, 15},
  };
  ASSERT_EQ(expected, *tokens);
}

TEST(pipeline_token_stream_test, keyword_child_generic_identity_pinned) {
  auto delim = MakeDelimiter(",");
  auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeKeywordChild}, true);
  for (const auto& v : ChainValues()) {
    SCOPED_TRACE(v);
    ASSERT_EQ(tests::Analyze(*delim, v), tests::Analyze(*pipe, v));
  }
}

namespace {

irs::analysis::Tokenizer::ptr MakeNormLowerEn() {
  return MakeNorm("en", irs::Case::Lower);
}

irs::analysis::Tokenizer::ptr MakeNormLowerTr() {
  return MakeNorm("tr", irs::Case::Lower);
}

irs::analysis::Tokenizer::ptr MakeStemEn() {
  irs::analysis::StemmingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  return irs::analysis::StemmingTokenizer::Make(std::move(opts));
}

irs::analysis::Tokenizer::ptr MakeNgram23() { return MakeNgram(2, 3, false); }

const std::vector<std::string>& RewriteValues() {
  static const std::vector<std::string> values = {
    "QUICK,Brown,BIG",
    "The,QUICK,and,Big",
    "running,JUMPS,the",
    "Gr\xc3\xbc\xc3\x9f"
    "e,QUICK",
    "",
    "a,,B",
    "supercalifragilisticexpialidocious,RUNNING",
    "The,and",
  };
  return values;
}

}  // namespace

TEST(pipeline_token_stream_test, rewriter_generic_semantics_pinned) {
  {
    auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeNormLowerEn}, true);
    const auto tokens = tests::Analyze(*pipe, "QUICK,Brown");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected = {
      {"quick", 1, 0, 5},
      {"brown", 2, 6, 11},
    };
    ASSERT_EQ(expected, *tokens);
  }
  {
    auto pipe = MakeChainPipe(
      {&MakeCommaDelim, &MakeNormLowerEn, &MakeChainStopwords}, true);
    const auto tokens = tests::Analyze(*pipe, "The,QUICK");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected = {
      {"quick", 1, 4, 9},
    };
    ASSERT_EQ(expected, *tokens);
  }
  {
    auto pipe = MakeChainPipe(
      {&MakeCommaDelim, &MakeChainStopwords, &MakeNormLowerEn}, true);
    const auto tokens = tests::Analyze(*pipe, "The,QUICK");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected = {
      {"the", 1, 0, 3},
      {"quick", 2, 4, 9},
    };
    ASSERT_EQ(expected, *tokens);
  }
  {
    auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeStemEn}, true);
    const auto tokens = tests::Analyze(*pipe, "running,jumps");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected = {
      {"run", 1, 0, 7},
      {"jump", 2, 8, 13},
    };
    ASSERT_EQ(expected, *tokens);
  }
}

TEST(pipeline_token_stream_test, rewriter_eligibility) {
  const auto eligible = [](std::initializer_list<SubFactory> factories) {
    return MakeChainPipe(factories, false)->FastPathEligible();
  };
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeNormLowerEn}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeStemEn}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeNormLowerEn, &MakeStemEn}));
  ASSERT_TRUE(
    eligible({&MakeCommaDelim, &MakeNormLowerEn, &MakeChainStopwords}));
  ASSERT_TRUE(
    eligible({&MakeCommaDelim, &MakeChainStopwords, &MakeNormLowerEn}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeNormLowerEn, &MakeSolrSyn}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeNormLowerEn, &MakeStemEn,
                        &MakeChainStopwords, &MakeWordnetSyn}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeKeywordChild, &MakeNormLowerEn}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeNormLowerTr}));
  ASSERT_FALSE(eligible({&MakeCommaDelim, &MakeSolrSyn, &MakeNormLowerEn}));
  ASSERT_FALSE(eligible({&MakeCommaDelim, &MakeWordnetSyn, &MakeStemEn}));
  ASSERT_FALSE(eligible({&MakeCommaDelim, &MakeNormLowerEn, &MakeNgram23}));
  ASSERT_FALSE(eligible({&MakeCommaDelim, &MakeNgram23, &MakeNormLowerEn}));
}

TEST(pipeline_token_stream_test, rewriter_fast_matches_generic) {
  AssertChainEquivalence({&MakeCommaDelim, &MakeNormLowerEn}, RewriteValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeStemEn}, RewriteValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeNormLowerEn, &MakeStemEn},
                         RewriteValues());
  AssertChainEquivalence(
    {&MakeCommaDelim, &MakeNormLowerEn, &MakeChainStopwords}, RewriteValues());
  AssertChainEquivalence(
    {&MakeCommaDelim, &MakeChainStopwords, &MakeNormLowerEn}, RewriteValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeNormLowerEn, &MakeStemEn,
                          &MakeChainStopwords, &MakeSolrSyn},
                         RewriteValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeNormLowerTr}, RewriteValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeNormLowerEn, &MakeKeywordChild},
                         RewriteValues());
}

TEST(pipeline_token_stream_test, expander_eligibility) {
  const auto eligible = [](std::initializer_list<SubFactory> factories) {
    return MakeChainPipe(factories, false)->FastPathEligible();
  };
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeSolrSyn}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeWordnetSyn}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeKeywordChild}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeChainStopwords, &MakeSolrSyn}));
  ASSERT_TRUE(eligible({&MakeCommaDelim, &MakeSolrSyn, &MakeKeywordChild}));
  ASSERT_FALSE(eligible({&MakeCommaDelim, &MakeSolrSyn, &MakeChainStopwords}));
  ASSERT_FALSE(eligible({&MakeCommaDelim, &MakeSolrSyn, &MakeWordnetSyn}));
  ASSERT_FALSE(eligible({&MakeCommaDelim, &MakeWordnetSyn, &MakeSolrSyn}));
}

TEST(pipeline_token_stream_test, expander_fast_matches_generic) {
  AssertChainEquivalence({&MakeCommaDelim, &MakeSolrSyn}, ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeWordnetSyn}, ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeKeywordChild}, ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeChainStopwords, &MakeSolrSyn},
                         ChainValues());
  AssertChainEquivalence(
    {&MakeCommaDelim, &MakeChainStopwords, &MakeWordnetSyn}, ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeSolrSyn, &MakeKeywordChild},
                         ChainValues());
}

TEST(pipeline_token_stream_test, drop_filter_fast_matches_generic_column) {
  auto fast = MakeDropFilterPipe(false);
  auto generic = MakeDropFilterPipe(true);
  const std::vector<std::string> raw = {
    "the,quick,the,brown",       "", "the,and", "fox,jumps",
    "and,over,the,lazy,dog,the",
  };
  std::vector<duckdb::string_t> values;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < raw.size(); ++i) {
    values.emplace_back(raw[i].data(), static_cast<uint32_t>(raw[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(10 + 3 * i));
  }
  const auto expected = CollectColumn(*generic, values, docs);
  const auto actual = CollectColumn(*fast, values, docs);
  ASSERT_FALSE(expected.tokens.empty());
  ASSERT_EQ(expected.runs.size(), raw.size());
  ASSERT_EQ(expected, actual);
}
