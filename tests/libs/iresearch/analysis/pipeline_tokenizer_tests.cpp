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

#include <cstring>
#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/collation_tokenizer.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/keyword_tokenizer.hpp"
#include "iresearch/analysis/multi_delimited_tokenizer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/pipeline_tokenizer.hpp"
#include "iresearch/analysis/process_tokens.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/analysis/solr_synonyms_tokenizer.hpp"
#include "iresearch/analysis/stemming_tokenizer.hpp"
#include "iresearch/analysis/stopwords_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/wordnet_synonyms_tokenizer.hpp"
#include "pg/sql_exception.h"
#include "pipeline_reference.hpp"
#include "test_resources.hpp"
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

irs::analysis::Tokenizer::ptr MakeNGram(size_t min_gram, size_t max_gram,
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
    opts.explicit_stopwords.push_back(std::move(w));
  }
  opts.explicit_stopwords_set = true;
  return irs::analysis::TextTokenizer::Make(std::move(opts), tests::Cache());
}

class PipelineTestAnalyzer
  : public irs::analysis::TypedTokenizer<PipelineTestAnalyzer>,
    private irs::util::Noncopyable {
 public:
  PipelineTestAnalyzer(bool has_offset, irs::bytes_view /*payload*/)
    : _has_offset{has_offset} {}

  irs::TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos = true,
      .offsets = _has_offset,
    };
  }

  template<irs::TokenLayout Layout>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view value{raw.GetData(), raw.GetSize()};
    tests::EmitCopy<Layout>(sink, irs::ViewCast<irs::byte_type>(value), 1,
                            irs::Offs{0, static_cast<uint32_t>(value.size())});
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
    return {
      .explicit_pos = true,
      .offsets = true,
    };
  }

  template<irs::TokenLayout Layout>
  bool DoFill(duckdb::string_t, irs::TokenSink& sink) {
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
      tests::EmitCopy<Layout>(sink, term, pos, irs::Offs{start, end});
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
  ASSERT_TRUE(pipe.VisitMembers(visitor));
  ASSERT_EQ(i, expected.size());
}

}  // namespace

TEST(pipeline_token_stream_test, consts) {
  static_assert("pipeline" ==
                irs::Type<irs::analysis::PipelineTokenizer>::name());
}

namespace {

class CopyThroughStage final
  : public irs::analysis::TypedTokenizer<CopyThroughStage>,
    public irs::analysis::TypedTokenStage<CopyThroughStage> {
 public:
  irs::TokenTraits Traits() const noexcept final {
    return {.unique = true, .offsets = true};
  }

  template<irs::TokenLayout Layout, typename Sink>
  bool DoFill(const duckdb::string_t& raw, Sink& sink) {
    const auto n = static_cast<uint32_t>(raw.GetSize());
    sink.template Emit<Layout>(n, [&](irs::byte_type* out) {
      std::memcpy(out, raw.GetData(), n);
      return n;
    });
    return true;
  }
};

}  // namespace

TEST(pipeline_token_stream_test, stage_may_build_a_term_from_its_input_slot) {
  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  pipeline_options.emplace_back(MakeDelimiter(","));
  pipeline_options.emplace_back(std::make_unique<CopyThroughStage>());
  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));

  const auto terms = tests::AnalyzeTerms(pipe, "abc,de,f,ghijklmnopqrstuvwxyz");
  ASSERT_TRUE(terms.has_value());
  const std::vector<std::string> expected{"abc", "de", "f",
                                          "ghijklmnopqrstuvwxyz"};
  ASSERT_EQ(expected, *terms);
}

TEST(pipeline_token_stream_test, empty_pipeline) {
  auto made = irs::analysis::PipelineTokenizer::Make(
    irs::analysis::PipelineTokenizer::Options{}, tests::Cache());
  ASSERT_NE(nullptr, made);
  const auto made_tokens = tests::Analyze(*made, "quick brown fox");
  ASSERT_TRUE(made_tokens.has_value());
  ASSERT_TRUE(made_tokens->empty());
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
  auto ngram = MakeNGram(2, 2, /*preserve_original=*/true);

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
  auto ngram = MakeNGram(6, 7, /*preserve_original=*/false);
  auto ngram2 = MakeNGram(2, 3, /*preserve_original=*/false);

  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
  pipeline_options.emplace_back(std::move(ngram));
  pipeline_options.emplace_back(std::move(ngram2));
  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));

  std::string data = "ABCDEFJH";
  const AnalyzerTokens expected{
    {"AB", 0, 2, 0},   {"ABC", 0, 3, 0},  {"BC", 1, 3, 1},   {"BCD", 1, 4, 1},
    {"CD", 2, 4, 2},   {"CDE", 2, 5, 2},  {"DE", 3, 5, 3},   {"DEF", 3, 6, 3},
    {"EF", 4, 6, 4},   {"AB", 0, 2, 4},   {"ABC", 0, 3, 4},  {"BC", 1, 3, 5},
    {"BCD", 1, 4, 5},  {"CD", 2, 4, 6},   {"CDE", 2, 5, 6},  {"DE", 3, 5, 7},
    {"DEF", 3, 6, 7},  {"EF", 4, 6, 8},   {"EFJ", 4, 7, 8},  {"FJ", 5, 7, 9},
    {"BC", 1, 3, 10},  {"BCD", 1, 4, 10}, {"CD", 2, 4, 11},  {"CDE", 2, 5, 11},
    {"DE", 3, 5, 12},  {"DEF", 3, 6, 12}, {"EF", 4, 6, 13},  {"EFJ", 4, 7, 13},
    {"FJ", 5, 7, 14},  {"BC", 1, 3, 14},  {"BCD", 1, 4, 14}, {"CD", 2, 4, 15},
    {"CDE", 2, 5, 15}, {"DE", 3, 5, 16},  {"DEF", 3, 6, 16}, {"EF", 4, 6, 17},
    {"EFJ", 4, 7, 17}, {"FJ", 5, 7, 18},  {"FJH", 5, 8, 18}, {"JH", 6, 8, 19},
    {"CD", 2, 4, 20},  {"CDE", 2, 5, 20}, {"DE", 3, 5, 21},  {"DEF", 3, 6, 21},
    {"EF", 4, 6, 22},  {"EFJ", 4, 7, 22}, {"FJ", 5, 7, 23},  {"FJH", 5, 8, 23},
    {"JH", 6, 8, 24},
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
    auto ngram = MakeNGram(3, 3, /*preserve_original=*/false);
    auto norm = MakeNorm("en", irs::Case::Upper);
    std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
    pipeline_options.emplace_back(std::move(ngram));
    pipeline_options.emplace_back(std::move(norm));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
  {
    auto ngram = MakeNGram(3, 3, /*preserve_original=*/false);
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
  irs::analysis::TextTokenizer::Options text_opts;
  text_opts.locale = icu::Locale::createFromName("en_US.UTF-8");
  text_opts.case_convert = irs::Case::Lower;
  text_opts.stemming = true;
  text_opts.accent = true;
  text_opts.explicit_stopwords_set = true;

  irs::analysis::PipelineTokenizer::Options opts;
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{std::move(text_opts)}));

  auto stream =
    irs::analysis::PipelineTokenizer::Make(std::move(opts), tests::Cache());
  ASSERT_NE(nullptr, stream);
  const std::string data = "QuIck broWn fox jumps";
  const AnalyzerTokens expected{{"quick", 0, 5, 0},
                                {"brown", 6, 11, 1},
                                {"fox", 12, 15, 2},
                                {"jump", 16, 21, 3}};
  AssertPipeline(stream.get(), data, expected);
}

TEST(pipeline_token_stream_test, signle_non_tokenizer) {
  irs::analysis::PipelineTokenizer::Options opts;
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{irs::analysis::NormalizingTokenizer::Options{
      .locale = icu::Locale::createFromName("en"),
      .case_convert = irs::Case::Lower,
    }}));

  auto stream =
    irs::analysis::PipelineTokenizer::Make(std::move(opts), tests::Cache());
  ASSERT_NE(nullptr, stream);
  const std::string data = "QuIck";
  const AnalyzerTokens expected{{"quick", 0, 5, 0}};
  AssertPipeline(stream.get(), data, expected);
}

TEST(pipeline_token_stream_test, hold_position_tokenizer) {
  std::string data = "QuIck";
  const AnalyzerTokens expected{
    {"qu", 0, 2, 0},  {"qui", 0, 3, 0}, {"quick", 0, 5, 0}, {"ui", 1, 3, 1},
    {"uic", 1, 4, 1}, {"ic", 2, 4, 2},  {"ick", 2, 5, 2},   {"ck", 3, 5, 3},
  };
  {
    auto ngram = MakeNGram(2, 3, /*preserve_original=*/true);
    auto norm = MakeNorm("en", irs::Case::Lower);
    std::vector<irs::analysis::Tokenizer::ptr> pipeline_options;
    pipeline_options.emplace_back(std::move(ngram));
    pipeline_options.emplace_back(std::move(norm));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
  {
    auto ngram = MakeNGram(2, 3, /*preserve_original=*/true);
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

  const AnalyzerTokens expected{{data, 0, 5, 0}, {data, 2, 3, 0}};
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
  text_opts.explicit_stopwords.push_back("fox");
  text_opts.explicit_stopwords_set = true;
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{std::move(text_opts)}));

  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{irs::analysis::NormalizingTokenizer::Options{
      .locale = icu::Locale::createFromName("en_US.UTF-8"),
      .case_convert = irs::Case::Upper,
    }}));

  auto stream =
    irs::analysis::PipelineTokenizer::Make(std::move(opts), tests::Cache());
  ASSERT_NE(nullptr, stream);
  const AnalyzerTokens expected{
    {"QUICK", 0, 5, 0}, {"BROWN", 6, 11, 1}, {"JUMP", 16, 21, 2}};
  AssertPipeline(stream.get(), "QuickABrownAFOXAjUmps", expected);
}

TEST(pipeline_token_stream_test, empty_pipeline_construct) {
  auto stream = irs::analysis::PipelineTokenizer::Make(
    irs::analysis::PipelineTokenizer::Options{}, tests::Cache());
  ASSERT_NE(nullptr, stream);
  ASSERT_NE(irs::Type<irs::analysis::PipelineTokenizer>::id(), stream->type());
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

  ASSERT_ANY_THROW(
    irs::analysis::PipelineTokenizer::Make(std::move(opts), tests::Cache()));
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
  auto ngram = MakeNGram(2, 2, /*preserve_original=*/true);
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
  pipeline_options2.emplace_back(std::move(ngram));

  auto pipe2 = std::make_unique<irs::analysis::PipelineTokenizer>(
    std::move(pipeline_options2));

  std::vector<irs::analysis::Tokenizer::ptr> pipeline_options3;
  pipeline_options3.emplace_back(std::move(pipe));
  pipeline_options3.emplace_back(std::move(pipe2));
  irs::analysis::PipelineTokenizer pipe3(std::move(pipeline_options3));
  AssertPipelineMembers(pipe3, expected_nested);
}

namespace {

std::vector<irs::analysis::Tokenizer::ptr> MakePipeSubs() {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(MakeDelimiter(" "));
  subs.push_back(MakeNGram(2, 3, /*preserve_original=*/true));
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
}

TEST(pipeline_token_stream_test, column_fill_matches_pull) {
  auto pull_stream =
    std::make_unique<irs::analysis::PipelineTokenizer>(MakePipeSubs());
  auto fill_stream =
    std::make_unique<irs::analysis::PipelineTokenizer>(MakePipeSubs());

  const std::vector<std::string> raw = {"quick brown fox", "", "a b c",
                                        "the lazy dog"};
  std::vector<duckdb::string_t> values;
  for (size_t i = 0; i < raw.size(); ++i) {
    values.emplace_back(raw[i].data(), static_cast<uint32_t>(raw[i].size()));
  }

  size_t flushes = 0;
  const auto check = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
    ++flushes;
    ASSERT_EQ(raw.size(), runs.size());
    uint32_t token_idx = 0;
    for (size_t v = 0; v < raw.size(); ++v) {
      SCOPED_TRACE(raw[v]);
      const auto pulled = SingleFill(*pull_stream, raw[v]);
      ASSERT_EQ(100 + v, runs[v].doc);
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
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
  tests::FillColumn(*fill_stream, values, 100, sink.writer, sink.layout);
  sink.writer.Finish();
  ASSERT_EQ(1, flushes);
}

namespace {

irs::analysis::Tokenizer::ptr MakeStopwords(
  std::initializer_list<std::string_view> words) {
  std::vector<std::string> mask;
  for (const auto w : words) {
    mask.emplace_back(w);
  }
  return irs::analysis::StopwordsTokenizer::Make({.mask = std::move(mask)},
                                                 tests::Cache());
}

std::vector<irs::analysis::Tokenizer::ptr> MakeDropFilterChildren(
  bool two_filters = false) {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(MakeDelimiter(","));
  if (two_filters) {
    subs.push_back(MakeStopwords({"the"}));
    subs.push_back(MakeStopwords({"and"}));
  } else {
    subs.push_back(MakeStopwords({"the", "and"}));
  }
  return subs;
}

std::unique_ptr<irs::analysis::PipelineTokenizer> MakeDropFilterPipe(
  bool two_filters = false) {
  return std::make_unique<irs::analysis::PipelineTokenizer>(
    MakeDropFilterChildren(two_filters));
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
                             irs::doc_id_t first_doc,
                             size_t* flushes = nullptr) {
  ChainCollected out;
  const bool dense = !pipe.Traits().explicit_pos;
  irs::doc_id_t ramp_doc = irs::doc_limits::invalid();
  uint32_t ramp_pos = 0;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    if (flushes != nullptr) {
      ++*flushes;
    }
    uint32_t tok = 0;
    for (const auto& r : runs) {
      if (dense && r.doc != ramp_doc) {
        ramp_doc = r.doc;
        ramp_pos = 0;
      }
      for (uint32_t k = 0; k < r.ntokens; ++k) {
        const auto& t = batch.terms[tok + k];
        const uint32_t pos = dense ? ++ramp_pos : batch.pos[tok + k];
        out.tokens.push_back({std::string{t.GetData(), t.GetSize()}, pos,
                              batch.offs_start[tok + k],
                              batch.offs_end[tok + k]});
      }
      tok += r.ntokens;
      if (!out.runs.empty() && out.runs.back().first == r.doc) {
        out.runs.back().second += r.ntokens;
      } else {
        out.runs.emplace_back(r.doc, r.ntokens);
      }
    }
    EXPECT_EQ(batch.count, tok);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
  tests::FillColumn(pipe, values, first_doc, sink.writer, sink.layout);
  sink.writer.Finish();
  return out;
}

ChainCollected ColumnReference(
  std::span<const irs::analysis::Tokenizer::ptr> children,
  std::span<const duckdb::string_t> values, irs::doc_id_t first_doc) {
  ChainCollected out;
  for (size_t i = 0; i < values.size(); ++i) {
    const std::string_view v{values[i].GetData(), values[i].GetSize()};
    const auto toks =
      tests::ChainReference(children, v, irs::TokenLayout::TermsPosOffs);
    uint32_t n = 0;
    if (toks) {
      n = static_cast<uint32_t>(toks->size());
      for (const auto& t : *toks) {
        out.tokens.push_back({t.term, t.pos, t.offs_start, t.offs_end});
      }
    }
    out.runs.emplace_back(first_doc + static_cast<irs::doc_id_t>(i), n);
  }
  return out;
}

}  // namespace

TEST(pipeline_token_stream_test, drop_filter_matches_reference) {
  for (const bool two_filters : {false, true}) {
    auto fast = MakeDropFilterPipe(two_filters);
    const auto children = MakeDropFilterChildren(two_filters);
    for (const auto& v : DropFilterValues()) {
      SCOPED_TRACE(v);
      const auto expected =
        tests::ChainReference(children, v, irs::TokenLayout::TermsPosOffs);
      const auto actual = tests::Analyze(*fast, v);
      ASSERT_EQ(expected.has_value(), actual.has_value());
      if (expected) {
        ASSERT_EQ(*expected, *actual);
      }
      const auto expected_terms = tests::ChainReferenceTerms(children, v);
      const auto actual_terms = tests::AnalyzeTerms(*fast, v);
      ASSERT_EQ(expected_terms.has_value(), actual_terms.has_value());
      if (expected_terms) {
        ASSERT_EQ(*expected_terms, *actual_terms);
      }
    }
  }
}

TEST(pipeline_token_stream_test, drop_filter_matches_reference_long_value) {
  auto fast = MakeDropFilterPipe();
  const auto children = MakeDropFilterChildren();
  const auto value = LongDropFilterValue();
  const auto expected =
    tests::ChainReference(children, value, irs::TokenLayout::TermsPosOffs);
  const auto actual = tests::Analyze(*fast, value);
  ASSERT_TRUE(expected.has_value());
  ASSERT_TRUE(actual.has_value());
  ASSERT_GT(expected->size(), irs::TokenBatch::kCapacity);
  ASSERT_EQ(*expected, *actual);
  const auto expected_terms = tests::ChainReferenceTerms(children, value);
  const auto actual_terms = tests::AnalyzeTerms(*fast, value);
  ASSERT_TRUE(expected_terms.has_value());
  ASSERT_TRUE(actual_terms.has_value());
  ASSERT_EQ(*expected_terms, *actual_terms);
}

namespace {

irs::analysis::Tokenizer::ptr MakeSolrSyn() {
  return irs::analysis::SolrSynonymsTokenizer::Make(
    {.synonyms_text = "quick, fast, speedy\nbig => large\n"}, tests::Cache());
}

irs::analysis::Tokenizer::ptr MakeWordnetSyn() {
  return irs::analysis::WordnetSynonymsTokenizer::Make(
    {.synonyms_text = "s(100,1,'quick',a,1,0).\n"
                      "s(300,2,'quick',a,1,0).\n"
                      "s(200,1,'brown',a,1,0).\n"},
    tests::Cache());
}

irs::analysis::Tokenizer::ptr MakeKeywordChild() {
  return irs::KeywordTokenizer::Make({});
}

using SubFactory = irs::analysis::Tokenizer::ptr (*)();

std::vector<irs::analysis::Tokenizer::ptr> MakeChainChildren(
  std::initializer_list<SubFactory> factories) {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  for (const auto f : factories) {
    subs.push_back(f());
  }
  return subs;
}

std::unique_ptr<irs::analysis::PipelineTokenizer> MakeChainPipe(
  std::initializer_list<SubFactory> factories) {
  return std::make_unique<irs::analysis::PipelineTokenizer>(
    MakeChainChildren(factories));
}

irs::analysis::Tokenizer::ptr MakeCommaDelim() { return MakeDelimiter(","); }

irs::analysis::Tokenizer::ptr MakeCommaMultiDelim() {
  return irs::analysis::MultiDelimitedTokenizer::Make(
    {.delimiters = {
       irs::bstring{reinterpret_cast<const irs::byte_type*>(","), 1}}});
}

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
    [] {
      std::string long_value;
      for (size_t i = 0; i < 1500; ++i) {
        long_value += (i % 5 == 0)   ? "big,"
                      : (i % 7 == 0) ? "the,"
                                     : ("word" + std::to_string(i % 11) + ",");
      }
      long_value += "quick";
      return long_value;
    }(),
  };
  return values;
}

void AssertChainEquivalence(std::initializer_list<SubFactory> factories,
                            const std::vector<std::string>& values) {
  auto fast = MakeChainPipe(factories);
  const auto children = MakeChainChildren(factories);
  const auto layout = fast->Traits().offsets ? irs::TokenLayout::TermsPosOffs
                                             : irs::TokenLayout::TermsPos;
  for (const auto& v : values) {
    SCOPED_TRACE(v);
    const auto expected = tests::ChainReference(children, v, layout);
    const auto actual = tests::Analyze(*fast, v);
    ASSERT_EQ(expected.has_value(), actual.has_value());
    if (expected) {
      ASSERT_EQ(*expected, *actual);
    }
    const auto expected_terms = tests::ChainReferenceTerms(children, v);
    const auto actual_terms = tests::AnalyzeTerms(*fast, v);
    ASSERT_EQ(expected_terms.has_value(), actual_terms.has_value());
    if (expected_terms) {
      ASSERT_EQ(*expected_terms, *actual_terms);
    }
  }
}

}  // namespace

TEST(pipeline_token_stream_test, solr_synonyms_semantics_pinned) {
  auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeSolrSyn});
  const auto tokens = tests::Analyze(*pipe, "quick,brown,big");
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected = {
    {"fast", 1, 0, 5},   {"quick", 1, 0, 5},   {"speedy", 1, 0, 5},
    {"brown", 2, 6, 11}, {"large", 3, 12, 15},
  };
  ASSERT_EQ(expected, *tokens);
  const auto reference =
    tests::ChainReference(MakeChainChildren({&MakeCommaDelim, &MakeSolrSyn}),
                          "quick,brown,big", irs::TokenLayout::TermsPosOffs);
  ASSERT_TRUE(reference.has_value());
  ASSERT_EQ(expected, *reference);
}

TEST(pipeline_token_stream_test, wordnet_synonyms_semantics_pinned) {
  auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeWordnetSyn});
  const auto tokens = tests::Analyze(*pipe, "quick,zzz,brown");
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected = {
    {"100", 1, 0, 5},
    {"300", 2, 0, 5},
    {"200", 3, 10, 15},
  };
  ASSERT_EQ(expected, *tokens);
  const auto reference =
    tests::ChainReference(MakeChainChildren({&MakeCommaDelim, &MakeWordnetSyn}),
                          "quick,zzz,brown", irs::TokenLayout::TermsPosOffs);
  ASSERT_TRUE(reference.has_value());
  ASSERT_EQ(expected, *reference);
}

TEST(pipeline_token_stream_test, keyword_child_identity_pinned) {
  auto delim = MakeDelimiter(",");
  auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeKeywordChild});
  ASSERT_EQ(delim->Traits().offsets, pipe->Traits().offsets);
  ASSERT_EQ(delim->Traits().explicit_pos, pipe->Traits().explicit_pos);
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

irs::analysis::Tokenizer::ptr MakeCollationEn() {
  return MakeCollation("en_US.UTF-8");
}

irs::analysis::Tokenizer::ptr MakeStemEn() {
  irs::analysis::StemmingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  return irs::analysis::StemmingTokenizer::Make(std::move(opts));
}

irs::analysis::Tokenizer::ptr MakeNGram23() { return MakeNGram(2, 3, false); }

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

TEST(pipeline_token_stream_test, rewriter_semantics_pinned) {
  const auto pin = [](std::initializer_list<SubFactory> factories,
                      std::string_view value,
                      const std::vector<tests::AnalyzerToken>& expected) {
    auto pipe = MakeChainPipe(factories);
    const auto tokens = tests::Analyze(*pipe, value);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(expected, *tokens);
    const auto reference = tests::ChainReference(
      MakeChainChildren(factories), value, irs::TokenLayout::TermsPosOffs);
    ASSERT_TRUE(reference.has_value());
    ASSERT_EQ(expected, *reference);
  };
  pin({&MakeCommaDelim, &MakeNormLowerEn}, "QUICK,Brown",
      {{"quick", 1, 0, 5}, {"brown", 2, 6, 11}});
  pin({&MakeCommaDelim, &MakeNormLowerEn, &MakeChainStopwords}, "The,QUICK",
      {{"quick", 1, 4, 9}});
  pin({&MakeCommaDelim, &MakeChainStopwords, &MakeNormLowerEn}, "The,QUICK",
      {{"the", 1, 0, 3}, {"quick", 2, 4, 9}});
  pin({&MakeCommaDelim, &MakeStemEn}, "running,jumps",
      {{"run", 1, 0, 7}, {"jump", 2, 8, 13}});
}

TEST(pipeline_token_stream_test, rewriter_matches_reference) {
  AssertChainEquivalence({&MakeCommaDelim, &MakeNormLowerEn}, RewriteValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeStemEn}, RewriteValues());
  AssertChainEquivalence({&MakeCommaMultiDelim, &MakeChainStopwords},
                         RewriteValues());
  AssertChainEquivalence({&MakeCommaMultiDelim, &MakeNormLowerEn},
                         RewriteValues());
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
  AssertChainEquivalence({&MakeCommaDelim, &MakeCollationEn}, ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeCollationEn}, RewriteValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeNormLowerEn, &MakeCollationEn},
                         RewriteValues());
}

namespace {

class SelfPassthroughStage final
  : public irs::analysis::TypedTokenizer<SelfPassthroughStage>,
    private irs::util::Noncopyable {
 public:
  irs::TokenTraits Traits() const noexcept final {
    return {.unique = true, .offsets = true};
  }

  template<irs::TokenLayout Layout>
  bool DoFill(const duckdb::string_t& raw, irs::TokenSink& sink) {
    if (std::string_view{raw.GetData(), raw.GetSize()} == "FAIL") {
      sink.Emit<Layout>(raw);
      return true;
    }
    sink.Emit<Layout>(raw);
    return true;
  }
};

class FailOnMarkerExpander final
  : public irs::analysis::TypedTokenizer<FailOnMarkerExpander>,
    private irs::util::Noncopyable {
 public:
  irs::TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  template<irs::TokenLayout Layout>
  bool DoFill(const duckdb::string_t& raw, irs::TokenSink& sink) {
    if (std::string_view{raw.GetData(), raw.GetSize()} == "FAIL") {
      return false;
    }
    sink.Emit<Layout>(raw);
    sink.Emit<Layout>(raw);
    return true;
  }
};

}  // namespace

TEST(pipeline_token_stream_test, kernel_self_passthrough) {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(MakeCommaDelim());
  subs.push_back(std::make_unique<SelfPassthroughStage>());
  auto pipe =
    std::make_unique<irs::analysis::PipelineTokenizer>(std::move(subs));
  const auto tokens = tests::Analyze(*pipe, "aa,FAIL,bb");
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected = {
    {"aa", 1, 0, 2},
    {"FAIL", 2, 3, 7},
    {"bb", 3, 8, 10},
  };
  ASSERT_EQ(expected, *tokens);
}

TEST(pipeline_token_stream_test, chain_kernel_failure_drops_token) {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(MakeCommaDelim());
  subs.push_back(std::make_unique<FailOnMarkerExpander>());
  auto pipe =
    std::make_unique<irs::analysis::PipelineTokenizer>(std::move(subs));
  const auto tokens = tests::Analyze(*pipe, "aa,FAIL,bb");
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected = {
    {"aa", 1, 0, 2},
    {"aa", 2, 0, 2},
    {"bb", 3, 8, 10},
    {"bb", 4, 8, 10},
  };
  ASSERT_EQ(expected, *tokens);
}

TEST(pipeline_token_stream_test, column_fill_mixed_ascii_matches_reference) {
  const auto factories = {&MakeCommaDelim, &MakeNormLowerEn, &MakeStemEn};
  auto fast = MakeChainPipe(factories);
  const auto children = MakeChainChildren(factories);
  const std::vector<std::string> raw = {
    "QUICK,Brown",
    "Gr\xc3\xbc\xc3\x9f"
    "e,RUNNING",
    "",
    "JUMPS,the",
    "Gr\xc3\xbcn",
    "running,QUICK,jumps",
  };
  std::vector<duckdb::string_t> values;
  for (const auto& s : raw) {
    values.emplace_back(s.data(), static_cast<uint32_t>(s.size()));
  }
  const auto expected = ColumnReference(children, values, 42);
  const auto actual = CollectColumn(*fast, values, 42);
  ASSERT_FALSE(expected.tokens.empty());
  ASSERT_EQ(expected, actual);
}

TEST(pipeline_token_stream_test, rewriter_matches_reference_long_value) {
  std::string value;
  for (size_t i = 0; i < 3000; ++i) {
    if (i % 3 == 0) {
      value += "RUNNING,";
    } else if (i % 7 == 0) {
      value += "The,";
    } else {
      value += "Word" + std::to_string(i) + ",";
    }
  }
  value += "JUMPS";
  const std::vector<std::string> values{value};
  AssertChainEquivalence({&MakeCommaDelim, &MakeNormLowerEn, &MakeStemEn},
                         values);
  AssertChainEquivalence(
    {&MakeCommaDelim, &MakeNormLowerEn, &MakeChainStopwords, &MakeStemEn},
    values);
}

TEST(pipeline_token_stream_test, retokenizer_matches_reference) {
  AssertChainEquivalence({&MakeCommaDelim, &MakeNGram23}, ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeNGram23}, RewriteValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeChainStopwords, &MakeNGram23},
                         ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeNGram23, &MakeNormLowerEn},
                         RewriteValues());
  AssertChainEquivalence(
    {&MakeCommaDelim, &MakeNormLowerEn, &MakeNGram23, &MakeStemEn},
    RewriteValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeNGram23, &MakeNGram23},
                         ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeSolrSyn, &MakeNGram23},
                         ChainValues());
}

TEST(pipeline_token_stream_test, mid_expander_matches_reference) {
  AssertChainEquivalence({&MakeCommaDelim, &MakeSolrSyn, &MakeChainStopwords},
                         ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeSolrSyn, &MakeNormLowerEn},
                         ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeWordnetSyn, &MakeStemEn},
                         ChainValues());
  AssertChainEquivalence({&MakeCommaDelim, &MakeWordnetSyn, &MakeSolrSyn},
                         ChainValues());
}

TEST(pipeline_token_stream_test, expander_matches_reference) {
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

TEST(pipeline_token_stream_test, column_fill_producer_arena_terms_stable) {
  const auto make_children = [] {
    std::vector<irs::analysis::Tokenizer::ptr> subs;
    subs.push_back(MakeNorm("en", irs::Case::Lower));
    subs.push_back(MakeStopwords({"unused"}));
    return subs;
  };
  auto fast =
    std::make_unique<irs::analysis::PipelineTokenizer>(make_children());
  const auto children = make_children();
  std::vector<std::string> raw;
  for (int i = 0; i < 8; ++i) {
    raw.push_back("LONG_UPPERCASE_VALUE_NUMBER_" + std::to_string(i));
  }
  std::vector<duckdb::string_t> values;
  for (const auto& s : raw) {
    values.emplace_back(s.data(), static_cast<uint32_t>(s.size()));
  }
  const auto expected = ColumnReference(children, values, 10);
  const auto actual = CollectColumn(*fast, values, 10);
  ASSERT_EQ(expected.tokens.size(), raw.size());
  ASSERT_EQ(expected, actual);
}

TEST(pipeline_token_stream_test, drop_filter_matches_reference_column) {
  auto fast = MakeDropFilterPipe();
  const auto children = MakeDropFilterChildren();
  const std::vector<std::string> raw = {
    "the,quick,the,brown",       "", "the,and", "fox,jumps",
    "and,over,the,lazy,dog,the",
  };
  std::vector<duckdb::string_t> values;
  for (size_t i = 0; i < raw.size(); ++i) {
    values.emplace_back(raw[i].data(), static_cast<uint32_t>(raw[i].size()));
  }
  const auto expected = ColumnReference(children, values, 10);
  const auto actual = CollectColumn(*fast, values, 10);
  ASSERT_FALSE(expected.tokens.empty());
  ASSERT_EQ(expected.runs.size(), raw.size());
  ASSERT_EQ(expected, actual);
}

namespace {

class RejectValueTokenizer final
  : public irs::analysis::TypedTokenizer<RejectValueTokenizer>,
    private irs::util::Noncopyable {
 public:
  irs::TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  template<irs::TokenLayout Layout>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view value{raw.GetData(), raw.GetSize()};
    if (value == "REJECT") {
      return false;
    }
    tests::EmitCopy<Layout>(sink, irs::ViewCast<irs::byte_type>(value),
                            irs::Offs{0, static_cast<uint32_t>(value.size())});
    return true;
  }
};

std::unique_ptr<irs::analysis::PipelineTokenizer> MakeRejectPipe() {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(std::make_unique<RejectValueTokenizer>());
  subs.push_back(MakeStopwords({"zzz"}));
  return std::make_unique<irs::analysis::PipelineTokenizer>(std::move(subs));
}

class BigFanoutTokenizer final
  : public irs::analysis::TypedTokenizer<BigFanoutTokenizer>,
    private irs::util::Noncopyable {
 public:
  irs::TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  template<irs::TokenLayout Layout>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view value{raw.GetData(), raw.GetSize()};
    const bool fail = value == "BIGFAIL";
    const size_t n = fail ? 1500 : 3;
    for (size_t i = 0; i < n; ++i) {
      tests::EmitCopy<Layout>(
        sink, irs::ViewCast<irs::byte_type>(value),
        irs::Offs{0, static_cast<uint32_t>(value.size())});
    }
    return !fail;
  }
};

}  // namespace

TEST(pipeline_token_stream_test, chain_token_failure_keeps_flushed_prefix) {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(MakeCommaDelim());
  subs.push_back(std::make_unique<BigFanoutTokenizer>());
  irs::analysis::PipelineTokenizer pipe{std::move(subs)};
  const auto tokens = tests::Analyze(pipe, "aa,BIGFAIL,bb");
  ASSERT_TRUE(tokens.has_value());
  constexpr size_t kFlushed = irs::TokenBatch::kCapacity - 3;
  ASSERT_EQ(3 + kFlushed + 3, tokens->size());
  EXPECT_EQ("aa", (*tokens)[0].term);
  EXPECT_EQ(3, (*tokens)[2].pos);
  EXPECT_EQ("BIGFAIL", (*tokens)[3].term);
  EXPECT_EQ(4, (*tokens)[3].pos);
  EXPECT_EQ("bb", (*tokens)[3 + kFlushed].term);
  EXPECT_EQ(3 + kFlushed + 1, (*tokens)[3 + kFlushed].pos);
  const auto next = tests::Analyze(pipe, "x,y");
  ASSERT_TRUE(next.has_value());
  ASSERT_EQ(6, next->size());
  EXPECT_EQ(1, (*next)[0].pos);
}

TEST(pipeline_token_stream_test, multi_link_column_batching) {
  const std::initializer_list<SubFactory> shapes[] = {
    {&MakeCommaDelim, &MakeChainStopwords, &MakeSolrSyn},
    {&MakeCommaDelim, &MakeNGram23},
  };
  for (const auto& factories : shapes) {
    auto fast = MakeChainPipe(factories);
    const auto children = MakeChainChildren(factories);
    std::vector<std::string> raw;
    for (int i = 0; i < 1500; ++i) {
      raw.push_back(i % 5 == 0 ? std::string{"the,quick"}
                               : "word" + std::to_string(i % 11) + ",big");
    }
    {
      std::string giant;
      for (int i = 0; i < 2000; ++i) {
        giant += "w" + std::to_string(i) + ",";
      }
      giant += "quick";
      raw.push_back(std::move(giant));
    }
    std::vector<duckdb::string_t> values;
    for (const auto& s : raw) {
      values.emplace_back(s.data(), static_cast<uint32_t>(s.size()));
    }
    const auto expected = ColumnReference(children, values, 100);
    size_t flushes = 0;
    const auto actual = CollectColumn(*fast, values, 100, &flushes);
    ASSERT_FALSE(expected.tokens.empty());
    ASSERT_EQ(expected.runs.size(), actual.runs.size());
    for (size_t i = 0; i < expected.runs.size(); ++i) {
      ASSERT_EQ(expected.runs[i], actual.runs[i]) << "run " << i;
    }
    ASSERT_EQ(expected.tokens.size(), actual.tokens.size());
    for (size_t i = 0; i < expected.tokens.size(); ++i) {
      ASSERT_EQ(expected.tokens[i].term, actual.tokens[i].term) << "tok " << i;
      ASSERT_EQ(expected.tokens[i].pos, actual.tokens[i].pos) << "tok " << i;
      ASSERT_EQ(expected.tokens[i].offs_start, actual.tokens[i].offs_start)
        << "tok " << i;
      ASSERT_EQ(expected.tokens[i].offs_end, actual.tokens[i].offs_end)
        << "tok " << i;
    }
    ASSERT_LT(flushes, values.size());
  }
}

TEST(pipeline_token_stream_test, front_failure_rejects_value) {
  auto pipe = MakeRejectPipe();
  ASSERT_FALSE(tests::Analyze(*pipe, "REJECT").has_value());
  const auto tokens = tests::Analyze(*pipe, "ok");
  ASSERT_TRUE(tokens.has_value());
  ASSERT_EQ(1, tokens->size());
}

TEST(pipeline_token_stream_test, front_failure_records_empty_run) {
  auto pipe = MakeRejectPipe();
  const std::vector<std::string> raw = {"ok", "REJECT", "also"};
  std::vector<duckdb::string_t> values;
  for (const auto& v : raw) {
    values.emplace_back(v.data(), static_cast<uint32_t>(v.size()));
  }
  const auto out = CollectColumn(*pipe, values, 42);
  const std::vector<std::pair<uint32_t, uint32_t>> runs = {
    {42, 1}, {43, 0}, {44, 1}};
  ASSERT_EQ(runs, out.runs);
  ASSERT_EQ(2, out.tokens.size());
  ASSERT_EQ("ok", out.tokens[0].term);
  ASSERT_EQ("also", out.tokens[1].term);
}

TEST(pipeline_token_stream_test, collation_oversize_token_dropped) {
  auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeCollationEn});
  const std::string huge(50000, 'x');
  const auto kept = tests::Analyze(*pipe, "aa,bb");
  const auto mixed = tests::Analyze(*pipe, "aa," + huge + ",bb");
  ASSERT_TRUE(kept.has_value());
  ASSERT_TRUE(mixed.has_value());
  ASSERT_EQ(2, kept->size());
  ASSERT_EQ(kept->size(), mixed->size());
  for (size_t i = 0; i < kept->size(); ++i) {
    EXPECT_EQ((*kept)[i].term, (*mixed)[i].term) << "token " << i;
    EXPECT_EQ((*kept)[i].pos, (*mixed)[i].pos) << "token " << i;
  }
}

TEST(pipeline_token_stream_test, keyword_child_dropped_by_make) {
  irs::analysis::PipelineTokenizer::Options opts;
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{
      irs::analysis::DelimitedTokenizer::Options{.delimiter = ","}}));
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{irs::KeywordTokenizer::Options{}}));

  auto stream =
    irs::analysis::PipelineTokenizer::Make(std::move(opts), tests::Cache());
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<irs::analysis::DelimitedTokenizer>::id(), stream->type());

  const auto tokens = tests::Analyze(*stream, "a,b");
  ASSERT_TRUE(tokens.has_value());
  ASSERT_EQ(2, tokens->size());
}

TEST(pipeline_token_stream_test, sole_stage_unwrapped_by_make) {
  irs::analysis::PipelineTokenizer::Options opts;
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{
      irs::analysis::DelimitedTokenizer::Options{.delimiter = ","}}));

  auto stream =
    irs::analysis::PipelineTokenizer::Make(std::move(opts), tests::Cache());
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<irs::analysis::DelimitedTokenizer>::id(), stream->type());
}

std::vector<irs::analysis::Tokenizer::ptr> MakeStableChildren() {
  using Convert = irs::analysis::SegmentationTokenizer::Options::Convert;
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(
    irs::analysis::SegmentationTokenizer::Make({.convert = Convert::None}));
  subs.push_back(MakeStopwords({"the", "and", "of", "a"}));
  return subs;
}

TEST(pipeline_token_stream_test, stable_producer_filter_matches_reference) {
  auto children = MakeStableChildren();
  ASSERT_TRUE(children.front()->Traits().stable);
  ASSERT_TRUE(children.back()->Traits().stable);

  auto fast =
    std::make_unique<irs::analysis::PipelineTokenizer>(MakeStableChildren());
  ASSERT_TRUE(fast->Traits().stable);

  for (const auto& value : ChainValues()) {
    const auto expected =
      tests::ChainReference(children, value, irs::TokenLayout::TermsPos);
    const auto actual =
      tests::Analyze(*fast, value, irs::TokenLayout::TermsPos);
    ASSERT_EQ(expected.has_value(), actual.has_value());
    if (expected) {
      ASSERT_EQ(*expected, *actual);
    }
    const auto expected_terms = tests::ChainReferenceTerms(children, value);
    const auto actual_terms = tests::AnalyzeTerms(*fast, value);
    ASSERT_EQ(expected_terms.has_value(), actual_terms.has_value());
    if (expected_terms) {
      ASSERT_EQ(*expected_terms, *actual_terms);
    }
  }
}

TEST(pipeline_token_stream_test, memory_usage_reports_chain) {
  auto pipe = MakeChainPipe({&MakeCommaDelim, &MakeNGram23});
  const size_t base = pipe->MemoryUsage();
  ASSERT_GE(base, sizeof(irs::TokenSink));
  ASSERT_TRUE(tests::Analyze(*pipe, "ab,cd").has_value());
  ASSERT_GE(pipe->MemoryUsage(), base + sizeof(irs::TokenSink));
}

TEST(pipeline_token_stream_test, traits_derivation) {
  {
    const auto traits = MakeDropFilterPipe()->Traits();
    EXPECT_FALSE(traits.explicit_pos);
    EXPECT_FALSE(traits.unique);
    EXPECT_TRUE(traits.offsets);
  }
  {
    const auto traits =
      MakeChainPipe({&MakeNormLowerEn, &MakeStemEn})->Traits();
    EXPECT_FALSE(traits.explicit_pos);
    EXPECT_TRUE(traits.unique);
  }
  {
    const auto traits =
      MakeChainPipe({&MakeNormLowerEn, &MakeChainStopwords})->Traits();
    EXPECT_FALSE(traits.explicit_pos);
    EXPECT_TRUE(traits.unique);
  }
  {
    const auto traits =
      MakeChainPipe({&MakeCommaDelim, &MakeSolrSyn})->Traits();
    EXPECT_TRUE(traits.explicit_pos);
    EXPECT_FALSE(traits.unique);
  }
  {
    const auto traits =
      MakeChainPipe({&MakeCommaDelim, &MakeNGram23})->Traits();
    EXPECT_TRUE(traits.explicit_pos);
    EXPECT_FALSE(traits.unique);
  }
  {
    std::vector<irs::analysis::Tokenizer::ptr> subs;
    subs.push_back(MakeCommaDelim());
    subs.push_back(MakeText("en_US.UTF-8", irs::Case::Lower, false));
    const auto traits =
      irs::analysis::PipelineTokenizer{std::move(subs)}.Traits();
    // Plain text fans out but emits dense child positions, so the chain still
    // produces a ramp; only explicit child positions make it non-dense.
    EXPECT_FALSE(traits.explicit_pos);
    EXPECT_FALSE(traits.unique);
  }
}

TEST(pipeline_token_stream_test, traits_propagate_ends) {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(MakeNormLowerEn());
  subs.push_back(MakeCollation("en_US.UTF-8"));
  const auto traits =
    irs::analysis::PipelineTokenizer{std::move(subs)}.Traits();
  EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR, traits.input);
  EXPECT_EQ(duckdb::LogicalTypeId::BLOB, traits.output);
  EXPECT_TRUE(traits.unique);
  EXPECT_FALSE(traits.explicit_pos);
}

namespace {

class StoreProducingAnalyzer
  : public irs::analysis::TypedTokenizer<StoreProducingAnalyzer>,
    private irs::util::Noncopyable {
 public:
  irs::TokenTraits Traits() const noexcept final { return {.store = true}; }

  template<irs::TokenLayout Layout>
  bool DoFill(duckdb::string_t, irs::TokenSink&) {
    return true;
  }
};

}  // namespace

TEST(pipeline_token_stream_test, store_producing_member_rejected) {
  std::vector<irs::analysis::Tokenizer::ptr> subs;
  subs.push_back(MakeCommaDelim());
  subs.push_back(std::make_unique<StoreProducingAnalyzer>());
  ASSERT_THROW(irs::analysis::PipelineTokenizer pipe(std::move(subs)),
               sdb::SqlException);
}
