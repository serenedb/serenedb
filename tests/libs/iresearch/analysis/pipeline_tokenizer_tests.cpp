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

#include <vpack/common.h>
#include <vpack/parser.h>

#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/pipeline_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "tests_config.hpp"

namespace {

class PipelineTestAnalyzer
  : public irs::analysis::TypedAnalyzer<PipelineTestAnalyzer>,
    private irs::util::Noncopyable {
 public:
  PipelineTestAnalyzer(bool has_offset, irs::bytes_view payload) {
    if (!irs::IsNull(payload)) {
      std::get<irs::AttributePtr<irs::PayAttr>>(_attrs) = &_payload;
    }
    if (has_offset) {
      std::get<irs::AttributePtr<irs::OffsAttr>>(_attrs) = &_offs;
    }
    _payload.value = payload;
  }
  bool next() final {
    if (_term_emitted) {
      return false;
    }
    _term_emitted = false;
    std::get<irs::IncAttr>(_attrs).value = 1;
    _offs.start = 0;
    _offs.end =
      static_cast<uint32_t>(std::get<irs::TermAttr>(_attrs).value.size());
    return true;
  }
  bool reset(std::string_view data) final {
    _term_emitted = false;
    std::get<irs::TermAttr>(_attrs).value = irs::ViewCast<irs::byte_type>(data);
    return true;
  }
  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

 private:
  using Attributes =
    std::tuple<irs::TermAttr, irs::IncAttr, irs::AttributePtr<irs::OffsAttr>,
               irs::AttributePtr<irs::PayAttr>>;

  Attributes _attrs;
  bool _term_emitted{true};
  irs::PayAttr _payload;
  irs::OffsAttr _offs;
};

class PipelineTestAnalyzer2
  : public irs::analysis::TypedAnalyzer<PipelineTestAnalyzer2>,
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
  bool next() final {
    if (_current_next != _nexts.end()) {
      auto next_val = *(_current_next++);
      if (next_val) {
        if (auto& offs = std::get<irs::OffsAttr>(_attrs);
            _current_offset != _offsets.end()) {
          auto value = *(_current_offset++);
          offs.start = value.first;
          offs.end = value.second;
        } else {
          offs.start = 0;
          offs.end = 0;
        }

        if (auto& inc = std::get<irs::IncAttr>(_attrs);
            _current_increment != _increments.end()) {
          inc.value = *(_current_increment++);
        } else {
          inc.value = 0;
        }

        if (auto& term = std::get<irs::TermAttr>(_attrs);
            _current_term != _terms.end()) {
          term.value = *(_current_term++);
        } else {
          term.value = irs::bytes_view{};
        }
      }
      return next_val;
    }
    return false;
  }
  bool reset(std::string_view /*data*/) final {
    if (_current_reset != _resets.end()) {
      return *(_current_reset++);
    }
    return false;
  }
  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

 private:
  using Attributes = std::tuple<irs::TermAttr, irs::IncAttr, irs::OffsAttr>;

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
  Attributes _attrs;
};

struct AnalyzerToken {
  std::string_view value;
  size_t start;
  size_t end;
  uint32_t pos;
};

using AnalyzerTokens = std::vector<AnalyzerToken>;

void AssertPipeline(irs::analysis::Analyzer* pipe, const std::string& data,
                    const AnalyzerTokens& expected_tokens) {
  SCOPED_TRACE(data);
  auto* offset = irs::get<irs::OffsAttr>(*pipe);
  ASSERT_TRUE(offset);
  auto* term = irs::get<irs::TermAttr>(*pipe);
  ASSERT_TRUE(term);
  auto* inc = irs::get<irs::IncAttr>(*pipe);
  ASSERT_TRUE(inc);
  ASSERT_TRUE(pipe->reset(data));
  uint32_t pos{std::numeric_limits<uint32_t>::max()};
  auto expected_token = expected_tokens.begin();
  while (pipe->next()) {
    auto term_value =
      std::string(irs::ViewCast<char>(term->value).data(), term->value.size());
    SCOPED_TRACE(testing::Message("Term:") << term_value);
    pos += inc->value;
    ASSERT_NE(expected_token, expected_tokens.end());
    ASSERT_EQ(irs::ViewCast<irs::byte_type>(expected_token->value),
              term->value);
    ASSERT_EQ(expected_token->start, offset->start);
    ASSERT_EQ(expected_token->end, offset->end);
    ASSERT_EQ(expected_token->pos, pos);
    ++expected_token;
  }
  ASSERT_EQ(expected_token, expected_tokens.end());
}

void AssertPipelineMembers(
  irs::analysis::PipelineTokenizer& pipe,
  const std::vector<irs::TypeInfo::type_id>& expected) {
  size_t i{0};
  auto visitor = [&expected, &i](const irs::analysis::Analyzer& a) {
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
  irs::analysis::PipelineTokenizer::options_t pipeline_options;
  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));

  std::string data = "quick broWn,, FOX  jumps,  over lazy dog";
  ASSERT_FALSE(pipe.reset(data));
}

TEST(pipeline_token_stream_test, many_tokenizers) {
  auto delimiter = irs::analysis::analyzers::Get(
    "delimiter", irs::Type<irs::text_format::Json>::get(),
    "{\"delimiter\":\",\"}");

  auto delimiter2 = irs::analysis::analyzers::Get(
    "delimiter", irs::Type<irs::text_format::Json>::get(),
    "{\"delimiter\":\" \"}");

  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[], \"case\":\"none\", "
    "\"stemming\":false }");

  auto ngram = irs::analysis::analyzers::Get(
    "ngram", irs::Type<irs::text_format::Json>::get(),
    "{\"min\":2, \"max\":2, \"preserveOriginal\":true }");

  irs::analysis::PipelineTokenizer::options_t pipeline_options;
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
  auto ngram = irs::analysis::analyzers::Get(
    "ngram", irs::Type<irs::text_format::Json>::get(),
    "{\"min\":6, \"max\":7, \"preserveOriginal\":false }");
  auto ngram2 = irs::analysis::analyzers::Get(
    "ngram", irs::Type<irs::text_format::Json>::get(),
    "{\"min\":2, \"max\":3, \"preserveOriginal\":false }");

  irs::analysis::PipelineTokenizer::options_t pipeline_options;
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
    auto ngram = irs::analysis::analyzers::Get(
      "ngram", irs::Type<irs::text_format::Json>::get(),
      "{\"min\":3, \"max\":3, \"preserveOriginal\":false }");
    auto norm = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"case\":\"upper\"}");
    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(ngram));
    pipeline_options.emplace_back(std::move(norm));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
  {
    auto ngram = irs::analysis::analyzers::Get(
      "ngram", irs::Type<irs::text_format::Json>::get(),
      "{\"min\":3, \"max\":3, \"preserveOriginal\":false }");
    auto norm = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"case\":\"upper\"}");
    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(norm));
    pipeline_options.emplace_back(std::move(ngram));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
}

TEST(pipeline_token_stream_test, no_tokenizers) {
  std::string data = "QuIck";
  auto norm1 = irs::analysis::analyzers::Get(
    "norm", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"en\", \"case\":\"upper\"}");
  auto norm2 = irs::analysis::analyzers::Get(
    "norm", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"en\", \"case\":\"lower\"}");
  const AnalyzerTokens expected{
    {"quick", 0, 5, 0},
  };
  irs::analysis::PipelineTokenizer::options_t pipeline_options;
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
    auto text = irs::analysis::analyzers::Get(
      "text", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[], \"case\":\"none\", "
      "\"stemming\":true }");
    auto norm = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"case\":\"lower\"}");
    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(text));
    pipeline_options.emplace_back(std::move(norm));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
  {
    auto text = irs::analysis::analyzers::Get(
      "text", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[], \"case\":\"none\", "
      "\"stemming\":true }");
    auto norm = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"case\":\"lower\"}");
    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(norm));
    pipeline_options.emplace_back(std::move(text));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
}

TEST(pipeline_token_stream_test, signle_tokenizer) {
  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[], \"case\":\"lower\", "
    "\"stemming\":true }");
  std::string data = "QuIck broWn fox jumps";
  const AnalyzerTokens expected{{"quick", 0, 5, 0},
                                {"brown", 6, 11, 1},
                                {"fox", 12, 15, 2},
                                {"jump", 16, 21, 3}};
  irs::analysis::PipelineTokenizer::options_t pipeline_options;
  pipeline_options.emplace_back(std::move(text));
  irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
  AssertPipeline(&pipe, data, expected);
}

TEST(pipeline_token_stream_test, signle_non_tokenizer) {
  auto norm = irs::analysis::analyzers::Get(
    "norm", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"en\", \"case\":\"lower\"}");
  std::string data = "QuIck";
  const AnalyzerTokens expected{{"quick", 0, 5, 0}};
  irs::analysis::PipelineTokenizer::options_t pipeline_options;
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
    auto ngram = irs::analysis::analyzers::Get(
      "ngram", irs::Type<irs::text_format::Json>::get(),
      "{\"min\":2, \"max\":3, \"preserveOriginal\":true }");
    auto norm = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"case\":\"lower\"}");
    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(ngram));
    pipeline_options.emplace_back(std::move(norm));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
  {
    auto ngram = irs::analysis::analyzers::Get(
      "ngram", irs::Type<irs::text_format::Json>::get(),
      "{\"min\":2, \"max\":3, \"preserveOriginal\":true }");
    auto norm = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"case\":\"lower\"}");
    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(norm));
    pipeline_options.emplace_back(std::move(ngram));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
}

TEST(pipeline_token_stream_test, hold_position_tokenizer2) {
  std::string data = "A";
  irs::bytes_view term = irs::ViewCast<irs::byte_type>(std::string_view(data));
  irs::analysis::Analyzer::ptr tokenizer1;
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
  irs::analysis::Analyzer::ptr tokenizer2;
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
  irs::analysis::Analyzer::ptr tokenizer3;
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
    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(tokenizer1));
    pipeline_options.emplace_back(std::move(tokenizer2));
    pipeline_options.emplace_back(std::move(tokenizer3));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    AssertPipeline(&pipe, data, expected);
  }
}

TEST(pipeline_token_stream_test, test_construct) {
  std::string config =
    "{\"pipeline\":["
    "{\"type\":\"delimiter\", \"properties\": {\"delimiter\":\"A\"}},"
    "{\"type\":\"text\", "
    "\"properties\":{\"locale\":\"en_US.UTF-8\",\"case\":\"lower\","
    "\"accent\":false,\"stemming\":true,\"stopwords\":[\"fox\"]}},"
    "{\"type\":\"norm\", \"properties\": {\"locale\":\"en_US.UTF-8\", "
    "\"case\":\"upper\"}}"
    "]}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_NE(nullptr, stream);
  const AnalyzerTokens expected{
    {"QUICK", 0, 5, 0}, {"BROWN", 6, 11, 1}, {"JUMP", 16, 21, 2}};
  AssertPipeline(stream.get(), "QuickABrownAFOXAjUmps", expected);
}

TEST(pipeline_token_stream_test, test_normalized_construct) {
  std::string config =
    "{\"pipeline\":["
    "{\"type\":\"delimiter\", \"properties\": {\"delimiter\":\"A\"}},"
    "{\"type\":\"text\", "
    "\"properties\":{\"locale\":\"en_US.UTF-8\",\"case\":\"lower\","
    "  \"accent\":false,\"stemming\":true,\"stopwords\":[\"fox\"]}},"
    "{\"type\":\"norm\", \"properties\": {\"locale\":\"en_US.UTF-8\", "
    "\"case\":\"upper\"}}"
    "]}";
  std::string normalized;
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    normalized, "pipeline", irs::Type<irs::text_format::Json>::get(), config));
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), normalized);
  ASSERT_NE(nullptr, stream);
  const AnalyzerTokens expected{
    {"QUICK", 0, 5, 0}, {"BROWN", 6, 11, 1}, {"JUMP", 16, 21, 2}};
  AssertPipeline(stream.get(), "QuickABrownAFOXAjUmps", expected);
}

TEST(pipeline_token_stream_test, test_construct_invalid_json) {
  std::string config = "INVALID_JSON}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, test_construct_not_object_json) {
  std::string config = "[1,2,3]";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, test_construct_no_pipeline) {
  std::string config =
    "{\"NOT_pipeline\":["
    "{\"type\":\"delimiter\", \"properties\": {\"delimiter\":\"A\"}},"
    "{\"type\":\"text\", "
    "\"properties\":{\"locale\":\"en_US.UTF-8\",\"case\":\"lower\","
    "  \"accent\":false,\"stemming\":true,\"stopwords\":[\"fox\"]}},"
    "{\"type\":\"norm\", \"properties\": {\"locale\":\"en_US.UTF-8\", "
    "\"case\":\"upper\"}}"
    "]}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, test_construct_not_array_pipeline) {
  std::string config = "{\"pipeline\": \"text\"}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, test_construct_not_pipeline_objects) {
  std::string config = "{\"pipeline\":[\"123\"]}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, test_construct_no_type) {
  std::string config =
    "{\"pipeline\":["
    "{\"type\":\"delimiter\", \"properties\": {\"delimiter\":\"A\"}},"
    "{\"properties\":{\"locale\":\"en_US.UTF-8\",\"case\":\"lower\","
    "  \"accent\":false,\"stemming\":true,\"stopwords\":[\"fox\"]}},"
    "{\"type\":\"norm\", \"properties\": {\"locale\":\"en_US.UTF-8\", "
    "\"case\":\"upper\"}}"
    "]}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, test_construct_non_string_type) {
  std::string config =
    "{\"pipeline\":[{\"type\":1, \"properties\": {\"delimiter\":\"A\"}}]}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, test_construct_no_properties) {
  std::string config =
    "{\"pipeline\":["
    "{\"type\":\"delimiter\", \"properties\": {\"delimiter\":\"A\"}},"
    "{\"type\":\"text\"}"
    "]}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, test_construct_invalid_analyzer) {
  std::string config =
    "{\"pipeline\":["
    "{\"type\":\"UNKNOWN\", \"properties\": {\"delimiter\":\"A\"}},"
    "{\"properties\":{\"locale\":\"en_US.UTF-8\",\"case\":\"lower\","
    "  \"accent\":false,\"stemming\":true,\"stopwords\":[\"fox\"]}},"
    "{\"type\":\"norm\", \"properties\": {\"locale\":\"en_US.UTF-8\", "
    "\"case\":\"upper\"}}"
    "]}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, empty_pipeline_construct) {
  std::string config = "{\"pipeline\":[]}";
  auto stream = irs::analysis::analyzers::Get(
    "pipeline", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(pipeline_token_stream_test, normalize_json) {
  // with unknown parameter
  {
    std::string config =
      "{ \"unknown_parameter\":123,  \"pipeline\":[{\"type\":\"delimiter\", "
      "\"properties\": {\"delimiter\":\"A\"}}]}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "pipeline", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson("{\"pipeline\":[{\"type\":\"delimiter\","
                                      "\"properties\":{\"delimiter\":\"A\"}}]}")
                ->toString(),
              actual);
  }
  // with unknown parameter in pipeline member
  {
    std::string config =
      "{\"pipeline\":[{\"unknown_parameter\":123, \"type\":\"delimiter\", "
      "\"properties\": {\"delimiter\":\"A\"}}]}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "pipeline", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson("{\"pipeline\":[{\"type\":\"delimiter\","
                                      "\"properties\":{\"delimiter\":\"A\"}}]}")
                ->toString(),
              actual);
  }
  // with unknown parameter in analyzer properties
  {
    std::string config =
      "{\"pipeline\":[{\"type\":\"delimiter\", \"properties\": "
      "{\"unknown_paramater\":123, \"delimiter\":\"A\"}}]}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "pipeline", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson("{\"pipeline\":[{\"type\":\"delimiter\","
                                      "\"properties\":{\"delimiter\":\"A\"}}]}")
                ->toString(),
              actual);
  }

  // with unknown analyzer
  {
    std::string config =
      "{\"pipeline\":[{\"type\":\"unknown\", \"properties\": "
      "{\"unknown_paramater\":123, \"delimiter\":\"A\"}}]}";
    std::string actual;
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      actual, "pipeline", irs::Type<irs::text_format::Json>::get(), config));
  }

  // with invalid properties
  {
    std::string config =
      "{\"pipeline\":[{\"type\":\"delimiter\", \"properties\": "
      "{\"wrong_delimiter\":\"A\"}}]}";
    std::string actual;
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      actual, "pipeline", irs::Type<irs::text_format::Json>::get(), config));
  }
}

TEST(pipeline_token_stream_test, analyzers_with_payload_offset) {
  // store as separate arrays to make asan happy
  irs::byte_type p1[] = {0x1, 0x2, 0x3};
  irs::byte_type p2[] = {0x11, 0x22, 0x33};

  {
    auto payload_offset = std::make_unique<PipelineTestAnalyzer>(
      true, irs::bytes_view{p1, std::size(p1)});
    auto only_offset =
      std::make_unique<PipelineTestAnalyzer>(true, irs::bytes_view{});

    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(payload_offset));
    pipeline_options.emplace_back(std::move(only_offset));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    auto* offset = irs::get<irs::OffsAttr>(pipe);
    ASSERT_TRUE(offset);
    auto* term = irs::get<irs::TermAttr>(pipe);
    ASSERT_TRUE(term);
    auto* inc = irs::get<irs::IncAttr>(pipe);
    ASSERT_TRUE(inc);
    auto* pay = irs::get<irs::PayAttr>(pipe);
    ASSERT_TRUE(pay);
    ASSERT_TRUE(pipe.reset("A"));
    ASSERT_TRUE(pipe.next());
    ASSERT_EQ(p1, pay->value.data());
  }
  {
    auto payload_offset = std::make_unique<PipelineTestAnalyzer>(
      true, irs::bytes_view{p1, std::size(p1)});
    auto only_offset =
      std::make_unique<PipelineTestAnalyzer>(true, irs::bytes_view{});

    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(only_offset));
    pipeline_options.emplace_back(std::move(payload_offset));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    auto* offset = irs::get<irs::OffsAttr>(pipe);
    ASSERT_TRUE(offset);
    auto* term = irs::get<irs::TermAttr>(pipe);
    ASSERT_TRUE(term);
    auto* inc = irs::get<irs::IncAttr>(pipe);
    ASSERT_TRUE(inc);
    auto* pay = irs::get<irs::PayAttr>(pipe);
    ASSERT_TRUE(pay);
    ASSERT_TRUE(pipe.reset("A"));
    ASSERT_TRUE(pipe.next());
    ASSERT_EQ(p1, pay->value.data());
  }
  {
    auto payload_offset = std::make_unique<PipelineTestAnalyzer>(
      true, irs::bytes_view{p1, std::size(p1)});
    auto only_payload = std::make_unique<PipelineTestAnalyzer>(
      false, irs::bytes_view{p2, std::size(p2)});

    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(payload_offset));
    pipeline_options.emplace_back(std::move(only_payload));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    auto* offset = irs::get<irs::OffsAttr>(pipe);
    ASSERT_FALSE(offset);
    auto* term = irs::get<irs::TermAttr>(pipe);
    ASSERT_TRUE(term);
    auto* inc = irs::get<irs::IncAttr>(pipe);
    ASSERT_TRUE(inc);
    auto* pay = irs::get<irs::PayAttr>(pipe);
    ASSERT_TRUE(pay);
    ASSERT_TRUE(pipe.reset("A"));
    ASSERT_TRUE(pipe.next());
    ASSERT_EQ(p2, pay->value.data());
  }
  {
    auto payload_offset = std::make_unique<PipelineTestAnalyzer>(
      true, irs::bytes_view{p1, std::size(p1)});
    auto only_payload = std::make_unique<PipelineTestAnalyzer>(
      false, irs::bytes_view{p2, std::size(p2)});

    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(only_payload));
    pipeline_options.emplace_back(std::move(payload_offset));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    auto* offset = irs::get<irs::OffsAttr>(pipe);
    ASSERT_FALSE(offset);
    auto* term = irs::get<irs::TermAttr>(pipe);
    ASSERT_TRUE(term);
    auto* inc = irs::get<irs::IncAttr>(pipe);
    ASSERT_TRUE(inc);
    auto* pay = irs::get<irs::PayAttr>(pipe);
    ASSERT_TRUE(pay);
    ASSERT_TRUE(pipe.reset("A"));
    ASSERT_TRUE(pipe.next());
    ASSERT_EQ(p1, pay->value.data());
  }
  {
    auto only_payload = std::make_unique<PipelineTestAnalyzer>(
      false, irs::bytes_view{p2, std::size(p2)});
    auto no_payload_no_offset =
      std::make_unique<PipelineTestAnalyzer>(false, irs::bytes_view{});

    irs::analysis::PipelineTokenizer::options_t pipeline_options;
    pipeline_options.emplace_back(std::move(only_payload));
    pipeline_options.emplace_back(std::move(no_payload_no_offset));
    irs::analysis::PipelineTokenizer pipe(std::move(pipeline_options));
    auto* offset = irs::get<irs::OffsAttr>(pipe);
    ASSERT_FALSE(offset);
    auto* term = irs::get<irs::TermAttr>(pipe);
    ASSERT_TRUE(term);
    auto* inc = irs::get<irs::IncAttr>(pipe);
    ASSERT_TRUE(inc);
    auto* pay = irs::get<irs::PayAttr>(pipe);
    ASSERT_TRUE(pay);
    ASSERT_TRUE(pipe.reset("A"));
    ASSERT_TRUE(pipe.next());
    ASSERT_EQ(p2, pay->value.data());
  }
}

TEST(pipeline_token_stream_test, members_visitor) {
  auto delimiter = irs::analysis::analyzers::Get(
    "delimiter", irs::Type<irs::text_format::Json>::get(),
    "{\"delimiter\":\",\"}");

  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[], \"case\":\"none\", "
    "\"stemming\":false }");

  auto ngram = irs::analysis::analyzers::Get(
    "ngram", irs::Type<irs::text_format::Json>::get(),
    "{\"min\":2, \"max\":2, \"preserveOriginal\":true }");

  auto norm = irs::analysis::analyzers::Get(
    "norm", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"en\", \"case\":\"upper\"}");

  std::vector<irs::TypeInfo::type_id> expected{delimiter->type(), norm->type()};
  std::vector<irs::TypeInfo::type_id> expected_nested{
    delimiter->type(), norm->type(), text->type(), ngram->type()};
  irs::analysis::PipelineTokenizer::options_t pipeline_options;
  pipeline_options.emplace_back(std::move(delimiter));
  pipeline_options.emplace_back(std::move(norm));
  auto pipe = std::make_unique<irs::analysis::PipelineTokenizer>(
    std::move(pipeline_options));
  AssertPipelineMembers(*pipe, expected);

  irs::analysis::PipelineTokenizer::options_t pipeline_options2;
  pipeline_options2.emplace_back(std::move(text));

  auto pipe2 = std::make_unique<irs::analysis::PipelineTokenizer>(
    std::move(pipeline_options2));

  irs::analysis::PipelineTokenizer::options_t pipeline_options3;
  pipeline_options3.emplace_back(std::move(pipe));
  pipeline_options3.emplace_back(std::move(pipe2));
  pipeline_options3.emplace_back(std::move(ngram));
  irs::analysis::PipelineTokenizer pipe3(std::move(pipeline_options3));
  AssertPipelineMembers(pipe3, expected_nested);
}
