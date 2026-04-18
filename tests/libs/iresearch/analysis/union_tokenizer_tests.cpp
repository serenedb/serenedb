////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <vpack/common.h>
#include <vpack/parser.h>

#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/analysis/union_tokenizer.hpp"
#include "tests_config.hpp"

namespace {

struct UnionToken {
  std::string_view value;
  uint32_t inc;
  uint32_t pos;
};

using UnionTokens = std::vector<UnionToken>;

// Assert a union tokenizer produces the expected token sequence.
// No offset checking; union does not expose OffsAttr.
void AssertUnion(irs::analysis::Analyzer* u, const std::string& data,
                 const UnionTokens& expected_tokens) {
  SCOPED_TRACE(data);
  auto* offset = irs::get<irs::OffsAttr>(*u);
  ASSERT_FALSE(offset) << "Union must not expose OffsAttr";
  auto* term = irs::get<irs::TermAttr>(*u);
  ASSERT_TRUE(term);
  auto* inc = irs::get<irs::IncAttr>(*u);
  ASSERT_TRUE(inc);
  ASSERT_TRUE(u->reset(data));
  uint32_t pos{0};  // consumer starts at pos_limits::invalid() == 0
  auto expected = expected_tokens.begin();
  while (u->next()) {
    pos += inc->value;
    auto term_str =
      std::string(irs::ViewCast<char>(term->value).data(), term->value.size());
    SCOPED_TRACE(testing::Message("Term:") << term_str);
    ASSERT_NE(expected, expected_tokens.end())
      << "More tokens produced than expected";
    EXPECT_EQ(expected->value,
              std::string_view(irs::ViewCast<char>(term->value).data(),
                               term->value.size()));
    EXPECT_EQ(expected->inc, inc->value) << "inc mismatch for: " << term_str;
    EXPECT_EQ(expected->pos, pos) << "pos mismatch for: " << term_str;
    ++expected;
  }
  ASSERT_EQ(expected, expected_tokens.end())
    << "Fewer tokens produced than expected";
}

void AssertUnionMembers(irs::analysis::UnionTokenizer& u,
                        const std::vector<irs::TypeInfo::type_id>& expected) {
  size_t i{0};
  auto visitor = [&expected, &i](const irs::analysis::Analyzer& a) {
    EXPECT_LT(i, expected.size());
    if (i >= expected.size()) {
      return false;
    }
    EXPECT_EQ(a.type(), expected[i++]);
    return true;
  };
  ASSERT_TRUE(u.VisitMembers(visitor));
  ASSERT_EQ(i, expected.size());
}

}  // namespace

TEST(union_tokenizer_test, consts) {
  static_assert("union" == irs::Type<irs::analysis::UnionTokenizer>::name());
}

TEST(union_tokenizer_test, empty_union) {
  irs::analysis::UnionTokenizer::OptionsT options;
  irs::analysis::UnionTokenizer u(std::move(options));

  std::string data = "hello world";
  ASSERT_FALSE(u.reset(data));
}

TEST(union_tokenizer_test, single_sub) {
  // A union with a single text tokenizer should produce the same tokens
  // as the text tokenizer alone.
  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":[], "case":"lower",
        "stemming":false})");
  ASSERT_NE(nullptr, text);

  irs::analysis::UnionTokenizer::OptionsT options;
  options.emplace_back(std::move(text));
  irs::analysis::UnionTokenizer u(std::move(options));

  const UnionTokens expected{
    {"hello", 1, 1},
    {"world", 1, 2},
  };
  AssertUnion(&u, "hello world", expected);
}

TEST(union_tokenizer_test, text_plus_ngram) {
  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":[], "case":"lower",
        "stemming":false})");
  ASSERT_NE(nullptr, text);

  auto ngram = irs::analysis::analyzers::Get(
    "ngram", irs::Type<irs::text_format::Json>::get(),
    R"({"min":3, "max":3, "preserveOriginal":false})");
  ASSERT_NE(nullptr, ngram);

  irs::analysis::UnionTokenizer::OptionsT options;
  options.emplace_back(std::move(text));
  options.emplace_back(std::move(ngram));
  irs::analysis::UnionTokenizer u(std::move(options));

  // text: "hello"(pos=1) "world"(pos=2)
  // ngram(3,3): "hel"(1) "ell"(2) "llo"(3) "lo "(4) "o w"(5)
  //             " wo"(6) "wor"(7) "orl"(8) "rld"(9)
  // Interleaved by position, sub-index order (text=0, ngram=1):
  const UnionTokens expected{
    {"hello", 1, 1},  // text pos=1
    {"hel", 0, 1},    // ngram pos=1, same union position
    {"world", 1, 2},  // text pos=2
    {"ell", 0, 2},    // ngram pos=2, same union position
    {"llo", 1, 3},    // ngram only from here on
    {"lo ", 1, 4},   {"o w", 1, 5}, {" wo", 1, 6},
    {"wor", 1, 7},   {"orl", 1, 8}, {"rld", 1, 9},
  };
  AssertUnion(&u, "hello world", expected);
}

TEST(union_tokenizer_test, two_text_tokenizers_homogeneous) {
  // For homogeneous sub-tokenizers, positions are truly comparable.
  auto text_lower = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":[], "case":"lower",
        "stemming":false})");
  ASSERT_NE(nullptr, text_lower);

  auto text_none = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":[], "case":"none",
        "stemming":false})");
  ASSERT_NE(nullptr, text_none);

  irs::analysis::UnionTokenizer::OptionsT options;
  options.emplace_back(std::move(text_lower));
  options.emplace_back(std::move(text_none));
  irs::analysis::UnionTokenizer u(std::move(options));

  // Sub 0 (lower): "hello"(1), "world"(2)
  // Sub 1 (none):  "Hello"(1), "World"(2)
  const UnionTokens expected{
    {"hello", 1, 1},
    {"Hello", 0, 1},
    {"world", 1, 2},
    {"World", 0, 2},
  };
  AssertUnion(&u, "Hello World", expected);
}

TEST(union_tokenizer_test, unequal_token_counts) {
  // Sub with fewer tokens exhausts first; remaining sub continues alone.
  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":["quick","brown","fox"],
        "case":"lower", "stemming":false})");
  ASSERT_NE(nullptr, text);

  auto text2 = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":[],
        "case":"lower", "stemming":false})");
  ASSERT_NE(nullptr, text2);

  irs::analysis::UnionTokenizer::OptionsT options;
  options.emplace_back(std::move(text));   // sub 0: stopwords filtered
  options.emplace_back(std::move(text2));  // sub 1: all words
  irs::analysis::UnionTokenizer u(std::move(options));

  // Input: "quick brown fox"
  // Sub 0 (with stopwords): all filtered: no tokens
  // Sub 1 (no stopwords): "quick"(1) "brown"(2) "fox"(3)
  // Union: only sub 1 tokens
  const UnionTokens expected{
    {"quick", 1, 1},
    {"brown", 1, 2},
    {"fox", 1, 3},
  };
  AssertUnion(&u, "quick brown fox", expected);
}

TEST(union_tokenizer_test, no_offset_attr) {
  // Union must NOT expose OffsAttr.
  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":[], "case":"lower",
        "stemming":false})");
  ASSERT_NE(nullptr, text);

  irs::analysis::UnionTokenizer::OptionsT options;
  options.emplace_back(std::move(text));
  irs::analysis::UnionTokenizer u(std::move(options));

  auto* offset = irs::get<irs::OffsAttr>(u);
  ASSERT_EQ(nullptr, offset);
}

TEST(union_tokenizer_test, get_mutable_non_core) {
  // GetMutable returns nullptr for non-managed attributes.
  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":[], "case":"lower",
        "stemming":false})");
  ASSERT_NE(nullptr, text);

  irs::analysis::UnionTokenizer::OptionsT options;
  options.emplace_back(std::move(text));
  irs::analysis::UnionTokenizer u(std::move(options));

  ASSERT_NE(nullptr, irs::get<irs::TermAttr>(u));
  ASSERT_NE(nullptr, irs::get<irs::IncAttr>(u));
  ASSERT_EQ(nullptr, irs::get<irs::OffsAttr>(u));
}

TEST(union_tokenizer_test, VisitMembers) {
  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":[], "case":"lower",
        "stemming":false})");
  auto ngram = irs::analysis::analyzers::Get(
    "ngram", irs::Type<irs::text_format::Json>::get(),
    R"({"min":2, "max":3, "preserveOriginal":false})");

  irs::analysis::UnionTokenizer::OptionsT options;
  options.emplace_back(std::move(text));
  options.emplace_back(std::move(ngram));
  irs::analysis::UnionTokenizer u(std::move(options));

  AssertUnionMembers(
    u, {irs::Type<irs::analysis::TextTokenizer>::id(),
        irs::Type<irs::analysis::NGramTokenizer<
          irs::analysis::NGramTokenizerBase::InputType::UTF8>>::id()});
}

TEST(union_tokenizer_test, json_construction) {
  std::string config =
    R"({"union":[
      {"type":"text", "properties":{"locale":"en_US.UTF-8","stopwords":[],
       "case":"lower","stemming":false}},
      {"type":"ngram", "properties":{"min":3,"max":3,
       "preserveOriginal":false}}
    ]})";
  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<irs::analysis::UnionTokenizer>::id(), stream->type());

  const UnionTokens expected{
    {"hello", 1, 1}, {"hel", 0, 1}, {"world", 1, 2}, {"ell", 0, 2},
    {"llo", 1, 3},   {"lo ", 1, 4}, {"o w", 1, 5},   {" wo", 1, 6},
    {"wor", 1, 7},   {"orl", 1, 8}, {"rld", 1, 9},
  };
  AssertUnion(stream.get(), "hello world", expected);
}

TEST(union_tokenizer_test, vpack_construction) {
  std::string json_config =
    R"({"union":[
      {"type":"text", "properties":{"locale":"en_US.UTF-8","stopwords":[],
       "case":"lower","stemming":false}},
      {"type":"ngram", "properties":{"min":3,"max":3,
       "preserveOriginal":false}}
    ]})";
  auto vpack = vpack::Parser::fromJson(json_config);

  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::VPack>::get(),
    {vpack->slice().startAs<char>(), vpack->slice().byteSize()});
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<irs::analysis::UnionTokenizer>::id(), stream->type());
}

TEST(union_tokenizer_test, normalize_json) {
  // Unknown top-level parameters should be stripped
  {
    std::string config =
      R"({"unknown_param":123, "union":[
        {"type":"delimiter","properties":{"delimiter":"A"}}]})";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "union", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(
      vpack::Parser::fromJson(
        R"({"union":[{"type":"delimiter","properties":{"delimiter":"A"}}]})")
        ->toString(),
      actual);
  }
  // Unknown member parameters should be stripped
  {
    std::string config =
      R"({"union":[{"unknown":1,"type":"delimiter",
        "properties":{"delimiter":"A"}}]})";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "union", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(
      vpack::Parser::fromJson(
        R"({"union":[{"type":"delimiter","properties":{"delimiter":"A"}}]})")
        ->toString(),
      actual);
  }
  // Unknown analyzer type: normalization fails
  {
    std::string config =
      R"({"union":[{"type":"UNKNOWN","properties":{"foo":"bar"}}]})";
    std::string actual;
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      actual, "union", irs::Type<irs::text_format::Json>::get(), config));
  }
}

// Invalid config tests

TEST(union_tokenizer_test, construct_empty_array) {
  std::string config = R"({"union":[]})";
  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(union_tokenizer_test, construct_missing_union_key) {
  std::string config = R"({"pipeline":[{"type":"delimiter",
    "properties":{"delimiter":"A"}}]})";
  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(union_tokenizer_test, construct_invalid_child_type) {
  std::string config = R"({"union":[{"type":"NONEXISTENT","properties":{}}]})";
  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(union_tokenizer_test, construct_non_string_type) {
  std::string config =
    R"({"union":[{"type":1, "properties":{"delimiter":"A"}}]})";
  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(union_tokenizer_test, construct_no_properties) {
  std::string config = R"({"union":[{"type":"delimiter"}]})";
  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(union_tokenizer_test, construct_member_not_object) {
  std::string config = R"({"union":["not_an_object"]})";
  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(union_tokenizer_test, construct_union_not_array) {
  std::string config = R"({"union":"not_an_array"})";
  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::Json>::get(), config);
  ASSERT_EQ(nullptr, stream);
}

TEST(union_tokenizer_test, construct_null_args) {
  auto stream = irs::analysis::analyzers::Get(
    "union", irs::Type<irs::text_format::Json>::get(), std::string_view{});
  ASSERT_EQ(nullptr, stream);
}

TEST(union_tokenizer_test, reset_twice) {
  // Verify that reset() works correctly when called multiple times.
  auto text = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en_US.UTF-8", "stopwords":[], "case":"lower",
        "stemming":false})");
  ASSERT_NE(nullptr, text);

  irs::analysis::UnionTokenizer::OptionsT options;
  options.emplace_back(std::move(text));
  irs::analysis::UnionTokenizer u(std::move(options));

  {
    const UnionTokens expected{{"hello", 1, 1}, {"world", 1, 2}};
    AssertUnion(&u, "hello world", expected);
  }
  // Reset with different data
  {
    const UnionTokens expected{{"foo", 1, 1}};
    AssertUnion(&u, "foo", expected);
  }
}
