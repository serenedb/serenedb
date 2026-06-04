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

#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
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

irs::analysis::Analyzer::ptr MakeText(std::string_view locale,
                                      irs::Case case_convert, bool stemming,
                                      std::vector<std::string> stopwords = {}) {
  irs::analysis::TextTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName(std::string(locale).c_str());
  opts.case_convert = case_convert;
  opts.stemming = stemming;
  for (auto& w : stopwords) {
    opts.explicit_stopwords.insert(std::move(w));
  }
  opts.explicit_stopwords_set = true;
  return irs::analysis::TextTokenizer::Make(std::move(opts));
}

irs::analysis::Analyzer::ptr MakeNgram(size_t min_gram, size_t max_gram,
                                       bool preserve_original) {
  return irs::analysis::NGramTokenizerBase::Make(
    irs::analysis::NGramTokenizerBase::Options{
      .min_gram = min_gram,
      .max_gram = max_gram,
      .preserve_original = preserve_original,
    });
}

}  // namespace

TEST(union_tokenizer_test, consts) {
  static_assert("union" == irs::Type<irs::analysis::UnionTokenizer>::name());
}

TEST(union_tokenizer_test, empty_union) {
  std::vector<irs::analysis::Analyzer::ptr> options;
  irs::analysis::UnionTokenizer u(std::move(options));

  std::string data = "hello world";
  ASSERT_FALSE(u.reset(data));
}

TEST(union_tokenizer_test, single_sub) {
  auto text = MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/false);
  ASSERT_NE(nullptr, text);

  std::vector<irs::analysis::Analyzer::ptr> options;
  options.emplace_back(std::move(text));
  irs::analysis::UnionTokenizer u(std::move(options));

  const UnionTokens expected{
    {"hello", 1, 1},
    {"world", 1, 2},
  };
  AssertUnion(&u, "hello world", expected);
}

TEST(union_tokenizer_test, text_plus_ngram) {
  auto text = MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/false);
  ASSERT_NE(nullptr, text);

  auto ngram = MakeNgram(3, 3, /*preserve_original=*/false);
  ASSERT_NE(nullptr, ngram);

  std::vector<irs::analysis::Analyzer::ptr> options;
  options.emplace_back(std::move(text));
  options.emplace_back(std::move(ngram));
  irs::analysis::UnionTokenizer u(std::move(options));

  const UnionTokens expected{
    {"hello", 1, 1}, {"hel", 0, 1}, {"world", 1, 2}, {"ell", 0, 2},
    {"llo", 1, 3},   {"lo ", 1, 4}, {"o w", 1, 5},   {" wo", 1, 6},
    {"wor", 1, 7},   {"orl", 1, 8}, {"rld", 1, 9},
  };
  AssertUnion(&u, "hello world", expected);
}

TEST(union_tokenizer_test, two_text_tokenizers_homogeneous) {
  auto text_lower =
    MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/false);
  ASSERT_NE(nullptr, text_lower);

  auto text_none = MakeText("en_US.UTF-8", irs::Case::None, /*stemming=*/false);
  ASSERT_NE(nullptr, text_none);

  std::vector<irs::analysis::Analyzer::ptr> options;
  options.emplace_back(std::move(text_lower));
  options.emplace_back(std::move(text_none));
  irs::analysis::UnionTokenizer u(std::move(options));

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
  auto text = MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/false,
                       {"quick", "brown", "fox"});
  ASSERT_NE(nullptr, text);

  auto text2 = MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/false);
  ASSERT_NE(nullptr, text2);

  std::vector<irs::analysis::Analyzer::ptr> options;
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
  auto text = MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/false);
  ASSERT_NE(nullptr, text);

  std::vector<irs::analysis::Analyzer::ptr> options;
  options.emplace_back(std::move(text));
  irs::analysis::UnionTokenizer u(std::move(options));

  auto* offset = irs::get<irs::OffsAttr>(u);
  ASSERT_EQ(nullptr, offset);
}

TEST(union_tokenizer_test, get_mutable_non_core) {
  auto text = MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/false);
  ASSERT_NE(nullptr, text);

  std::vector<irs::analysis::Analyzer::ptr> options;
  options.emplace_back(std::move(text));
  irs::analysis::UnionTokenizer u(std::move(options));

  ASSERT_NE(nullptr, irs::get<irs::TermAttr>(u));
  ASSERT_NE(nullptr, irs::get<irs::IncAttr>(u));
  ASSERT_EQ(nullptr, irs::get<irs::OffsAttr>(u));
}

TEST(union_tokenizer_test, VisitMembers) {
  auto text = MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/false);
  auto ngram = MakeNgram(2, 3, /*preserve_original=*/false);

  std::vector<irs::analysis::Analyzer::ptr> options;
  options.emplace_back(std::move(text));
  options.emplace_back(std::move(ngram));
  irs::analysis::UnionTokenizer u(std::move(options));

  AssertUnionMembers(
    u, {irs::Type<irs::analysis::TextTokenizer>::id(),
        irs::Type<irs::analysis::NGramTokenizer<
          irs::analysis::NGramTokenizerBase::InputType::UTF8>>::id()});
}

TEST(union_tokenizer_test, options_construction) {
  irs::analysis::UnionTokenizer::Options opts;

  irs::analysis::TextTokenizer::Options text_opts;
  text_opts.locale = icu::Locale::createFromName("en_US.UTF-8");
  text_opts.case_convert = irs::Case::Lower;
  text_opts.stemming = false;
  text_opts.explicit_stopwords_set = true;
  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{std::move(text_opts)}));

  opts.children.push_back(std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{irs::analysis::NGramTokenizerBase::Options{
      .min_gram = 3,
      .max_gram = 3,
      .preserve_original = false,
    }}));

  auto stream = irs::analysis::UnionTokenizer::Make(std::move(opts));
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<irs::analysis::UnionTokenizer>::id(), stream->type());

  const UnionTokens expected{
    {"hello", 1, 1}, {"hel", 0, 1}, {"world", 1, 2}, {"ell", 0, 2},
    {"llo", 1, 3},   {"lo ", 1, 4}, {"o w", 1, 5},   {" wo", 1, 6},
    {"wor", 1, 7},   {"orl", 1, 8}, {"rld", 1, 9},
  };
  AssertUnion(stream.get(), "hello world", expected);
}

TEST(union_tokenizer_test, construct_empty_children) {
  // `Make` rejects a union with no children: there is nothing meaningful to
  // construct, so we fail loudly rather than return a degenerate analyzer
  // whose `reset()` always fails.
  irs::analysis::UnionTokenizer::Options opts;
  ASSERT_ANY_THROW(irs::analysis::UnionTokenizer::Make(std::move(opts)));
}

TEST(union_tokenizer_test, construct_null_child) {
  // Defensive: a children vector containing a null pointer is rejected.
  irs::analysis::UnionTokenizer::Options opts;
  opts.children.push_back(nullptr);
  ASSERT_ANY_THROW(irs::analysis::UnionTokenizer::Make(std::move(opts)));
}

// NOTE: The legacy `construct_missing_union_key`,
// `construct_invalid_child_type`, `construct_non_string_type`,
// `construct_no_properties`, `construct_member_not_object`,
// `construct_union_not_array`, `construct_null_args`, and `vpack_construction`
// tests all probed loader-side JSON/VPack parsing edge cases (wrong wrapper
// key, wrong field types, missing `properties`, etc.). The direct
// `Make(Options)` API doesn't accept a textual config -- there is no JSON
// parser to reject. These checks belong to the JSON loader's own tests; nothing
// analogous remains at the analyzer level.

TEST(union_tokenizer_test, reset_twice) {
  auto text = MakeText("en_US.UTF-8", irs::Case::Lower, /*stemming=*/false);
  ASSERT_NE(nullptr, text);

  std::vector<irs::analysis::Analyzer::ptr> options;
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
