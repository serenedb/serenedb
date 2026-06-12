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

#include <set>
#include <string>

#include "iresearch/analysis/sparse_ngram_tokenizer.hpp"
#include "tests_shared.hpp"

namespace {

std::set<std::string> Grams(std::initializer_list<std::string> grams) {
  return {grams};
}

std::set<std::string> Collect(std::string_view data, bool covering,
                              size_t max_ngram_length = 16) {
  auto stream = irs::analysis::SparseNGramTokenizer::Make(
    irs::analysis::SparseNGramTokenizer::Options{
      .max_ngram_length = max_ngram_length, .covering = covering});
  EXPECT_NE(nullptr, stream);
  EXPECT_TRUE(stream->reset(data));
  const auto* term = irs::get<irs::TermAttr>(*stream);
  EXPECT_NE(nullptr, term);
  std::set<std::string> out;
  while (stream->next()) {
    out.emplace(irs::ViewCast<char>(irs::bytes_view{term->value}));
  }
  return out;
}

TEST(sparse_ngram_tokenizer_test, consts) {
  static_assert("sparse_ngram" ==
                irs::Type<irs::analysis::SparseNGramTokenizer>::name());
}

TEST(sparse_ngram_tokenizer_test, attributes) {
  auto stream = irs::analysis::SparseNGramTokenizer::Make({});
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<irs::analysis::SparseNGramTokenizer>::id(),
            stream->type());
  ASSERT_NE(nullptr, irs::get<irs::TermAttr>(*stream));
  ASSERT_NE(nullptr, irs::get<irs::IncAttr>(*stream));
  ASSERT_EQ(nullptr, irs::get<irs::OffsAttr>(*stream));
}

TEST(sparse_ngram_tokenizer_test, all_ngrams_simple) {
  EXPECT_EQ(Collect("", false), Grams({}));
  EXPECT_EQ(Collect("he", false), Grams({}));
  EXPECT_EQ(Collect("hel", false), Grams({"hel"}));
  EXPECT_EQ(Collect("hell", false), Grams({"hel", "ell"}));
  EXPECT_EQ(Collect("hello world", false),
            Grams({"hel", "ell", "llo", "lo ", "o w", "lo w", " wo", "lo wo",
                   "wor", "orl", "worl", "rld"}));
}

TEST(sparse_ngram_tokenizer_test, covering_simple) {
  EXPECT_EQ(Collect("he", true), Grams({}));
  EXPECT_EQ(Collect("hel", true), Grams({"hel"}));
  EXPECT_EQ(Collect("hell", true), Grams({"hel", "ell"}));
  EXPECT_EQ(Collect("hello world", true),
            Grams({"hel", "ell", "llo", "rld", "worl", "lo wo"}));
}

TEST(sparse_ngram_tokenizer_test, split_github_codesearch) {
  EXPECT_EQ(
    Collect("chester ", false),
    Grams({"che", "hes", "ches", "est", "chest", "ste", "ter", "ster", "er "}));
  EXPECT_EQ(Collect("chester ", true), Grams({"chest", "ster", "er "}));
  EXPECT_EQ(Collect("chester", true), Grams({"chest", "ster"}));
}

TEST(sparse_ngram_tokenizer_test, split_for_loop) {
  EXPECT_EQ(
    Collect("for(int i=42", false),
    Grams({"for", "or(", "for(", "r(i", "for(i", "(in", "int", "(int", "nt ",
           "t i", " i=", "t i=", "i=4", "t i=4", "nt i=4", "(int i=4", "=42"}));
  EXPECT_EQ(Collect("for(int i=42", true), Grams({"for(i", "(int i=4", "=42"}));
}

TEST(sparse_ngram_tokenizer_test, covering_subset_of_all) {
  const std::string_view code =
    "for (size_t i = 0; i < n; ++i) { sum += data[i] * data[i]; }";
  const auto all = Collect(code, false);
  for (size_t begin = 0; begin < code.size(); ++begin) {
    for (size_t len = 3; begin + len <= code.size(); ++len) {
      for (const auto& gram : Collect(code.substr(begin, len), true)) {
        EXPECT_TRUE(all.contains(gram))
          << "covering gram '" << gram << "' of substring '"
          << code.substr(begin, len) << "' missing from index grams";
      }
    }
  }
}

TEST(sparse_ngram_tokenizer_test, covering_max_ngram_length) {
  for (const auto max_len : {size_t{3}, size_t{5}, size_t{8}, size_t{16}}) {
    for (const auto& gram :
         Collect("abcdefghijklmnopqrstuvwxyz0123456789", true, max_len)) {
      EXPECT_LE(gram.size(), max_len);
    }
  }
}

TEST(sparse_ngram_tokenizer_test, all_ngrams_max_ngram_length) {
  for (const auto max_len : {size_t{3}, size_t{5}, size_t{16}}) {
    for (const auto& gram :
         Collect("abcdefghijklmnopqrstuvwxyz0123456789", false, max_len)) {
      EXPECT_LE(gram.size(), max_len);
    }
  }
}

TEST(sparse_ngram_tokenizer_test, increments) {
  auto stream = irs::analysis::SparseNGramTokenizer::Make({});
  ASSERT_TRUE(stream->reset("hello world"));
  const auto* inc = irs::get<irs::IncAttr>(*stream);
  ASSERT_NE(nullptr, inc);
  size_t count = 0;
  while (stream->next()) {
    ASSERT_EQ(1, inc->value);
    ++count;
  }
  ASSERT_EQ(12, count);
}

TEST(sparse_ngram_tokenizer_test, reset_reuse) {
  auto stream = irs::analysis::SparseNGramTokenizer::Make({});
  ASSERT_TRUE(stream->reset("hello world"));
  size_t count = 0;
  while (stream->next()) {
    ++count;
  }
  ASSERT_EQ(12, count);
  ASSERT_TRUE(stream->reset("hel"));
  const auto* term = irs::get<irs::TermAttr>(*stream);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ("hel", irs::ViewCast<char>(irs::bytes_view{term->value}));
  ASSERT_FALSE(stream->next());
}

}  // namespace
