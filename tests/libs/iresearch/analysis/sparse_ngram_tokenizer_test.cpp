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

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/sparse_ngram_tokenizer.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

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
  std::set<std::string> out;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> /*runs*/) {
    EXPECT_TRUE(stream->Traits().dense_pos);
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      out.emplace(t.GetData(), t.GetSize());
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, collect};
  EXPECT_TRUE(stream->Fill(data, sink.writer, sink.layout));
  sink.writer.Finish();
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
  ASSERT_TRUE(stream->Traits().offsets);
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
  auto tokens = tests::Analyze(*stream, "hello world");
  ASSERT_TRUE(tokens.has_value());
  ASSERT_EQ(12, tokens->size());
  for (size_t i = 0; i < tokens->size(); ++i) {
    ASSERT_EQ(i + 1, (*tokens)[i].pos);
  }
}

TEST(sparse_ngram_tokenizer_test, reset_reuse) {
  auto stream = irs::analysis::SparseNGramTokenizer::Make({});
  auto first = tests::AnalyzeTerms(*stream, "hello world");
  ASSERT_TRUE(first.has_value());
  ASSERT_EQ(12, first->size());
  auto second = tests::AnalyzeTerms(*stream, "hel");
  ASSERT_TRUE(second.has_value());
  ASSERT_EQ(std::vector<std::string>{"hel"}, *second);
}

}  // namespace

TEST(sparse_ngram_tokenizer_test, native_fills_match_pull) {
  std::string huge;
  for (size_t i = 0; i < 2000; ++i) {
    huge += "int x" + std::to_string(i) + " = " + std::to_string(i * 7) + ";\n";
  }
  const std::vector<std::string> values = {
    "",  "a", "ab", "abc", "for (int i = 0; i < n; i++)", std::string(30, 'a'),
    huge};

  for (const bool covering : {false, true}) {
    for (const size_t max_len : {size_t{4}, size_t{16}}) {
      irs::analysis::SparseNGramTokenizer::Options opts{
        .max_ngram_length = max_len, .covering = covering};
      auto pull_stream = irs::analysis::SparseNGramTokenizer::Make(
        irs::analysis::SparseNGramTokenizer::Options{opts});
      auto fill_stream = irs::analysis::SparseNGramTokenizer::Make(
        irs::analysis::SparseNGramTokenizer::Options{opts});
      for (const auto& v : values) {
        SCOPED_TRACE(testing::Message()
                     << "covering=" << covering << " max=" << max_len
                     << " value.size=" << v.size());
        auto pulled_tokens = tests::Analyze(*pull_stream, v);
        ASSERT_TRUE(pulled_tokens.has_value());
        std::vector<std::string> pulled;
        std::vector<uint32_t> pstarts;
        std::vector<uint32_t> pends;
        for (auto& t : *pulled_tokens) {
          pulled.push_back(std::move(t.term));
          pstarts.push_back(t.offs_start);
          pends.push_back(t.offs_end);
        }

        std::vector<std::string> filled;
        std::vector<uint32_t> fstarts;
        std::vector<uint32_t> fends;
        const auto collect = [&](irs::TokenBatch& batch,
                                 std::span<const irs::DocRun> /*runs*/) {
          for (uint32_t i = 0; i < batch.count; ++i) {
            const auto& t = batch.terms[i];
            filled.emplace_back(t.GetData(), t.GetSize());
            fstarts.push_back(batch.offs_start[i]);
            fends.push_back(batch.offs_end[i]);
          }
        };
        tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
        ASSERT_TRUE(fill_stream->Fill(v, sink.writer, sink.layout));
        sink.writer.Finish();

        ASSERT_EQ(pulled, filled);
        ASSERT_EQ(pstarts, fstarts);
        ASSERT_EQ(pends, fends);
      }
    }
  }
}
