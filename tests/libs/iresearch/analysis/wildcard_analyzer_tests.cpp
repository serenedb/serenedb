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

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

std::unique_ptr<irs::analysis::WildcardAnalyzer> MakeWildcard(
  size_t ngram_size, std::string_view base_delimiter = {}) {
  irs::analysis::Tokenizer::ptr base;
  if (!base_delimiter.empty()) {
    base = std::make_unique<irs::analysis::DelimitedTokenizer>(base_delimiter);
  }
  return std::make_unique<irs::analysis::WildcardAnalyzer>(std::move(base),
                                                           ngram_size);
}

std::vector<irs::bstring> PullTokens(irs::analysis::WildcardAnalyzer& stream,
                                     std::string_view data) {
  std::vector<irs::bstring> out;
  const auto terms = tests::AnalyzeTerms(stream, data);
  if (!terms) {
    return out;
  }
  for (const auto& t : *terms) {
    out.emplace_back(reinterpret_cast<const irs::byte_type*>(t.data()),
                     t.size());
  }
  return out;
}

std::vector<irs::bstring> FillTokens(irs::analysis::WildcardAnalyzer& stream,
                                     std::string_view data, size_t* flushes) {
  std::vector<irs::bstring> out;
  bool finishing = false;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    EXPECT_TRUE(runs.empty());
    if (!finishing) {
      if (flushes != nullptr) {
        ++*flushes;
      }
      EXPECT_EQ(irs::TokenBatch::kCapacity, batch.count);
    }
    EXPECT_TRUE(stream.Traits().dense_pos);
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      out.emplace_back(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                       t.GetSize());
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, collect};
  if (!stream.Fill(data, sink.writer, sink.layout)) {
    return out;
  }
  finishing = true;
  sink.writer.Finish();
  return out;
}

void AssertFillsMatchPull(size_t ngram_size, std::string_view base_delimiter,
                          const std::vector<std::string>& values) {
  auto pull_stream = MakeWildcard(ngram_size, base_delimiter);
  auto fill_stream = MakeWildcard(ngram_size, base_delimiter);
  for (const auto& v : values) {
    SCOPED_TRACE(testing::Message()
                 << "n=" << ngram_size << " delim='" << base_delimiter
                 << "' value.size=" << v.size());
    const auto pulled = PullTokens(*pull_stream, v);
    const auto filled = FillTokens(*fill_stream, v, nullptr);
    ASSERT_EQ(pulled.size(), filled.size());
    for (size_t i = 0; i < pulled.size(); ++i) {
      SCOPED_TRACE(testing::Message() << "token=" << i);
      ASSERT_EQ(pulled[i], filled[i]);
    }
  }
}

}  // namespace

TEST(wildcard_analyzer_tests, traits) {
  auto stream = MakeWildcard(3);
  ASSERT_FALSE(stream->Traits().offsets);
  ASSERT_TRUE(stream->Traits().store);
}

TEST(wildcard_analyzer_tests, native_fills_match_pull) {
  const std::vector<std::string> values = {
    "",
    "a",
    "ab",
    "abc",
    "abcd",
    "quick brown fox",
    "a\xc2\xa2"
    "b\xc2\xa3"
    "c\xc2\xa4",
    "\xe2\x82\xac\xe2\x82\xac",
    "\xf0\x9f\x98\x80x\xf0\x9f\x98\x81",
    "abc\xc2",
    "\x80\x81stray-continuations",
    std::string(100, 'x'),
    std::string(40, 'z') + "\xc2\xa2" + std::string(40, 'w')};

  for (const size_t n : {1, 2, 3, 5}) {
    AssertFillsMatchPull(n, {}, values);
  }
}

TEST(wildcard_analyzer_tests, native_fills_match_pull_multi_term_base) {
  std::string many;
  for (size_t i = 0; i < 400; ++i) {
    many += "term" + std::to_string(i) + " ";
  }
  many.pop_back();

  const std::vector<std::string> values = {
    "alpha beta gamma", "single", "", "a b c", "caf\xc3\xa9 men\xc3\xba", many};

  for (const size_t n : {1, 3}) {
    AssertFillsMatchPull(n, " ", values);
  }
}

TEST(wildcard_analyzer_tests, fill_flushes_across_batches) {
  std::string many;
  for (size_t i = 0; i < 600; ++i) {
    many += "token" + std::to_string(i) + " ";
  }
  many.pop_back();

  auto pull_stream = MakeWildcard(3, " ");
  auto fill_stream = MakeWildcard(3, " ");
  const auto pulled = PullTokens(*pull_stream, many);
  ASSERT_GT(pulled.size(), 2 * irs::TokenBatch::kCapacity);

  size_t flushes = 0;
  const auto filled = FillTokens(*fill_stream, many, &flushes);
  ASSERT_EQ(pulled.size() / irs::TokenBatch::kCapacity, flushes);
  ASSERT_EQ(pulled, filled);
}

TEST(wildcard_analyzer_tests, store_survives_fill_reset) {
  auto stream = MakeWildcard(3);
  ASSERT_TRUE(stream->Traits().store);
  irs::TokenCollector sink{irs::TokenLayout::Terms};
  ASSERT_TRUE(stream->Fill("hello", sink.writer, sink.layout));
  sink.writer.Finish();
  const irs::bstring stored{sink.store};
  ASSERT_FALSE(stored.empty());

  auto other_stream = MakeWildcard(3);
  irs::TokenCollector other_sink{irs::TokenLayout::Terms};
  ASSERT_TRUE(
    other_stream->Fill("hello", other_sink.writer, other_sink.layout));
  other_sink.writer.Finish();
  ASSERT_EQ(other_sink.store, stored);
}
