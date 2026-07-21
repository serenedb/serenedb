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
#include "iresearch/analysis/split_by_non_alpha_tokenizer.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

using irs::analysis::SplitByNonAlphaTokenizer;

struct Tok {
  std::string term;
  uint32_t offs_start;
  uint32_t offs_end;
};

std::vector<Tok> Pull(irs::analysis::Tokenizer& a, std::string_view data) {
  std::vector<Tok> out;
  auto tokens = tests::Analyze(a, data);
  EXPECT_TRUE(tokens.has_value());
  if (!tokens) {
    return out;
  }
  for (auto& t : *tokens) {
    out.push_back({std::move(t.term), t.offs_start, t.offs_end});
  }
  return out;
}

}  // namespace

TEST(split_by_non_alpha_tokenizer_test, consts) {
  static_assert("split_by_non_alpha" ==
                irs::Type<SplitByNonAlphaTokenizer>::name());
}

TEST(split_by_non_alpha_tokenizer_test, basic_pull) {
  auto a = SplitByNonAlphaTokenizer::Make({});
  auto tokens = Pull(*a, "Hello, World! 123abc");
  ASSERT_EQ(3u, tokens.size());
  EXPECT_EQ("Hello", tokens[0].term);
  EXPECT_EQ(0u, tokens[0].offs_start);
  EXPECT_EQ(5u, tokens[0].offs_end);
  EXPECT_EQ("World", tokens[1].term);
  EXPECT_EQ("123abc", tokens[2].term);
  EXPECT_EQ(14u, tokens[2].offs_start);
  EXPECT_EQ(20u, tokens[2].offs_end);
}

TEST(split_by_non_alpha_tokenizer_test, to_lower_pull) {
  auto a = SplitByNonAlphaTokenizer::Make({.to_lower = true});
  auto tokens = Pull(*a, "Hello WORLD");
  ASSERT_EQ(2u, tokens.size());
  EXPECT_EQ("hello", tokens[0].term);
  EXPECT_EQ("world", tokens[1].term);
}

TEST(split_by_non_alpha_tokenizer_test, native_fill_matches_pull) {
  const std::vector<std::string> values = {"Hello, World! 123abc",
                                           "",
                                           "   ...   ",
                                           "one",
                                           "a-b-c-d-e",
                                           "Trailing punctuation here!!!",
                                           "UPPER lower MiXeD 42"};

  for (const bool to_lower : {false, true}) {
    SCOPED_TRACE(to_lower);
    auto pull_a = SplitByNonAlphaTokenizer::Make({.to_lower = to_lower});
    auto fill_a = SplitByNonAlphaTokenizer::Make({.to_lower = to_lower});

    for (const auto& v : values) {
      SCOPED_TRACE(v);
      const auto pulled = Pull(*pull_a, v);

      auto batch = std::make_unique<irs::TokenBatch>();
      std::vector<irs::DocRun> runs;
      std::vector<Tok> filled;
      const auto collect = [&](irs::TokenBatch& batch,
                               std::span<const irs::DocRun> /*runs*/) {
        EXPECT_TRUE(batch.dense_pos);
        for (uint32_t i = 0; i < batch.count; ++i) {
          const auto& t = batch.terms[i];
          filled.push_back({std::string{t.GetData(), t.GetSize()},
                            batch.offs_start[i], batch.offs_end[i]});
        }
      };
      tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
      ASSERT_TRUE(fill_a->Fill(v, sink.writer, sink.layout));
      sink.writer.Finish();

      ASSERT_EQ(pulled.size(), filled.size());
      for (size_t i = 0; i < pulled.size(); ++i) {
        SCOPED_TRACE(i);
        ASSERT_EQ(pulled[i].term, filled[i].term);
        ASSERT_EQ(pulled[i].offs_start, filled[i].offs_start);
        ASSERT_EQ(pulled[i].offs_end, filled[i].offs_end);
      }
    }
  }
}

TEST(split_by_non_alpha_tokenizer_test, column_fill_matches_pull) {
  const std::vector<std::string> raw = {"Hello World", "", "a1 b2 c3",
                                        "no-delimiters-here", "END"};
  std::vector<duckdb::string_t> values;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < raw.size(); ++i) {
    values.emplace_back(raw[i].data(), static_cast<uint32_t>(raw[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(100 + i));
  }

  auto pull_a = SplitByNonAlphaTokenizer::Make({.to_lower = true});
  auto fill_a = SplitByNonAlphaTokenizer::Make({.to_lower = true});

  tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
  fill_a->Fill(values, docs, sink.writer, sink.layout);
  ASSERT_FALSE(sink.flushed());
  auto& batch = sink.writer.buf;
  const auto runs = sink.writer.Runs();
  ASSERT_EQ(raw.size(), runs.size());

  uint32_t token_idx = 0;
  for (size_t v = 0; v < raw.size(); ++v) {
    SCOPED_TRACE(raw[v]);
    ASSERT_EQ(docs[v], runs[v].doc);
    const auto pulled = Pull(*pull_a, raw[v]);
    ASSERT_EQ(pulled.size(), runs[v].ntokens);
    for (const auto& expected : pulled) {
      const auto& t = batch.terms[token_idx];
      ASSERT_EQ(expected.term, (std::string{t.GetData(), t.GetSize()}));
      ASSERT_EQ(expected.offs_start, batch.offs_start[token_idx]);
      ASSERT_EQ(expected.offs_end, batch.offs_end[token_idx]);
      ++token_idx;
    }
  }
  ASSERT_EQ(batch.count, token_idx);
}
