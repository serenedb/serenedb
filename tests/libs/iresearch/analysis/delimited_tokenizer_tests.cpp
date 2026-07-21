////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <cstring>

#include "gtest/gtest.h"
#include "iresearch/analysis/batch/term_view.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "tests_config.hpp"
#include "token_sink_utils.hpp"

namespace {

class DelimitedTokenizerTests : public ::testing::Test {
  void SetUp() final {
    // Code here will be called immediately after the constructor (right before
    // each test).
  }

  void TearDown() final {
    // Code here will be called immediately after each test (right before the
    // destructor).
  }
};

}  // namespace

TEST_F(DelimitedTokenizerTests, consts) {
  static_assert("delimiter" ==
                irs::Type<irs::analysis::DelimitedTokenizer>::name());
}

namespace {

struct BlockToken {
  std::string term;
  uint32_t start;
  uint32_t end;
};

void AssertBlockTokens(irs::analysis::Tokenizer& stream, std::string_view data,
                       const std::vector<BlockToken>& expected) {
  size_t tok = 0;
  const auto check = [&](irs::TokenBatch& batch,
                         std::span<const irs::DocRun> /*runs*/) {
    ASSERT_TRUE(batch.dense_pos);
    for (uint32_t i = 0; i < batch.count; ++i, ++tok) {
      SCOPED_TRACE(testing::Message() << "token=" << tok);
      ASSERT_LT(tok, expected.size());
      const auto& t = batch.terms[i];
      ASSERT_EQ(expected[tok].term, std::string_view(t.GetData(), t.GetSize()));
      ASSERT_EQ(expected[tok].start, batch.offs_start[i]);
      ASSERT_EQ(expected[tok].end, batch.offs_end[i]);
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
  ASSERT_TRUE(stream.Fill(data, sink.writer, sink.layout));
  sink.writer.Finish();
  ASSERT_EQ(expected.size(), tok);
}

void AssertBlockTokens(std::string_view delim, std::string_view data,
                       const std::vector<BlockToken>& expected) {
  irs::analysis::DelimitedTokenizer stream(delim);
  AssertBlockTokens(stream, data, expected);
}

}  // namespace

TEST_F(DelimitedTokenizerTests, test_delimiter) {
  // test delimiter std::string_view{}
  {
    irs::analysis::DelimitedTokenizer stream(std::string_view{});
    ASSERT_EQ(irs::Type<irs::analysis::DelimitedTokenizer>::id(),
              stream.type());
    AssertBlockTokens(stream, "abc,def\"\",\"\"ghi",
                      {{"abc,def\"\",\"\"ghi", 0, 15}});
  }

  // test delimiter ''
  AssertBlockTokens(
    "", "abc,\"def\"",
    {{"a", 0, 1}, {"b", 1, 2}, {"c", 2, 3}, {",", 3, 4}, {"def", 4, 9}});

  // test delimiter ','
  AssertBlockTokens(",", "abc,\"def,\"", {{"abc", 0, 3}, {"def,", 4, 10}});

  // test delimiter '\t'
  AssertBlockTokens("\t", "abc,\t\"def\t\"",
                    {{"abc,", 0, 4}, {"def\t", 5, 11}});

  // test delimiter '"'
  AssertBlockTokens(
    "\"", "abc,\"\"def\t\"",
    {{"abc,", 0, 4}, {"", 5, 5}, {"def\t", 6, 10}, {"", 11, 11}});

  // test delimiter 'abc'
  AssertBlockTokens("123", "abc,123\"def123\"",
                    {{"abc,", 0, 4}, {"def123", 7, 15}});
}

TEST_F(DelimitedTokenizerTests, test_quote) {
  // test quoted field
  {
    const std::vector<BlockToken> expected = {
      {"abc", 0, 3}, {"def", 4, 9}, {"\"\"ghi", 10, 15}};
    AssertBlockTokens(",", "abc,\"def\",\"\"ghi", expected);
    auto stream = irs::analysis::DelimitedTokenizer::Make(
      irs::analysis::DelimitedTokenizer::Options{.delimiter = ","});
    AssertBlockTokens(*stream, "abc,\"def\",\"\"ghi", expected);
  }

  // test unterminated "
  AssertBlockTokens(",", "abc,\"def\",\"ghi",
                    {{"abc", 0, 3}, {"def", 4, 9}, {"\"ghi", 10, 14}});

  // test unterminated single "
  AssertBlockTokens(",", "abc,\"def\",\"",
                    {{"abc", 0, 3}, {"def", 4, 9}, {"\"", 10, 11}});

  // test " escape
  AssertBlockTokens(",", "abc,\"\"\"def\",\"\"ghi",
                    {{"abc", 0, 3}, {"\"def", 4, 11}, {"\"\"ghi", 12, 17}});

  // test non-quoted field with "
  AssertBlockTokens(",", "abc,\"def\",ghi\"",
                    {{"abc", 0, 3}, {"def", 4, 9}, {"ghi\"", 10, 14}});
}

TEST_F(DelimitedTokenizerTests, test_load) {
  // happy path -- explicit delimiter.
  {
    std::string_view data("abc,def,ghi");  // quoted terms should be honoured
    auto stream = irs::analysis::DelimitedTokenizer::Make(
      irs::analysis::DelimitedTokenizer::Options{.delimiter = ","});

    ASSERT_NE(nullptr, stream);

    auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected = {
      {"abc", 1, 0, 3}, {"def", 2, 4, 7}, {"ghi", 3, 8, 11}};
    ASSERT_EQ(expected, *tokens);
  }

  // .........................................................................
  // The old JSON-only invalid cases ("{}", "[]", "1", `{"delimiter":1}`,
  // empty string) are all parser-level rejections with no direct-API
  // analogue: with the Options API the delimiter is a `std::string` so the
  // only way to construct it "invalid" is to leave it empty -- which the
  // analyzer intentionally treats as the "no delimiter" sentinel (the whole
  // input becomes a single token), not an error. Construction with default
  // Options must therefore succeed, so we explicitly cover the default-
  // constructed case here as a regression guard.
  // .........................................................................
  {
    auto stream = irs::analysis::DelimitedTokenizer::Make(
      irs::analysis::DelimitedTokenizer::Options{});
    ASSERT_NE(nullptr, stream);
  }
}

namespace {

struct PulledToken {
  irs::bstring term;
  uint32_t start;
  uint32_t end;
};

std::vector<PulledToken> PullTokens(irs::analysis::DelimitedTokenizer& stream,
                                    std::string_view value) {
  std::vector<PulledToken> out;
  auto tokens = tests::Analyze(stream, value);
  EXPECT_TRUE(tokens.has_value());
  if (!tokens) {
    return out;
  }
  for (auto& t : *tokens) {
    out.push_back(
      {irs::bstring{reinterpret_cast<const irs::byte_type*>(t.term.data()),
                    t.term.size()},
       t.offs_start, t.offs_end});
  }
  return out;
}

void AssertFillsMatchPull(std::string_view delim,
                          const std::vector<std::string>& values) {
  irs::analysis::DelimitedTokenizer pull_stream{delim};
  std::vector<std::vector<PulledToken>> expected;
  for (const auto& v : values) {
    expected.push_back(PullTokens(pull_stream, v));
  }

  irs::analysis::DelimitedTokenizer stream{delim};

  for (size_t v = 0; v < values.size(); ++v) {
    size_t tok = 0;
    const auto check = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> /*runs*/) {
      ASSERT_TRUE(batch.dense_pos);
      for (uint32_t i = 0; i < batch.count; ++i, ++tok) {
        SCOPED_TRACE(testing::Message() << "value=" << v << " token=" << tok);
        ASSERT_LT(tok, expected[v].size());
        const auto& t = batch.terms[i];
        ASSERT_EQ(
          expected[v][tok].term,
          irs::bstring(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                       t.GetSize()));
        ASSERT_EQ(expected[v][tok].start, batch.offs_start[i]);
        ASSERT_EQ(expected[v][tok].end, batch.offs_end[i]);
      }
    };
    tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
    ASSERT_TRUE(stream.Fill(values[v], sink.writer, sink.layout));
    sink.writer.Finish();
    ASSERT_EQ(expected[v].size(), tok);
  }

  {
    std::vector<duckdb::string_t> vals;
    std::vector<irs::doc_id_t> docs;
    for (size_t i = 0; i < values.size(); ++i) {
      vals.emplace_back(values[i].data(),
                        static_cast<uint32_t>(values[i].size()));
      docs.push_back(static_cast<irs::doc_id_t>(i + 1));
    }
    std::vector<std::vector<irs::bstring>> got(values.size());
    const auto collect = [&](irs::TokenBatch& batch,
                             std::span<const irs::DocRun> runs) {
      uint32_t tok = 0;
      for (const auto& run : runs) {
        if (run.doc == irs::DocRun::kOpenValue) {
          continue;
        }
        for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
          const auto& t = batch.terms[tok];
          got[run.doc - 1].emplace_back(
            reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
        }
      }
      ASSERT_EQ(batch.count, tok);
    };
    tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
    stream.Fill(vals, docs, sink.writer, sink.layout);
    sink.writer.Finish();
    for (size_t v = 0; v < values.size(); ++v) {
      SCOPED_TRACE(testing::Message() << "column value=" << v);
      ASSERT_EQ(expected[v].size(), got[v].size());
      for (size_t t = 0; t < got[v].size(); ++t) {
        ASSERT_EQ(expected[v][t].term, got[v][t]);
      }
    }
  }
}

}  // namespace

TEST_F(DelimitedTokenizerTests, native_fills_match_pull) {
  AssertFillsMatchPull(
    ",", {"a,bb,ccc", "", "single", ",", "a,,b,", "trailing,",
          std::string(100, 'x') + "," + std::string(40, 'y'),
          "\"quoted,term\",plain", "\"esc\"\"aped\",tail", "\"mismatched,still",
          "no-delims-here-at-all"});

  AssertFillsMatchPull("||", {"a||bb||ccc", "x", "||", "a||"});

  AssertFillsMatchPull(" ", {"alpha beta gamma", "  double  spaces "});

  std::string huge;
  for (size_t i = 0; i < 1500; ++i) {
    huge += "t" + std::to_string(i % 97) + ",";
  }
  huge.pop_back();
  AssertFillsMatchPull(",", {huge, "after,split"});
}

TEST_F(DelimitedTokenizerTests, column_suspension) {
  constexpr size_t kCap = irs::TokenBatch::kCapacity;

  size_t id = 0;
  const auto make_doc = [&](size_t n) {
    std::vector<std::string> terms(n);
    for (auto& t : terms) {
      t = "t" + std::to_string(id++);
    }
    return terms;
  };

  std::vector<std::vector<std::string>> expected;
  expected.push_back(make_doc(kCap));
  expected.push_back(make_doc(2));
  expected.push_back(make_doc(kCap + 2));
  expected.push_back(make_doc(3 * kCap + 5));
  expected.push_back(make_doc(kCap + 50));
  const size_t kQuotedDoc = 4;

  std::vector<std::string> values;
  for (size_t v = 0; v < expected.size(); ++v) {
    std::string s;
    for (const auto& t : expected[v]) {
      s += v == kQuotedDoc ? "\"" + t + "\"," : t + ",";
    }
    s.pop_back();
    values.push_back(std::move(s));
  }

  std::vector<duckdb::string_t> vals;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(i + 1));
  }

  irs::analysis::DelimitedTokenizer stream{","};
  std::vector<std::vector<std::string>> got(values.size());
  std::vector<uint32_t> off_cursor(values.size(), 0);
  size_t flushes = 0;

  const auto drain = [&](irs::TokenBatch& batch,
                         std::span<const irs::DocRun> runs, bool at_flush) {
    ASSERT_FALSE(runs.empty());
    const bool open = runs.back().doc == irs::DocRun::kOpenValue;
    ASSERT_TRUE(!open || at_flush);

    uint32_t tok = 0;
    for (size_t r = 0; r < runs.size(); ++r) {
      const auto& run = runs[r];
      if (run.doc == irs::DocRun::kOpenValue) {
        ASSERT_EQ(runs.size() - 1, r);
        continue;
      }
      const size_t v = run.doc - 1;
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        got[v].emplace_back(t.GetData(), t.GetSize());
        if (v != kQuotedDoc) {
          ASSERT_EQ(off_cursor[v], batch.offs_start[tok]);
          ASSERT_EQ(off_cursor[v] + t.GetSize(), batch.offs_end[tok]);
          off_cursor[v] += t.GetSize() + 1;
        }
      }
    }
    ASSERT_EQ(batch.count, tok);

    const auto& last = open ? runs[runs.size() - 2] : runs.back();
    const bool mid_value =
      got[last.doc - 1].size() < expected[last.doc - 1].size();
    ASSERT_EQ(mid_value, open);
  };

  const auto on_flush = [&](irs::TokenBatch& batch,
                            std::span<const irs::DocRun> runs) {
    ++flushes;
    SCOPED_TRACE(testing::Message() << "flush=" << flushes);
    ASSERT_EQ(kCap, batch.count);
    drain(batch, runs, true);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, on_flush};
  stream.Fill(vals, docs, sink.writer, sink.layout);
  drain(sink.writer.buf, sink.writer.Runs(), false);

  ASSERT_EQ(6, flushes);
  ASSERT_EQ(expected, got);
}

TEST(TermViewTest, null_empty_term) {
  const duckdb::string_t expected{static_cast<const char*>(nullptr), 0};
  const duckdb::string_t actual =
    irs::MakeTermView(static_cast<const char*>(nullptr), 0);
  ASSERT_EQ(0, std::memcmp(&expected, &actual, sizeof expected));
  ASSERT_EQ(0u, actual.GetSize());
}

TEST(TermViewTest, matches_duckdb_ctor) {
  std::string buf;
  for (size_t i = 0; i < 64; ++i) {
    buf += static_cast<char>('!' + (i * 7) % 90);
  }
  for (uint32_t size = 0; size <= 20; ++size) {
    for (uint32_t off : {0u, 1u, 3u, 13u}) {
      SCOPED_TRACE(testing::Message() << "size=" << size << " off=" << off);
      const char* data = buf.data() + off;
      const duckdb::string_t expected{data, size};
      const duckdb::string_t actual = irs::MakeTermView(data, size);
      ASSERT_EQ(0, std::memcmp(&expected, &actual, sizeof expected));
      ASSERT_TRUE(expected == actual);
      ASSERT_EQ(expected.GetSize(), actual.GetSize());
      ASSERT_EQ(0, std::memcmp(expected.GetData(), actual.GetData(), size));
    }
  }
}
