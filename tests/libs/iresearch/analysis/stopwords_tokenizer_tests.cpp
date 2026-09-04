////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include <filesystem>
#include <fstream>

#include "gtest/gtest.h"
#include "iresearch/analysis/stopwords_tokenizer.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception.h"
#include "test_resources.hpp"
#include "token_sink_utils.hpp"

TEST(token_stopwords_stream_tests, consts) {
  static_assert("stopwords" ==
                irs::Type<irs::analysis::StopwordsTokenizer>::name());
}

namespace {

duckdb::shared_ptr<const irs::analysis::StopwordSet> Stopwords(
  std::vector<std::string> mask) {
  return irs::analysis::StopwordSet::GetOrBuild(
    tests::Cache(),
    duckdb::make_uniq<irs::analysis::StopwordSet>(std::move(mask)));
}

void AssertStopwordsBlock(irs::analysis::StopwordsTokenizer& stream,
                          std::string_view data, bool expect_pass) {
  size_t flushes = 0;
  const auto check = [&](irs::TokenBatch& batch, irs::DocRuns /*runs*/) {
    ++flushes;
    // A masked value still closes its bracket: one run, zero tokens.
    ASSERT_EQ(expect_pass ? 1 : 0, batch.count);
    if (!expect_pass) {
      return;
    }
    const auto& t = batch.terms[0];
    ASSERT_EQ(data, std::string_view(t.GetData(), t.GetSize()));
    ASSERT_EQ(0, batch.offs_start[0]);
    ASSERT_EQ(data.size(), batch.offs_end[0]);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
  ASSERT_TRUE(stream.Fill(tests::ToStringT(data), irs::doc_limits::min(),
                          sink.writer, {sink.layout}));
  sink.writer.Finish();
  ASSERT_EQ(1, flushes);
}

}  // namespace

TEST(token_stopwords_stream_tests, test_masking) {
  // test mask nothing
  {
    std::string_view data0("abc");
    std::string_view data1("ghi");
    irs::analysis::StopwordsTokenizer stream(Stopwords({}));
    ASSERT_EQ(irs::Type<irs::analysis::StopwordsTokenizer>::id(),
              stream.type());

    AssertStopwordsBlock(stream, data0, true);
    AssertStopwordsBlock(stream, data1, true);
  }

  // test mask something
  {
    std::string_view data0("abc");
    std::string_view data1("ghi");
    irs::analysis::StopwordsTokenizer stream(Stopwords({"abc"}));

    AssertStopwordsBlock(stream, data0, false);
    AssertStopwordsBlock(stream, data1, true);
  }
}

TEST(token_stopwords_stream_tests, load_from_path) {
  const auto dir =
    std::filesystem::path{::testing::TempDir()} / "sdb_stopwords_path_test";
  std::filesystem::create_directories(dir);
  const auto file = dir / "words.txt";
  {
    std::ofstream out{file};
    out << "the trailing-ignored\n\nthe-quick-brown-fox\n";
  }

  for (const auto& path : {file, dir}) {
    irs::analysis::StopwordsTokenizer::Options opts;
    opts.mask = {"of"};
    opts.stopwords_path = path.string();
    const auto stream =
      irs::analysis::StopwordsTokenizer::Make(std::move(opts), tests::Cache());
    ASSERT_NE(nullptr, stream);

    for (const std::string_view masked :
         {std::string_view{"of"}, std::string_view{"the"},
          std::string_view{"the-quick-brown-"
                           "fox"}}) {
      const auto tokens = tests::Analyze(*stream, masked);
      ASSERT_TRUE(tokens.has_value());
      EXPECT_TRUE(tokens->empty()) << masked;
    }

    const auto tokens = tests::Analyze(*stream, "fox");
    ASSERT_TRUE(tokens.has_value());
    EXPECT_EQ(1, tokens->size());
  }

  irs::analysis::StopwordsTokenizer::Options opts;
  opts.stopwords_path = (dir / "missing.txt").string();
  EXPECT_THROW(
    irs::analysis::StopwordsTokenizer::Make(std::move(opts), tests::Cache()),
    sdb::SqlException);
}

TEST(token_stopwords_stream_tests, test_load) {
  std::string_view data0("abc");
  std::string_view data1("ghi");

  auto test_func = [](const std::string_view& data0,
                      const std::string_view& data1,
                      irs::analysis::Tokenizer* stream) {
    ASSERT_NE(nullptr, stream);
    auto masked = tests::Analyze(*stream, data0);
    ASSERT_TRUE(masked.has_value());
    ASSERT_TRUE(masked->empty());

    auto kept = tests::Analyze(*stream, data1);
    ASSERT_TRUE(kept.has_value());
    const std::vector<tests::AnalyzerToken> expected = {{"ghi", 1, 0, 3}};
    ASSERT_EQ(expected, *kept);
  };

  {
    irs::analysis::StopwordsTokenizer::Options opts;
    opts.mask = {"abc", "646566", "6D6e6F"};
    auto stream =
      irs::analysis::StopwordsTokenizer::Make(std::move(opts), tests::Cache());
    test_func(data0, data1, stream.get());
  }
  // check with another order of mask
  {
    irs::analysis::StopwordsTokenizer::Options opts;
    opts.mask = {"6D6e6F", "abc", "646566"};
    auto stream =
      irs::analysis::StopwordsTokenizer::Make(std::move(opts), tests::Cache());
    test_func(data0, data1, stream.get());
  }
  // hex-decoded mask: the legacy JSON loader auto-decoded hex strings when
  // `"hex":true`; the direct Options API expects the pre-decoded bytes.
  // Here `"abc"` is the decoded form of `"616263"` and `"mnO"` is the decoded
  // form of `"6D6e6F"`. Verify masking works on the pre-decoded values.
  {
    std::string_view data_masked("abc");
    std::string_view data_kept("646566");
    auto test_hex = [](const std::string_view& masked,
                       const std::string_view& kept,
                       irs::analysis::Tokenizer* stream) {
      ASSERT_NE(nullptr, stream);
      auto masked_tokens = tests::Analyze(*stream, masked);
      ASSERT_TRUE(masked_tokens.has_value());
      ASSERT_TRUE(masked_tokens->empty());

      auto kept_tokens = tests::Analyze(*stream, kept);
      ASSERT_TRUE(kept_tokens.has_value());
      const std::vector<tests::AnalyzerToken> expected = {{"646566", 1, 0, 6}};
      ASSERT_EQ(expected, *kept_tokens);
    };
    irs::analysis::StopwordsTokenizer::Options opts;
    opts.mask = {"abc", "mnO"};  // pre-decoded equivalents of 616263 / 6D6e6F
    auto stream =
      irs::analysis::StopwordsTokenizer::Make(std::move(opts), tests::Cache());
    test_hex(data_masked, data_kept, stream.get());
  }
  // Construction with an empty mask is valid -- the analyzer simply masks
  // nothing. The legacy JSON-driven "{}" / "{\"stopwords\":1}" / "1" /
  // "\"abc\"" / std::string_view{} cases were JSON-parser-level rejections
  // (wrong shape, wrong type, malformed input); those checks live in the
  // JSON parser tests and have no analogue in the direct Options API.
  // The post-decode-shape cases ("hex":1, "stopwords":[\"1aa\"...] etc.)
  // are likewise loader-side concerns: by the time the byte string reaches
  // `Options::mask`, every entry is just an opaque token to compare against.
  {
    irs::analysis::StopwordsTokenizer::Options opts;
    auto stream =
      irs::analysis::StopwordsTokenizer::Make(std::move(opts), tests::Cache());
    ASSERT_NE(nullptr, stream);
    auto tokens = tests::Analyze(*stream, data0);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());  // nothing is masked
  }
}

TEST(token_stopwords_stream_tests, column_fill_filters) {
  irs::analysis::StopwordsTokenizer stream(Stopwords({"the", "and"}));

  constexpr size_t kCap = irs::TokenBatch::kCapacity;
  std::vector<std::string> values;
  std::vector<bool> pass;
  for (size_t i = 0; i < kCap + 200; ++i) {
    switch (i % 3) {
      case 0:
        values.push_back("the");
        pass.push_back(false);
        break;
      case 1:
        values.push_back("word" + std::to_string(i));
        pass.push_back(true);
        break;
      default:
        values.push_back("and");
        pass.push_back(false);
        break;
    }
  }

  std::vector<duckdb::string_t> vals;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
  }

  std::vector<std::vector<irs::bstring>> got(values.size());
  const auto collect = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
    ASSERT_FALSE(runs.tail_open);
    uint32_t tok = 0;
    for (const auto& run : runs) {
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        got[run.doc - 1].emplace_back(
          reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
      }
    }
    ASSERT_EQ(batch.count, tok);
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, collect};
  tests::FillColumn(stream, vals, 1, sink.writer, sink.layout);
  sink.writer.Finish();

  for (size_t v = 0; v < values.size(); ++v) {
    SCOPED_TRACE(testing::Message() << "doc=" << v + 1);
    if (pass[v]) {
      ASSERT_EQ(1, got[v].size());
      ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view{values[v]}),
                got[v][0]);
    } else {
      ASSERT_TRUE(got[v].empty());
    }
  }
}

TEST(token_stopwords_stream_tests, stopword_set_is_shared_between_instances) {
  duckdb::DuckDB db{nullptr};
  auto& cache = db.instance->GetSharedObjectCache();
  const auto make = [&](std::vector<std::string> mask) {
    irs::analysis::StopwordsTokenizer::Options opts;
    opts.mask = std::move(mask);
    return irs::analysis::StopwordsTokenizer::Make(std::move(opts), cache);
  };
  ASSERT_EQ(0u, cache.GetEntryCount());
  auto a = make({"the", "a", "an"});
  auto b = make({"an", "the", "a"});
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  EXPECT_EQ(1u, cache.GetEntryCount());
  EXPECT_EQ(tests::AnalyzeTerms(*a, "the"), tests::AnalyzeTerms(*b, "the"));
  EXPECT_EQ(tests::AnalyzeTerms(*a, "fox"), tests::AnalyzeTerms(*b, "fox"));
  auto c = make({"the"});
  ASSERT_TRUE(c);
  EXPECT_EQ(2u, cache.GetEntryCount());
  a.reset();
  b.reset();
  c.reset();
  EXPECT_EQ(0u, cache.GetEntryCount());
}
