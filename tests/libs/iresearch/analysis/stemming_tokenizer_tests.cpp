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

#include "gtest/gtest.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/stemming_tokenizer.hpp"
#include "token_sink_utils.hpp"

namespace {

class StemmingTokenizerTests : public ::testing::Test {};

}  // namespace

TEST_F(StemmingTokenizerTests, consts) {
  static_assert("stem" == irs::Type<irs::analysis::StemmingTokenizer>::name());
}

namespace {

void AssertStemBlock(irs::analysis::StemmingTokenizer& stream,
                     std::string_view data, std::string_view expected) {
  tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
  ASSERT_TRUE(stream.Fill(data, sink.writer, sink.layout));
  ASSERT_FALSE(sink.flushed());
  auto& batch = sink.writer.buf;
  ASSERT_EQ(1, batch.count);
  ASSERT_TRUE(batch.dense_pos);
  const auto& t = batch.terms[0];
  ASSERT_EQ(expected, std::string_view(t.GetData(), t.GetSize()));
  ASSERT_EQ(0, batch.offs_start[0]);
  ASSERT_EQ(data.size(), batch.offs_end[0]);
}

}  // namespace

TEST_F(StemmingTokenizerTests, test_stemming) {
  // test stemming (locale std::string_view{})
  // there is no Snowball stemmer for "C" locale
  {
    irs::analysis::StemmingTokenizer::Options opts;
    opts.locale = icu::Locale{"C"};

    std::string_view data("running");
    irs::analysis::StemmingTokenizer stream(opts);
    ASSERT_EQ(irs::Type<irs::analysis::StemmingTokenizer>::id(), stream.type());
    ASSERT_TRUE(stream.Traits().unique);
    ASSERT_FALSE(stream.Traits().keyword);

    AssertStemBlock(stream, data, "running");
  }

  // test stemming (stemmer exists)
  {
    std::string_view data("running");

    irs::analysis::StemmingTokenizer::Options opts;
    opts.locale = icu::Locale::createFromName("en");

    irs::analysis::StemmingTokenizer stream(opts);
    AssertStemBlock(stream, data, "run");
  }

  // test stemming (stemmer does not exist)
  // there is no Snowball stemmer for Chinese
  {
    std::string_view data("running");

    irs::analysis::StemmingTokenizer::Options opts;
    opts.locale = icu::Locale::createFromName("zh");

    irs::analysis::StemmingTokenizer stream(opts);
    AssertStemBlock(stream, data, "running");
  }
}

TEST_F(StemmingTokenizerTests, test_load) {
  std::string_view data("running");
  auto stream = irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{
      .locale = icu::Locale::createFromName("en"),
    });

  ASSERT_NE(nullptr, stream);

  auto tokens = tests::Analyze(*stream, data);
  ASSERT_TRUE(tokens.has_value());
  ASSERT_EQ(1, tokens->size());
  EXPECT_EQ((tests::AnalyzerToken{"run", 1, 0, 7}), tokens->front());
}

TEST_F(StemmingTokenizerTests, test_load_invalid) {
  // Ported from the legacy "load jSON invalid" cases (empty/non-object
  // root, non-string locale). The strongly-typed Options API collapses
  // these to a single bogus-locale assertion.
  ASSERT_ANY_THROW(irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{}));
  ASSERT_ANY_THROW(irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{
      .locale = irs::MakeBogusLocale(),
    }));
}

TEST_F(StemmingTokenizerTests, test_invalid_locale) {
  // The legacy test fed `{"locale":"invalid12345.UTF-8"}` to the JSON
  // parser. With the direct-Options API, a missing/invalid locale shows
  // up as a bogus `icu::Locale`, which `Make` rejects.
  ASSERT_ANY_THROW(irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{
      .locale = irs::MakeBogusLocale(),
    }));
}

TEST_F(StemmingTokenizerTests, native_fills_match_pull) {
  irs::analysis::StemmingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  irs::analysis::StemmingTokenizer pull_stream{opts};
  irs::analysis::StemmingTokenizer fill_stream{opts};

  const std::vector<std::string> values = {
    "running",     "jumps", "consideration", "", "a", std::string(64, 'x'),
    "caf\xc3\xa9s"};

  for (const auto& v : values) {
    SCOPED_TRACE(v);
    auto terms = tests::AnalyzeTerms(pull_stream, v);
    ASSERT_TRUE(terms.has_value());
    ASSERT_EQ(1, terms->size());
    AssertStemBlock(fill_stream, v, terms->front());
  }
}

TEST_F(StemmingTokenizerTests, column_fill_runs) {
  irs::analysis::StemmingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  irs::analysis::StemmingTokenizer stream{opts};

  constexpr size_t kCap = irs::TokenBatch::kCapacity;
  constexpr size_t kTotal = kCap + 3;
  const std::vector<std::string> inputs = {"running", "consideration"};

  std::vector<std::string> stemmed;
  {
    irs::analysis::StemmingTokenizer one{opts};
    for (const auto& v : inputs) {
      auto terms = tests::AnalyzeTerms(one, v);
      ASSERT_TRUE(terms.has_value());
      ASSERT_EQ(1, terms->size());
      stemmed.push_back(std::move(terms->front()));
    }
  }

  std::vector<duckdb::string_t> vals;
  std::vector<irs::doc_id_t> docs(kTotal);
  for (size_t i = 0; i < kTotal; ++i) {
    const auto& v = inputs[i % inputs.size()];
    vals.emplace_back(v.data(), static_cast<uint32_t>(v.size()));
    docs[i] = static_cast<irs::doc_id_t>(i + 1);
  }

  size_t consumed = 0;
  size_t flushes = 0;
  const auto check = [&](irs::TokenBatch& batch,
                         std::span<const irs::DocRun> runs) {
    ++flushes;
    if (flushes == 1) {
      ASSERT_EQ(kCap, batch.count);
    }
    ASSERT_EQ(batch.count, runs.size());
    for (uint32_t i = 0; i < batch.count; ++i) {
      ASSERT_EQ(consumed + i + 1, runs[i].doc);
      ASSERT_EQ(1, runs[i].ntokens);
    }
    for (uint32_t i = 0; i < batch.count; ++i, ++consumed) {
      const auto& t = batch.terms[i];
      ASSERT_EQ(stemmed[consumed % inputs.size()],
                std::string_view(t.GetData(), t.GetSize()));
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, check};
  stream.Fill(vals, docs, sink.writer, sink.layout);
  ASSERT_EQ(1, flushes);
  ASSERT_EQ(3, sink.writer.buf.count);
  sink.writer.Finish();
  ASSERT_EQ(kTotal, consumed);
}

TEST(StemmingTokenizerCache, repeated_terms_match_fresh_analyzer) {
  const std::vector<std::string> vocab = {
    "running",   "jumps",    "easily",         "connection", "connections",
    "connected", "national", "nationality",    "generously", "cats",
    "x",         "",         "already-stemmed"};
  irs::analysis::StemmingTokenizer::Options opts{
    .locale = icu::Locale::createFromName("en")};

  auto cached = irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{opts});
  uint64_t seed = 42;
  for (size_t iter = 0; iter < 2000; ++iter) {
    seed = seed * 6364136223846793005ULL + 1;
    const auto& word = vocab[(seed >> 33) % vocab.size()];
    SCOPED_TRACE(testing::Message() << "iter=" << iter << " word=" << word);
    auto fresh = irs::analysis::StemmingTokenizer::Make(
      irs::analysis::StemmingTokenizer::Options{opts});
    const auto got = tests::Analyze(*cached, word);
    const auto want = tests::Analyze(*fresh, word);
    ASSERT_TRUE(got.has_value());
    ASSERT_TRUE(want.has_value());
    ASSERT_EQ(*want, *got);
  }
}

TEST(StemmingTokenizerCache, cap_overflow_stays_correct) {
  irs::analysis::StemmingTokenizer::Options opts{
    .locale = icu::Locale::createFromName("en")};
  auto cached = irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{opts});
  auto fresh = irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{opts});
  for (size_t i = 0; i < 70000; ++i) {
    const std::string word = "word" + std::to_string(i) + "ing";
    const auto got = tests::Analyze(*cached, word);
    ASSERT_TRUE(got.has_value());
    if ((i % 9973) == 0 || i > 69990) {
      SCOPED_TRACE(word);
      const auto want = tests::Analyze(*fresh, word);
      ASSERT_EQ(*want, *got);
    }
  }
  const auto again = tests::Analyze(*cached, "running");
  const auto want = tests::Analyze(*fresh, "running");
  ASSERT_EQ(*want, *again);
}

TEST(StemmingTokenizerCache, long_words_bypass_cache) {
  irs::analysis::StemmingTokenizer::Options opts{
    .locale = icu::Locale::createFromName("en")};
  auto cached = irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{opts});
  auto fresh = irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{opts});
  const std::string long_word(80, 'a');
  for (int rep = 0; rep < 3; ++rep) {
    const auto got = tests::Analyze(*cached, long_word + "ing");
    const auto want = tests::Analyze(*fresh, long_word + "ing");
    ASSERT_EQ(*want, *got);
  }
}
