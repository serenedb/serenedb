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
#include "iresearch/analysis/batch/token_sinks.hpp"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "token_sink_utils.hpp"

namespace {

class NormalizingTokenizerTests : public ::testing::Test {};

}  // namespace

TEST_F(NormalizingTokenizerTests, consts) {
  static_assert("norm" ==
                irs::Type<irs::analysis::NormalizingTokenizer>::name());
}

namespace {

void AssertBlockTerm(irs::analysis::NormalizingTokenizer::Options options,
                     std::string_view data, std::string_view expected) {
  irs::analysis::NormalizingTokenizer stream(std::move(options));
  tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
  ASSERT_TRUE(stream.Fill(data, sink.writer, sink.layout));
  ASSERT_FALSE(sink.flushed());
  auto& batch = sink.writer.buf;
  ASSERT_EQ(1, batch.count);
  ASSERT_TRUE(sink.writer.dense_pos);
  const auto& t = batch.terms[0];
  ASSERT_EQ(expected, std::string_view(t.GetData(), t.GetSize()));
  ASSERT_EQ(0, batch.offs_start[0]);
  ASSERT_EQ(data.size(), batch.offs_end[0]);
}

}  // namespace

TEST_F(NormalizingTokenizerTests, test_normalizing) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;

  // test default normalization
  {
    OptionsT options;
    options.locale = icu::Locale::createFromName("en");
    irs::analysis::NormalizingTokenizer stream(options);
    ASSERT_EQ(irs::Type<irs::analysis::NormalizingTokenizer>::id(),
              stream.type());
    AssertBlockTerm(options, "rUnNiNg\xd0\x81", "rUnNiNg\xd0\x81");
  }

  // test accent removal
  {
    OptionsT options;
    options.locale = icu::Locale::createFromName("en.utf8");
    options.accent = false;
    AssertBlockTerm(options, "rUnNiNg\xd0\x81", "rUnNiNg\xd0\x95");
  }

  // test lower case
  {
    OptionsT options;
    options.locale = icu::Locale::createFromName("en.utf8");
    options.case_convert = irs::Case::Lower;
    AssertBlockTerm(options, "rUnNiNg\xd0\x81", "running\xd1\x91");
  }

  // test upper case
  {
    OptionsT options;
    options.locale = icu::Locale::createFromName("en.utf8");
    options.case_convert = irs::Case::Upper;
    AssertBlockTerm(options, "rUnNiNg\xd1\x91", "RUNNING\xd0\x81");
  }
}

TEST_F(NormalizingTokenizerTests, test_load) {
  // default normalization
  {
    std::string_view data("running");
    auto stream = irs::analysis::NormalizingTokenizer::Make(
      irs::analysis::NormalizingTokenizer::Options{
        .locale = icu::Locale::createFromName("en"),
      });

    ASSERT_NE(nullptr, stream);

    auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"running", 1, 0, 7}), tokens->front());
  }

  // with UPPER case
  {
    std::string_view data("ruNNing");
    auto stream = irs::analysis::NormalizingTokenizer::Make(
      irs::analysis::NormalizingTokenizer::Options{
        .locale = icu::Locale::createFromName("en"),
        .case_convert = irs::Case::Upper,
      });

    ASSERT_NE(nullptr, stream);

    auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"RUNNING", 1, 0, 7}), tokens->front());
  }

  // with LOWER case
  {
    std::string_view data("ruNNing");
    auto stream = irs::analysis::NormalizingTokenizer::Make(
      irs::analysis::NormalizingTokenizer::Options{
        .locale = icu::Locale::createFromName("en"),
        .case_convert = irs::Case::Lower,
      });

    ASSERT_NE(nullptr, stream);

    auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"running", 1, 0, 7}), tokens->front());
  }

  // with NONE case
  {
    std::string_view data("ruNNing");
    auto stream = irs::analysis::NormalizingTokenizer::Make(
      irs::analysis::NormalizingTokenizer::Options{
        .locale = icu::Locale::createFromName("en"),
        .case_convert = irs::Case::None,
      });

    ASSERT_NE(nullptr, stream);

    auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"ruNNing", 1, 0, 7}), tokens->front());
  }

  // remove accent
  {
    constexpr std::u8string_view kData{u8"öõ"};
    const auto ref = irs::ViewCast<char>(kData);

    auto stream = irs::analysis::NormalizingTokenizer::Make(
      irs::analysis::NormalizingTokenizer::Options{
        .locale = icu::Locale::createFromName("de_DE.UTF8"),
        .case_convert = irs::Case::Lower,
        .accent = false,
      });

    ASSERT_NE(nullptr, stream);

    auto tokens = tests::Analyze(*stream, ref);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ(
      (tests::AnalyzerToken{"oo", 1, 0, static_cast<uint32_t>(kData.size())}),
      tokens->front());
  }

  // invalid options -- bogus locale (ported from "load jSON invalid"
  // cases that exercised empty/missing-locale JSON). The legacy JSON
  // parse-time rejections (non-string locale, non-string case, non-bool
  // accent) have no direct-API analogue, so they collapse to this
  // single bogus-locale assertion.
  {
    ASSERT_ANY_THROW(irs::analysis::NormalizingTokenizer::Make(
      irs::analysis::NormalizingTokenizer::Options{}));
    ASSERT_ANY_THROW(irs::analysis::NormalizingTokenizer::Make(
      irs::analysis::NormalizingTokenizer::Options{
        .locale = irs::MakeBogusLocale(),
      }));
  }
}

TEST_F(NormalizingTokenizerTests, test_invalid_locale) {
  // The legacy test fed `{"locale":"invalid12345.UTF-8"}` to the JSON
  // parser. With the direct-Options API, a missing/invalid locale shows
  // up as a bogus `icu::Locale`, which `Make` rejects.
  ASSERT_ANY_THROW(irs::analysis::NormalizingTokenizer::Make(
    irs::analysis::NormalizingTokenizer::Options{
      .locale = irs::MakeBogusLocale(),
    }));
}

TEST_F(NormalizingTokenizerTests, native_fills_match_pull) {
  irs::analysis::NormalizingTokenizer::Options options;
  options.locale = icu::Locale::createFromName("en");
  options.case_convert = irs::Case::Lower;
  options.accent = false;
  irs::analysis::NormalizingTokenizer stream(options);

  ASSERT_TRUE(stream.Traits().unique);
  ASSERT_FALSE(stream.Traits().keyword);

  const std::vector<std::string> values = {
    "rUnNiNg", "Caf\xc3\xa9", std::string(64, 'X'),
    "MIXED case \xc3\x85\xc3\x84\xc3\x96"};

  std::vector<irs::bstring> expected;
  for (const auto& v : values) {
    auto tokens = tests::AnalyzeTerms(stream, v);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    expected.emplace_back(
      reinterpret_cast<const irs::byte_type*>(tokens->front().data()),
      tokens->front().size());
  }

  for (size_t i = 0; i < values.size(); ++i) {
    tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
    ASSERT_TRUE(stream.Fill(values[i], sink.writer, sink.layout));
    ASSERT_FALSE(sink.flushed());
    auto& batch = sink.writer.buf;
    ASSERT_EQ(1, batch.count);
    ASSERT_TRUE(sink.writer.dense_pos);
    const auto& t = batch.terms[0];
    ASSERT_EQ(expected[i],
              irs::bstring(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                           t.GetSize()));
    ASSERT_EQ(0, batch.offs_start[0]);
    ASSERT_EQ(values[i].size(), batch.offs_end[0]);
  }

  {
    std::vector<irs::bstring> out;
    irs::TermVectorSink vec_sink{out};
    ASSERT_TRUE(
      stream.Fill(values[0], vec_sink.writer, irs::TokenLayout::Terms));
    vec_sink.writer.Finish();
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(expected[0], out[0]);
  }

  {
    std::vector<duckdb::string_t> vals;
    std::vector<irs::doc_id_t> docs;
    for (size_t i = 0; i < values.size(); ++i) {
      vals.emplace_back(values[i].data(),
                        static_cast<uint32_t>(values[i].size()));
      docs.push_back(static_cast<irs::doc_id_t>(i + 1));
    }
    tests::OneBatchSink sink{irs::TokenLayout::Terms};
    stream.Fill(vals, docs, sink.writer, sink.layout);
    ASSERT_FALSE(sink.flushed());
    auto& batch = sink.writer.buf;
    const auto runs = sink.writer.Runs();
    ASSERT_EQ(values.size(), runs.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(docs[i], runs[i].doc);
      ASSERT_EQ(1, runs[i].ntokens);
    }
    ASSERT_EQ(values.size(), batch.count);
    for (size_t i = 0; i < values.size(); ++i) {
      const auto& t = batch.terms[i];
      ASSERT_EQ(
        expected[i],
        irs::bstring(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                     t.GetSize()));
    }
  }

  {
    const size_t total = irs::TokenBatch::kCapacity + 100;
    std::vector<duckdb::string_t> vals(
      total, duckdb::string_t{values[0].data(),
                              static_cast<uint32_t>(values[0].size())});
    std::vector<irs::doc_id_t> docs(total);
    for (size_t i = 0; i < total; ++i) {
      docs[i] = static_cast<irs::doc_id_t>(i + 1);
    }
    size_t flushes = 0;
    const auto on_flush = [&](irs::TokenBatch& batch,
                              std::span<const irs::DocRun> runs) {
      ++flushes;
      ASSERT_EQ(irs::TokenBatch::kCapacity, batch.count);
      ASSERT_EQ(batch.count, runs.size());
      for (uint32_t i = 0; i < batch.count; ++i) {
        ASSERT_EQ(i + 1, runs[i].doc);
        ASSERT_EQ(1, runs[i].ntokens);
      }
    };
    tests::FnTokenSink sink{irs::TokenLayout::Terms, on_flush};
    stream.Fill(vals, docs, sink.writer, sink.layout);
    ASSERT_EQ(1, flushes);
    ASSERT_EQ(100, sink.writer.buf.count);
    const auto staged = sink.writer.Runs();
    ASSERT_EQ(100, staged.size());
    for (uint32_t i = 0; i < 100; ++i) {
      ASSERT_EQ(irs::TokenBatch::kCapacity + i + 1, staged[i].doc);
      ASSERT_EQ(1, staged[i].ntokens);
    }
  }
}

TEST_F(NormalizingTokenizerTests, column_suspension) {
  irs::analysis::NormalizingTokenizer::Options options;
  options.locale = icu::Locale::createFromName("en");
  options.case_convert = irs::Case::Lower;
  options.accent = false;
  irs::analysis::NormalizingTokenizer stream(options);

  const std::vector<std::string> inputs = {"RUnNiNg", "Caf\xc3\xa9"};
  std::vector<irs::bstring> normalized;
  for (const auto& v : inputs) {
    const duckdb::string_t one{v.data(), static_cast<uint32_t>(v.size())};
    const irs::doc_id_t doc = 1;
    tests::OneBatchSink sink{irs::TokenLayout::Terms};
    stream.Fill({&one, 1}, {&doc, 1}, sink.writer, sink.layout);
    ASSERT_FALSE(sink.flushed());
    ASSERT_EQ(1, sink.writer.buf.count);
    const auto& t = sink.writer.buf.terms[0];
    normalized.emplace_back(
      reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
  }

  constexpr size_t kCap = irs::TokenBatch::kCapacity;
  constexpr size_t kTotal = kCap + 3;
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
      ASSERT_EQ(
        normalized[consumed % inputs.size()],
        irs::bstring(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                     t.GetSize()));
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, check};
  stream.Fill(vals, docs, sink.writer, sink.layout);
  ASSERT_EQ(1, flushes);
  ASSERT_EQ(3, sink.writer.buf.count);
  sink.writer.Finish();
  ASSERT_EQ(kTotal, consumed);
}

namespace {

std::vector<tests::AnalyzerToken> NormAnalyzeWith(
  const irs::analysis::NormalizingTokenizer::Options& opts,
  std::string_view value, bool force_unicode) {
  auto stream = irs::analysis::NormalizingTokenizer::Make(
    irs::analysis::NormalizingTokenizer::Options{opts});
  auto* norm = dynamic_cast<irs::analysis::NormalizingTokenizer*>(stream.get());
  EXPECT_NE(nullptr, norm);
  norm->ForceUnicodePath(force_unicode);
  auto tokens = tests::Analyze(*stream, value);
  EXPECT_TRUE(tokens.has_value());
  return std::move(*tokens);
}

void AssertNormAsciiMatchesUnicode(
  const irs::analysis::NormalizingTokenizer::Options& opts,
  std::string_view value) {
  const auto slow = NormAnalyzeWith(opts, value, true);
  const auto fast = NormAnalyzeWith(opts, value, false);
  ASSERT_EQ(slow.size(), fast.size());
  for (size_t i = 0; i < slow.size(); ++i) {
    SCOPED_TRACE(testing::Message() << "token=" << i);
    ASSERT_EQ(slow[i].term, fast[i].term);
    ASSERT_EQ(slow[i].pos, fast[i].pos);
    ASSERT_EQ(slow[i].offs_start, fast[i].offs_start);
    ASSERT_EQ(slow[i].offs_end, fast[i].offs_end);
  }
}

}  // namespace

TEST(NormalizingTokenizerAsciiFastPath, property_oracle_full_ascii) {
  std::string all_ascii;
  for (int c = 1; c < 128; ++c) {
    all_ascii += static_cast<char>(c);
  }
  uint64_t seed = 0xa5c11a5c11ULL;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (const char* locale : {"en", "en_US.utf8", "de_DE", "ru"}) {
    for (const auto cc :
         {irs::Case::None, irs::Case::Lower, irs::Case::Upper}) {
      for (const bool accent : {true, false}) {
        irs::analysis::NormalizingTokenizer::Options opts{
          .locale = icu::Locale::createFromName(locale),
          .case_convert = cc,
          .accent = accent};
        SCOPED_TRACE(testing::Message() << "locale=" << locale << " case="
                                        << int(cc) << " accent=" << accent);
        AssertNormAsciiMatchesUnicode(opts, "");
        AssertNormAsciiMatchesUnicode(opts, "The Quick BROWN fox 42!");
        AssertNormAsciiMatchesUnicode(opts, all_ascii);
        for (size_t iter = 0; iter < 100; ++iter) {
          std::string v;
          const size_t len = next() % 100;
          for (size_t i = 0; i < len; ++i) {
            v += static_cast<char>(1 + next() % 127);
          }
          AssertNormAsciiMatchesUnicode(opts, v);
        }
      }
    }
  }
}

TEST(NormalizingTokenizerAsciiFastPath, turkish_locale_stays_unicode) {
  irs::analysis::NormalizingTokenizer::Options opts{
    .locale = icu::Locale::createFromName("tr_TR"),
    .case_convert = irs::Case::Lower,
    .accent = true};
  const auto fast = NormAnalyzeWith(opts, "III", false);
  const auto slow = NormAnalyzeWith(opts, "III", true);
  ASSERT_EQ(1, fast.size());
  ASSERT_EQ(slow[0].term, fast[0].term);
  ASSERT_EQ("\xc4\xb1\xc4\xb1\xc4\xb1", fast[0].term);
}

TEST(NormalizingTokenizerAsciiFastPath, non_ascii_takes_unicode_path) {
  irs::analysis::NormalizingTokenizer::Options opts{
    .locale = icu::Locale::createFromName("de_DE"),
    .case_convert = irs::Case::Lower,
    .accent = false};
  AssertNormAsciiMatchesUnicode(opts,
                                "S\xc3\x9c"
                                "D mixed ascii");
  AssertNormAsciiMatchesUnicode(opts, "caf\xc3\xa9");
}
