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

#include <unicode/bytestream.h>

#include <random>

#include "gtest/gtest.h"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/utf8_utils.hpp"
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
  size_t flushes = 0;
  const auto check = [&](irs::TokenBatch& batch, irs::DocRuns) {
    ++flushes;
    ASSERT_EQ(1, batch.count);
    const auto& t = batch.terms[0];
    ASSERT_EQ(expected, std::string_view(t.GetData(), t.GetSize()));
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

TEST_F(NormalizingTokenizerTests, test_normalizing) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;

  {
    OptionsT options;
    options.locale = icu::Locale::createFromName("en");
    irs::analysis::NormalizingTokenizer stream(options);
    ASSERT_EQ(irs::Type<irs::analysis::NormalizingTokenizer>::id(),
              stream.type());
    AssertBlockTerm(options, "rUnNiNg\xd0\x81", "rUnNiNg\xd0\x81");
  }

  {
    OptionsT options;
    options.locale = icu::Locale::createFromName("en.utf8");
    options.accent = false;
    AssertBlockTerm(options, "rUnNiNg\xd0\x81", "rUnNiNg\xd0\x95");
  }

  {
    OptionsT options;
    options.locale = icu::Locale::createFromName("en.utf8");
    options.case_convert = irs::Case::Lower;
    AssertBlockTerm(options, "rUnNiNg\xd0\x81", "running\xd1\x91");
  }

  {
    OptionsT options;
    options.locale = icu::Locale::createFromName("en.utf8");
    options.case_convert = irs::Case::Upper;
    AssertBlockTerm(options, "rUnNiNg\xd1\x91", "RUNNING\xd0\x81");
  }
}

TEST_F(NormalizingTokenizerTests, test_load) {
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
}

TEST_F(NormalizingTokenizerTests, omitted_locale_means_simple_case) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;
  AssertBlockTerm(OptionsT{.case_convert = irs::Case::Lower},
                  "\xCE\x9F\xCE\x94\xCE\x9F\xCE\xA3 AbC",
                  "\xCE\xBF\xCE\xB4\xCE\xBF\xCF\x83 abc");
  AssertBlockTerm(OptionsT{}, "Caf\xC3\xA9", "Caf\xC3\xA9");
  AssertBlockTerm(OptionsT{.accent = false}, "Caf\xC3\xA9", "Cafe");
  AssertBlockTerm(OptionsT{.case_convert = irs::Case::Lower, .accent = false},
                  "Caf\xC3\xA9", "cafe");
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
    size_t flushes = 0;
    const auto check = [&](irs::TokenBatch& batch, irs::DocRuns) {
      ++flushes;
      ASSERT_EQ(1, batch.count);
      const auto& t = batch.terms[0];
      ASSERT_EQ(
        expected[i],
        irs::bstring(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                     t.GetSize()));
      ASSERT_EQ(0, batch.offs_start[0]);
      ASSERT_EQ(values[i].size(), batch.offs_end[0]);
    };
    tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
    ASSERT_TRUE(stream.Fill(values[i], irs::doc_limits::min(), sink.writer,
                            {sink.layout}));
    sink.writer.Finish();
    ASSERT_EQ(1, flushes);
  }

  {
    irs::ValueAnalyzer analyzer;
    irs::ValueTokens tokens;
    ASSERT_TRUE(analyzer.Analyze(stream, values[0], tokens));
    ASSERT_EQ(1, tokens.terms().size());
    ASSERT_EQ(expected[0], irs::AsBytesView(tokens.terms()[0]));
  }

  {
    std::vector<duckdb::string_t> vals;
    for (size_t i = 0; i < values.size(); ++i) {
      vals.emplace_back(values[i].data(),
                        static_cast<uint32_t>(values[i].size()));
    }
    size_t flushes = 0;
    const auto check = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
      ++flushes;
      ASSERT_EQ(values.size(), runs.size());
      for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(i + 1, runs[i].doc);
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
    };
    tests::FnTokenSink sink{irs::TokenLayout::Terms, check};
    tests::FillColumn(stream, vals, 1, sink.writer, sink.layout);
    sink.writer.Finish();
    ASSERT_EQ(1, flushes);
  }

  {
    const size_t total = irs::TokenBatch::kCapacity + 100;
    std::vector<duckdb::string_t> vals(
      total, duckdb::string_t{values[0].data(),
                              static_cast<uint32_t>(values[0].size())});
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
    tests::FillColumn(stream, vals, 1, sink.writer, sink.layout);
    ASSERT_EQ(1, flushes);
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
    size_t flushes = 0;
    const auto check = [&](irs::TokenBatch& batch, irs::DocRuns) {
      ++flushes;
      ASSERT_EQ(1, batch.count);
      const auto& t = batch.terms[0];
      normalized.emplace_back(
        reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
    };
    tests::FnTokenSink sink{irs::TokenLayout::Terms, check};
    tests::FillColumn(stream, {&one, 1}, doc, sink.writer, sink.layout);
    sink.writer.Finish();
    ASSERT_EQ(1, flushes);
  }

  constexpr size_t kCap = irs::TokenBatch::kCapacity;
  constexpr size_t kTotal = kCap + 3;
  std::vector<duckdb::string_t> vals;
  for (size_t i = 0; i < kTotal; ++i) {
    const auto& v = inputs[i % inputs.size()];
    vals.emplace_back(v.data(), static_cast<uint32_t>(v.size()));
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
  tests::FillColumn(stream, vals, 1, sink.writer, sink.layout);
  ASSERT_EQ(1, flushes);
  sink.writer.Finish();
  ASSERT_EQ(2, flushes);
  ASSERT_EQ(kTotal, consumed);
}

namespace {

std::string ReferenceNorm(
  const irs::analysis::NormalizingTokenizer::Options& opts,
  std::string_view value) {
  const bool nfkc = opts.form == irs::analysis::NormForm::Nfkc;
  auto err = UErrorCode::U_ZERO_ERROR;
  const auto* normalizer = nfkc ? icu::Normalizer2::getNFKCInstance(err)
                                : icu::Normalizer2::getNFCInstance(err);
  EXPECT_TRUE(U_SUCCESS(err) && normalizer);
  const auto raw = icu::UnicodeString::fromUTF8(
    icu::StringPiece{value.data(), static_cast<int32_t>(value.size())});
  icu::UnicodeString token;
  normalizer->normalize(raw, token, err);
  EXPECT_TRUE(U_SUCCESS(err));
  if (opts.case_convert == irs::Case::Lower) {
    token.toLower(opts.locale);
  } else if (opts.case_convert == irs::Case::Upper) {
    token.toUpper(opts.locale);
  }
  if (!opts.accent) {
    const auto make_tr = [](const char* rule) {
      auto e = UErrorCode::U_ZERO_ERROR;
      return std::unique_ptr<icu::Transliterator>{
        icu::Transliterator::createInstance(
          icu::UnicodeString{rule}, UTransDirection::UTRANS_FORWARD, e)};
    };
    static const auto kNfcStrip =
      make_tr("NFD; [:Nonspacing Mark:] Remove; NFC");
    static const auto kNfkcStrip =
      make_tr("NFKD; [:Nonspacing Mark:] Remove; NFKC");
    const auto& tr = nfkc ? kNfkcStrip : kNfcStrip;
    EXPECT_NE(nullptr, tr);
    tr->transliterate(token);
  }
  std::string out;
  token.toUTF8String(out);
  return out;
}

void AssertNormMatchesReference(
  const irs::analysis::NormalizingTokenizer::Options& opts,
  std::string_view value) {
  irs::analysis::NormalizingTokenizer stream{
    irs::analysis::NormalizingTokenizer::Options{opts}};
  const auto tokens = tests::Analyze(stream, value);
  ASSERT_TRUE(tokens.has_value());
  ASSERT_EQ(1, tokens->size());
  const auto& t = tokens->front();
  ASSERT_EQ(ReferenceNorm(opts, value), t.term);
  ASSERT_EQ(1, t.pos);
  ASSERT_EQ(0, t.offs_start);
  ASSERT_EQ(value.size(), t.offs_end);
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
        AssertNormMatchesReference(opts, "");
        AssertNormMatchesReference(opts, "The Quick BROWN fox 42!");
        AssertNormMatchesReference(opts, all_ascii);
        for (size_t iter = 0; iter < 100; ++iter) {
          std::string v;
          const size_t len = next() % 100;
          for (size_t i = 0; i < len; ++i) {
            v += static_cast<char>(1 + next() % 127);
          }
          AssertNormMatchesReference(opts, v);
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
  AssertNormMatchesReference(opts, "III");
  irs::analysis::NormalizingTokenizer stream{
    irs::analysis::NormalizingTokenizer::Options{opts}};
  const auto tokens = tests::Analyze(stream, "III");
  ASSERT_TRUE(tokens.has_value());
  ASSERT_EQ(1, tokens->size());
  ASSERT_EQ("\xc4\xb1\xc4\xb1\xc4\xb1", tokens->front().term);
}

TEST(NormalizingTokenizerAsciiFastPath, non_ascii_takes_unicode_path) {
  irs::analysis::NormalizingTokenizer::Options opts{
    .locale = icu::Locale::createFromName("de_DE"),
    .case_convert = irs::Case::Lower,
    .accent = false};
  AssertNormMatchesReference(opts,
                             "S\xc3\x9c"
                             "D mixed ascii");
  AssertNormMatchesReference(opts, "caf\xc3\xa9");
}

TEST(NormalizingTokenizerAsciiFastPath, case_none_is_locale_safe) {
  for (const char* locale : {"tr_TR", "az", "lt"}) {
    for (const bool accent : {true, false}) {
      irs::analysis::NormalizingTokenizer::Options opts{
        .locale = icu::Locale::createFromName(locale),
        .case_convert = irs::Case::None,
        .accent = accent};
      SCOPED_TRACE(testing::Message()
                   << "locale=" << locale << " accent=" << accent);
      auto stream = irs::analysis::NormalizingTokenizer::Make(
        irs::analysis::NormalizingTokenizer::Options{opts});
      auto* norm =
        dynamic_cast<irs::analysis::NormalizingTokenizer*>(stream.get());
      ASSERT_NE(nullptr, norm);
      EXPECT_TRUE(norm->WantedBlockTraits().ascii);
      AssertNormMatchesReference(opts, "Istanbul III i I");
      AssertNormMatchesReference(opts, "The Quick BROWN fox 42!");
    }
  }
  for (const auto cc : {irs::Case::Lower, irs::Case::Upper}) {
    irs::analysis::NormalizingTokenizer::Options opts{
      .locale = icu::Locale::createFromName("tr_TR"),
      .case_convert = cc,
      .accent = true};
    auto stream = irs::analysis::NormalizingTokenizer::Make(std::move(opts));
    auto* norm =
      dynamic_cast<irs::analysis::NormalizingTokenizer*>(stream.get());
    ASSERT_NE(nullptr, norm);
    EXPECT_FALSE(norm->WantedBlockTraits().ascii);
  }
}

namespace {

std::string EncodeCps(const std::vector<uint32_t>& cps) {
  std::string out;
  irs::byte_type buf[irs::utf8_utils::kMaxCharSize];
  for (const auto cp : cps) {
    out.append(reinterpret_cast<const char*>(buf),
               irs::utf8_utils::FromChar32(cp, buf));
  }
  return out;
}

}  // namespace

TEST(NormalizingTokenizerFastPath, icu_parity_case_none) {
  constexpr uint32_t kPool[] = {
    'a',    'b',    'Z',    '1',    ' ',    0x00E9, 0x0301, 0x0308, 0x0401,
    0x03B1, 0x03AC, 0x0387, 0x0483, 0x4E2D, 0xAC00, 0x1100, 0x1161, 0x11A8,
    0x05E7, 0x05B4, 0x0645, 0x064E, 0x1E9E, 0x0130, 0x1F88, 0x2126};
  std::mt19937_64 rng{29};
  for (const bool accent : {true, false}) {
    irs::analysis::NormalizingTokenizer::Options opts{
      .locale = icu::Locale::createFromName("en"),
      .case_convert = irs::Case::None,
      .accent = accent};
    SCOPED_TRACE(testing::Message() << "accent=" << accent);
    for (size_t iter = 0; iter < 500; ++iter) {
      std::vector<uint32_t> cps(1 + rng() % 40);
      for (auto& cp : cps) {
        cp = kPool[rng() % std::size(kPool)];
      }
      AssertNormMatchesReference(opts, EncodeCps(cps));
    }
  }
}

TEST(NormalizingTokenizerFastPath, simple_case_drift_pins) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;
  const auto opts = [](irs::Case cc, bool accent) {
    return OptionsT{.locale = icu::Locale::createFromName("en"),
                    .case_convert = cc,
                    .accent = accent};
  };
  AssertBlockTerm(opts(irs::Case::Lower, true),
                  "\xCE\x9F\xCE\x94\xCE\x9F\xCE\xA3",
                  "\xCE\xBF\xCE\xB4\xCE\xBF\xCF\x83");
  AssertBlockTerm(opts(irs::Case::Upper, true),
                  "stra\xC3\x9F"
                  "e",
                  "STRA\xC3\x9F"
                  "E");
  AssertBlockTerm(opts(irs::Case::Lower, true), "\xC4\xB0stanbul", "istanbul");
  AssertBlockTerm(opts(irs::Case::Lower, false), "Caf\xC3\xA9", "cafe");
  AssertBlockTerm(opts(irs::Case::None, true), "Cafe\xCC\x81", "Caf\xC3\xA9");
  AssertBlockTerm(opts(irs::Case::None, false), "\xD0\x81lka", "\xD0\x95lka");
}

TEST(NormalizingTokenizerFastPath, tailored_locale_keeps_icu) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;
  const auto opts = [](const char* locale) {
    return OptionsT{.locale = icu::Locale::createFromName(locale),
                    .case_convert = irs::Case::Lower,
                    .accent = true};
  };
  AssertBlockTerm(opts("tr_TR"), "ISPARTA", "\xC4\xB1sparta");
  AssertBlockTerm(opts("tr_TR"), "\xC4\xB0STANBUL", "istanbul");
  AssertBlockTerm(opts("el"), "\xCE\x9F\xCE\x94\xCE\x9F\xCE\xA3",
                  "\xCE\xBF\xCE\xB4\xCE\xBF\xCF\x82");
  AssertNormMatchesReference(opts("tr_TR"), "ISPARTA i I");
  AssertNormMatchesReference(opts("el"),
                             "\xCE\x9F\xCE\x94\xCE\x9F\xCE\xA3 abc");
}

TEST(NormalizingTokenizerFastPath, nfkc_goldens) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;
  const auto opts = [](irs::Case cc) {
    return OptionsT{.locale = icu::Locale::createFromName("en"),
                    .case_convert = cc,
                    .accent = true,
                    .form = irs::analysis::NormForm::Nfkc};
  };
  AssertBlockTerm(opts(irs::Case::None), "\xEF\xAC\x81nancial", "financial");
  AssertBlockTerm(opts(irs::Case::None), "\xE2\x91\xA0", "1");
  AssertBlockTerm(opts(irs::Case::Lower),
                  "\xEF\xBC\xA6\xEF\xBC\xB5\xEF\xBC\xAC\xEF\xBC\xAC", "full");
  AssertBlockTerm(opts(irs::Case::None),
                  "a\xC2\xA0"
                  "b",
                  "a b");
  AssertBlockTerm(opts(irs::Case::None), "\xE3\x8D\x8D",
                  "\xE3\x83\xA1\xE3\x83\xBC\xE3\x83\x88\xE3\x83\xAB");
  AssertBlockTerm(opts(irs::Case::Lower), "caf\xC3\xA9 2\xC2\xB2",
                  "caf\xC3\xA9 22");
}

TEST(NormalizingTokenizerFastPath, icu_parity_nfkc) {
  constexpr uint32_t kPool[] = {
    'a',    'b',    'Z',    '1',    ' ',    0x00E9, 0x0301, 0x0308, 0x0401,
    0x03B1, 0x03AC, 0x0387, 0x0483, 0x4E2D, 0xAC00, 0x1100, 0x1161, 0x11A8,
    0x05E7, 0x05B4, 0x0645, 0x064E, 0x1E9E, 0x0130, 0x1F88, 0x2126, 0xFB01,
    0x2460, 0xFF26, 0x00A0, 0x00B2, 0x33CD, 0x03D0, 0x03C3};
  std::mt19937_64 rng{31};
  for (const bool accent : {true, false}) {
    irs::analysis::NormalizingTokenizer::Options opts{
      .locale = icu::Locale::createFromName("en"),
      .case_convert = irs::Case::None,
      .accent = accent,
      .form = irs::analysis::NormForm::Nfkc};
    SCOPED_TRACE(testing::Message() << "accent=" << accent);
    for (size_t iter = 0; iter < 500; ++iter) {
      std::vector<uint32_t> cps(1 + rng() % 40);
      for (auto& cp : cps) {
        cp = kPool[rng() % std::size(kPool)];
      }
      AssertNormMatchesReference(opts, EncodeCps(cps));
    }
  }
}
