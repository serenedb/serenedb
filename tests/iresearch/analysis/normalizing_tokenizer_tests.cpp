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

#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <random>

#include "gtest/gtest.h"
#include "token_sink_utils.hpp"

namespace {

class NormalizingTokenizerTests : public ::testing::Test {};

}  // namespace

TEST_F(NormalizingTokenizerTests, consts) {
  static_assert("normalize_tokens" ==
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

const icu::Normalizer2* ReferenceNormalizer(irs::analysis::NormForm form) {
  using irs::analysis::NormForm;
  auto err = UErrorCode::U_ZERO_ERROR;
  const icu::Normalizer2* normalizer = nullptr;
  switch (form) {
    case NormForm::Nfc:
      normalizer = icu::Normalizer2::getNFCInstance(err);
      break;
    case NormForm::Nfkc:
      normalizer = icu::Normalizer2::getNFKCInstance(err);
      break;
    case NormForm::Nfd:
      normalizer = icu::Normalizer2::getNFDInstance(err);
      break;
    case NormForm::Nfkd:
      normalizer = icu::Normalizer2::getNFKDInstance(err);
      break;
    case NormForm::NfkcCf:
      normalizer = icu::Normalizer2::getNFKCCasefoldInstance(err);
      break;
  }
  EXPECT_TRUE(U_SUCCESS(err) && normalizer);
  return normalizer;
}

const icu::Transliterator& ReferenceStrip(irs::analysis::NormForm form) {
  const auto make_tr = [](const char* rule) {
    auto e = UErrorCode::U_ZERO_ERROR;
    return std::unique_ptr<icu::Transliterator>{
      icu::Transliterator::createInstance(icu::UnicodeString{rule},
                                          UTransDirection::UTRANS_FORWARD, e)};
  };
  static const std::unique_ptr<icu::Transliterator> kStrips[] = {
    make_tr("NFD; [:Nonspacing Mark:] Remove; NFC"),
    make_tr("NFKD; [:Nonspacing Mark:] Remove; NFKC"),
    make_tr("NFD; [:Nonspacing Mark:] Remove"),
    make_tr("NFKD; [:Nonspacing Mark:] Remove"),
    make_tr("NFKD; [:Nonspacing Mark:] Remove; NFKC"),
  };
  const auto& tr = kStrips[static_cast<size_t>(form)];
  EXPECT_NE(nullptr, tr);
  return *tr;
}

std::string ReferenceNorm(
  const irs::analysis::NormalizingTokenizer::Options& opts,
  std::string_view value) {
  using irs::analysis::NormForm;
  const auto* normalizer = ReferenceNormalizer(opts.form);
  const auto* renormalizer = ReferenceNormalizer(
    opts.form == NormForm::NfkcCf ? NormForm::Nfkc : opts.form);
  auto err = UErrorCode::U_ZERO_ERROR;
  const auto raw = icu::UnicodeString::fromUTF8(
    icu::StringPiece{value.data(), static_cast<int32_t>(value.size())});
  icu::UnicodeString token;
  normalizer->normalize(raw, token, err);
  EXPECT_TRUE(U_SUCCESS(err));
  const std::string_view language{opts.locale.getLanguage()};
  const bool turkic = language == "tr" || language == "az";
  if (opts.fold) {
    token.foldCase(turkic ? U_FOLD_CASE_EXCLUDE_SPECIAL_I
                          : U_FOLD_CASE_DEFAULT);
  } else if (opts.case_convert == irs::Case::Lower) {
    token.toLower(opts.locale);
  } else if (opts.case_convert == irs::Case::Upper) {
    token.toUpper(opts.locale);
  }
  if (!opts.accent) {
    ReferenceStrip(opts.form).transliterate(token);
  } else if (opts.fold || opts.case_convert != irs::Case::None) {
    icu::UnicodeString renormalized;
    renormalizer->normalize(token, renormalized, err);
    EXPECT_TRUE(U_SUCCESS(err));
    token = renormalized;
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

TEST(NormalizingTokenizerFold, goldens) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;
  using irs::analysis::NormForm;
  const auto fold = [](NormForm form, bool accent) {
    return OptionsT{.locale = icu::Locale::createFromName("en"),
                    .accent = accent,
                    .form = form,
                    .fold = true};
  };
  AssertBlockTerm(fold(NormForm::Nfc, true), "ABC xyz", "abc xyz");
  AssertBlockTerm(fold(NormForm::Nfc, true),
                  "Stra\xC3\x9F"
                  "e",
                  "strasse");
  AssertBlockTerm(fold(NormForm::Nfc, true), "\xE1\xBA\x9E", "ss");
  AssertBlockTerm(fold(NormForm::Nfc, true), "\xCE\x9F\xCE\x94\xCE\x9F\xCE\xA3",
                  "\xCE\xBF\xCE\xB4\xCE\xBF\xCF\x83");
  AssertBlockTerm(fold(NormForm::Nfc, true), "\xCF\x82", "\xCF\x83");
  AssertBlockTerm(fold(NormForm::Nfc, true), "\xEF\xAC\x81", "fi");
  AssertBlockTerm(fold(NormForm::Nfc, true), "\xC4\xB0", "i\xCC\x87");
  AssertBlockTerm(fold(NormForm::Nfc, true), "\xC7\xB0", "\xC7\xB0");
  AssertBlockTerm(fold(NormForm::Nfc, true), "\xEA\xAD\xB0", "\xE1\x8E\xA0");
  AssertBlockTerm(fold(NormForm::Nfc, true), "\xE1\xBE\xB3",
                  "\xCE\xB1\xCE\xB9");
  AssertBlockTerm(fold(NormForm::Nfc, false), "\xC5\xB8", "y");
  AssertBlockTerm(fold(NormForm::Nfkc, true), "\xE2\x85\xAB", "xii");
  AssertBlockTerm(fold(NormForm::Nfd, true), "\xC3\x89lan", "e\xCC\x81lan");
  AssertBlockTerm(fold(NormForm::Nfkd, false), "\xEF\xAC\x81\xC3\x89", "fie");
}

TEST(NormalizingTokenizerFold, turkic_locales_keep_dotless_i) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;
  for (const char* locale : {"tr_TR", "az"}) {
    SCOPED_TRACE(testing::Message() << "locale=" << locale);
    const OptionsT opts{.locale = icu::Locale::createFromName(locale),
                        .fold = true};
    AssertBlockTerm(opts, "ISPARTA \xC4\xB0zmir", "\xC4\xB1sparta izmir");
    AssertBlockTerm(opts, "\xC7\xB0", "\xC7\xB0");
    AssertNormMatchesReference(opts,
                               "ISPARTA \xC4\xB0zmir \xC7\xB0 Stra\xC3\x9F");
    auto stream = irs::analysis::NormalizingTokenizer::Make(OptionsT{opts});
    auto* norm =
      dynamic_cast<irs::analysis::NormalizingTokenizer*>(stream.get());
    ASSERT_NE(nullptr, norm);
    EXPECT_FALSE(norm->WantedBlockTraits().ascii);
    EXPECT_FALSE(norm->Traits().keeps_ascii);
  }
  const OptionsT opts{.locale = icu::Locale::createFromName("en"),
                      .fold = true};
  AssertBlockTerm(opts, "ISPARTA \xC4\xB0zmir", "isparta i\xCC\x87zmir");
  irs::analysis::NormalizingTokenizer stream{OptionsT{opts}};
  EXPECT_TRUE(stream.WantedBlockTraits().ascii);
  EXPECT_TRUE(stream.Traits().keeps_ascii);
}

TEST(NormalizingTokenizerFold, icu_parity) {
  constexpr uint32_t kPool[] = {
    'a',    'Z',    'I',    'i',    '1',    ' ',    0x00C9,  0x00E9,
    0x00DF, 0x1E9E, 0x0130, 0x0131, 0x01F0, 0x0390, 0x03B0,  0x0149,
    0xFB01, 0xFB03, 0xFB13, 0x03A3, 0x03C2, 0x03D0, 0x017F,  0x212A,
    0x212B, 0x2126, 0x00B5, 0x01C4, 0x01C5, 0x023A, 0x2C6F,  0x1E9A,
    0x0587, 0x0531, 0x1C90, 0x13A0, 0xAB70, 0x13F8, 0x10400, 0x24B6,
    0xFF21, 0x0401, 0x0301, 0x0308, 0x4E2D, 0xAC00, 0x216B};
  constexpr uint32_t kIotaPool[] = {0x0345, 0x1F88, 0x1FB3,
                                    0x1FB4, 0x1FBC, 0x1FFC};
  std::mt19937_64 rng{37};
  for (const auto form :
       {irs::analysis::NormForm::Nfc, irs::analysis::NormForm::Nfkc,
        irs::analysis::NormForm::Nfd, irs::analysis::NormForm::Nfkd}) {
    for (const bool accent : {true, false}) {
      const irs::analysis::NormalizingTokenizer::Options opts{
        .locale = icu::Locale::createFromName("en"),
        .accent = accent,
        .form = form,
        .fold = true};
      SCOPED_TRACE(testing::Message() << "form=" << magic_enum::enum_name(form)
                                      << " accent=" << accent);
      for (size_t iter = 0; iter < 300; ++iter) {
        std::vector<uint32_t> cps(1 + rng() % 40);
        for (auto& cp : cps) {
          cp = accent && rng() % 8 == 0
                 ? kIotaPool[rng() % std::size(kIotaPool)]
                 : kPool[rng() % std::size(kPool)];
        }
        AssertNormMatchesReference(opts, EncodeCps(cps));
      }
    }
  }
}

TEST(NormalizingTokenizerForms, goldens) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;
  using irs::analysis::NormForm;
  const auto opts = [](NormForm form, irs::Case cc, bool accent) {
    return OptionsT{.locale = icu::Locale::createFromName("en"),
                    .case_convert = cc,
                    .accent = accent,
                    .form = form};
  };
  constexpr auto kNone = irs::Case::None;
  AssertBlockTerm(opts(NormForm::Nfd, kNone, true), "caf\xC3\xA9",
                  "cafe\xCC\x81");
  AssertBlockTerm(opts(NormForm::Nfd, kNone, true), "\xE2\x84\xAB",
                  "A\xCC\x8A");
  AssertBlockTerm(opts(NormForm::Nfd, kNone, true), "\xEF\xAC\x81",
                  "\xEF\xAC\x81");
  AssertBlockTerm(opts(NormForm::Nfd, irs::Case::Upper, true), "caf\xC3\xA9",
                  "CAFE\xCC\x81");
  AssertBlockTerm(opts(NormForm::Nfd, kNone, false), "caf\xC3\xA9", "cafe");
  AssertBlockTerm(opts(NormForm::Nfkd, kNone, true), "\xEF\xAC\x81", "fi");
  AssertBlockTerm(opts(NormForm::Nfkd, kNone, true), "\xE2\x91\xA0", "1");
  AssertBlockTerm(opts(NormForm::Nfkd, kNone, true), "\xC3\xA9\xC2\xB2",
                  "e\xCC\x81"
                  "2");
  AssertBlockTerm(opts(NormForm::NfkcCf, kNone, true), "ABC", "abc");
  AssertBlockTerm(opts(NormForm::NfkcCf, kNone, true),
                  "\xEF\xBC\xA1\xEF\xBC\xA2", "ab");
  AssertBlockTerm(opts(NormForm::NfkcCf, kNone, true),
                  "Stra\xC3\x9F"
                  "e",
                  "strasse");
  AssertBlockTerm(opts(NormForm::NfkcCf, kNone, true),
                  "a\xC2\xAD"
                  "b",
                  "ab");
  AssertBlockTerm(opts(NormForm::NfkcCf, kNone, true), "\xE2\x85\xAB", "xii");
  AssertBlockTerm(opts(NormForm::NfkcCf, kNone, true), "\xEA\xAD\xB0",
                  "\xE1\x8E\xA0");
  AssertBlockTerm(opts(NormForm::NfkcCf, kNone, false), "\xC3\x89t\xC3\xA9",
                  "ete");
  AssertBlockTerm(opts(NormForm::NfkcCf, irs::Case::Upper, true),
                  "\xEF\xAC\x81x", "FIX");
}

TEST(NormalizingTokenizerForms, icu_parity_case_none) {
  constexpr uint32_t kPool[] = {
    'a',    'b',    'Z',    '1',    ' ',    0x00E9, 0x0301, 0x0308,
    0x0401, 0x03B1, 0x03AC, 0x0387, 0x0483, 0x4E2D, 0xAC00, 0x1100,
    0x1161, 0x11A8, 0x05E7, 0x05B4, 0x0645, 0x064E, 0x1E9E, 0x0130,
    0x1F88, 0x2126, 0xFB01, 0x2460, 0xFF26, 0x00A0, 0x00B2, 0x33CD,
    0x03D0, 0x03C3, 0x00AD, 0x212B, 0xAB70, 0x216B, 0x0345, 0x00DF};
  std::mt19937_64 rng{41};
  for (const auto form :
       {irs::analysis::NormForm::Nfd, irs::analysis::NormForm::Nfkd,
        irs::analysis::NormForm::NfkcCf}) {
    for (const bool accent : {true, false}) {
      const irs::analysis::NormalizingTokenizer::Options opts{
        .locale = icu::Locale::createFromName("en"),
        .case_convert = irs::Case::None,
        .accent = accent,
        .form = form};
      SCOPED_TRACE(testing::Message() << "form=" << magic_enum::enum_name(form)
                                      << " accent=" << accent);
      for (size_t iter = 0; iter < 300; ++iter) {
        std::vector<uint32_t> cps(1 + rng() % 40);
        for (auto& cp : cps) {
          cp = kPool[rng() % std::size(kPool)];
        }
        AssertNormMatchesReference(opts, EncodeCps(cps));
      }
    }
  }
}

TEST(NormalizingTokenizerIcuPath, case_conversion_output_stays_normalized) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;
  const OptionsT upper{.locale = icu::Locale::createFromName("tr_TR"),
                       .case_convert = irs::Case::Upper};
  AssertBlockTerm(upper, "\xCE\x90", "\xCE\xAA\xCC\x81");
  AssertNormMatchesReference(upper, "\xCE\x90 \xCE\xB0 \xC7\xB0 i");
  for (const auto form :
       {irs::analysis::NormForm::Nfd, irs::analysis::NormForm::Nfkd,
        irs::analysis::NormForm::NfkcCf}) {
    for (const auto cc : {irs::Case::Lower, irs::Case::Upper}) {
      OptionsT opts{.locale = icu::Locale::createFromName("tr_TR"),
                    .case_convert = cc,
                    .form = form};
      SCOPED_TRACE(testing::Message() << "form=" << magic_enum::enum_name(form)
                                      << " case=" << int(cc));
      AssertNormMatchesReference(opts,
                                 "ISPARTA \xC4\xB0zmir \xCE\x90 "
                                 "\xEF\xAC\x81 \xC3\xA9");
    }
  }
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
