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
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace {

class NormalizingTokenizerTests : public ::testing::Test {};

}  // namespace

TEST_F(NormalizingTokenizerTests, consts) {
  static_assert("norm" ==
                irs::Type<irs::analysis::NormalizingTokenizer>::name());
}

TEST_F(NormalizingTokenizerTests, test_normalizing) {
  typedef irs::analysis::NormalizingTokenizer::Options OptionsT;

  // test default normalization
  {
    OptionsT options;
    options.locale = icu::Locale::createFromName("en");

    std::string_view data("rUnNiNg\xd0\x81");
    irs::analysis::NormalizingTokenizer stream(options);
    ASSERT_EQ(irs::Type<irs::analysis::NormalizingTokenizer>::id(),
              stream.type());

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.reset(data));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(9, offset->end);
    ASSERT_EQ(data, irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test accent removal
  {
    OptionsT options;

    options.locale = icu::Locale::createFromName("en.utf8");
    options.accent = false;

    std::string_view data("rUnNiNg\xd0\x81");
    std::string_view expected("rUnNiNg\xd0\x95");
    irs::analysis::NormalizingTokenizer stream(options);

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.reset(data));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(9, offset->end);
    ASSERT_EQ(expected, irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test lower case
  {
    OptionsT options;

    options.locale = icu::Locale::createFromName("en.utf8");
    options.case_convert = irs::Case::Lower;

    std::string_view data("rUnNiNg\xd0\x81");
    std::string_view expected("running\xd1\x91");
    irs::analysis::NormalizingTokenizer stream(options);

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.reset(data));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(9, offset->end);
    ASSERT_EQ(expected, irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test upper case
  {
    OptionsT options;

    options.locale = icu::Locale::createFromName("en.utf8");
    options.case_convert = irs::Case::Upper;

    std::string_view data("rUnNiNg\xd1\x91");
    std::string_view expected("RUNNING\xd0\x81");
    irs::analysis::NormalizingTokenizer stream(options);

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.reset(data));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(9, offset->end);
    ASSERT_EQ(expected, irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
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
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("running", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
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
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("RUNNING", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
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
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("running", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
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
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("ruNNing", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
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
    ASSERT_TRUE(stream->reset(ref));
    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());

    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(kData.size(), offset->end);
    ASSERT_TRUE(u8"oo" == irs::ViewCast<char8_t>(term->value));

    ASSERT_FALSE(stream->next());
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
