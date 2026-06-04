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
#include "iresearch/analysis/stemming_tokenizer.hpp"

namespace {

class StemmingTokenizerTests : public ::testing::Test {};

}  // namespace

TEST_F(StemmingTokenizerTests, consts) {
  static_assert("stem" == irs::Type<irs::analysis::StemmingTokenizer>::name());
}

TEST_F(StemmingTokenizerTests, test_stemming) {
  // test stemming (locale std::string_view{})
  // there is no Snowball stemmer for "C" locale
  {
    irs::analysis::StemmingTokenizer::Options opts;
    opts.locale = icu::Locale{"C"};

    std::string_view data("running");
    irs::analysis::StemmingTokenizer stream(opts);
    ASSERT_EQ(irs::Type<irs::analysis::StemmingTokenizer>::id(), stream.type());

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.reset(data));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("running", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test stemming (stemmer exists)
  {
    std::string_view data("running");

    irs::analysis::StemmingTokenizer::Options opts;
    opts.locale = icu::Locale::createFromName("en");

    irs::analysis::StemmingTokenizer stream(opts);

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.reset(data));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("run", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test stemming (stemmer does not exist)
  // there is no Snowball stemmer for Chinese
  {
    std::string_view data("running");

    irs::analysis::StemmingTokenizer::Options opts;
    opts.locale = icu::Locale::createFromName("zh");

    irs::analysis::StemmingTokenizer stream(opts);

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("running", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }
}

TEST_F(StemmingTokenizerTests, test_load) {
  std::string_view data("running");
  auto stream = irs::analysis::StemmingTokenizer::Make(
    irs::analysis::StemmingTokenizer::Options{
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
  ASSERT_EQ("run", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream->next());
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
