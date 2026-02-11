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

#include <vpack/common.h>
#include <vpack/parser.h>

#include <iresearch/analysis/stemming_tokenizer.hpp>

#include "gtest/gtest.h"

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
    irs::analysis::StemmingTokenizer::OptionsT opts;
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

    irs::analysis::StemmingTokenizer::OptionsT opts;
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

    irs::analysis::StemmingTokenizer::OptionsT opts;
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
  // load jSON object
  {
    std::string_view data("running");
    auto stream = irs::analysis::analyzers::Get(
      "stem", irs::Type<irs::text_format::Json>::get(), "{\"locale\":\"en\"}");

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

  // load jSON invalid
  {
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "stem", irs::Type<irs::text_format::Json>::get(),
                         std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "stem", irs::Type<irs::text_format::Json>::get(), "1"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "stem", irs::Type<irs::text_format::Json>::get(), "[]"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "stem", irs::Type<irs::text_format::Json>::get(), "{}"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "stem", irs::Type<irs::text_format::Json>::get(),
                         "{\"locale\":1}"));
  }

  // load text
  {
    std::string_view data("running");
    auto stream = irs::analysis::analyzers::Get(
      "stem", irs::Type<irs::text_format::Json>::get(), R"({ "locale":"en" })");

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
}

TEST_F(StemmingTokenizerTests, test_make_config_json) {
  // with unknown parameter
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"invalid_parameter\":true}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "stem", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson("{\"locale\":\"ru\"}")->toString(),
              actual);
  }

  // test vpack
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"invalid_parameter\":true}";
    auto in_vpack = vpack::Parser::fromJson(config.c_str(), config.size());
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      out_str, "stem", irs::Type<irs::text_format::VPack>::get(), in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(vpack::Parser::fromJson("{\"locale\":\"ru\"}")->toString(),
              out_slice.toString());
  }

  // test vpack with variant
  {
    std::string config =
      "{\"locale\":\"ru_RU_TRADITIONAL.UTF-8\",\"invalid_parameter\":true}";
    auto in_vpack = vpack::Parser::fromJson(config.c_str(), config.size());
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      out_str, "stem", irs::Type<irs::text_format::VPack>::get(), in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(vpack::Parser::fromJson("{\"locale\":\"ru\"}")->toString(),
              out_slice.toString());
  }
}

TEST_F(StemmingTokenizerTests, test_invalid_locale) {
  auto stream = irs::analysis::analyzers::Get(
    "stem", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"invalid12345.UTF-8\"}");
  ASSERT_EQ(nullptr, stream);
}

TEST_F(StemmingTokenizerTests, test_make_config_text) {
  std::string config = "RU";
  std::string actual;
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    actual, "stem", irs::Type<irs::text_format::Text>::get(), config));
}
