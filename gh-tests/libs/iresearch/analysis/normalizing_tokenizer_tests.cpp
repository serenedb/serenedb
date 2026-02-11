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

#include <iresearch/analysis/normalizing_tokenizer.hpp>

#include "gtest/gtest.h"

namespace {

class NormalizingTokenizerTests : public ::testing::Test {};

}  // namespace

TEST_F(NormalizingTokenizerTests, consts) {
  static_assert("norm" ==
                irs::Type<irs::analysis::NormalizingTokenizer>::name());
}

TEST_F(NormalizingTokenizerTests, test_normalizing) {
  typedef irs::analysis::NormalizingTokenizer::OptionsT OptionsT;

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
    options.case_convert = irs::analysis::NormalizingTokenizer::kLower;

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
    options.case_convert = irs::analysis::NormalizingTokenizer::kUpper;

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
  // load jSON object
  {
    std::string_view data("running");
    auto stream = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(), "{\"locale\":\"en\"}");

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
    auto stream = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"case\":\"upper\"}");

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
    auto stream = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"case\":\"lower\"}");

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
    auto stream = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"case\":\"none\"}");

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
    constexpr std::u8string_view kData{u8"\u00F6\u00F5"};
    const auto ref = irs::ViewCast<char>(kData);

    auto stream = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"de_DE.UTF8\", \"case\":\"lower\", \"accent\":false}");

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(ref));
    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());

    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(kData.size(), offset->end);
    ASSERT_TRUE(u8"\u006F\u006F" == irs::ViewCast<char8_t>(term->value));

    ASSERT_FALSE(stream->next());
  }

  // load jSON invalid
  {
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "norm", irs::Type<irs::text_format::Json>::get(),
                         std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "norm", irs::Type<irs::text_format::Json>::get(), "1"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "norm", irs::Type<irs::text_format::Json>::get(), "[]"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "norm", irs::Type<irs::text_format::Json>::get(), "{}"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "norm", irs::Type<irs::text_format::Json>::get(),
                         "{\"locale\":1}"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "norm", irs::Type<irs::text_format::Json>::get(),
                         "{\"locale\":\"en\", \"case\":42}"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "norm", irs::Type<irs::text_format::Json>::get(),
                         "{\"locale\":\"en\", \"accent\":42}"));
  }

  // load text
  {
    std::string_view data("running");
    auto stream = irs::analysis::analyzers::Get(
      "norm", irs::Type<irs::text_format::Json>::get(), R"({"locale":"en"})");

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
}

TEST_F(NormalizingTokenizerTests, test_make_config_json) {
  // with unknown parameter
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"case\":\"lower\",\"invalid_parameter\":"
      "true,\"accent\":true}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "norm", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"accent\":true}")
                ->toString(),
              actual);
  }

  // test vpack
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"case\":\"lower\",\"invalid_parameter\":"
      "true,\"accent\":true}";
    auto in_vpack = vpack::Parser::fromJson(config.c_str(), config.size());
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      out_str, "norm", irs::Type<irs::text_format::VPack>::get(), in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"accent\":true}")
                ->toString(),
              out_slice.toString());
  }

  // test vpack with variant
  {
    std::string config =
      "{\"locale\":\"ru_RU_TRADITIONAL.UTF-8\",\"case\":\"lower\",\"invalid_"
      "parameter\":true,\"accent\":true}";
    auto in_vpack = vpack::Parser::fromJson(config.c_str(), config.size());
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      out_str, "norm", irs::Type<irs::text_format::VPack>::get(), in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(vpack::Parser::fromJson("{\"locale\":\"ru_RU_TRADITIONAL.UTF-8\","
                                      "\"case\":\"lower\",\"accent\":true}")
                ->toString(),
              out_slice.toString());
  }

  // test vpack with variant
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8@EURO\",\"case\":\"lower\",\"invalid_"
      "parameter\":true,\"accent\":true}";
    auto in_vpack = vpack::Parser::fromJson(config.c_str(), config.size());
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      out_str, "norm", irs::Type<irs::text_format::VPack>::get(), in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"accent\":true}")
                ->toString(),
              out_slice.toString());
  }

  // no case convert in creation. Default value shown
  {
    std::string config = "{\"locale\":\"ru_RU.UTF-8\",\"accent\":true}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "norm", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"none\",\"accent\":true}")
                ->toString(),
              actual);
  }

  // no accent in creation. Default value shown
  {
    std::string config = "{\"locale\":\"ru_RU.UTF-8\",\"case\":\"lower\"}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "norm", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"accent\":true}")
                ->toString(),
              actual);
  }

  // non default values for accent and case
  {
    std::string config =
      "{\"locale\":\"ru_RU.utf-8\",\"case\":\"upper\",\"accent\":true}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "norm", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"upper\",\"accent\":true}")
                ->toString(),
              actual);
  }

  // non default values for accent and case
  {
    std::string config =
      "{\"locale\":\"de_DE@collation=phonebook\",\"case\":\"upper\",\"accent\":"
      "true}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "norm", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"de_DE\",\"case\":\"upper\",\"accent\":true}")
                ->toString(),
              actual);
  }
}

TEST_F(NormalizingTokenizerTests, test_make_config_text) {
  std::string config = "RU";
  std::string actual;
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    actual, "norm", irs::Type<irs::text_format::Text>::get(), config));
}

TEST_F(NormalizingTokenizerTests, test_invalid_locale) {
  auto stream = irs::analysis::analyzers::Get(
    "norm", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"invalid12345.UTF-8\"}");
  ASSERT_EQ(nullptr, stream);
}
