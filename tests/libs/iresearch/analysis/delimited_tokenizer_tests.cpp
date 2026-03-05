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

#include <vpack/common.h>
#include <vpack/parser.h>

#include "gtest/gtest.h"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "tests_config.hpp"

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

TEST_F(DelimitedTokenizerTests, test_delimiter) {
  // test delimiter std::string_view{}
  {
    std::string_view data("abc,def\"\",\"\"ghi");
    irs::analysis::DelimitedTokenizer stream(std::string_view{});
    ASSERT_EQ(irs::Type<irs::analysis::DelimitedTokenizer>::id(),
              stream.type());

    ASSERT_TRUE(stream.reset(data));

    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ("abc,def\"\",\"\"ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test delimteter ''
  {
    std::string_view data("abc,\"def\"");  // quoted terms should be honoured
    irs::analysis::DelimitedTokenizer stream("");

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(1, offset->end);
    ASSERT_EQ("a", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(1, offset->start);
    ASSERT_EQ(2, offset->end);
    ASSERT_EQ("b", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(2, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("c", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(3, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ(",", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(9, offset->end);
    ASSERT_EQ("def", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test delimiter ','
  {
    std::string_view data("abc,\"def,\"");  // quoted terms should be honoured
    irs::analysis::DelimitedTokenizer stream(",");

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(10, offset->end);
    ASSERT_EQ("def,", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test delimiter '\t'
  {
    std::string_view data(
      "abc,\t\"def\t\"");  // quoted terms should be honoured
    irs::analysis::DelimitedTokenizer stream("\t");

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ("abc,", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(5, offset->start);
    ASSERT_EQ(11, offset->end);
    ASSERT_EQ("def\t", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test delimiter '"'
  {
    std::string_view data(
      "abc,\"\"def\t\"");  // quoted terms should be honoured
    irs::analysis::DelimitedTokenizer stream("\"");

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ("abc,", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(5, offset->start);
    ASSERT_EQ(5, offset->end);
    ASSERT_EQ("", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(6, offset->start);
    ASSERT_EQ(10, offset->end);
    ASSERT_EQ("def\t", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(11, offset->start);
    ASSERT_EQ(11, offset->end);
    ASSERT_EQ("", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test delimiter 'abc'
  {
    std::string_view data(
      "abc,123\"def123\"");  // quoted terms should be honoured
    irs::analysis::DelimitedTokenizer stream("123");

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ("abc,", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(7, offset->start);
    ASSERT_EQ(15, offset->end);
    ASSERT_EQ("def123", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }
}

TEST_F(DelimitedTokenizerTests, test_quote) {
  // test quoted field
  {
    std::string_view data(
      "abc,\"def\",\"\"ghi");  // quoted terms should be honoured

    auto test_func = [](std::string_view data,
                        irs::analysis::Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* offset = irs::get<irs::OffsAttr>(*p_stream);
      auto* payload = irs::get<irs::PayAttr>(*p_stream);
      ASSERT_EQ(nullptr, payload);
      auto* term = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(3, offset->end);
      ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(4, offset->start);
      ASSERT_EQ(9, offset->end);
      ASSERT_EQ("def", irs::ViewCast<char>(term->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(10, offset->start);
      ASSERT_EQ(15, offset->end);
      ASSERT_EQ("\"\"ghi", irs::ViewCast<char>(term->value));
      ASSERT_FALSE(p_stream->next());
    };

    {
      irs::analysis::DelimitedTokenizer stream(",");
      test_func(data, &stream);
    }
    {
      auto stream = irs::analysis::analyzers::Get(
        "delimiter", irs::Type<irs::text_format::Json>::get(),
        "{\"delimiter\":\",\"}");
      test_func(data, stream.get());
    }
  }

  // test unterminated "
  {
    std::string_view data(
      "abc,\"def\",\"ghi");  // quoted terms should be honoured
    irs::analysis::DelimitedTokenizer stream(",");

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(9, offset->end);
    ASSERT_EQ("def", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(10, offset->start);
    ASSERT_EQ(14, offset->end);
    ASSERT_EQ("\"ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test unterminated single "
  {
    std::string_view data("abc,\"def\",\"");  // quoted terms should be honoured
    irs::analysis::DelimitedTokenizer stream(",");

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(9, offset->end);
    ASSERT_EQ("def", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(10, offset->start);
    ASSERT_EQ(11, offset->end);
    ASSERT_EQ("\"", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test " escape
  {
    std::string_view data(
      "abc,\"\"\"def\",\"\"ghi");  // quoted terms should be honoured
    irs::analysis::DelimitedTokenizer stream(",");

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(11, offset->end);
    ASSERT_EQ("\"def", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(12, offset->start);
    ASSERT_EQ(17, offset->end);
    ASSERT_EQ("\"\"ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test non-quoted field with "
  {
    std::string_view data(
      "abc,\"def\",ghi\"");  // quoted terms should be honoured
    irs::analysis::DelimitedTokenizer stream(",");

    ASSERT_TRUE(stream.reset(data));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* payload = irs::get<irs::PayAttr>(stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(9, offset->end);
    ASSERT_EQ("def", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(10, offset->start);
    ASSERT_EQ(14, offset->end);
    ASSERT_EQ("ghi\"", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }
}

TEST_F(DelimitedTokenizerTests, test_load) {
  // load jSON string
  {
    std::string_view data("abc,def,ghi");  // quoted terms should be honoured
    auto stream = irs::analysis::analyzers::Get(
      "delimiter", irs::Type<irs::text_format::Json>::get(), "\",\"");

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("def", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(8, offset->start);
    ASSERT_EQ(11, offset->end);
    ASSERT_EQ("ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  // load jSON object
  {
    std::string_view data("abc,def,ghi");  // quoted terms should be honoured
    auto stream = irs::analysis::analyzers::Get(
      "delimiter", irs::Type<irs::text_format::Json>::get(),
      "{\"delimiter\":\",\"}");

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("def", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(8, offset->start);
    ASSERT_EQ(11, offset->end);
    ASSERT_EQ("ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  // load jSON invalid
  {
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "delimiter", irs::Type<irs::text_format::Json>::get(),
                         std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "delimiter", irs::Type<irs::text_format::Json>::get(), "1"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "delimiter", irs::Type<irs::text_format::Json>::get(), "[]"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "delimiter", irs::Type<irs::text_format::Json>::get(), "{}"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "delimiter", irs::Type<irs::text_format::Json>::get(),
                         "{\"delimiter\":1}"));
  }

  // load text
  {
    std::string_view data("abc,def,ghi");  // quoted terms should be honoured
    auto stream = irs::analysis::analyzers::Get(
      "delimiter", irs::Type<irs::text_format::Text>::get(), ",");

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ("def", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(8, offset->start);
    ASSERT_EQ(11, offset->end);
    ASSERT_EQ("ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }

  // load text, wide symbols
  {
    std::string_view data(
      "\x61\x62\x63\x2C\xD0\x9F");  // quoted terms should be honoured
    auto stream = irs::analysis::analyzers::Get(
      "delimiter", irs::Type<irs::text_format::Text>::get(), ",");

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(6, offset->end);
    ASSERT_EQ("\xD0\x9F", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
  }
}

TEST_F(DelimitedTokenizerTests, test_make_config_json) {
  // with unknown parameter
  {
    std::string config = "{\"delimiter\":\",\",\"invalid_parameter\":true}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "delimiter", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson("{\"delimiter\":\",\"}")->toString(),
              actual);
  }

  // test vpack
  {
    std::string config = "{\"delimiter\":\",\",\"invalid_parameter\":true}";
    auto in_vpack = vpack::Parser::fromJson(config.c_str(), config.size());
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      out_str, "delimiter", irs::Type<irs::text_format::VPack>::get(), in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(vpack::Parser::fromJson("{\"delimiter\":\",\"}")->toString(),
              out_slice.toString());
  }
}

TEST_F(DelimitedTokenizerTests, test_make_config_text) {
  std::string config = ",";
  std::string actual;
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    actual, "delimiter", irs::Type<irs::text_format::Text>::get(), config));
  ASSERT_EQ(config, actual);
}
