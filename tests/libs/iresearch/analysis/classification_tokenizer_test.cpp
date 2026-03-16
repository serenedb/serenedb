////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// @author Alex Geenen
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <vpack/common.h>
#include <vpack/parser.h>

#include "iresearch/analysis/classification_tokenizer.hpp"
#include "tests_shared.hpp"

namespace {

std::string_view gExpectedModel;

irs::analysis::ClassificationTokenizer::model_ptr NullProvider(
  std::string_view model) {
  EXPECT_EQ(gExpectedModel, model);
  return nullptr;
}

irs::analysis::ClassificationTokenizer::model_ptr ThrowingProvider(
  std::string_view model) {
  EXPECT_EQ(gExpectedModel, model);
  throw std::exception();
}

}  // namespace

TEST(classification_tokenizer_test, consts) {
  static_assert("classification" ==
                irs::Type<irs::analysis::ClassificationTokenizer>::name());
}

TEST(classification_tokenizer_test, test_load) {
  // load json string
  {
#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
    std::string_view data{"baking"};
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::Get(
      "classification", irs::Type<irs::text_format::Json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(6, offset->end);
    ASSERT_EQ("__label__baking", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());
  }

  // multi-word input
  {
#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::Get(
      "classification", irs::Type<irs::text_format::Json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset("Why not put knives in the dishwasher?"));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(37, offset->end);
    ASSERT_EQ("__label__knives", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());

    ASSERT_TRUE(stream->reset("pasta coca-cola"));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(15, offset->end);
    ASSERT_EQ("__label__pasta", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());
  }

  // Multi line input
  {
    constexpr std::string_view kData{
      "Which baking dish is best to bake\na banana bread ?"};

#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
    auto input_json = "{\"model_location\": \"" + model_loc + "\"}";
    auto stream = irs::analysis::analyzers::Get(
      "classification", irs::Type<irs::text_format::Json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(kData));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(50, offset->end);
    ASSERT_EQ("__label__baking", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());
  }
  // top 2 labels
  {
    constexpr std::string_view kData{
      "Which baking dish is best to bake a banana bread ?"};

#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
    auto input_json =
      "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2}";
    auto stream = irs::analysis::analyzers::Get(
      "classification", irs::Type<irs::text_format::Json>::get(), input_json);

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(kData));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(50, offset->end);
    ASSERT_EQ("__label__baking", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(50, offset->end);
    ASSERT_EQ(0, inc->value);
    ASSERT_EQ("__label__bananas", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());
  }

  // failing cases
  {
#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif

    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "classification",
                         irs::Type<irs::text_format::Json>::get(), R"([])"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "classification", irs::Type<irs::text_format::Json>::get(),
                R"({"model_location": "invalid_localtion" })"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "classification", irs::Type<irs::text_format::Json>::get(),
                R"({"model_location": bool })"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "classification", irs::Type<irs::text_format::Json>::get(),
                R"({"model_location": {} })"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "classification", irs::Type<irs::text_format::Json>::get(),
                R"({"model_location": 42 })"));
    ASSERT_EQ(
      nullptr,
      irs::analysis::analyzers::Get(
        "classification", irs::Type<irs::text_format::Json>::get(),
        "{\"model_location\": \"" + model_loc + "\", \"top_k\": false}"));
    ASSERT_EQ(
      nullptr,
      irs::analysis::analyzers::Get(
        "classification", irs::Type<irs::text_format::Json>::get(),
        "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2147483648}"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "classification", irs::Type<irs::text_format::Json>::get(),
                "{\"model_location\": \"" + model_loc +
                  "\", \"top_k\": 42, \"threshold\": 1.1}"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "classification", irs::Type<irs::text_format::Json>::get(),
                "{\"model_location\": \"" + model_loc +
                  "\", \"top_k\": 42, \"threshold\": -0.1}"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "classification", irs::Type<irs::text_format::Json>::get(),
                "{\"model_location\": \"" + model_loc +
                  "\", \"top_k\": 42, \"threshold\": false}"));
  }
}

TEST(classification_tokenizer_test, test_custom_provider) {
#ifdef WIN32
  const auto model_loc =
    test_base::resource("model_cooking.bin").generic_string();
#else
  const auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
  gExpectedModel = model_loc;

  const auto input_json =
    "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2}";

  ASSERT_EQ(nullptr, irs::analysis::ClassificationTokenizer::set_model_provider(
                       &::NullProvider));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "classification",
                       irs::Type<irs::text_format::Json>::get(), input_json));

  ASSERT_EQ(&::NullProvider,
            irs::analysis::ClassificationTokenizer::set_model_provider(
              &::ThrowingProvider));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "classification",
                       irs::Type<irs::text_format::Json>::get(), input_json));

  ASSERT_EQ(
    &::ThrowingProvider,
    irs::analysis::ClassificationTokenizer::set_model_provider(nullptr));
}

TEST(classification_tokenizer_test, test_make_config_json) {
  // random extra param
  {
#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
    std::string config =
      "{\"model_location\": \"" + model_loc + "\", \"not_valid\": false}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc +
                                "\", \"top_k\": 1, \"threshold\": 0.0}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "classification", irs::Type<irs::text_format::Json>::get(),
      config));
    ASSERT_EQ(vpack::Parser::fromJson(expected_conf)->toString(), actual);
  }

  // test top k
  {
#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
    std::string config =
      "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc +
                                "\", \"top_k\": 2, \"threshold\": 0.0}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "classification", irs::Type<irs::text_format::Json>::get(),
      config));
    ASSERT_EQ(vpack::Parser::fromJson(expected_conf)->toString(), actual);
  }

  // test threshold
  {
#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
    std::string config =
      "{\"model_location\": \"" + model_loc + "\", \"threshold\": 0.1}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc +
                                "\", \"top_k\": 1, \"threshold\": 0.1}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "classification", irs::Type<irs::text_format::Json>::get(),
      config));
    ASSERT_EQ(vpack::Parser::fromJson(expected_conf)->toString(), actual);
  }

  // test all 3 params
  {
#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
    std::string config = "{\"model_location\": \"" + model_loc +
                         "\", \"threshold\": 0.1, \"top_k\": 2}";
    std::string expected_conf = "{\"model_location\": \"" + model_loc +
                                "\", \"top_k\": 2, \"threshold\": 0.1}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "classification", irs::Type<irs::text_format::Json>::get(),
      config));
    ASSERT_EQ(vpack::Parser::fromJson(expected_conf)->toString(), actual);
  }

  // test VPack
  {
#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif
    std::string config =
      "{\"model_location\":\"" + model_loc + "\", \"not_valid\": false}";
    auto expected_conf = "{\"model_location\": \"" + model_loc +
                         "\", \"top_k\": 1, \"threshold\": 0.0}";
    auto in_vpack = vpack::Parser::fromJson(config);
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      out_str, "classification", irs::Type<irs::text_format::VPack>::get(),
      in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(vpack::Parser::fromJson(expected_conf)->toString(),
              out_slice.toString());
  }

  // failing cases
  {
    std::string out;
#ifdef WIN32
    auto model_loc = test_base::resource("model_cooking.bin").generic_string();
#else
    auto model_loc = TestBase::resource("model_cooking.bin").string();
#endif

    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      out, "classification", irs::Type<irs::text_format::Json>::get(),
      R"([])"));
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      out, "classification", irs::Type<irs::text_format::Json>::get(),
      R"({"model_location": bool })"));
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      out, "classification", irs::Type<irs::text_format::Json>::get(),
      R"({"model_location": {} })"));
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      out, "classification", irs::Type<irs::text_format::Json>::get(),
      R"({"model_location": 42 })"));
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      out, "classification", irs::Type<irs::text_format::Json>::get(),
      "{\"model_location\": \"" + model_loc + "\", \"top_k\": false}"));
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      out, "classification", irs::Type<irs::text_format::Json>::get(),
      "{\"model_location\": \"" + model_loc +
        "\", \"top_k\": 42, \"threshold\": 25}"));
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      out, "classification", irs::Type<irs::text_format::Json>::get(),
      "{\"model_location\": \"" + model_loc +
        "\", \"top_k\": 42, \"threshold\": false}"));
    ASSERT_FALSE(irs::analysis::analyzers::Normalize(
      out, "classification", irs::Type<irs::text_format::Json>::get(),
      "{\"model_location\": \"" + model_loc + "\", \"top_k\": 2147483648}"));
  }
}
