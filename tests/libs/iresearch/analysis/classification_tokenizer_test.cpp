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

std::string ModelLocation() {
#ifdef WIN32
  return test_base::resource("model_cooking.bin").generic_string();
#else
  return TestBase::resource("model_cooking.bin").string();
#endif
}

}  // namespace

TEST(classification_tokenizer_test, consts) {
  static_assert("classification" ==
                irs::Type<irs::analysis::ClassificationTokenizer>::name());
}

TEST(classification_tokenizer_test, test_load) {
  // load json string
  {
    std::string_view data{"baking"};
    auto stream = irs::analysis::ClassificationTokenizer::Make(
      irs::analysis::ClassificationTokenizer::Options{
        .model_location = ModelLocation(),
      });

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
    auto stream = irs::analysis::ClassificationTokenizer::Make(
      irs::analysis::ClassificationTokenizer::Options{
        .model_location = ModelLocation(),
      });

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

    auto stream = irs::analysis::ClassificationTokenizer::Make(
      irs::analysis::ClassificationTokenizer::Options{
        .model_location = ModelLocation(),
      });

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

    auto stream = irs::analysis::ClassificationTokenizer::Make(
      irs::analysis::ClassificationTokenizer::Options{
        .model_location = ModelLocation(),
        .top_k = 2,
      });

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

  // invalid model location
  ASSERT_ANY_THROW(irs::analysis::ClassificationTokenizer::Make(
    irs::analysis::ClassificationTokenizer::Options{
      .model_location = "invalid_localtion",
    }));

  // .........................................................................
  // additional invalid-Options cases ported from the old JSON failing-case
  // suite. JSON-parser-internal rejections (e.g. `"top_k": bool`,
  // `"model_location": 42`) have no direct-API analogue and are dropped.
  // .........................................................................

  // missing required model_location (default-constructed Options has empty
  // model_location).
  ASSERT_ANY_THROW(irs::analysis::ClassificationTokenizer::Make(
    irs::analysis::ClassificationTokenizer::Options{}));

  // top_k must be > 0.
  ASSERT_ANY_THROW(irs::analysis::ClassificationTokenizer::Make(
    irs::analysis::ClassificationTokenizer::Options{
      .model_location = ModelLocation(),
      .top_k = 0,
    }));
  ASSERT_ANY_THROW(irs::analysis::ClassificationTokenizer::Make(
    irs::analysis::ClassificationTokenizer::Options{
      .model_location = ModelLocation(),
      .top_k = -1,
    }));

  // threshold must be in [0.0, 1.0].
  ASSERT_ANY_THROW(irs::analysis::ClassificationTokenizer::Make(
    irs::analysis::ClassificationTokenizer::Options{
      .model_location = ModelLocation(),
      .threshold = 1.1,
      .top_k = 42,
    }));
  ASSERT_ANY_THROW(irs::analysis::ClassificationTokenizer::Make(
    irs::analysis::ClassificationTokenizer::Options{
      .model_location = ModelLocation(),
      .threshold = -0.1,
      .top_k = 42,
    }));
}

TEST(classification_tokenizer_test, test_custom_provider) {
  const auto model_loc = ModelLocation();
  gExpectedModel = model_loc;

  ASSERT_EQ(nullptr, irs::analysis::ClassificationTokenizer::set_model_provider(
                       &::NullProvider));
  ASSERT_ANY_THROW(irs::analysis::ClassificationTokenizer::Make(
    irs::analysis::ClassificationTokenizer::Options{
      .model_location = model_loc,
      .top_k = 2,
    }));

  ASSERT_EQ(&::NullProvider,
            irs::analysis::ClassificationTokenizer::set_model_provider(
              &::ThrowingProvider));
  ASSERT_ANY_THROW(irs::analysis::ClassificationTokenizer::Make(
    irs::analysis::ClassificationTokenizer::Options{
      .model_location = model_loc,
      .top_k = 2,
    }));

  ASSERT_EQ(
    &::ThrowingProvider,
    irs::analysis::ClassificationTokenizer::set_model_provider(nullptr));
}
