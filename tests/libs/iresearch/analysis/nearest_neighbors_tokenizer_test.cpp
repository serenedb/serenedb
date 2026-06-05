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

#include "iresearch/analysis/nearest_neighbors_tokenizer.hpp"
#include "tests_shared.hpp"

namespace {

std::string_view gExpectedModel;

irs::analysis::NearestNeighborsTokenizer::model_ptr NullProvider(
  std::string_view model) {
  EXPECT_EQ(gExpectedModel, model);
  return nullptr;
}

irs::analysis::NearestNeighborsTokenizer::model_ptr ThrowingProvider(
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

TEST(nearest_neighbors_tokenizer_test, consts) {
  static_assert("nearest_neighbors" ==
                irs::Type<irs::analysis::NearestNeighborsTokenizer>::name());
}

TEST(nearest_neighbors_tokenizer_test, test_custom_provider) {
  const auto model_loc = ModelLocation();
  gExpectedModel = model_loc;

  ASSERT_EQ(nullptr,
            irs::analysis::NearestNeighborsTokenizer::set_model_provider(
              &::NullProvider));
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = model_loc,
      .top_k = 2,
    }));

  ASSERT_EQ(&::NullProvider,
            irs::analysis::NearestNeighborsTokenizer::set_model_provider(
              &::ThrowingProvider));
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = model_loc,
      .top_k = 2,
    }));

  ASSERT_EQ(
    &::ThrowingProvider,
    irs::analysis::NearestNeighborsTokenizer::set_model_provider(nullptr));
}

TEST(nearest_neighbors_tokenizer_test, test_load) {
  // load json string
  {
    std::string_view data{"salt"};
    auto stream = irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
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
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ("homogenized", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());
  }

  {
    auto stream = irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
        .model_location = ModelLocation(),
        .top_k = 2,
      });

    ASSERT_NE(nullptr, stream);
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    ASSERT_TRUE(stream->reset("salt"));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ("homogenized", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, inc->value);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ("teach", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());

    ASSERT_TRUE(stream->reset("pizza"));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(5, offset->end);
    ASSERT_EQ("\"prepared\"", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(5, offset->end);
    ASSERT_EQ(0, inc->value);
    ASSERT_EQ("tinfoil", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());
  }

  // test longer string
  {
    std::string_view data{"salt oil"};
    auto stream = irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
        .model_location = ModelLocation(),
        .top_k = 2,
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
    ASSERT_EQ(8, offset->end);
    ASSERT_EQ("homogenized", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, inc->value);
    ASSERT_EQ("teach", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("tube\"", irs::ViewCast<char>(term->value));
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, inc->value);
    ASSERT_EQ("\"breather", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
    ASSERT_FALSE(stream->next());
  }

  // invalid model location
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = "invalid_localtion",
    }));

  // .........................................................................
  // additional invalid-Options cases ported from the old JSON failing-case
  // suite. JSON-parser-internal rejections (e.g. `"model_location": 42`,
  // `"top_k": false`) have no direct-API analogue and are dropped.
  // .........................................................................

  // missing required model_location.
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{}));

  // top_k must be > 0.
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = ModelLocation(),
      .top_k = 0,
    }));
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = ModelLocation(),
      .top_k = -1,
    }));
}
