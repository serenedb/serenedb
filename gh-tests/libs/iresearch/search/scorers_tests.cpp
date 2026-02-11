////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/index/field_meta.hpp>
#include <iresearch/search/scorers.hpp>

#include "tests_shared.hpp"

TEST(scorers_tests, duplicate_register) {
  struct DummyScorer : public irs::ScorerBase<DummyScorer, void> {
    irs::IndexFeatures GetIndexFeatures() const final {
      return irs::IndexFeatures::None;
    }

    irs::ScoreFunction PrepareScorer(
      const irs::ColumnProvider& /*segment*/,
      const irs::FieldProperties& /*features*/, const irs::byte_type* /*stats*/,
      const irs::AttributeProvider& /*doc_attrs*/,
      irs::score_t /*boost*/) const final {
      return {};
    }

    static irs::Scorer::ptr Make(std::string_view) {
      return std::make_unique<DummyScorer>();
    }
  };

  static bool gInitialExpected = true;

  // check required for tests with repeat (static maps are not cleared between
  // runs)
  if (gInitialExpected) {
    ASSERT_FALSE(
      irs::scorers::Exists(irs::Type<DummyScorer>::name(),
                           irs::Type<irs::text_format::VPack>::get()));
    ASSERT_FALSE(
      irs::scorers::Exists(irs::Type<DummyScorer>::name(),
                           irs::Type<irs::text_format::Json>::get()));
    ASSERT_FALSE(
      irs::scorers::Exists(irs::Type<DummyScorer>::name(),
                           irs::Type<irs::text_format::Text>::get()));
    ASSERT_EQ(nullptr,
              irs::scorers::Get(irs::Type<DummyScorer>::name(),
                                irs::Type<irs::text_format::VPack>::get(),
                                std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::scorers::Get(irs::Type<DummyScorer>::name(),
                                irs::Type<irs::text_format::Json>::get(),
                                std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::scorers::Get(irs::Type<DummyScorer>::name(),
                                irs::Type<irs::text_format::Text>::get(),
                                std::string_view{}));

    irs::ScorerRegistrar initial0(irs::Type<DummyScorer>::get(),
                                  irs::Type<irs::text_format::VPack>::get(),
                                  &DummyScorer::Make);
    irs::ScorerRegistrar initial1(irs::Type<DummyScorer>::get(),
                                  irs::Type<irs::text_format::Json>::get(),
                                  &DummyScorer::Make);
    irs::ScorerRegistrar initial2(irs::Type<DummyScorer>::get(),
                                  irs::Type<irs::text_format::Text>::get(),
                                  &DummyScorer::Make);

    ASSERT_EQ(!gInitialExpected, !initial0);
    ASSERT_EQ(!gInitialExpected, !initial1);
    ASSERT_EQ(!gInitialExpected, !initial2);
  }

  gInitialExpected =
    false;  // next test iteration will not be able to register the same scorer
  irs::ScorerRegistrar duplicate0(irs::Type<DummyScorer>::get(),
                                  irs::Type<irs::text_format::VPack>::get(),
                                  &DummyScorer::Make);
  irs::ScorerRegistrar duplicate1(irs::Type<DummyScorer>::get(),
                                  irs::Type<irs::text_format::Json>::get(),
                                  &DummyScorer::Make);
  irs::ScorerRegistrar duplicate2(irs::Type<DummyScorer>::get(),
                                  irs::Type<irs::text_format::Text>::get(),
                                  &DummyScorer::Make);
  ASSERT_TRUE(!duplicate0);
  ASSERT_TRUE(!duplicate1);
  ASSERT_TRUE(!duplicate2);

  ASSERT_TRUE(irs::scorers::Exists(irs::Type<DummyScorer>::name(),
                                   irs::Type<irs::text_format::VPack>::get()));
  ASSERT_TRUE(irs::scorers::Exists(irs::Type<DummyScorer>::name(),
                                   irs::Type<irs::text_format::Json>::get()));
  ASSERT_TRUE(irs::scorers::Exists(irs::Type<DummyScorer>::name(),
                                   irs::Type<irs::text_format::Text>::get()));
  ASSERT_NE(nullptr,
            irs::scorers::Get(irs::Type<DummyScorer>::name(),
                              irs::Type<irs::text_format::VPack>::get(),
                              std::string_view{}));
  ASSERT_NE(nullptr, irs::scorers::Get(irs::Type<DummyScorer>::name(),
                                       irs::Type<irs::text_format::Json>::get(),
                                       std::string_view{}));
  ASSERT_NE(nullptr, irs::scorers::Get(irs::Type<DummyScorer>::name(),
                                       irs::Type<irs::text_format::Text>::get(),
                                       std::string_view{}));
}
