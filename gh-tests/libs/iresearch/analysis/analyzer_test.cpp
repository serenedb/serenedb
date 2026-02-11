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

#include <iresearch/analysis/analyzers.hpp>

#include "basics/runtime_utils.hpp"
#include "tests_config.hpp"
#include "tests_shared.hpp"

namespace tests {

class AnalyzerTest : public ::testing::Test {
  void SetUp() final {
    // Code here will be called immediately after the constructor (right before
    // each test).

    // ensure stopwords are loaded/cached for the 'en' locale used for text
    // analysis below
    {
      // same env variable name as
      // irs::analysis::text_token_stream::STOPWORD_PATH_ENV_VARIABLE
      const auto text_stopword_path_var = "IRESEARCH_TEXT_STOPWORD_PATH";
      const char* cz_old_stopword_path = irs::Getenv(text_stopword_path_var);
      std::string s_old_stopword_path =
        cz_old_stopword_path == nullptr ? "" : cz_old_stopword_path;

      irs::Setenv(text_stopword_path_var, IRS_TEST_RESOURCE_DIR, true);
      irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Text>::get(),
        "en");  // stream needed only to load stopwords

      if (cz_old_stopword_path) {
        irs::Setenv(text_stopword_path_var, s_old_stopword_path.c_str(), true);
      }
    }
  }

  void TearDown() final {
    // Code here will be called immediately after each test (right before the
    // destructor).
  }
};

}  // namespace tests

using namespace tests;

TEST_F(AnalyzerTest, duplicate_register) {
  struct DummyAnalyzer : public irs::analysis::TypedAnalyzer<DummyAnalyzer> {
    static ptr Make(std::string_view) { return ptr(new DummyAnalyzer()); }
    static bool Normalize(std::string_view, std::string&) { return true; }
    irs::Attribute* GetMutable(irs::TypeInfo::type_id) final { return nullptr; }
    bool next() final { return false; }
    bool reset(std::string_view) final { return false; }
  };

  static bool gInitialExpected = true;

  // check required for tests with repeat (static maps are not cleared between
  // runs)
  if (gInitialExpected) {
    ASSERT_FALSE(irs::analysis::analyzers::Exists(
      irs::Type<DummyAnalyzer>::name(),
      irs::Type<irs::text_format::VPack>::get()));
    ASSERT_FALSE(irs::analysis::analyzers::Exists(
      irs::Type<DummyAnalyzer>::name(),
      irs::Type<irs::text_format::Json>::get()));
    ASSERT_FALSE(irs::analysis::analyzers::Exists(
      irs::Type<DummyAnalyzer>::name(),
      irs::Type<irs::text_format::Text>::get()));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                irs::Type<DummyAnalyzer>::name(),
                irs::Type<irs::text_format::Json>::get(), std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                irs::Type<DummyAnalyzer>::name(),
                irs::Type<irs::text_format::Text>::get(), std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                irs::Type<DummyAnalyzer>::name(),
                irs::Type<irs::text_format::VPack>::get(), std::string_view{}));

    irs::analysis::AnalyzerRegistrar initial0(
      irs::Type<DummyAnalyzer>::get(),
      irs::Type<irs::text_format::VPack>::get(), &DummyAnalyzer::Make,
      &DummyAnalyzer::Normalize);
    irs::analysis::AnalyzerRegistrar initial1(
      irs::Type<DummyAnalyzer>::get(), irs::Type<irs::text_format::Json>::get(),
      &DummyAnalyzer::Make, &DummyAnalyzer::Normalize);
    irs::analysis::AnalyzerRegistrar initial2(
      irs::Type<DummyAnalyzer>::get(), irs::Type<irs::text_format::Text>::get(),
      &DummyAnalyzer::Make, &DummyAnalyzer::Normalize);
    ASSERT_EQ(!gInitialExpected, !initial0);
    ASSERT_EQ(!gInitialExpected, !initial1);
    ASSERT_EQ(!gInitialExpected, !initial2);
  }

  gInitialExpected = false;  // next test iteration will not be able to register
                             // the same analyzer
  irs::analysis::AnalyzerRegistrar duplicate0(
    irs::Type<DummyAnalyzer>::get(), irs::Type<irs::text_format::VPack>::get(),
    &DummyAnalyzer::Make, &DummyAnalyzer::Normalize);
  irs::analysis::AnalyzerRegistrar duplicate1(
    irs::Type<DummyAnalyzer>::get(), irs::Type<irs::text_format::Json>::get(),
    &DummyAnalyzer::Make, &DummyAnalyzer::Normalize);
  irs::analysis::AnalyzerRegistrar duplicate2(
    irs::Type<DummyAnalyzer>::get(), irs::Type<irs::text_format::Text>::get(),
    &DummyAnalyzer::Make, &DummyAnalyzer::Normalize);
  ASSERT_TRUE(!duplicate0);
  ASSERT_TRUE(!duplicate1);
  ASSERT_TRUE(!duplicate2);

  ASSERT_TRUE(irs::analysis::analyzers::Exists(
    irs::Type<DummyAnalyzer>::name(),
    irs::Type<irs::text_format::VPack>::get()));
  ASSERT_TRUE(
    irs::analysis::analyzers::Exists(irs::Type<DummyAnalyzer>::name(),
                                     irs::Type<irs::text_format::Json>::get()));
  ASSERT_TRUE(
    irs::analysis::analyzers::Exists(irs::Type<DummyAnalyzer>::name(),
                                     irs::Type<irs::text_format::Text>::get()));
  ASSERT_NE(nullptr,
            irs::analysis::analyzers::Get(
              irs::Type<DummyAnalyzer>::name(),
              irs::Type<irs::text_format::VPack>::get(), std::string_view{}));
  ASSERT_NE(nullptr,
            irs::analysis::analyzers::Get(
              irs::Type<DummyAnalyzer>::name(),
              irs::Type<irs::text_format::Json>::get(), std::string_view{}));
  ASSERT_NE(nullptr,
            irs::analysis::analyzers::Get(
              irs::Type<DummyAnalyzer>::name(),
              irs::Type<irs::text_format::Text>::get(), std::string_view{}));
}

TEST_F(AnalyzerTest, test_load) {
  {
    auto analyzer = irs::analysis::analyzers::Get(
      "text", irs::Type<irs::text_format::Text>::get(), "en");

    ASSERT_NE(nullptr, analyzer);
    ASSERT_TRUE(analyzer->reset("abc"));
  }

  // locale with default ingnored_words
  {
    auto analyzer = irs::analysis::analyzers::Get(
      "text", irs::Type<irs::text_format::Json>::get(), "{\"locale\":\"en\"}");

    ASSERT_NE(nullptr, analyzer);
    ASSERT_TRUE(analyzer->reset("abc"));
  }

  // locale with provided ignored_words
  {
    auto analyzer = irs::analysis::analyzers::Get(
      "text", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en\", \"stopwords\":[\"abc\", \"def\", \"ghi\"]}");

    ASSERT_NE(nullptr, analyzer);
    ASSERT_TRUE(analyzer->reset("abc"));
  }

  // ...........................................................................
  // invalid
  // ...........................................................................

  // missing required locale
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "text", irs::Type<irs::text_format::Json>::get(), "{}"));

  // invalid ignored_words
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "text", irs::Type<irs::text_format::Json>::get(),
                       "{{\"locale\":\"en\", \"stopwords\":\"abc\"}}"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "text", irs::Type<irs::text_format::Json>::get(),
                       "{{\"locale\":\"en\", \"stopwords\":[1, 2, 3]}}"));
}
