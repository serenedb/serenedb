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

#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace tests {

class AnalyzerTest : public ::testing::Test {};

}  // namespace tests

using namespace tests;

// NOTE: the legacy `duplicate_register` test exercised the now-deleted
// AnalyzerRegistrar / analyzers::Exists APIs and has been removed alongside
// the registry. Variant-based dispatch has no runtime registration step so
// there is nothing to verify here.

TEST_F(AnalyzerTest, test_load) {
  // locale with default ingnored_words
  {
    auto analyzer =
      irs::analysis::TextTokenizer::Make(irs::analysis::TextTokenizer::Options{
        .locale = icu::Locale::createFromName("en"),
      });

    ASSERT_NE(nullptr, analyzer);
    ASSERT_TRUE(tests::Analyze(*analyzer, "abc").has_value());
  }

  // locale with provided ignored_words
  {
    irs::analysis::TextTokenizer::Options opts{
      .locale = icu::Locale::createFromName("en"),
    };
    opts.explicit_stopwords.insert("abc");
    opts.explicit_stopwords.insert("def");
    opts.explicit_stopwords.insert("ghi");
    opts.explicit_stopwords_set = true;
    auto analyzer = irs::analysis::TextTokenizer::Make(std::move(opts));

    ASSERT_NE(nullptr, analyzer);
    ASSERT_TRUE(tests::Analyze(*analyzer, "abc").has_value());
  }

  // .........................................................................
  // invalid: missing required locale -- the old JSON path rejected "{}".
  // Direct Options API: a default-constructed Options has `locale` set to
  // `MakeBogusLocale()`; Make should reject because BreakIterator cannot be
  // built from a bogus locale.
  // .........................................................................
  {
    ASSERT_ANY_THROW(irs::analysis::TextTokenizer::Make(
      irs::analysis::TextTokenizer::Options{}));
  }
}
