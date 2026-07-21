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

#include <unicode/coll.h>      // for icu::Collator
#include <unicode/decimfmt.h>  // for icu::DecimalFormat
#include <unicode/numfmt.h>    // for icu::NumberFormat
#include <unicode/ucnv.h>      // for UConverter
#include <unicode/ustring.h>   // for u_strToUTF32, u_strToUTF8

#include <unordered_set>

#include "basics/file_utils_ext.hpp"
#include "gtest/gtest.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "tests_config.hpp"
#include "token_sink_utils.hpp"

namespace {

// Helper to build a TextTokenizer::Options structure mirroring the most
// common combinations the legacy JSON-driven tests used. Boolean knobs
// (accent, stemming) follow TextTokenizer defaults (`accent=false`,
// `stemming=true`); the variants below override the few that mattered.
// Holds the subset of `TextTokenizer::Options` fields the JSON-driven legacy
// tests used to flip. `explicit_stopwords_set` tracks whether `stopwords`
// was explicitly set in the source JSON (mirroring the loader's behaviour;
// MakeText auto-derives it from whether `stopwords` is non-empty unless the
// caller overrides it via `explicit_stopwords_set_override`).
struct TextOpts {
  std::string locale = "en_US.UTF-8";
  irs::Case case_convert = irs::Case::Lower;
  std::vector<std::string> stopwords;
  // Allow callers to force `explicit_stopwords_set` (e.g. tests that want an
  // empty stopwords list to mean "don't load defaults from path").
  std::optional<bool> explicit_stopwords_set_override;
  std::string stopwords_path = std::string(1, '\0');  // sentinel = unset
  bool accent = false;
  bool stemming = true;
  size_t min_gram = 0;
  size_t max_gram = 0;
  bool min_gram_set = false;
  bool max_gram_set = false;
  bool preserve_original = false;
  bool preserve_original_set = false;
};

irs::analysis::Tokenizer::ptr MakeText(TextOpts opts = {}) {
  irs::analysis::TextTokenizer::Options o;
  o.locale = icu::Locale::createFromName(opts.locale.c_str());
  o.case_convert = opts.case_convert;
  o.accent = opts.accent;
  o.stemming = opts.stemming;
  for (auto& w : opts.stopwords) {
    o.explicit_stopwords.insert(std::move(w));
  }
  // Default to "explicit_stopwords was set" -- the JSON-driven legacy tests
  // always passed a `stopwords` field (often empty []) to mean "use this set,
  // do not load defaults from the path". Tests that genuinely want the loader
  // to look up defaults from the env path opt in via
  // `explicit_stopwords_set_override = false`.
  o.explicit_stopwords_set =
    opts.explicit_stopwords_set_override.value_or(true);
  o.stopwords_path = std::move(opts.stopwords_path);
  o.min_gram = opts.min_gram;
  o.max_gram = opts.max_gram;
  o.min_gram_set = opts.min_gram_set;
  o.max_gram_set = opts.max_gram_set;
  o.preserve_original = opts.preserve_original;
  o.preserve_original_set = opts.preserve_original_set;
  return irs::analysis::TextTokenizer::Make(std::move(o));
}

}  // namespace
namespace tests {

class TextAnalyzerParserTestSuite : public ::testing::Test {
 protected:
  void SetStopwordsPath(const char* path) {
    _stopwords_path_set = true;
    _old_stopwords_path_set = false;

    const char* old_stopwords_path =
      std::getenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable);
    if (old_stopwords_path) {
      _old_stopwords_path = old_stopwords_path;
      _old_stopwords_path_set = true;
    }

    if (path) {
      ::setenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable, path,
               /*overwrite=*/1);
    } else {
      ::unsetenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable);
      ASSERT_EQ(
        nullptr,
        std::getenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable));
    }
  }

  void TearDown() final {
    if (_stopwords_path_set) {
      if (_old_stopwords_path_set) {
        ::setenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable,
                 _old_stopwords_path.data(), /*overwrite=*/1);
      } else {
        ::unsetenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable);
      }
    }
  }

 private:
  std::string _old_stopwords_path;
  bool _old_stopwords_path_set{false};
  bool _stopwords_path_set{false};
};

}  // namespace tests

using namespace tests;
using namespace irs::analysis;

TEST_F(TextAnalyzerParserTestSuite, consts) {
  static_assert("text" == irs::Type<irs::analysis::TextTokenizer>::name());
}

TEST_F(TextAnalyzerParserTestSuite, test_nbsp_whitespace) {
  irs::analysis::TextTokenizer::Options options;

  options.locale =
    icu::Locale::createFromName("C.UTF-8");  // utf8 encoding used bellow

  std::string s_data_ut_f8 = "1,24 prosenttia";

  irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
  ASSERT_EQ(irs::Type<irs::analysis::TextTokenizer>::id(), stream.type());

  auto tokens = tests::Analyze(stream, s_data_ut_f8);
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected{{"1,24", 1, 0, 4},
                                                   {"prosenttia", 2, 5, 15}};
  ASSERT_EQ(expected, *tokens);
}

TEST_F(TextAnalyzerParserTestSuite, test_text_analyzer) {
  std::unordered_set<std::string> empty_set;
  std::string s_field = "test field";

  // default behaviour
  {
    irs::analysis::TextTokenizer::Options options;

    options.locale = icu::Locale::createFromName("en_US.UTF-8");

    std::string data =
      " A  hErd of   quIck brown  foXes ran    and Jumped over  a     "
      "runninG dog";
    irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      auto tokens = tests::Analyze(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<tests::AnalyzerToken> expected{
        {"a", 1, 1, 2},       {"herd", 2, 4, 8},    {"of", 3, 9, 11},
        {"quick", 4, 14, 19}, {"brown", 5, 20, 25}, {"fox", 6, 27, 32},
        {"ran", 7, 33, 36},   {"and", 8, 40, 43},   {"jump", 9, 44, 50},
        {"over", 10, 51, 55}, {"a", 11, 57, 58},    {"run", 12, 63, 70},
        {"dog", 13, 71, 74}};
      ASSERT_EQ(expected, *tokens);
    };

    {
      irs::analysis::TextTokenizer::Options options;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");

      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {},
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // case convert (lower)
  {
    std::string data = "A qUiCk brOwn FoX";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"a", "quick", "brown", "fox"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      irs::analysis::TextTokenizer::Options options;
      options.case_convert = irs::Case::Lower;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .case_convert = irs::Case::Lower,
        .stopwords = {},
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // case convert (upper)
  {
    std::string data = "A qUiCk brOwn FoX";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"A", "QUICK", "BROWN", "FOX"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      irs::analysis::TextTokenizer::Options options;
      options.case_convert = irs::Case::Upper;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .case_convert = irs::Case::Upper,
        .stopwords = {},
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // case convert (none)
  {
    std::string data = "A qUiCk brOwn FoX";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"A", "qUiCk", "brOwn", "FoX"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      irs::analysis::TextTokenizer::Options options;
      options.case_convert = irs::Case::None;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .case_convert = irs::Case::None,
        .stopwords = {},
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // ignored words
  {
    std::string data = " A thing of some KIND and ANoTher ";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      auto tokens = tests::Analyze(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"thing", "some", "kind", "anoth"};
      ASSERT_EQ(expected.size(), tokens->size());
      for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i], (*tokens)[i].term);
        ASSERT_EQ(i + 1, (*tokens)[i].pos);
      }
    };

    {
      irs::analysis::TextTokenizer::Options options;
      options.explicit_stopwords = {"a", "of", "and"};
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      irs::analysis::TextTokenizer::Options options;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      options.explicit_stopwords = {"a", "of", "and"};
      options.explicit_stopwords_set = true;
      auto stream = irs::analysis::TextTokenizer::Make(std::move(options));
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  {
    constexpr std::u8string_view kData(
      u8"\u043f\u043e\u0020\u0432\u0435\u0447\u0435\u0440\u0430\u043c\u0020"
      u8"\u0435\u0436\u0438\u043a\u0020\u0445\u043e\u0434\u0438\u043b\u0020"
      u8"\u043a\u0020\u043c\u0435\u0434\u0432\u0435\u0436\u043e\u043d\u043a"
      u8"\u0443\u0020\u0441\u0447\u0438\u0442\u0430\u0442\u044c\u0020\u0437"
      u8"\u0432\u0435\u0437\u0434\u044b");

    auto test_func = [](std::string_view data, Tokenizer* p_stream) {
      auto tokens = tests::Analyze(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<tests::AnalyzerToken> expected{
        {"\xD0\xBF\xD0\xBE", 1, 0, 4},
        {"\xD0\xB2\xD0\xB5\xD1\x87\xD0\xB5\xD1\x80", 2, 5, 19},
        {"\xD0\xB5\xD0\xB6\xD0\xB8\xD0\xBA", 3, 20, 28},
        {"\xD1\x85\xD0\xBE\xD0\xB4", 4, 29, 39},
        {"\xD0\xBA", 5, 40, 42},
        {"\xD0\xBC\xD0\xB5\xD0\xB4\xD0\xB2\xD0\xB5\xD0\xB6\xD0\xBE"
         "\xD0\xBD\xD0\xBA",
         6, 43, 63},
        {"\xD1\x81\xD1\x87\xD0\xB8\xD1\x82\xD0\xB0", 7, 64, 78},
        {"\xD0\xB7\xD0\xB2\xD0\xB5\xD0\xB7\xD0\xB4", 8, 79, 91}};
      ASSERT_EQ(expected, *tokens);
    };

    {
      irs::analysis::TextTokenizer::Options options;
      // we ignore encoding specified in locale
      options.locale = icu::Locale::createFromName("ru_RU.UTF-16");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(irs::ViewCast<char>(kData), &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = MakeText(TextOpts{
        .locale = "ru_RU.UTF-16",
        .stopwords = {},
      });
      ASSERT_NE(nullptr, stream);
      test_func(irs::ViewCast<char>(kData), stream.get());
    }
  }

  {
    const std::u8string_view data(
      u8"\U0000043f\U0000043e\U00000020\U00000432\U00000435\U00000447"
      u8"\U00000435\U00000440\U00000430\U0000043c\U00000020\U00000435"
      u8"\U00000436\U00000438\U0000043a");

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      auto tokens = tests::Analyze(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<tests::AnalyzerToken> expected{
        {"\xD0\xBF\xD0\xBE", 1, 0, 4},
        {"\xD0\xB2\xD0\xB5\xD1\x87\xD0\xB5\xD1\x80\xD0\xB0\xD0\xBC", 2, 5, 19},
        {"\xD0\xB5\xD0\xB6\xD0\xB8\xD0\xBA", 3, 20, 28}};
      ASSERT_EQ(expected, *tokens);
    };

    {
      irs::analysis::TextTokenizer::Options options;
      options.locale =
        icu::Locale::createFromName("en_US.utf32");  // ignore encoding
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);

      test_func(irs::ViewCast<char>(data), &stream);
    }
    {
      // stopwords should be set to empty - or default values will interfere
      // with test data
      auto stream = MakeText(TextOpts{
        .locale = "en_US.utf32",
      });
      ASSERT_NE(nullptr, stream);
      test_func(irs::ViewCast<char>(data), stream.get());
    }
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_fail_load_default_stopwords) {
  SetStopwordsPath("invalid stopwords path");

  // invalid custom stopwords path set -> fail
  {
    ASSERT_ANY_THROW(MakeText(TextOpts{
      .locale = "en_US.UTF-8",
      .explicit_stopwords_set_override = false,
    }));
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_load_stopwords) {
  SetStopwordsPath(IRS_TEST_RESOURCE_DIR);

  {
    std::string s_data_ascii = "A E I O U";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      auto tokens = tests::Analyze(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<tests::AnalyzerToken> expected{{"e", 1, 2, 3},
                                                       {"u", 2, 8, 9}};
      ASSERT_EQ(expected, *tokens);
    };

    // valid custom stopwords path -> ok
    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .explicit_stopwords_set_override = false,
      });
      ASSERT_NE(nullptr, stream);
      test_func(s_data_ascii, stream.get());
    }

    // empty "edgeNGram" object
    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .explicit_stopwords_set_override = false,
      });
      ASSERT_NE(nullptr, stream);
      test_func(s_data_ascii, stream.get());
    }
  }

  // ...........................................................................
  // invalid
  // ...........................................................................
  std::string s_data_ascii = "abc";
  {
    // unknown locale with no stopwords path -> can't load default stopwords
    {
      ASSERT_ANY_THROW(MakeText(TextOpts{
        .locale = "C",
        .explicit_stopwords_set_override = false,
      }));
    }
    {
      // min > max: Make rejects this edgeNGram configuration.
      ASSERT_ANY_THROW(MakeText(TextOpts{
        .locale = "ru_RU.UTF-8",
        .stopwords = {},
        .min_gram = 2,
        .max_gram = 1,
        .min_gram_set = true,
        .max_gram_set = true,
        .preserve_original = false,
        .preserve_original_set = true,
      }));
    }
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_load_no_default_stopwords) {
  SetStopwordsPath(nullptr);

  {
    const std::string s_data_ascii = "A E I O U";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      auto tokens = tests::Analyze(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<tests::AnalyzerToken> expected{{"a", 1, 0, 1},
                                                       {"e", 2, 2, 3},
                                                       {"i", 3, 4, 5},
                                                       {"o", 4, 6, 7},
                                                       {"u", 5, 8, 9}};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .explicit_stopwords_set_override = false,
      });
      ASSERT_NE(nullptr, stream);
      test_func(s_data_ascii, stream.get());
    }
  }
}

TEST_F(TextAnalyzerParserTestSuite,
       test_load_no_default_stopwords_fallback_cwd) {
  SetStopwordsPath(nullptr);

  // no stopwords, but valid CWD
  irs::Finally reset_stopword_path =
    [old_cwd = std::filesystem::current_path()]() noexcept {
      EXPECT_TRUE(irs::file_utils::SetCwd(old_cwd.c_str()));
    };
  irs::file_utils::SetCwd(std::filesystem::path(IRS_TEST_RESOURCE_DIR).c_str());

  {
    const std::string s_data_ascii = "A E I O U";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      auto tokens = tests::Analyze(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<tests::AnalyzerToken> expected{{"e", 1, 2, 3},
                                                       {"u", 2, 8, 9}};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .explicit_stopwords_set_override = false,
      });
      ASSERT_NE(nullptr, stream);
      test_func(s_data_ascii, stream.get());
    }
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_load_stopwords_path_override) {
  SetStopwordsPath("some invalid path");

  std::string s_data_ascii = "A E I O U";

  auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
    auto tokens = tests::Analyze(*p_stream, data);
    ASSERT_TRUE(tokens.has_value());
    const std::vector<tests::AnalyzerToken> expected{{"e", 1, 2, 3},
                                                     {"u", 2, 8, 9}};
    ASSERT_EQ(expected, *tokens);
  };

  // overriding ignored words path
  auto stream = MakeText(TextOpts{
    .locale = "en_US.UTF-8",
    .explicit_stopwords_set_override = false,
    .stopwords_path = IRS_TEST_RESOURCE_DIR,
  });
  ASSERT_NE(nullptr, stream);
  test_func(s_data_ascii, stream.get());
}

TEST_F(TextAnalyzerParserTestSuite,
       test_load_stopwords_path_override_emptypath) {
  // no stopwords, but empty stopwords path (we need to shift CWD to our test
  // resources, to be able to load stopwords)
  irs::Finally reset_stopword_path =
    [old_cwd = std::filesystem::current_path()]() noexcept {
      EXPECT_TRUE(irs::file_utils::SetCwd(old_cwd.c_str()));
    };
  irs::file_utils::SetCwd(std::filesystem::path(IRS_TEST_RESOURCE_DIR).c_str());

  auto stream = MakeText(TextOpts{
    .locale = "en_US.UTF-8",
    .case_convert = irs::Case::Lower,
    .explicit_stopwords_set_override = false,
    .stopwords_path = "",
    .accent = false,
    .stemming = true,
  });
  ASSERT_NE(nullptr, stream);

  // Checking that default stowords are loaded
  std::string s_data_ascii = "A E I O U";
  auto tokens = tests::Analyze(*stream, s_data_ascii);
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected{{"e", 1, 2, 3},
                                                   {"u", 2, 8, 9}};
  ASSERT_EQ(expected, *tokens);
}

TEST_F(TextAnalyzerParserTestSuite, test_text_ngrams) {
  // text ngrams

  // default behaviour
  {
    std::string data = " A  hErd of   quIck ";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"he", "her", "of", "qu", "qui"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .min_gram = 2,
        .max_gram = 3,
        .min_gram_set = true,
        .max_gram_set = true,
        .preserve_original = false,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // min == 0
  {
    std::string data = " A  hErd of   quIck ";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"h",  "he", "her", "o",
                                              "of", "q",  "qu",  "qui"};
      ASSERT_EQ(expected, *tokens);

      const auto again = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(again.has_value());
      ASSERT_FALSE(again->empty());
      ASSERT_EQ("h", (*again)[0]);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .min_gram = 0,
        .max_gram = 3,
        .min_gram_set = true,
        .max_gram_set = true,
        .preserve_original = false,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // preserveOriginal == true
  {
    std::string data = " A  hErd of   quIck ";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"he", "her", "herd", "of",
                                              "qu", "qui", "quick"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .min_gram = 2,
        .max_gram = 3,
        .min_gram_set = true,
        .max_gram_set = true,
        .preserve_original = true,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // min == max
  {
    std::string data = " A  hErd of   quIck ";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"her", "qui"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .min_gram = 3,
        .max_gram = 3,
        .min_gram_set = true,
        .max_gram_set = true,
        .preserve_original = false,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // min > max and preserveOriginal == false
  {
    std::string data = " A  hErd of   quIck ";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_TRUE(tokens->empty());
    };

    {
      irs::analysis::TextTokenizer::Options options;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      options.explicit_stopwords.emplace("a");
      options.min_gram = 4;
      options.min_gram_set = true;
      options.max_gram = 3;
      options.max_gram_set = true;
      options.preserve_original = false;
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);

      test_func(data, &stream);
    }
  }

  // min > max and preserveOriginal == true
  {
    std::string data = " A  hErd of   quIck ";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"herd", "of", "quick"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      irs::analysis::TextTokenizer::Options options;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      options.explicit_stopwords.emplace("a");
      options.min_gram = 4;
      options.min_gram_set = true;
      options.max_gram = 3;
      options.max_gram_set = true;
      options.preserve_original = true;
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);

      test_func(data, &stream);
    }
  }

  // min == max == 0 and no preserveOriginal
  {
    std::string data = " A  hErd of   quIck ";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_TRUE(tokens->empty());
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .min_gram = 0,
        .max_gram = 0,
        .min_gram_set = true,
        .max_gram_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // no min and preserveOriginal == false
  {
    std::string data = " A  hErd of";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"h", "o"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .max_gram = 1,
        .max_gram_set = true,
        .preserve_original = false,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // no min and preserveOriginal == true
  {
    std::string data = " A  hErd of";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"h", "herd", "o", "of"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .max_gram = 1,
        .max_gram_set = true,
        .preserve_original = true,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // no max
  {
    std::string data = " A  hErd of";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"h",    "he", "her",
                                              "herd", "o",  "of"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .min_gram = 1,
        .min_gram_set = true,
        .preserve_original = false,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // no min and no max and preserveOriginal == false
  {
    std::string data = " A  hErd of";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"h",    "he", "her",
                                              "herd", "o",  "of"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .preserve_original = false,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // no min and no max and preserveOriginal == true
  {
    std::string data = " A  hErd of";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::AnalyzeTerms(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<std::string> expected{"h",    "he", "her",
                                              "herd", "o",  "of"};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "en_US.UTF-8",
        .stopwords = {"a"},
        .preserve_original = true,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(data, stream.get());
    }
  }

  // wide symbols
  {
    std::string s_data_ut_f8 =
      "\xD0\x9F\xD0\xBE\x20\xD0\xB2\xD0\xB5\xD1\x87\xD0\xB5\xD1\x80\xD0\xB0"
      "\xD0\xBC\x20\xD0\xBA\x20\xD0\x9C\xD0\xB5\xD0\xB4\xD0\xB2\xD0\xB5\xD0"
      "\xB6\xD0\xBE\xD0\xBD\xD0\xBA\xD1\x83";

    auto test_func = [](const std::string_view& data, Tokenizer* p_stream) {
      const auto tokens = tests::Analyze(*p_stream, data);
      ASSERT_TRUE(tokens.has_value());
      const std::vector<tests::AnalyzerToken> expected{
        {"\xD0\xBF", 1, 0, 2},   {"\xD0\xBF\xD0\xBE", 1, 0, 4},
        {"\xD0\xB2", 2, 5, 7},   {"\xD0\xB2\xD0\xB5", 2, 5, 9},
        {"\xD0\xBC", 3, 23, 25}, {"\xD0\xBC\xD0\xB5", 3, 23, 27}};
      ASSERT_EQ(expected, *tokens);
    };

    {
      auto stream = MakeText(TextOpts{
        .locale = "ru_RU.UTF-8",
        .stopwords = {"\xD0\xBA"},
        .min_gram = 1,
        .max_gram = 2,
        .min_gram_set = true,
        .max_gram_set = true,
        .preserve_original = false,
        .preserve_original_set = true,
      });
      ASSERT_NE(nullptr, stream);
      test_func(std::string_view(s_data_ut_f8.data(), s_data_ut_f8.size()),
                stream.get());
    }
  }
}

namespace {

struct TextTok {
  std::string term;
  uint32_t pos;
  uint32_t offs_start;
  uint32_t offs_end;
};

std::vector<TextTok> PullText(irs::analysis::Tokenizer& stream,
                              std::string_view data) {
  std::vector<TextTok> out;
  const auto tokens = tests::Analyze(stream, data);
  if (!tokens) {
    return out;
  }
  for (auto& tok : *tokens) {
    out.push_back({std::move(tok.term), tok.pos, tok.offs_start, tok.offs_end});
  }
  return out;
}

}  // namespace

TEST(text_tokenizer_batch, native_fills_match_pull) {
  const std::vector<std::string> values = {"The Quick Brown Fox", "",
                                           "running runner runs", "a",
                                           "Ma\xc3\xb1"
                                           "ana caf\xc3\xa9 na\xc3\xaf"
                                           "ve"};

  auto run_case = [&](TextOpts base) {
    auto pull_stream = MakeText(base);
    auto fill_stream = MakeText(base);
    ASSERT_NE(nullptr, pull_stream);
    ASSERT_NE(nullptr, fill_stream);

    for (const auto& v : values) {
      SCOPED_TRACE(v);
      const auto pulled = PullText(*pull_stream, v);

      std::vector<TextTok> filled;
      const auto collect = [&](irs::TokenBatch& batch,
                               std::span<const irs::DocRun> /*runs*/) {
        for (uint32_t i = 0; i < batch.count; ++i) {
          const auto& t = batch.terms[i];
          filled.push_back({std::string{t.GetData(), t.GetSize()}, batch.pos[i],
                            batch.offs_start[i], batch.offs_end[i]});
        }
      };
      tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
      fill_stream->Fill(v, sink.writer, sink.layout);
      sink.writer.Finish();

      ASSERT_EQ(pulled.size(), filled.size());
      for (size_t i = 0; i < pulled.size(); ++i) {
        SCOPED_TRACE(i);
        ASSERT_EQ(pulled[i].term, filled[i].term);
        ASSERT_EQ(pulled[i].pos, filled[i].pos);
        ASSERT_EQ(pulled[i].offs_start, filled[i].offs_start);
        ASSERT_EQ(pulled[i].offs_end, filled[i].offs_end);
      }
    }
  };

  // word mode (stemming on)
  run_case(TextOpts{});
  // word mode, no stemming, no accent-stripping
  run_case(TextOpts{.accent = true, .stemming = false});
  // ngram mode
  run_case(TextOpts{.stemming = false,
                    .min_gram = 2,
                    .max_gram = 3,
                    .min_gram_set = true,
                    .max_gram_set = true,
                    .preserve_original = true,
                    .preserve_original_set = true});
}

TEST(text_tokenizer_batch, column_fill_matches_pull) {
  auto base = TextOpts{};
  auto pull_stream = MakeText(base);
  auto fill_stream = MakeText(base);
  ASSERT_NE(nullptr, pull_stream);
  ASSERT_NE(nullptr, fill_stream);

  const std::vector<std::string> raw = {"The Quick Brown Fox", "",
                                        "running jumps", "lazy Dog"};
  std::vector<duckdb::string_t> values;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < raw.size(); ++i) {
    values.emplace_back(raw[i].data(), static_cast<uint32_t>(raw[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(100 + i));
  }

  tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
  fill_stream->Fill(values, docs, sink.writer, sink.layout);
  ASSERT_FALSE(sink.flushed());
  auto& batch = sink.writer.buf;
  const auto runs = sink.writer.Runs();

  ASSERT_EQ(raw.size(), runs.size());
  size_t run_idx = 0;
  uint32_t token_idx = 0;
  for (size_t v = 0; v < raw.size(); ++v) {
    SCOPED_TRACE(raw[v]);
    const auto pulled = PullText(*pull_stream, raw[v]);
    ASSERT_EQ(docs[v], runs[run_idx].doc);
    ASSERT_EQ(pulled.size(), runs[run_idx].ntokens);
    ++run_idx;
    for (const auto& expected : pulled) {
      const auto& t = batch.terms[token_idx];
      ASSERT_EQ(expected.term, (std::string{t.GetData(), t.GetSize()}));
      ASSERT_EQ(expected.pos, batch.pos[token_idx]);
      ASSERT_EQ(expected.offs_start, batch.offs_start[token_idx]);
      ASSERT_EQ(expected.offs_end, batch.offs_end[token_idx]);
      ++token_idx;
    }
  }
  ASSERT_EQ(run_idx, runs.size());
  ASSERT_EQ(batch.count, token_idx);
}

namespace {

std::vector<tests::AnalyzerToken> TextAnalyzeWith(const TextOpts& opts,
                                                  std::string_view value,
                                                  bool force_unicode) {
  auto stream = MakeText(TextOpts{opts});
  auto* text = dynamic_cast<irs::analysis::TextTokenizer*>(stream.get());
  EXPECT_NE(nullptr, text);
  text->ForceUnicodePath(force_unicode);
  auto tokens = tests::Analyze(*stream, value);
  EXPECT_TRUE(tokens.has_value());
  return std::move(*tokens);
}

void AssertTextAsciiMatchesUnicode(const TextOpts& opts,
                                   std::string_view value) {
  const auto slow = TextAnalyzeWith(opts, value, true);
  const auto fast = TextAnalyzeWith(opts, value, false);
  ASSERT_EQ(slow.size(), fast.size());
  for (size_t i = 0; i < slow.size(); ++i) {
    SCOPED_TRACE(testing::Message() << "token=" << i);
    ASSERT_EQ(slow[i].term, fast[i].term);
    ASSERT_EQ(slow[i].pos, fast[i].pos);
    ASSERT_EQ(slow[i].offs_start, fast[i].offs_start);
    ASSERT_EQ(slow[i].offs_end, fast[i].offs_end);
  }
}

}  // namespace

TEST(TextTokenizerAsciiFastPath, word_mode_goldens) {
  const std::vector<std::string> values = {
    "The Quick BROWN foxes are Running easily",
    "don't stop believing",
    "3.14 and 1,234 numbers",
    "connection connections connected",
    "a_b snake_case mixed",
    "  spaces\tand\r\nnewlines  ",
    "M.I.T. e.g. i.e.",
    "",
    "x",
    "!!!",
    "STOP the And of THE road",
  };
  for (const auto cc : {irs::Case::None, irs::Case::Lower, irs::Case::Upper}) {
    for (const bool stemming : {true, false}) {
      for (const bool accent : {true, false}) {
        TextOpts opts{.case_convert = cc,
                      .stopwords = {"the", "and", "stop"},
                      .accent = accent,
                      .stemming = stemming};
        for (const auto& v : values) {
          SCOPED_TRACE(testing::Message()
                       << "case=" << int(cc) << " stem=" << stemming
                       << " accent=" << accent << " value=\"" << v << "\"");
          AssertTextAsciiMatchesUnicode(opts, v);
        }
      }
    }
  }
}

TEST(TextTokenizerAsciiFastPath, ngram_mode_matches) {
  const std::vector<std::string> values = {"quick brown foxes", "don't", "ab",
                                           "abcdefgh 123456", ""};
  for (const auto& [mn, mx, preserve] :
       std::vector<std::tuple<size_t, size_t, bool>>{
         {2, 3, false}, {2, 3, true}, {1, 0, false}, {3, 3, true}}) {
    TextOpts opts{.stopwords = {"the"},
                  .min_gram = mn,
                  .max_gram = mx,
                  .min_gram_set = mn != 0,
                  .max_gram_set = mx != 0,
                  .preserve_original = preserve,
                  .preserve_original_set = true};
    for (const auto& v : values) {
      SCOPED_TRACE(testing::Message()
                   << "min=" << mn << " max=" << mx << " preserve=" << preserve
                   << " value=\"" << v << "\"");
      AssertTextAsciiMatchesUnicode(opts, v);
    }
  }
}

TEST(TextTokenizerAsciiFastPath, property_oracle_random_ascii) {
  constexpr std::string_view kCharset =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    "'.,:;_\"!?-()[] \t\r\n"
    "the running foxes  ..''";
  uint64_t seed = 0x7e47;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (const bool stemming : {true, false}) {
    for (const bool ngram : {false, true}) {
      TextOpts opts{.stopwords = {"the", "a"}, .stemming = stemming};
      if (ngram) {
        opts.min_gram = 2;
        opts.max_gram = 4;
        opts.min_gram_set = true;
        opts.max_gram_set = true;
      }
      for (size_t iter = 0; iter < 200; ++iter) {
        std::string v;
        const size_t len = next() % 120;
        for (size_t i = 0; i < len; ++i) {
          v += kCharset[next() % kCharset.size()];
        }
        SCOPED_TRACE(testing::Message()
                     << "stem=" << stemming << " ngram=" << ngram
                     << " iter=" << iter << " value=\"" << v << "\"");
        AssertTextAsciiMatchesUnicode(opts, v);
      }
    }
  }
}

TEST(TextTokenizerAsciiFastPath, non_ascii_and_turkish_stay_unicode) {
  TextOpts opts{};
  AssertTextAsciiMatchesUnicode(opts, "caf\xc3\xa9 running dogs");
  TextOpts tr{.locale = "tr_TR.UTF-8", .stemming = false};
  const auto fast = TextAnalyzeWith(tr, "III", false);
  const auto slow = TextAnalyzeWith(tr, "III", true);
  ASSERT_EQ(slow.size(), fast.size());
  ASSERT_EQ(slow[0].term, fast[0].term);
}
