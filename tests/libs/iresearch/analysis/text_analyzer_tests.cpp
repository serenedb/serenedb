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
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "tests_config.hpp"

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

irs::analysis::Analyzer::ptr MakeText(TextOpts opts = {}) {
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

  ASSERT_TRUE(stream.reset(s_data_ut_f8));

  auto p_stream = &stream;

  ASSERT_NE(nullptr, p_stream);

  auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
  ASSERT_NE(nullptr, p_offset);
  auto* p_inc = irs::get<irs::IncAttr>(*p_stream);
  ASSERT_NE(nullptr, p_inc);
  auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
  ASSERT_EQ(nullptr, p_payload);
  auto* p_value = irs::get<irs::TermAttr>(*p_stream);
  ASSERT_NE(nullptr, p_value);

  ASSERT_TRUE(p_stream->next());
  ASSERT_EQ("1,24",
            std::string((char*)(p_value->value.data()), p_value->value.size()));
  ASSERT_EQ(1, p_inc->value);
  ASSERT_EQ(0, p_offset->start);
  ASSERT_EQ(4, p_offset->end);
  ASSERT_TRUE(p_stream->next());
  ASSERT_EQ("prosenttia",
            std::string((char*)(p_value->value.data()), p_value->value.size()));
  ASSERT_EQ(1, p_inc->value);
  ASSERT_EQ(5, p_offset->start);
  ASSERT_EQ(15, p_offset->end);
  ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
      ASSERT_NE(nullptr, p_offset);
      auto* p_inc = irs::get<irs::IncAttr>(*p_stream);
      ASSERT_NE(nullptr, p_inc);
      auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
      ASSERT_EQ(nullptr, p_payload);
      auto* p_value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, p_value);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("a", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(1, p_offset->start);
      ASSERT_EQ(2, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("herd", std::string((char*)(p_value->value.data()),
                                    p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(4, p_offset->start);
      ASSERT_EQ(8, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("of", std::string((char*)(p_value->value.data()),
                                  p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(9, p_offset->start);
      ASSERT_EQ(11, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("quick", std::string((char*)(p_value->value.data()),
                                     p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(14, p_offset->start);
      ASSERT_EQ(19, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("brown", std::string((char*)(p_value->value.data()),
                                     p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(20, p_offset->start);
      ASSERT_EQ(25, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("fox", std::string((char*)(p_value->value.data()),
                                   p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(27, p_offset->start);
      ASSERT_EQ(32, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("ran", std::string((char*)(p_value->value.data()),
                                   p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(33, p_offset->start);
      ASSERT_EQ(36, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("and", std::string((char*)(p_value->value.data()),
                                   p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(40, p_offset->start);
      ASSERT_EQ(43, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("jump", std::string((char*)(p_value->value.data()),
                                    p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(44, p_offset->start);
      ASSERT_EQ(50, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("over", std::string((char*)(p_value->value.data()),
                                    p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(51, p_offset->start);
      ASSERT_EQ(55, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("a", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(57, p_offset->start);
      ASSERT_EQ(58, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("run", std::string((char*)(p_value->value.data()),
                                   p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(63, p_offset->start);
      ASSERT_EQ(70, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("dog", std::string((char*)(p_value->value.data()),
                                   p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(71, p_offset->start);
      ASSERT_EQ(74, p_offset->end);
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("a", irs::ViewCast<char>(value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("quick", irs::ViewCast<char>(value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("brown", irs::ViewCast<char>(value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("fox", irs::ViewCast<char>(value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("A", irs::ViewCast<char>(value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("QUICK", irs::ViewCast<char>(value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("BROWN", irs::ViewCast<char>(value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("FOX", irs::ViewCast<char>(value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("A", irs::ViewCast<char>(value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("qUiCk", irs::ViewCast<char>(value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("brOwn", irs::ViewCast<char>(value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("FoX", irs::ViewCast<char>(value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
      ASSERT_NE(nullptr, p_offset);
      auto* p_inc = irs::get<irs::IncAttr>(*p_stream);
      ASSERT_NE(nullptr, p_inc);
      auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
      ASSERT_EQ(nullptr, p_payload);
      auto* p_value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, p_value);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("thing", std::string((char*)(p_value->value.data()),
                                     p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("some", std::string((char*)(p_value->value.data()),
                                    p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("kind", std::string((char*)(p_value->value.data()),
                                    p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("anoth", std::string((char*)(p_value->value.data()),
                                     p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](std::string_view data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
      ASSERT_NE(nullptr, p_offset);
      auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
      ASSERT_EQ(nullptr, p_payload);
      auto* p_value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, p_value);
      auto* p_inc = irs::get<irs::IncAttr>(*p_stream);
      ASSERT_NE(nullptr, p_inc);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(
        std::string("\xD0\xBF\xD0\xBE"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(0, p_offset->start);
      ASSERT_EQ(4, p_offset->end);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(
        std::string("\xD0\xB2\xD0\xB5\xD1\x87\xD0\xB5\xD1\x80"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(5, p_offset->start);
      ASSERT_EQ(19, p_offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(
        std::string("\xD0\xB5\xD0\xB6\xD0\xB8\xD0\xBA"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(20, p_offset->start);
      ASSERT_EQ(28, p_offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(
        std::string("\xD1\x85\xD0\xBE\xD0\xB4"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(29, p_offset->start);
      ASSERT_EQ(39, p_offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(
        std::string("\xD0\xBA"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(40, p_offset->start);
      ASSERT_EQ(42, p_offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(
        std::string("\xD0\xBC\xD0\xB5\xD0\xB4\xD0\xB2\xD0\xB5\xD0\xB6\xD0\xBE"
                    "\xD0\xBD\xD0\xBA"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(43, p_offset->start);
      ASSERT_EQ(63, p_offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(
        std::string("\xD1\x81\xD1\x87\xD0\xB8\xD1\x82\xD0\xB0"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(64, p_offset->start);
      ASSERT_EQ(78, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(
        std::string("\xD0\xB7\xD0\xB2\xD0\xB5\xD0\xB7\xD0\xB4"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(79, p_offset->start);
      ASSERT_EQ(91, p_offset->end);
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
      ASSERT_NE(nullptr, p_offset);
      auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
      ASSERT_EQ(nullptr, p_payload);
      auto* p_value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, p_value);
      auto* p_inc = irs::get<irs::IncAttr>(*p_stream);
      ASSERT_NE(nullptr, p_inc);

      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(
        std::string("\xD0\xBF\xD0\xBE"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(0, p_offset->start);
      ASSERT_EQ(4, p_offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(
        std::string("\xD0\xB2\xD0\xB5\xD1\x87\xD0\xB5\xD1\x80\xD0\xB0\xD0\xBC"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(5, p_offset->start);
      ASSERT_EQ(19, p_offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(
        std::string("\xD0\xB5\xD0\xB6\xD0\xB8\xD0\xBA"),
        std::string((char*)p_value->value.data(), p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(20, p_offset->start);
      ASSERT_EQ(28, p_offset->end);
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));
      auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
      ASSERT_NE(nullptr, p_offset);
      auto* p_inc = irs::get<irs::IncAttr>(*p_stream);
      ASSERT_NE(nullptr, p_inc);
      auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
      ASSERT_EQ(nullptr, p_payload);
      auto* p_value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, p_value);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ("e", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(2, p_offset->start);
      ASSERT_EQ(3, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(8, p_offset->start);
      ASSERT_EQ(9, p_offset->end);
      ASSERT_EQ("u", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));
      auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
      ASSERT_NE(nullptr, p_offset);
      auto* p_inc = irs::get<irs::IncAttr>(*p_stream);
      ASSERT_NE(nullptr, p_inc);
      auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
      ASSERT_EQ(nullptr, p_payload);
      auto* p_value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, p_value);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ("a", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(0, p_offset->start);
      ASSERT_EQ(1, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("e", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(2, p_offset->start);
      ASSERT_EQ(3, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("i", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(4, p_offset->start);
      ASSERT_EQ(5, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("o", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(6, p_offset->start);
      ASSERT_EQ(7, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("u", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(8, p_offset->start);
      ASSERT_EQ(9, p_offset->end);
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));
      auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
      ASSERT_NE(nullptr, p_offset);
      auto* p_inc = irs::get<irs::IncAttr>(*p_stream);
      ASSERT_NE(nullptr, p_inc);
      auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
      ASSERT_EQ(nullptr, p_payload);
      auto* p_value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, p_value);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("e", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(2, p_offset->start);
      ASSERT_EQ(3, p_offset->end);
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("u", std::string((char*)(p_value->value.data()),
                                 p_value->value.size()));
      ASSERT_EQ(1, p_inc->value);
      ASSERT_EQ(8, p_offset->start);
      ASSERT_EQ(9, p_offset->end);
      ASSERT_FALSE(p_stream->next());
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

  auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
    ASSERT_TRUE(p_stream->reset(data));
    auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
    ASSERT_NE(nullptr, p_offset);
    auto* p_inc = irs::get<irs::IncAttr>(*p_stream);
    ASSERT_NE(nullptr, p_inc);
    auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
    ASSERT_EQ(nullptr, p_payload);
    auto* p_value = irs::get<irs::TermAttr>(*p_stream);
    ASSERT_NE(nullptr, p_value);

    ASSERT_TRUE(p_stream->next());
    ASSERT_EQ(1, p_inc->value);
    ASSERT_EQ(
      "e", std::string((char*)(p_value->value.data()), p_value->value.size()));
    ASSERT_EQ(2, p_offset->start);
    ASSERT_EQ(3, p_offset->end);
    ASSERT_TRUE(p_stream->next());
    ASSERT_EQ(1, p_inc->value);
    ASSERT_EQ(8, p_offset->start);
    ASSERT_EQ(9, p_offset->end);
    ASSERT_EQ(
      "u", std::string((char*)(p_value->value.data()), p_value->value.size()));
    ASSERT_FALSE(p_stream->next());
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
  ASSERT_TRUE(stream->reset(s_data_ascii));
  auto* p_offset = irs::get<irs::OffsAttr>(*stream);
  ASSERT_NE(nullptr, p_offset);
  auto* p_inc = irs::get<irs::IncAttr>(*stream);
  ASSERT_NE(nullptr, p_inc);
  auto* p_payload = irs::get<irs::PayAttr>(*stream);
  ASSERT_EQ(nullptr, p_payload);
  auto* p_value = irs::get<irs::TermAttr>(*stream);
  ASSERT_NE(nullptr, p_value);

  ASSERT_TRUE(stream->next());
  ASSERT_EQ(1, p_inc->value);
  ASSERT_EQ("e",
            std::string((char*)(p_value->value.data()), p_value->value.size()));
  ASSERT_EQ(2, p_offset->start);
  ASSERT_EQ(3, p_offset->end);
  ASSERT_TRUE(stream->next());
  ASSERT_EQ(1, p_inc->value);
  ASSERT_EQ(8, p_offset->start);
  ASSERT_EQ(9, p_offset->end);
  ASSERT_EQ("u",
            std::string((char*)(p_value->value.data()), p_value->value.size()));
  ASSERT_FALSE(stream->next());
}

TEST_F(TextAnalyzerParserTestSuite, test_text_ngrams) {
  // text ngrams

  // default behaviour
  {
    std::string data = " A  hErd of   quIck ";

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("he", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("her", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("of", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("qu", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("qui", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("h", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("he", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("her", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("o", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("of", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("q", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("qu", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("qui", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());

      ASSERT_TRUE(p_stream->reset(data));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ(
        "h", std::string(reinterpret_cast<const char*>((p_value->value.data())),
                         p_value->value.size()));
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("he", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("her", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("herd", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("of", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("qu", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("qui", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("quick", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("her", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("qui", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, p_value);

      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("herd", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("of", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("quick", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, p_value);

      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("h", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("o", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("h", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("herd", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("o", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("of", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("h", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("he", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("her", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("herd", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("o", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("of", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("h", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("he", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("her", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("herd", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("o", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("of", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* p_value = irs::get<irs::TermAttr>(*p_stream);

      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("h", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("he", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("her", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("herd", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("o", irs::ViewCast<char>(p_value->value));
      ASSERT_TRUE(p_stream->next());
      ASSERT_EQ("of", irs::ViewCast<char>(p_value->value));
      ASSERT_FALSE(p_stream->next());
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

    auto test_func = [](const std::string_view& data, Analyzer* p_stream) {
      ASSERT_TRUE(p_stream->reset(data));

      auto* offset = irs::get<irs::OffsAttr>(*p_stream);
      ASSERT_NE(nullptr, offset);
      auto* value = irs::get<irs::TermAttr>(*p_stream);
      ASSERT_NE(nullptr, value);

      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(std::string("\xD0\xBF"),
                std::string((char*)value->value.data(), value->value.size()));
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(2, offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(std::string("\xD0\xBF\xD0\xBE"),
                std::string((char*)value->value.data(), value->value.size()));
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(4, offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(std::string("\xD0\xB2"),
                std::string((char*)value->value.data(), value->value.size()));
      ASSERT_EQ(5, offset->start);
      ASSERT_EQ(7, offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(std::string("\xD0\xB2\xD0\xB5"),
                std::string((char*)value->value.data(), value->value.size()));
      ASSERT_EQ(5, offset->start);
      ASSERT_EQ(9, offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(std::string("\xD0\xBC"),
                std::string((char*)value->value.data(), value->value.size()));
      ASSERT_EQ(23, offset->start);
      ASSERT_EQ(25, offset->end);
      ASSERT_TRUE(p_stream->next());

      ASSERT_EQ(std::string("\xD0\xBC\xD0\xB5"),
                std::string((char*)value->value.data(), value->value.size()));
      ASSERT_EQ(23, offset->start);
      ASSERT_EQ(27, offset->end);
      ASSERT_FALSE(p_stream->next());
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
