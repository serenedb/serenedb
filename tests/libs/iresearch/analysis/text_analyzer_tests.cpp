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
#include <vpack/parser.h>

#include <unordered_set>

#include "basics/file_utils_ext.hpp"
#include "basics/runtime_utils.hpp"
#include "gtest/gtest.h"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "tests_config.hpp"

namespace tests {

class TextAnalyzerParserTestSuite : public ::testing::Test {
 protected:
  void SetStopwordsPath(const char* path) {
    _stopwords_path_set = true;
    _old_stopwords_path_set = false;

    const char* old_stopwords_path =
      irs::Getenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable);
    if (old_stopwords_path) {
      _old_stopwords_path = old_stopwords_path;
      _old_stopwords_path_set = true;
    }

    if (path) {
      irs::Setenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable, path,
                  true);
    } else {
      irs::Unsetenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable);
      ASSERT_EQ(
        nullptr,
        irs::Getenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable));
    }
  }

  void SetUp() final { irs::analysis::TextTokenizer::clear_cache(); }

  void TearDown() final {
    if (_stopwords_path_set) {
      if (_old_stopwords_path_set) {
        irs::Setenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable,
                    _old_stopwords_path.data(), true);
      } else {
        irs::Unsetenv(irs::analysis::TextTokenizer::gStopwordPathEnvVariable);
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
  irs::analysis::TextTokenizer::OptionsT options;

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
    irs::analysis::TextTokenizer::OptionsT options;

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
      irs::analysis::TextTokenizer::OptionsT options;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");

      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[]}");
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
      irs::analysis::TextTokenizer::OptionsT options;
      options.case_convert = irs::analysis::TextTokenizer::kLower;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[], \"case\":\"lower\"}");
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
      irs::analysis::TextTokenizer::OptionsT options;
      options.case_convert = irs::analysis::TextTokenizer::kUpper;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[], \"case\":\"upper\"}");
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
      irs::analysis::TextTokenizer::OptionsT options;
      options.case_convert = irs::analysis::TextTokenizer::kNone;
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[], \"case\":\"none\"}");
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
      irs::analysis::TextTokenizer::OptionsT options;
      options.explicit_stopwords = {"a", "of", "and"};
      options.locale = icu::Locale::createFromName("en_US.UTF-8");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(data, &stream);
    }
    {
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\", \"of\", "
        "\"and\"]}");
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
      irs::analysis::TextTokenizer::OptionsT options;
      // we ignore encoding specified in locale
      options.locale = icu::Locale::createFromName("ru_RU.UTF-16");
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);
      test_func(irs::ViewCast<char>(kData), &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        R"({"locale":"ru_RU.UTF-16", "stopwords":[]})");
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
      irs::analysis::TextTokenizer::OptionsT options;
      options.locale =
        icu::Locale::createFromName("en_US.utf32");  // ignore encoding
      irs::analysis::TextTokenizer stream(options, options.explicit_stopwords);

      test_func(irs::ViewCast<char>(data), &stream);
    }
    {
      // stopwords  should be set to empty - or default values will interfere
      // with test data
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),  // ignore encoding
        "{\"locale\":\"en_US.utf32\", \"stopwords\":[]}");
      ASSERT_NE(nullptr, stream);
      test_func(irs::ViewCast<char>(data), stream.get());
    }
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_fail_load_default_stopwords) {
  SetStopwordsPath("invalid stopwords path");

  // invalid custom stopwords path set -> fail
  {
    auto stream = irs::analysis::analyzers::Get(
      "text", irs::Type<irs::text_format::Json>::get(),
      "{\"locale\":\"en_US.UTF-8\"}");
    ASSERT_EQ(nullptr, stream);
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_load_stopwords) {
  SetStopwordsPath(IRS_TEST_RESOURCE_DIR);

  {
    auto locale = "en_US.UTF-8";
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

    {
      auto stream = TextTokenizer::make(locale);
      ASSERT_NE(nullptr, stream);
      test_func(s_data_ascii, stream.get());
    }

    // valid custom stopwords path -> ok
    {
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\"}");
      ASSERT_NE(nullptr, stream);
      test_func(s_data_ascii, stream.get());
    }

    // empty \"edgeNGram\" object
    {
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"edgeNGram\": {}}");
      ASSERT_NE(nullptr, stream);
      test_func(s_data_ascii, stream.get());
    }
  }

  // ...........................................................................
  // invalid
  // ...........................................................................
  std::string s_data_ascii = "abc";
  {
    {
      // load stopwords for the 'C' locale that does not have stopwords defined
      // in tests
      auto locale = std::locale::classic().name();

      auto stream = TextTokenizer::make(locale);
      auto p_stream = stream.get();

      ASSERT_EQ(nullptr, p_stream);
    }
    {
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(), "{\"locale\":\"C\"}");
      ASSERT_EQ(nullptr, stream);
    }
    {
      // min > max
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"ru_RU.UTF-8\", \"stopwords\":[], \"edgeNGram\" : "
        "{\"min\":2, \"max\":1, \"preserveOriginal\":false}}");
      ASSERT_EQ(nullptr, stream);
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\"}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\"}");
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
  auto stream = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    "{\"locale\":\"en_US.UTF-8\", "
    "\"stopwordsPath\":\"" IRS_TEST_RESOURCE_DIR "\"}");
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

  std::string config =
    "{\"locale\":\"en_US.UTF-8\",\"case\":\"lower\",\"accent\":false,"
    "\"stemming\":true,\"stopwordsPath\":\"\"}";
  auto stream = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(), config);
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

TEST_F(TextAnalyzerParserTestSuite, test_make_config_json) {
  // with unknown parameter
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"case\":\"lower\",\"invalid_parameter\":"
      "true,\"stopwords\":[],\"accent\":true,\"stemming\":false}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"stopwords\":[],"
                "\"accent\":true,\"stemming\":false}")
                ->toString(),
              actual);
  }

  // no case convert in creation. Default value shown
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"stopwords\":[],\"accent\":true,"
      "\"stemming\":false}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"stopwords\":[],"
                "\"accent\":true,\"stemming\":false}")
                ->toString(),
              actual);
  }

  // no accent in creation. Default value shown
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"case\":\"lower\",\"stopwords\":[],"
      "\"stemming\":false}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"stopwords\":[],"
                "\"accent\":false,\"stemming\":false}")
                ->toString(),
              actual);
  }

  // no stem in creation. Default value shown
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"case\":\"lower\",\"stopwords\":[],"
      "\"accent\":true}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"stopwords\":[],"
                "\"accent\":true,\"stemming\":true}")
                ->toString(),
              actual);
  }

  // non default values for stem, accent and case
  {
    std::string config =
      "{\"locale\":\"ru_RU.utf-8\",\"case\":\"upper\",\"stopwords\":[],"
      "\"accent\":true,\"stemming\":false}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"upper\",\"stopwords\":[],"
                "\"accent\":true,\"stemming\":false}")
                ->toString(),
              actual);
  }

  // no stopwords no stopwords path
  {
    SetStopwordsPath(IRS_TEST_RESOURCE_DIR);

    std::string config =
      "{\"locale\":\"en_US.utf-8\",\"case\":\"lower\",\"accent\":false,"
      "\"stemming\":true}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(
      vpack::Parser::fromJson("{\"locale\":\"en_US\",\"case\":\"lower\","
                              "\"accent\":false,\"stemming\":true}")
        ->toString(),
      actual);
  }

  // empty stopwords, but stopwords path
  {
    std::string config =
      "{\"locale\":\"en_US.utf-8\",\"case\":\"upper\",\"stopwords\":[],"
      "\"accent\":false,\"stemming\":true,\"stopwordsPath\":"
      "\"" IRS_TEST_RESOURCE_DIR "\"}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"en_US\",\"case\":\"upper\",\"stopwords\":[],"
                "\"accent\":false,\"stemming\":true,\"stopwordsPath\":"
                "\"" IRS_TEST_RESOURCE_DIR "\"}")
                ->toString(),
              actual);
  }

  // no stopwords, but stopwords path
  {
    std::string config =
      "{\"locale\":\"en_US.utf-8\",\"case\":\"upper\",\"accent\":false,"
      "\"stemming\":true,\"stopwordsPath\":\"" IRS_TEST_RESOURCE_DIR "\"}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(
      vpack::Parser::fromJson(
        "{\"locale\":\"en_US\",\"case\":\"upper\",\"accent\":false,"
        "\"stemming\":true,\"stopwordsPath\":\"" IRS_TEST_RESOURCE_DIR "\"}")
        ->toString(),
      actual);
  }

  // no stopwords, but empty stopwords path (we need to shift CWD to our test
  // resources, to be able to load stopwords)
  {
    irs::Finally reset_stopword_path =
      [old_cwd = std::filesystem::current_path()]() noexcept {
        EXPECT_TRUE(irs::file_utils::SetCwd(old_cwd.c_str()));
      };
    irs::file_utils::SetCwd(
      std::filesystem::path(IRS_TEST_RESOURCE_DIR).c_str());

    std::string config =
      "{\"locale\":\"en_US.utf-8\",\"case\":\"lower\",\"accent\":false,"
      "\"stemming\":true,\"stopwordsPath\":\"\"}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"en_US\",\"case\":\"lower\",\"accent\":false,"
                "\"stemming\":true,\"stopwordsPath\":\"\"}")
                ->toString(),
              actual);
  }

  // non-empty stopwords with duplicates
  {
    std::string config =
      "{\"locale\":\"en_US.utf-8\",\"case\":\"upper\",\"stopwords\":[\"z\","
      "\"a\",\"b\",\"a\"],\"accent\":false,\"stemming\":true,"
      "\"stopwordsPath\":\"" IRS_TEST_RESOURCE_DIR "\"}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));

    // stopwords order is not guaranteed. Need to deep check json
    auto builder = vpack::Parser::fromJson(actual);
    auto json = builder->slice();
    ASSERT_TRUE(json.isObject());
    auto stopwords = json.get("stopwords");
    EXPECT_FALSE(stopwords.isNone());
    EXPECT_TRUE(stopwords.isArray());

    std::unordered_set<std::string> expected_stopwords = {"z", "a", "b"};
    for (size_t itr = 0, end = stopwords.length(); itr != end; ++itr) {
      ASSERT_TRUE(stopwords.at(itr).isString());
      expected_stopwords.erase(stopwords.at(itr).copyString());
    }
    ASSERT_TRUE(expected_stopwords.empty());
  }

  // min, max, preserveOriginal
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"case\":\"lower\",\"stopwords\":[], "
      "\"edgeNGram\" : { \"min\":1,\"max\":1,\"preserveOriginal\":true }}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"stopwords\":[],"
                "\"accent\":false,\"stemming\":true,\"edgeNGram\":{\"min\":1,"
                "\"max\":1,\"preserveOriginal\":true}}")
                ->toString(),
              actual);
  }

  // without min (see above for without min, max, and preserveOriginal)
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8\",\"case\":\"lower\",\"stopwords\":[], "
      "\"edgeNGram\" : {\"max\":2,\"preserveOriginal\":false}}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"stopwords\":[],"
                "\"accent\":false,\"stemming\":true,\"edgeNGram\":{\"max\":2,"
                "\"preserveOriginal\":false}}")
                ->toString(),
              actual);
  }

  // without min (see above for without min, max, and preserveOriginal)
  {
    std::string config =
      "{\"locale\":\"ru_RU.UTF-8@collation=phonebook\",\"case\":\"lower\","
      "\"stopwords\":[], \"edgeNGram\" : "
      "{\"max\":2,\"preserveOriginal\":false}}";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "text", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"locale\":\"ru_RU\",\"case\":\"lower\",\"stopwords\":[],"
                "\"accent\":false,\"stemming\":true,\"edgeNGram\":{\"max\":2,"
                "\"preserveOriginal\":false}}")
                ->toString(),
              actual);
  }
}

TEST_F(TextAnalyzerParserTestSuite, test_make_config_text) {
  std::string config = "RU";
  std::string actual;
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    actual, "text", irs::Type<irs::text_format::Text>::get(), config));
  ASSERT_EQ("ru", actual);
}

TEST_F(TextAnalyzerParserTestSuite, test_deterministic_stopwords_order) {
  std::string config =
    "{\"locale\":\"ru_RU.utf-8\",\"case\":\"lower\",\"stopwords\":[ \"ag\",\
      \"of\", \"plc\", \"the\", \"inc\", \"co\", \"ltd\"], \"accent\":true}";
  std::string normalized1;
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    normalized1, "text", irs::Type<irs::text_format::Json>::get(), config));
  std::string normalized2;
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    normalized2, "text", irs::Type<irs::text_format::Json>::get(),
    normalized1));
  ASSERT_EQ(normalized1, normalized2);
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"min\":2, \"max\":3, \"preserveOriginal\":false}}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"min\":0, \"max\":3, \"preserveOriginal\":false}}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"min\":2, \"max\":3, \"preserveOriginal\":true}}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"min\":3, \"max\":3, \"preserveOriginal\":false}}");
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
      irs::analysis::TextTokenizer::OptionsT options;
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
      irs::analysis::TextTokenizer::OptionsT options;
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"min\":0, \"max\":0}}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"max\":1, \"preserveOriginal\":false}}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"max\":1, \"preserveOriginal\":true}}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"min\":1, \"preserveOriginal\":false}}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"preserveOriginal\":false}}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"en_US.UTF-8\", \"stopwords\":[\"a\"], \"edgeNGram\" : "
        "{\"preserveOriginal\":true}}");
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
      auto stream = irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"ru_RU.UTF-8\", \"stopwords\":[\"\\u043A\"], "
        "\"edgeNGram\" : {\"min\":1, \"max\":2, "
        "\"preserveOriginal\":false}}");
      ASSERT_NE(nullptr, stream);
      test_func(std::string_view(s_data_ut_f8.data(), s_data_ut_f8.size()),
                stream.get());
    }
  }
}
