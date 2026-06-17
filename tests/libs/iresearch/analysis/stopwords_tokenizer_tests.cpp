////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include "gtest/gtest.h"
#include "iresearch/analysis/stopwords_tokenizer.hpp"

TEST(token_stopwords_stream_tests, consts) {
  static_assert("stopwords" ==
                irs::Type<irs::analysis::StopwordsTokenizer>::name());
}

TEST(token_stopwords_stream_tests, test_masking) {
  // test mask nothing
  {
    std::string_view data0("abc");
    std::string_view data1("ghi");
    irs::analysis::StopwordsTokenizer::stopwords_set mask;
    irs::analysis::StopwordsTokenizer stream(std::move(mask));
    ASSERT_EQ(irs::Type<irs::analysis::StopwordsTokenizer>::id(),
              stream.type());

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data1));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test mask something
  {
    std::string_view data0("abc");
    std::string_view data1("ghi");
    irs::analysis::StopwordsTokenizer::stopwords_set mask = {"abc"};
    irs::analysis::StopwordsTokenizer stream(std::move(mask));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data1));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }
}

TEST(token_stopwords_stream_tests, test_load) {
  std::string_view data0("abc");
  std::string_view data1("ghi");

  auto test_func = [](const std::string_view& data0,
                      const std::string_view& data1,
                      irs::analysis::Analyzer* stream) {
    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data0));
    ASSERT_FALSE(stream->next());
    ASSERT_TRUE(stream->reset(data1));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream->next());
  };

  {
    irs::analysis::StopwordsTokenizer::Options opts;
    opts.mask = {"abc", "646566", "6D6e6F"};
    auto stream = irs::analysis::StopwordsTokenizer::Make(std::move(opts));
    test_func(data0, data1, stream.get());
  }
  // check with another order of mask
  {
    irs::analysis::StopwordsTokenizer::Options opts;
    opts.mask = {"6D6e6F", "abc", "646566"};
    auto stream = irs::analysis::StopwordsTokenizer::Make(std::move(opts));
    test_func(data0, data1, stream.get());
  }
  // hex-decoded mask: the legacy JSON loader auto-decoded hex strings when
  // `"hex":true`; the direct Options API expects the pre-decoded bytes.
  // Here `"abc"` is the decoded form of `"616263"` and `"mnO"` is the decoded
  // form of `"6D6e6F"`. Verify masking works on the pre-decoded values.
  {
    std::string_view data_masked("abc");
    std::string_view data_kept("646566");
    auto test_hex = [](const std::string_view& masked,
                       const std::string_view& kept,
                       irs::analysis::Analyzer* stream) {
      ASSERT_NE(nullptr, stream);
      ASSERT_TRUE(stream->reset(masked));
      ASSERT_FALSE(stream->next());
      ASSERT_TRUE(stream->reset(kept));

      auto* offset = irs::get<irs::OffsAttr>(*stream);
      auto* term = irs::get<irs::TermAttr>(*stream);

      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(6, offset->end);
      ASSERT_EQ("646566", irs::ViewCast<char>(term->value));
      ASSERT_FALSE(stream->next());
    };
    irs::analysis::StopwordsTokenizer::Options opts;
    opts.mask = {"abc", "mnO"};  // pre-decoded equivalents of 616263 / 6D6e6F
    auto stream = irs::analysis::StopwordsTokenizer::Make(std::move(opts));
    test_hex(data_masked, data_kept, stream.get());
  }
  // Construction with an empty mask is valid -- the analyzer simply masks
  // nothing. The legacy JSON-driven "{}" / "{\"stopwords\":1}" / "1" /
  // "\"abc\"" / std::string_view{} cases were JSON-parser-level rejections
  // (wrong shape, wrong type, malformed input); those checks live in the
  // JSON parser tests and have no analogue in the direct Options API.
  // The post-decode-shape cases ("hex":1, "stopwords":[\"1aa\"...] etc.)
  // are likewise loader-side concerns: by the time the byte string reaches
  // `Options::mask`, every entry is just an opaque token to compare against.
  {
    irs::analysis::StopwordsTokenizer::Options opts;
    auto stream = irs::analysis::StopwordsTokenizer::Make(std::move(opts));
    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data0));
    ASSERT_TRUE(stream->next());  // nothing is masked
    ASSERT_FALSE(stream->next());
  }
}
