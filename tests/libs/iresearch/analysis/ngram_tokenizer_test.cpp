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

#include <unicode/locid.h>
#include <utf8.h>

#include <sstream>

#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "tests_shared.hpp"

namespace {

irs::bstring operator""_b(const char* ptr, size_t size) {
  return irs::bstring{reinterpret_cast<const irs::byte_type*>(ptr), size};
}

}  // namespace

TEST(ngram_token_stream_test, consts) {
  static_assert(
    "ngram" ==
    irs::Type<irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>>::name());
  static_assert("ngram" ==
                irs::Type<irs::analysis::NGramTokenizer<
                  irs::analysis::NGramTokenizerBase::InputType::UTF8>>::name());
}

TEST(ngram_token_stream_test, construct) {
  // 1..3, preserve_original=true
  {
    auto stream = irs::analysis::NGramTokenizerBase::Make(
      irs::analysis::NGramTokenizerBase::Options{
        .min_gram = 1, .max_gram = 3, .preserve_original = true});
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::NGramTokenizer<
                irs::analysis::NGramTokenizerBase::InputType::Binary>>::id(),
              stream->type());

    auto& impl = dynamic_cast<irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>&>(*stream);
    ASSERT_EQ(1, impl.min_gram());
    ASSERT_EQ(3, impl.max_gram());
    ASSERT_EQ(true, impl.preserve_original());
  }

  // min=0 -> clamps to 1, max=1
  {
    auto stream = irs::analysis::NGramTokenizerBase::Make(
      irs::analysis::NGramTokenizerBase::Options{
        .min_gram = 0, .max_gram = 1, .preserve_original = false});
    ASSERT_NE(nullptr, stream);

    auto& impl = dynamic_cast<irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>&>(*stream);
    ASSERT_EQ(1, impl.min_gram());
    ASSERT_EQ(1, impl.max_gram());
    ASSERT_EQ(false, impl.preserve_original());
  }

  // min>max -> max clamped to min
  {
    auto stream = irs::analysis::NGramTokenizerBase::Make(
      irs::analysis::NGramTokenizerBase::Options{
        .min_gram = 2, .max_gram = 1, .preserve_original = false});
    ASSERT_NE(nullptr, stream);

    auto& impl = dynamic_cast<irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>&>(*stream);
    ASSERT_EQ(2, impl.min_gram());
    ASSERT_EQ(2, impl.max_gram());
    ASSERT_EQ(false, impl.preserve_original());
  }

  // .........................................................................
  // The old "load jSON invalid" block exercised JSON-parser type errors
  // (`"min":"1"`, `"preserveOriginal":"true"`), missing-field rejection
  // (`"{}"`) and an unknown enum string (`"streamType":"unknown"`). All of
  // these are parser-/registry-level failures with no direct-API analogue:
  //   - field type errors can't be expressed with typed `Options` members,
  //   - the "missing required min/max" rule no longer holds (the new
  //     Options has explicit numeric defaults and Make clamps to >= 1), and
  //   - the `InputType` enum is now type-checked at the call site.
  // What survives is the default-Options happy path -- it must construct.
  // .........................................................................
  {
    auto stream = irs::analysis::NGramTokenizerBase::Make(
      irs::analysis::NGramTokenizerBase::Options{});
    ASSERT_NE(nullptr, stream);
  }

  // 2-gram
  {
    auto stream = irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>::
      make(irs::analysis::NGramTokenizerBase::Options(2, 2, true));
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::NGramTokenizer<
                irs::analysis::NGramTokenizerBase::InputType::Binary>>::id(),
              stream->type());

    auto impl = dynamic_cast<irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>*>(stream.get());
    ASSERT_NE(nullptr, impl);
    ASSERT_EQ(2, impl->min_gram());
    ASSERT_EQ(2, impl->max_gram());
    ASSERT_EQ(true, impl->preserve_original());

    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_TRUE(term);
    ASSERT_TRUE(irs::IsNull(term->value));

    auto* increment = irs::get<irs::IncAttr>(*stream);
    ASSERT_TRUE(increment);
    ASSERT_EQ(1, increment->value);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_TRUE(offset);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(0, offset->end);
  }

  // 0 == min_gram
  {
    auto stream = irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>::
      make(irs::analysis::NGramTokenizerBase::Options(0, 2, true));
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::NGramTokenizer<
                irs::analysis::NGramTokenizerBase::InputType::Binary>>::id(),
              stream->type());

    auto impl = dynamic_cast<irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>*>(stream.get());
    ASSERT_NE(nullptr, impl);
    ASSERT_EQ(1, impl->min_gram());
    ASSERT_EQ(2, impl->max_gram());
    ASSERT_EQ(true, impl->preserve_original());

    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_TRUE(term);
    ASSERT_TRUE(irs::IsNull(term->value));

    auto* increment = irs::get<irs::IncAttr>(*stream);
    ASSERT_TRUE(increment);
    ASSERT_EQ(1, increment->value);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_TRUE(offset);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(0, offset->end);
  }

  // min_gram > max_gram
  {
    auto stream = irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>::
      make(irs::analysis::NGramTokenizerBase::Options(
        std::numeric_limits<size_t>::max(), 2, true));
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::NGramTokenizer<
                irs::analysis::NGramTokenizerBase::InputType::Binary>>::id(),
              stream->type());

    auto impl = dynamic_cast<irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>*>(stream.get());
    ASSERT_NE(nullptr, impl);
    ASSERT_EQ(std::numeric_limits<size_t>::max(), impl->min_gram());
    ASSERT_EQ(std::numeric_limits<size_t>::max(), impl->max_gram());
    ASSERT_EQ(true, impl->preserve_original());

    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_TRUE(term);
    ASSERT_TRUE(irs::IsNull(term->value));

    auto* increment = irs::get<irs::IncAttr>(*stream);
    ASSERT_TRUE(increment);
    ASSERT_EQ(1, increment->value);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_TRUE(offset);
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(0, offset->end);
  }
}

TEST(ngram_token_stream_test, next_utf8) {
  struct Utf8token {
    Utf8token(const std::string_view& value, size_t start, size_t end) noexcept
      : start_marker(""),
        end_marker(""),
        value(value.data(), value.size()),
        start(start),
        end(end) {}
    Utf8token(const std::string_view& value, size_t start, size_t end,
              std::string_view sm, std::string_view em) noexcept
      : start_marker(sm),
        end_marker(em),
        value(value.data(), value.size()),
        start(start),
        end(end) {}

    std::string_view start_marker;
    std::string_view end_marker;
    std::string_view value;
    size_t start;
    size_t end;
  };

  auto assert_utf8tokens =
    [](const std::vector<Utf8token>& expected, std::string_view data,
       irs::analysis::NGramTokenizer<
         irs::analysis::NGramTokenizerBase::InputType::UTF8>& stream) {
      ASSERT_TRUE(stream.reset(data));

      auto* value = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(value);

      auto* offset = irs::get<irs::OffsAttr>(stream);
      ASSERT_TRUE(offset);
      auto* inc = irs::get<irs::IncAttr>(stream);
      auto expected_token = expected.begin();
      uint32_t pos = std::numeric_limits<uint32_t>::max();
      while (stream.next()) {
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(expected_token->value),
                  value->value);
        ASSERT_EQ(expected_token->start, offset->start);
        ASSERT_EQ(expected_token->end, offset->end);
        pos += inc->value;
        auto start = reinterpret_cast<const irs::byte_type*>(data.data());
        utf8::unchecked::advance(start, pos);
        const auto size = value->value.size() -
                          expected_token->start_marker.size() -
                          expected_token->end_marker.size();
        ASSERT_GT(size, 0);
        irs::bstring bs;
        if (!expected_token->start_marker.empty()) {
          bs.append(reinterpret_cast<const irs::byte_type*>(
                      expected_token->start_marker.data()),
                    expected_token->start_marker.size());
        }
        bs.append(start, size);
        if (!expected_token->end_marker.empty()) {
          bs.append(reinterpret_cast<const irs::byte_type*>(
                      expected_token->end_marker.data()),
                    expected_token->end_marker.size());
        }

        ASSERT_EQ(bs, value->value);
        ++expected_token;
      }
      ASSERT_EQ(expected_token, expected.end());
      ASSERT_FALSE(stream.next());
    };

  auto locale = icu::Locale::createFromName("C.UTF-8");

  {
    SCOPED_TRACE("1-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 1, false, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{}, irs::bstring{}));

    std::u8string utf8data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";

    const std::vector<Utf8token> expected{
      {"a", 0, 1}, {"\xc2\xa2", 1, 3}, {"b", 3, 4},  {"\xc2\xa3", 4, 6},
      {"c", 6, 7}, {"\xc2\xa4", 7, 9}, {"d", 9, 10}, {"\xc2\xa5", 10, 12}};
    assert_utf8tokens(expected, std::string(utf8data.begin(), utf8data.end()),
                      stream);

    // let`s break utf-8. Cut the last byte of last 2-byte symbol
    utf8data.resize(utf8data.size() - 1);
    const std::vector<Utf8token> expected2{
      {"a", 0, 1}, {"\xc2\xa2", 1, 3}, {"b", 3, 4},  {"\xc2\xa3", 4, 6},
      {"c", 6, 7}, {"\xc2\xa4", 7, 9}, {"d", 9, 10}, {"\xc2", 10, 11}};
    assert_utf8tokens(expected2, std::string(utf8data.begin(), utf8data.end()),
                      stream);
  }

  {
    SCOPED_TRACE("2-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        2, 2, false, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{}, irs::bstring{}));

    constexpr std::u8string_view kUtf8data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
    const auto data = irs::ViewCast<char>(kUtf8data);

    const std::vector<Utf8token> expected{{"a\xc2\xa2", 0, 3},
                                          {"\xc2\xa2"
                                           "b",
                                           1, 4},
                                          {"b"
                                           "\xc2\xa3",
                                           3, 6},
                                          {"\xc2\xa3"
                                           "c",
                                           4, 7},
                                          {"c"
                                           "\xc2\xa4",
                                           6, 9},
                                          {"\xc2\xa4"
                                           "d",
                                           7, 10},
                                          {"d"
                                           "\xc2\xa5",
                                           9, 12}};
    assert_utf8tokens(expected, data, stream);
  }

  {
    SCOPED_TRACE("1-2-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 2, false, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{}, irs::bstring{}));

    constexpr std::u8string_view kUtf8data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
    const auto data = irs::ViewCast<char>(kUtf8data);

    const std::vector<Utf8token> expected{{"a", 0, 1},
                                          {"a\xc2\xa2", 0, 3},
                                          {"\xc2\xa2", 1, 3},
                                          {"\xc2\xa2"
                                           "b",
                                           1, 4},
                                          {"b", 3, 4},
                                          {"b"
                                           "\xc2\xa3",
                                           3, 6},
                                          {"\xc2\xa3", 4, 6},
                                          {"\xc2\xa3"
                                           "c",
                                           4, 7},
                                          {"c", 6, 7},
                                          {"c"
                                           "\xc2\xa4",
                                           6, 9},
                                          {"\xc2\xa4", 7, 9},
                                          {"\xc2\xa4"
                                           "d",
                                           7, 10},
                                          {"d", 9, 10},
                                          {"d"
                                           "\xc2\xa5",
                                           9, 12},
                                          {"\xc2\xa5", 10, 12}};
    assert_utf8tokens(expected, data, stream);
  }

  {
    SCOPED_TRACE("4-gram invalid utf8");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        4, 4, false, irs::analysis::NGramTokenizerBase::InputType::UTF8));

    const std::vector<Utf8token> expected{
      {"\xFFqui", 0, 4},
      {"quic", 1, 5},
      {"uick", 2, 6},
      {"ick\xFF", 3, 7},
    };

    assert_utf8tokens(expected, "\xFFquick\xFF", stream);
  }

  {
    SCOPED_TRACE("5-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        5, 5, false, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{}, irs::bstring{}));

    constexpr std::u8string_view kUtf8data = u8"\u00C0\u00C1\u00C2\u00C3\u00C4";
    const auto data = irs::ViewCast<char>(kUtf8data);

    const std::vector<Utf8token> expected{
      {"\xc3\x80\xc3\x81\xc3\x82\xc3\x83\xc3\x84", 0, 10}};
    assert_utf8tokens(expected, data, stream);
  }

  {
    SCOPED_TRACE("5-gram with markers");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        5, 5, false, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa2"), 2},
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa1"), 2}));

    constexpr std::u8string_view kUtf8data = u8"\u00C0\u00C1\u00C2\u00C3\u00C4";
    const auto data = irs::ViewCast<char>(kUtf8data);

    const std::vector<Utf8token> expected{
      {"\xc2\xa2\xc3\x80\xc3\x81\xc3\x82\xc3\x83\xc3\x84", 0, 10, "\xc2\xa2",
       ""},
      {"\xc3\x80\xc3\x81\xc3\x82\xc3\x83\xc3\x84\xc2\xa1", 0, 10, "",
       "\xc2\xa1"}};
    assert_utf8tokens(expected, data, stream);
  }

  {
    SCOPED_TRACE("5-gram preserve original");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        5, 5, true, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{}, irs::bstring{}));
    const std::u8string_view utf8data = u8"\u00C0\u00C1\u00C2\u00C3\u00C4";
    const auto data = irs::ViewCast<char>(utf8data);

    const std::vector<Utf8token> expected{
      {"\xc3\x80\xc3\x81\xc3\x82\xc3\x83\xc3\x84", 0, 10}};
    assert_utf8tokens(expected, data, stream);
  }

  {
    SCOPED_TRACE("6-gram preserve original");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        6, 6, true, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{}, irs::bstring{}));
    constexpr std::u8string_view kUtf8data = u8"\u00C0\u00C1\u00C2\u00C3\u00C4";
    const auto data = irs::ViewCast<char>(kUtf8data);

    const std::vector<Utf8token> expected{
      {"\xc3\x80\xc3\x81\xc3\x82\xc3\x83\xc3\x84", 0, 10}};
    assert_utf8tokens(expected, data, stream);
  }

  {
    SCOPED_TRACE("6-gram no output");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        6, 6, false, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{}, irs::bstring{}));
    const std::u8string_view utf8data = u8"\u00C0\u00C1\u00C2\u00C3\u00C4";
    const auto data = irs::ViewCast<char>(utf8data);

    ASSERT_TRUE(stream.reset(data));
    ASSERT_FALSE(stream.next());
  }

  {
    SCOPED_TRACE("1-2 gram no-preserve-original start-marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 2, false, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa1"), 2},
        irs::bstring{}));

    constexpr std::u8string_view kUtf8data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
    const auto data = irs::ViewCast<char>(kUtf8data);

    const std::vector<Utf8token> expected{{"\xc2\xa1"
                                           "a",
                                           0, 1, "\xc2\xa1", ""},
                                          {"\xc2\xa1"
                                           "a"
                                           "\xc2\xa2",
                                           0, 3, "\xc2\xa1", ""},
                                          {"\xc2\xa2", 1, 3},
                                          {"\xc2\xa2"
                                           "b",
                                           1, 4},
                                          {"b", 3, 4},
                                          {"b"
                                           "\xc2\xa3",
                                           3, 6},
                                          {"\xc2\xa3", 4, 6},
                                          {"\xc2\xa3"
                                           "c",
                                           4, 7},
                                          {"c", 6, 7},
                                          {"c"
                                           "\xc2\xa4",
                                           6, 9},
                                          {"\xc2\xa4", 7, 9},
                                          {"\xc2\xa4"
                                           "d",
                                           7, 10},
                                          {"d", 9, 10},
                                          {"d"
                                           "\xc2\xa5",
                                           9, 12},
                                          {"\xc2\xa5", 10, 12}};
    assert_utf8tokens(expected, data, stream);
  }
  {
    SCOPED_TRACE("1-2 gram preserve-original start-marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 2, true, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa1"), 2},
        irs::bstring{}));

    constexpr std::u8string_view kUtf8data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
    const auto data = irs::ViewCast<char>(kUtf8data);

    const std::vector<Utf8token> expected{{"\xc2\xa1"
                                           "a",
                                           0, 1, "\xc2\xa1", ""},
                                          {"\xc2\xa1"
                                           "a"
                                           "\xc2\xa2",
                                           0, 3, "\xc2\xa1", ""},
                                          {"\xc2\xa1"
                                           "a"
                                           "\xc2\xa2"
                                           "b"
                                           "\xc2\xa3"
                                           "c"
                                           "\xc2\xa4"
                                           "d"
                                           "\xc2\xa5",
                                           0, 12, "\xc2\xa1", ""},
                                          {"\xc2\xa2", 1, 3},
                                          {"\xc2\xa2"
                                           "b",
                                           1, 4},
                                          {"b", 3, 4},
                                          {"b"
                                           "\xc2\xa3",
                                           3, 6},
                                          {"\xc2\xa3", 4, 6},
                                          {"\xc2\xa3"
                                           "c",
                                           4, 7},
                                          {"c", 6, 7},
                                          {"c"
                                           "\xc2\xa4",
                                           6, 9},
                                          {"\xc2\xa4", 7, 9},
                                          {"\xc2\xa4"
                                           "d",
                                           7, 10},
                                          {"d", 9, 10},
                                          {"d"
                                           "\xc2\xa5",
                                           9, 12},
                                          {"\xc2\xa5", 10, 12}};
    assert_utf8tokens(expected, data, stream);
  }
  {
    SCOPED_TRACE("2-3 gram preserve-original end-marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        2, 3, true, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{},
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa1"), 2}));

    constexpr std::u8string_view kUtf8data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
    const auto data = irs::ViewCast<char>(kUtf8data);

    const std::vector<Utf8token> expected{{"a"
                                           "\xc2\xa2",
                                           0, 3},
                                          {"a"
                                           "\xc2\xa2"
                                           "b",
                                           0, 4},
                                          {"a"
                                           "\xc2\xa2"
                                           "b"
                                           "\xc2\xa3"
                                           "c"
                                           "\xc2\xa4"
                                           "d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           0, 12, "", "\xc2\xa1"},
                                          {"\xc2\xa2"
                                           "b",
                                           1, 4},
                                          {"\xc2\xa2"
                                           "b"
                                           "\xc2\xa3",
                                           1, 6},
                                          {"b"
                                           "\xc2\xa3",
                                           3, 6},
                                          {"b"
                                           "\xc2\xa3"
                                           "c",
                                           3, 7},
                                          {"\xc2\xa3"
                                           "c",
                                           4, 7},
                                          {"\xc2\xa3"
                                           "c"
                                           "\xc2\xa4",
                                           4, 9},
                                          {"c"
                                           "\xc2\xa4",
                                           6, 9},
                                          {"c"
                                           "\xc2\xa4"
                                           "d",
                                           6, 10},
                                          {"\xc2\xa4"
                                           "d",
                                           7, 10},
                                          {"\xc2\xa4"
                                           "d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           7, 12, "", "\xc2\xa1"},
                                          {"d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           9, 12, "", "\xc2\xa1"}};
    assert_utf8tokens(expected, data, stream);
  }

  {
    SCOPED_TRACE(" 1-3 gram preserve-original end-marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 3, true, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{},
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa1"), 2}));

    const std::u8string_view utf8data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
    const auto data = irs::ViewCast<char>(utf8data);

    const std::vector<Utf8token> expected{{"a", 0, 1},
                                          {"a"
                                           "\xc2\xa2",
                                           0, 3},
                                          {"a"
                                           "\xc2\xa2"
                                           "b",
                                           0, 4},
                                          {"a"
                                           "\xc2\xa2"
                                           "b"
                                           "\xc2\xa3"
                                           "c"
                                           "\xc2\xa4"
                                           "d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           0, 12, "", "\xc2\xa1"},
                                          {"\xc2\xa2", 1, 3},
                                          {"\xc2\xa2"
                                           "b",
                                           1, 4},
                                          {"\xc2\xa2"
                                           "b"
                                           "\xc2\xa3",
                                           1, 6},
                                          {"b", 3, 4},
                                          {"b"
                                           "\xc2\xa3",
                                           3, 6},
                                          {"b"
                                           "\xc2\xa3"
                                           "c",
                                           3, 7},
                                          {"\xc2\xa3", 4, 6},
                                          {"\xc2\xa3"
                                           "c",
                                           4, 7},
                                          {"\xc2\xa3"
                                           "c"
                                           "\xc2\xa4",
                                           4, 9},
                                          {"c", 6, 7},
                                          {"c"
                                           "\xc2\xa4",
                                           6, 9},
                                          {"c"
                                           "\xc2\xa4"
                                           "d",
                                           6, 10},
                                          {"\xc2\xa4", 7, 9},
                                          {"\xc2\xa4"
                                           "d",
                                           7, 10},
                                          {"\xc2\xa4"
                                           "d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           7, 12, "", "\xc2\xa1"},
                                          {"d", 9, 10},
                                          {"d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           9, 12, "", "\xc2\xa1"},
                                          {"\xc2\xa5"
                                           "\xc2\xa1",
                                           10, 12, "", "\xc2\xa1"}};
    assert_utf8tokens(expected, data, stream);
  }

  {
    SCOPED_TRACE("1-3 gram preserve-original start-marker end-marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 3, true, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa2"), 2},
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa1"), 2}));

    constexpr std::u8string_view kUtf8data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
    const auto data = irs::ViewCast<char>(kUtf8data);

    const std::vector<Utf8token> expected{{"\xc2\xa2"
                                           "a",
                                           0, 1, "\xc2\xa2", ""},
                                          {"\xc2\xa2"
                                           "a"
                                           "\xc2\xa2",
                                           0, 3, "\xc2\xa2", ""},
                                          {"\xc2\xa2"
                                           "a"
                                           "\xc2\xa2"
                                           "b",
                                           0, 4, "\xc2\xa2", ""},
                                          {"\xc2\xa2"
                                           "a"
                                           "\xc2\xa2"
                                           "b"
                                           "\xc2\xa3"
                                           "c"
                                           "\xc2\xa4"
                                           "d"
                                           "\xc2\xa5",
                                           0, 12, "\xc2\xa2", ""},
                                          {"a"
                                           "\xc2\xa2"
                                           "b"
                                           "\xc2\xa3"
                                           "c"
                                           "\xc2\xa4"
                                           "d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           0, 12, "", "\xc2\xa1"},
                                          {"\xc2\xa2", 1, 3},
                                          {"\xc2\xa2"
                                           "b",
                                           1, 4},
                                          {"\xc2\xa2"
                                           "b"
                                           "\xc2\xa3",
                                           1, 6},
                                          {"b", 3, 4},
                                          {"b"
                                           "\xc2\xa3",
                                           3, 6},
                                          {"b"
                                           "\xc2\xa3"
                                           "c",
                                           3, 7},
                                          {"\xc2\xa3", 4, 6},
                                          {"\xc2\xa3"
                                           "c",
                                           4, 7},
                                          {"\xc2\xa3"
                                           "c"
                                           "\xc2\xa4",
                                           4, 9},
                                          {"c", 6, 7},
                                          {"c"
                                           "\xc2\xa4",
                                           6, 9},
                                          {"c"
                                           "\xc2\xa4"
                                           "d",
                                           6, 10},
                                          {"\xc2\xa4", 7, 9},
                                          {"\xc2\xa4"
                                           "d",
                                           7, 10},
                                          {"\xc2\xa4"
                                           "d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           7, 12, "", "\xc2\xa1"},
                                          {"d", 9, 10},
                                          {"d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           9, 12, "", "\xc2\xa1"},
                                          {"\xc2\xa5"
                                           "\xc2\xa1",
                                           10, 12, "", "\xc2\xa1"}};
    assert_utf8tokens(expected, data, stream);
  }

  {
    SCOPED_TRACE("1-3 gram no-preserve-original start-marker end-marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::UTF8>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 3, false, irs::analysis::NGramTokenizerBase::InputType::UTF8,
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa2"), 2},
        irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa1"), 2}));

    const std::u8string_view utf8data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
    const auto data = irs::ViewCast<char>(utf8data);

    const std::vector<Utf8token> expected{{"\xc2\xa2"
                                           "a",
                                           0, 1, "\xc2\xa2", ""},
                                          {"\xc2\xa2"
                                           "a"
                                           "\xc2\xa2",
                                           0, 3, "\xc2\xa2", ""},
                                          {"\xc2\xa2"
                                           "a"
                                           "\xc2\xa2"
                                           "b",
                                           0, 4, "\xc2\xa2", ""},
                                          {"\xc2\xa2", 1, 3},
                                          {"\xc2\xa2"
                                           "b",
                                           1, 4},
                                          {"\xc2\xa2"
                                           "b"
                                           "\xc2\xa3",
                                           1, 6},
                                          {"b", 3, 4},
                                          {"b"
                                           "\xc2\xa3",
                                           3, 6},
                                          {"b"
                                           "\xc2\xa3"
                                           "c",
                                           3, 7},
                                          {"\xc2\xa3", 4, 6},
                                          {"\xc2\xa3"
                                           "c",
                                           4, 7},
                                          {"\xc2\xa3"
                                           "c"
                                           "\xc2\xa4",
                                           4, 9},
                                          {"c", 6, 7},
                                          {"c"
                                           "\xc2\xa4",
                                           6, 9},
                                          {"c"
                                           "\xc2\xa4"
                                           "d",
                                           6, 10},
                                          {"\xc2\xa4", 7, 9},
                                          {"\xc2\xa4"
                                           "d",
                                           7, 10},
                                          {"\xc2\xa4"
                                           "d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           7, 12, "", "\xc2\xa1"},
                                          {"d", 9, 10},
                                          {"d"
                                           "\xc2\xa5"
                                           "\xc2\xa1",
                                           9, 12, "", "\xc2\xa1"},
                                          {"\xc2\xa5"
                                           "\xc2\xa1",
                                           10, 12, "", "\xc2\xa1"}};
    assert_utf8tokens(expected, data, stream);
  }
}

TEST(ngram_token_stream_test, reset_too_big) {
  irs::analysis::NGramTokenizer<
    irs::analysis::NGramTokenizerBase::InputType::Binary>
    stream(irs::analysis::NGramTokenizerBase::Options(1, 1, false));

  const std::string_view input(
    reinterpret_cast<const char*>(&stream),
    size_t(std::numeric_limits<uint32_t>::max()) + 1);

  ASSERT_FALSE(stream.reset(input));

  auto* term = irs::get<irs::TermAttr>(stream);
  ASSERT_TRUE(term);
  ASSERT_TRUE(irs::IsNull(term->value));

  auto* increment = irs::get<irs::IncAttr>(stream);
  ASSERT_TRUE(increment);
  ASSERT_EQ(1, increment->value);

  auto* offset = irs::get<irs::OffsAttr>(stream);
  ASSERT_TRUE(offset);
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(0, offset->end);
}

TEST(ngram_token_stream_test, next) {
  struct Token {
    Token(std::string_view value, size_t start, size_t end) noexcept
      : start_marker(""),
        end_marker(""),
        value(value),
        start(start),
        end(end) {}

    Token(std::string_view value, size_t start, size_t end, std::string_view sm,
          std::string_view em) noexcept
      : start_marker(sm),
        end_marker(em),
        value(value.data(), value.size()),
        start(start),
        end(end) {}

    std::string_view start_marker;
    std::string_view end_marker;

    std::string_view value;
    size_t start;
    size_t end;
  };

  auto assert_tokens =
    [](const std::vector<Token>& expected, const std::string_view& data,
       irs::analysis::NGramTokenizer<
         irs::analysis::NGramTokenizerBase::InputType::Binary>& stream) {
      ASSERT_TRUE(stream.reset(data));

      auto* value = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(value);

      auto* offset = irs::get<irs::OffsAttr>(stream);
      ASSERT_TRUE(offset);
      auto* inc = irs::get<irs::IncAttr>(stream);
      auto expected_token = expected.begin();
      uint32_t pos = std::numeric_limits<uint32_t>::max();
      while (stream.next()) {
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(expected_token->value),
                  value->value);
        ASSERT_EQ(expected_token->start, offset->start);
        ASSERT_EQ(expected_token->end, offset->end);
        pos += inc->value;
        const auto size = value->value.size() -
                          expected_token->start_marker.size() -
                          expected_token->end_marker.size();
        ASSERT_GT(size, 0);
        irs::bstring bs;
        if (!expected_token->start_marker.empty()) {
          bs.append(reinterpret_cast<const irs::byte_type*>(
                      expected_token->start_marker.data()),
                    expected_token->start_marker.size());
        }
        bs.append(reinterpret_cast<const irs::byte_type*>(data.data()) + pos,
                  size);
        if (!expected_token->end_marker.empty()) {
          bs.append(reinterpret_cast<const irs::byte_type*>(
                      expected_token->end_marker.data()),
                    expected_token->end_marker.size());
        }
        ASSERT_EQ(bs, value->value);
        ++expected_token;
      }
      ASSERT_EQ(expected_token, expected.end());
      ASSERT_FALSE(stream.next());
    };

  {
    SCOPED_TRACE("1-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(1, 1, false));

    const std::vector<Token> expected{
      {"q", 0, 1}, {"u", 1, 2}, {"i", 2, 3}, {"c", 3, 4}, {"k", 4, 5}};

    assert_tokens(expected, "quick", stream);
  }
  {
    SCOPED_TRACE("1-gram start marker end marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 1, false, irs::analysis::NGramTokenizerBase::InputType::Binary,
        "$"_b, "^"_b));

    const std::vector<Token> expected{{"$q", 0, 1, "$", ""},
                                      {"u", 1, 2},
                                      {"i", 2, 3},
                                      {"c", 3, 4},
                                      {"k^", 4, 5, "", "^"}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("1-gram, preserve original");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(1, 1, true));

    const std::vector<Token> expected{
      {"q", 0, 1}, {"quick", 0, 5}, {"u", 1, 2},
      {"i", 2, 3}, {"c", 3, 4},     {"k", 4, 5},
    };

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("1-gram  preserve original start marker end marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 1, true, irs::analysis::NGramTokenizerBase::InputType::Binary, "$"_b,
        "^"_b));

    const std::vector<Token> expected{{"$q", 0, 1, "$", ""},
                                      {"$quick", 0, 5, "$", ""},
                                      {"quick^", 0, 5, "", "^"},
                                      {"u", 1, 2},
                                      {"i", 2, 3},
                                      {"c", 3, 4},
                                      {"k^", 4, 5, "", "^"}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("2-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(2, 2, false));

    const std::vector<Token> expected{
      {"qu", 0, 2},
      {"ui", 1, 3},
      {"ic", 2, 4},
      {"ck", 3, 5},
    };

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("2-gram, preserve original");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(2, 2, true));

    const std::vector<Token> expected{
      {"qu", 0, 2}, {"quick", 0, 5}, {"ui", 1, 3}, {"ic", 2, 4}, {"ck", 3, 5},
    };

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("1..2-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(1, 2, false));

    const std::vector<Token> expected{{"q", 0, 1},  {"qu", 0, 2}, {"u", 1, 2},
                                      {"ui", 1, 3}, {"i", 2, 3},  {"ic", 2, 4},
                                      {"c", 3, 4},  {"ck", 3, 5}, {"k", 4, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("3-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(3, 3, false));

    const std::vector<Token> expected{
      {"qui", 0, 3}, {"uic", 1, 4}, {"ick", 2, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("1..3-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(1, 3, false));

    const std::vector<Token> expected{{"q", 0, 1}, {"qu", 0, 2}, {"qui", 0, 3},
                                      {"u", 1, 2}, {"ui", 1, 3}, {"uic", 1, 4},
                                      {"i", 2, 3}, {"ic", 2, 4}, {"ick", 2, 5},
                                      {"c", 3, 4}, {"ck", 3, 5}, {"k", 4, 5}};

    assert_tokens(expected, "quick", stream);
  }
  {
    SCOPED_TRACE("1..3-gram start marker end marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(
        1, 3, false, irs::analysis::NGramTokenizerBase::InputType::Binary,
        "$"_b, "^"_b));

    const std::vector<Token> expected{
      {"$q", 0, 1, "$", ""}, {"$qu", 0, 2, "$", ""}, {"$qui", 0, 3, "$", ""},
      {"u", 1, 2},           {"ui", 1, 3},           {"uic", 1, 4},
      {"i", 2, 3},           {"ic", 2, 4},           {"ick^", 2, 5, "", "^"},
      {"c", 3, 4},           {"ck^", 3, 5, "", "^"}, {"k^", 4, 5, "", "^"}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("2..3-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(2, 3, false));

    const std::vector<Token> expected{
      {"qu", 0, 2}, {"qui", 0, 3}, {"ui", 1, 3}, {"uic", 1, 4},
      {"ic", 2, 4}, {"ick", 2, 5}, {"ck", 3, 5},
    };

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("2..3-gram, preserve origianl");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(2, 3, true));

    const std::vector<Token> expected{
      {"qu", 0, 2},  {"qui", 0, 3}, {"quick", 0, 5}, {"ui", 1, 3},
      {"uic", 1, 4}, {"ic", 2, 4},  {"ick", 2, 5},   {"ck", 3, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("2..3-gram, preserve origianl start marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(
        2, 3, true, irs::analysis::NGramTokenizerBase::InputType::Binary, "$"_b,
        irs::bstring{}));

    const std::vector<Token> expected{{"$qu", 0, 2, "$", ""},
                                      {"$qui", 0, 3, "$", ""},
                                      {"$quick", 0, 5, "$", ""},
                                      {"ui", 1, 3},
                                      {"uic", 1, 4},
                                      {"ic", 2, 4},
                                      {"ick", 2, 5},
                                      {"ck", 3, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("2..3-gram, preserve origianl end marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(
        2, 3, true, irs::analysis::NGramTokenizerBase::InputType::Binary,
        irs::bstring{}, "^"_b));

    const std::vector<Token> expected{{"qu", 0, 2},
                                      {"qui", 0, 3},
                                      {"quick^", 0, 5, "", "^"},
                                      {"ui", 1, 3},
                                      {"uic", 1, 4},
                                      {"ic", 2, 4},
                                      {"ick^", 2, 5, "", "^"},
                                      {"ck^", 3, 5, "", "^"}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("2..3-gram, preserve origianl start marker end marker");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(
        2, 3, true, irs::analysis::NGramTokenizerBase::InputType::Binary, "$"_b,
        "^"_b));

    const std::vector<Token> expected{{"$qu", 0, 2, "$", ""},
                                      {"$qui", 0, 3, "$", ""},
                                      {"$quick", 0, 5, "$", ""},
                                      {"quick^", 0, 5, "", "^"},
                                      {"ui", 1, 3},
                                      {"uic", 1, 4},
                                      {"ic", 2, 4},
                                      {"ick^", 2, 5, "", "^"},
                                      {"ck^", 3, 5, "", "^"}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("4-gram invalid utf8");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(4, 4, false));

    const std::vector<Token> expected{
      {"\xFFqui", 0, 4},
      {"quic", 1, 5},
      {"uick", 2, 6},
      {"ick\xFF", 3, 7},
    };

    assert_tokens(expected, "\xFFquick\xFF", stream);
  }

  {
    SCOPED_TRACE("4-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(4, 4, false));

    const std::vector<Token> expected{
      {"quic", 0, 4},
      {"uick", 1, 5},
    };

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("1..4-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(1, 4, false));

    const std::vector<Token> expected{
      {"q", 0, 1},  {"qu", 0, 2}, {"qui", 0, 3}, {"quic", 0, 4},
      {"u", 1, 2},  {"ui", 1, 3}, {"uic", 1, 4}, {"uick", 1, 5},
      {"i", 2, 3},  {"ic", 2, 4}, {"ick", 2, 5}, {"c", 3, 4},
      {"ck", 3, 5}, {"k", 4, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("5-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(5, 5, false));

    const std::vector<Token> expected{{"quick", 0, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("5-gram, preserve original");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(5, 5, true));

    const std::vector<Token> expected{{"quick", 0, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("4-5-gram, preserve original");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(4, 5, true));

    const std::vector<Token> expected{
      {"quic", 0, 4}, {"quick", 0, 5}, {"uick", 1, 5}};
    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("4-5-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(4, 5, false));

    const std::vector<Token> expected{
      {"quic", 0, 4}, {"quick", 0, 5}, {"uick", 1, 5}};
    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("6-gram, preserve original");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(6, 6, true));

    const std::vector<Token> expected{{"quick", 0, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("6-gram no output");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(6, 6, false));

    ASSERT_TRUE(stream.reset("quick"));
    ASSERT_FALSE(stream.next());
  }

  {
    SCOPED_TRACE("1..5-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(1, 5, false));

    const std::vector<Token> expected{
      {"q", 0, 1},     {"qu", 0, 2}, {"qui", 0, 3}, {"quic", 0, 4},
      {"quick", 0, 5}, {"u", 1, 2},  {"ui", 1, 3},  {"uic", 1, 4},
      {"uick", 1, 5},  {"i", 2, 3},  {"ic", 2, 4},  {"ick", 2, 5},
      {"c", 3, 4},     {"ck", 3, 5}, {"k", 4, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("3..5-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(3, 5, false));

    const std::vector<Token> expected{
      {"qui", 0, 3}, {"quic", 0, 4}, {"quick", 0, 5},
      {"uic", 1, 4}, {"uick", 1, 5}, {"ick", 2, 5},
    };

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("6-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(6, 6, false));

    const std::vector<Token> expected{};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("1..6-gram");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(1, 6, false));

    const std::vector<Token> expected{
      {"q", 0, 1},     {"qu", 0, 2}, {"qui", 0, 3}, {"quic", 0, 4},
      {"quick", 0, 5}, {"u", 1, 2},  {"ui", 1, 3},  {"uic", 1, 4},
      {"uick", 1, 5},  {"i", 2, 3},  {"ic", 2, 4},  {"ick", 2, 5},
      {"c", 3, 4},     {"ck", 3, 5}, {"k", 4, 5}};

    assert_tokens(expected, "quick", stream);
  }

  {
    SCOPED_TRACE("1..6-gram, preserve original");
    irs::analysis::NGramTokenizer<
      irs::analysis::NGramTokenizerBase::InputType::Binary>
      stream(irs::analysis::NGramTokenizerBase::Options(1, 6, true));

    const std::vector<Token> expected{
      {"q", 0, 1},     {"qu", 0, 2}, {"qui", 0, 3}, {"quic", 0, 4},
      {"quick", 0, 5}, {"u", 1, 2},  {"ui", 1, 3},  {"uic", 1, 4},
      {"uick", 1, 5},  {"i", 2, 3},  {"ic", 2, 4},  {"ick", 2, 5},
      {"c", 3, 4},     {"ck", 3, 5}, {"k", 4, 5}};

    assert_tokens(expected, "quick", stream);
  }
}

TEST(ngram_token_stream_test, test_out_of_range_pos_issue) {
  auto stream = irs::analysis::NGramTokenizerBase::Make(
    irs::analysis::NGramTokenizerBase::Options{
      .min_gram = 2, .max_gram = 3, .preserve_original = true});
  ASSERT_NE(nullptr, stream);
  auto* inc = irs::get<irs::IncAttr>(*stream);
  for (size_t i = 0; i < 10000; ++i) {
    std::basic_stringstream<char> ss;
    ss << "test_" << i;
    ASSERT_TRUE(stream->reset(ss.str()));
    uint32_t pos = std::numeric_limits<uint32_t>::max();
    uint32_t last_pos = 0;
    while (stream->next()) {
      pos += inc->value;
      ASSERT_GE(pos, last_pos);
      ASSERT_LT(pos, irs::pos_limits::eof());
      last_pos = pos;
    }
  }
}

// Performance tests below are convenient way to quickly analyze performance
// changes However  there is no point to run them as part of regular tests and
// no point to spoil output by marking them disabled
//
// TEST(ngram_token_stream_test, performance_next_utf8) {
//  irs::analysis::ngram_token_stream<irs::analysis::ngram_token_stream_base::InputType::UTF8>
//  stream(
//    irs::analysis::ngram_token_stream_base::Options(1, 3, true,
//      irs::analysis::ngram_token_stream_base::InputType::UTF8,
//      irs::EmptyRef<irs::byte_type>(), irs::EmptyRef<irs::byte_type>()));
//
//  std::string data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
//  for (size_t i = 0; i < 100000; ++i) {
//    data += u8"a\u00A2b\u00A3c\u00A4d\u00A5";
//  }
//  //std::cerr << "Set debug breakpoint here";
//  for (size_t i = 0; i < 10; ++i) {
//    stream.reset(data);
//    while (stream.next()) {}
//  }
//  ASSERT_FALSE(stream.next());
//}
//
// TEST(ngram_token_stream_test, performance_next) {
//  irs::analysis::ngram_token_stream<irs::analysis::ngram_token_stream_base::InputType::Binary>
//  stream(
//    irs::analysis::ngram_token_stream_base::Options(1, 3, true));
//
//  std::string data = "quickbro";
//  for (size_t i = 0; i < 100000; ++i) {
//    data += "quickbro";
//  }
//  //std::cerr << "Set debug breakpoint here";
//  for (size_t i = 0; i < 10; ++i) {
//    stream.reset(data);
//    while (stream.next()) {}
//  }
//  ASSERT_FALSE(stream.next());
//}
//
//
// TEST(ngram_token_stream_test, performance_next_utf8_marker) {
//  irs::analysis::ngram_token_stream<irs::analysis::ngram_token_stream_base::InputType::UTF8>
//  stream(
//    irs::analysis::ngram_token_stream_base::Options(1, 3, true,
//      irs::analysis::ngram_token_stream_base::InputType::UTF8,
//      irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa2"), 2},
//      std::string(reinterpret_cast<const char*>("\xc2\xa1"),
//      2)));
//
//  std::wstring data = u8"a\u00A2b\u00A3c\u00A4d\u00A5";
//  for (size_t i = 0; i < 100000; ++i) {
//    data += u8"a\u00A2b\u00A3c\u00A4d\u00A5";
//  }
//  //std::cerr << "Set debug breakpoint here";
//  for (size_t i = 0; i < 10; ++i) {
//    stream.reset(data);
//    while (stream.next()) {}
//  }
//  ASSERT_FALSE(stream.next());
//}
//
// TEST(ngram_token_stream_test, performance_next_marker) {
//  irs::analysis::ngram_token_stream<irs::analysis::ngram_token_stream_base::InputType::Binary>
//  stream(
//    irs::analysis::ngram_token_stream_base::Options(1, 3, true,
//      irs::analysis::ngram_token_stream_base::InputType::Binary,
//      irs::bstring{reinterpret_cast<const irs::byte_type*>("\xc2\xa2"), 2},
//      std::string(reinterpret_cast<const char*>("\xc2\xa1"),
//      2)));
//
//  std::string data = "quickbro";
//  for (size_t i = 0; i < 100000; ++i) {
//    data += "quickbro";;
//  }
//  //std::cerr << "Set debug breakpoint here";
//  for (size_t i = 0; i < 10; ++i) {
//    stream.reset(data);
//    while (stream.next()) {}
//  }
//  ASSERT_FALSE(stream.next());
//}

TEST(ngram_token_stream_test, test_load) {
  {
    std::string_view data("quick");
    auto stream = irs::analysis::NGramTokenizerBase::Make(
      irs::analysis::NGramTokenizerBase::Options{
        .min_gram = 5,
        .max_gram = 5,
        .preserve_original = false,
        .stream_bytes_type =
          irs::analysis::NGramTokenizerBase::InputType::Binary});

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* term = irs::get<irs::TermAttr>(*stream);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(5, offset->end);
    ASSERT_EQ("quick", irs::ViewCast<char>(term->value));
    ASSERT_EQ(1, inc->value);
    ASSERT_FALSE(stream->next());
  }

  {
    constexpr std::u8string_view kData = u8"\u00C0\u00C1\u00C2\u00C3\u00C4";
    const auto ref = irs::ViewCast<char>(kData);

    auto stream = irs::analysis::NGramTokenizerBase::Make(
      irs::analysis::NGramTokenizerBase::Options{
        .min_gram = 5,
        .max_gram = 5,
        .preserve_original = false,
        .stream_bytes_type =
          irs::analysis::NGramTokenizerBase::InputType::UTF8});

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(ref));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* term = irs::get<irs::TermAttr>(*stream);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(10, offset->end);
    ASSERT_EQ("\xc3\x80\xc3\x81\xc3\x82\xc3\x83\xc3\x84",
              irs::ViewCast<char>(term->value));
    ASSERT_EQ(1, inc->value);
    ASSERT_FALSE(stream->next());
  }
}
