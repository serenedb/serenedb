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

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

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
      std::vector<irs::bstring> terms;
      std::vector<uint32_t> poss;
      std::vector<uint32_t> starts;
      std::vector<uint32_t> ends;
      const auto collect = [&](irs::TokenBatch& batch,
                               std::span<const irs::DocRun> runs) {
        ASSERT_FALSE(batch.dense_pos);
        ASSERT_TRUE(runs.empty());
        for (uint32_t i = 0; i < batch.count; ++i) {
          const auto& t = batch.terms[i];
          terms.emplace_back(
            reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
          poss.push_back(batch.pos[i]);
          starts.push_back(batch.offs_start[i]);
          ends.push_back(batch.offs_end[i]);
        }
      };
      tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
      ASSERT_TRUE(stream.Fill(data, sink.writer, sink.layout));
      sink.writer.Finish();
      ASSERT_EQ(expected.size(), terms.size());
      for (size_t k = 0; k < expected.size(); ++k) {
        const auto& e = expected[k];
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(e.value), terms[k]);
        ASSERT_EQ(e.start, starts[k]);
        ASSERT_EQ(e.end, ends[k]);
        const uint32_t pos = poss[k] - 1;
        auto start = reinterpret_cast<const irs::byte_type*>(data.data());
        utf8::unchecked::advance(start, pos);
        const auto size =
          terms[k].size() - e.start_marker.size() - e.end_marker.size();
        ASSERT_GT(size, 0);
        irs::bstring bs;
        if (!e.start_marker.empty()) {
          bs.append(
            reinterpret_cast<const irs::byte_type*>(e.start_marker.data()),
            e.start_marker.size());
        }
        bs.append(start, size);
        if (!e.end_marker.empty()) {
          bs.append(
            reinterpret_cast<const irs::byte_type*>(e.end_marker.data()),
            e.end_marker.size());
        }
        ASSERT_EQ(bs, terms[k]);
      }
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

    auto tokens = tests::Analyze(stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_TRUE(tokens->empty());
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

  ASSERT_FALSE(tests::Analyze(stream, input).has_value());
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
      std::vector<irs::bstring> terms;
      std::vector<uint32_t> poss;
      std::vector<uint32_t> starts;
      std::vector<uint32_t> ends;
      const auto collect = [&](irs::TokenBatch& batch,
                               std::span<const irs::DocRun> runs) {
        ASSERT_FALSE(batch.dense_pos);
        ASSERT_TRUE(runs.empty());
        for (uint32_t i = 0; i < batch.count; ++i) {
          const auto& t = batch.terms[i];
          terms.emplace_back(
            reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
          poss.push_back(batch.pos[i]);
          starts.push_back(batch.offs_start[i]);
          ends.push_back(batch.offs_end[i]);
        }
      };
      tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
      ASSERT_TRUE(stream.Fill(data, sink.writer, sink.layout));
      sink.writer.Finish();
      ASSERT_EQ(expected.size(), terms.size());
      for (size_t k = 0; k < expected.size(); ++k) {
        const auto& e = expected[k];
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(e.value), terms[k]);
        ASSERT_EQ(e.start, starts[k]);
        ASSERT_EQ(e.end, ends[k]);
        const uint32_t pos = poss[k] - 1;
        const auto size =
          terms[k].size() - e.start_marker.size() - e.end_marker.size();
        ASSERT_GT(size, 0);
        irs::bstring bs;
        if (!e.start_marker.empty()) {
          bs.append(
            reinterpret_cast<const irs::byte_type*>(e.start_marker.data()),
            e.start_marker.size());
        }
        bs.append(reinterpret_cast<const irs::byte_type*>(data.data()) + pos,
                  size);
        if (!e.end_marker.empty()) {
          bs.append(
            reinterpret_cast<const irs::byte_type*>(e.end_marker.data()),
            e.end_marker.size());
        }
        ASSERT_EQ(bs, terms[k]);
      }
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

    auto tokens = tests::Analyze(stream, "quick");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_TRUE(tokens->empty());
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
  for (size_t i = 0; i < 10000; ++i) {
    std::basic_stringstream<char> ss;
    ss << "test_" << i;
    const std::string value = ss.str();
    tests::OneBatchSink sink{irs::TokenLayout::TermsPos};
    ASSERT_TRUE(stream->Fill(value, sink.writer, sink.layout));
    ASSERT_FALSE(sink.flushed());
    auto& batch = sink.writer.buf;
    ASSERT_FALSE(batch.dense_pos);
    uint32_t last_pos = 0;
    for (uint32_t t = 0; t < batch.count; ++t) {
      ASSERT_GE(batch.pos[t], last_pos);
      ASSERT_LT(batch.pos[t], irs::pos_limits::eof());
      last_pos = batch.pos[t];
    }
    sink.writer.Discard();
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

    auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"quick", 1, 0, 5}), tokens->front());
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

    auto tokens = tests::Analyze(*stream, ref);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"\xc3\x80\xc3\x81\xc3\x82\xc3\x83\xc3\x84",
                                    1, 0, 10}),
              tokens->front());
  }
}

namespace {

struct PulledNgram {
  irs::bstring term;
  uint32_t pos;
  uint32_t start;
  uint32_t end;
};

template<irs::analysis::NGramTokenizerBase::InputType StreamType>
std::vector<PulledNgram> PullNgrams(
  irs::analysis::NGramTokenizerBase::Options opts, std::string_view data) {
  opts.stream_bytes_type = StreamType;
  irs::analysis::NGramTokenizer<StreamType> stream{std::move(opts)};
  std::vector<PulledNgram> out;
  auto tokens = tests::Analyze(stream, data);
  EXPECT_TRUE(tokens.has_value());
  if (!tokens) {
    return out;
  }
  for (auto& t : *tokens) {
    out.push_back(
      {irs::bstring{reinterpret_cast<const irs::byte_type*>(t.term.data()),
                    t.term.size()},
       t.pos, t.offs_start, t.offs_end});
  }
  return out;
}

template<irs::analysis::NGramTokenizerBase::InputType StreamType>
std::vector<PulledNgram> FillNgrams(
  irs::analysis::NGramTokenizerBase::Options opts, std::string_view data) {
  opts.stream_bytes_type = StreamType;
  irs::analysis::NGramTokenizer<StreamType> stream{std::move(opts)};
  std::vector<PulledNgram> out;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    EXPECT_FALSE(batch.dense_pos);
    EXPECT_TRUE(runs.empty());
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      out.push_back(
        {irs::bstring{reinterpret_cast<const irs::byte_type*>(t.GetData()),
                      t.GetSize()},
         batch.pos[i], batch.offs_start[i], batch.offs_end[i]});
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
  EXPECT_TRUE(stream.Fill(data, sink.writer, sink.layout));
  sink.writer.Finish();
  return out;
}

template<irs::analysis::NGramTokenizerBase::InputType StreamType>
void AssertNgramFillsMatchPull() {
  const std::string big(300, 'x');
  std::string mixed;
  for (size_t i = 0; i < 100; ++i) {
    mixed += "ab\xc2\xa2";
  }
  const std::vector<std::string> values = {"",
                                           "a",
                                           "ab",
                                           "quick brown fox",
                                           "a\xc2\xa2"
                                           "b\xc2\xa3"
                                           "c\xc2\xa4"
                                           "d\xc2\xa5",
                                           "abc\xc2",  // truncated utf8
                                           big,
                                           mixed,
                                           std::string(40, 'y')};

  for (const auto& [mn, mx] :
       std::vector<std::pair<size_t, size_t>>{{1, 1}, {2, 3}, {1, 4}, {3, 3}}) {
    for (const bool preserve : {false, true}) {
      for (const auto& [sm, em] :
           std::vector<std::pair<std::string, std::string>>{
             {"", ""}, {"<", ">"}, {"<", ""}, {"", ">"}}) {
        irs::analysis::NGramTokenizerBase::Options opts;
        opts.min_gram = mn;
        opts.max_gram = mx;
        opts.preserve_original = preserve;
        opts.start_marker = irs::bstring{
          reinterpret_cast<const irs::byte_type*>(sm.data()), sm.size()};
        opts.end_marker = irs::bstring{
          reinterpret_cast<const irs::byte_type*>(em.data()), em.size()};
        for (const auto& v : values) {
          SCOPED_TRACE(testing::Message()
                       << "min=" << mn << " max=" << mx << " po=" << preserve
                       << " sm=" << sm << " em=" << em
                       << " value.size=" << v.size());
          const auto pulled = PullNgrams<StreamType>(opts, v);
          const auto filled = FillNgrams<StreamType>(opts, v);
          ASSERT_EQ(pulled.size(), filled.size());
          for (size_t i = 0; i < pulled.size(); ++i) {
            SCOPED_TRACE(testing::Message() << "token=" << i);
            ASSERT_EQ(pulled[i].term, filled[i].term);
            ASSERT_EQ(pulled[i].pos, filled[i].pos);
            ASSERT_EQ(pulled[i].start, filled[i].start);
            ASSERT_EQ(pulled[i].end, filled[i].end);
          }
        }
      }
    }
  }
}

}  // namespace

TEST(ngram_token_stream_test, native_fills_match_pull_binary) {
  ASSERT_EQ(irs::TokenTraits::Terms::Ngrams,
            irs::analysis::NGramTokenizer<
              irs::analysis::NGramTokenizerBase::InputType::Binary>{
              irs::analysis::NGramTokenizerBase::Options{}}
              .Traits()
              .terms);
  AssertNgramFillsMatchPull<
    irs::analysis::NGramTokenizerBase::InputType::Binary>();
}

TEST(ngram_token_stream_test, native_fills_match_pull_utf8) {
  AssertNgramFillsMatchPull<
    irs::analysis::NGramTokenizerBase::InputType::UTF8>();
}

TEST(ngram_token_stream_test, column_fill_runs) {
  using Stream = irs::analysis::NGramTokenizer<
    irs::analysis::NGramTokenizerBase::InputType::UTF8>;
  irs::analysis::NGramTokenizerBase::Options opts;
  opts.min_gram = 2;
  opts.max_gram = 3;
  opts.preserve_original = true;
  opts.stream_bytes_type = irs::analysis::NGramTokenizerBase::InputType::UTF8;
  opts.start_marker =
    irs::bstring{reinterpret_cast<const irs::byte_type*>("<"), 1};
  opts.end_marker =
    irs::bstring{reinterpret_cast<const irs::byte_type*>(">"), 1};

  std::string big;
  for (size_t i = 0; i < 300; ++i) {
    big += "xy\xc2\xa2";
  }
  const std::vector<std::string> values = {"abc", "", big, "a", "tail value"};

  std::vector<std::vector<irs::bstring>> expected(values.size());
  for (size_t v = 0; v < values.size(); ++v) {
    auto opts_copy = opts;
    for (auto& t :
         FillNgrams<irs::analysis::NGramTokenizerBase::InputType::UTF8>(
           std::move(opts_copy), values[v])) {
      expected[v].push_back(std::move(t.term));
    }
  }

  Stream stream{std::move(opts)};
  std::vector<duckdb::string_t> vals;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(i + 1));
  }

  std::vector<std::vector<irs::bstring>> got(values.size());
  size_t flushes = 0;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    if (batch.count == irs::TokenBatch::kCapacity) {
      ++flushes;
    }
    uint32_t tok = 0;
    for (size_t r = 0; r < runs.size(); ++r) {
      const auto& run = runs[r];
      if (run.doc == irs::DocRun::kOpenValue) {
        ASSERT_EQ(runs.size() - 1, r);
        continue;
      }
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        got[run.doc - 1].emplace_back(
          reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
      }
    }
    ASSERT_EQ(batch.count, tok);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPos, collect};
  stream.Fill(vals, docs, sink.writer, sink.layout);
  sink.writer.Finish();

  ASSERT_GT(flushes, 0);
  ASSERT_EQ(expected, got);
}

TEST(ngram_token_stream_test, ascii_shortcut_matches_binary) {
  for (const auto& [mn, mx] :
       std::vector<std::pair<size_t, size_t>>{{1, 1}, {2, 3}, {3, 3}}) {
    for (const bool preserve : {false, true}) {
      irs::analysis::NGramTokenizerBase::Options opts;
      opts.min_gram = mn;
      opts.max_gram = mx;
      opts.preserve_original = preserve;
      opts.start_marker =
        irs::bstring{reinterpret_cast<const irs::byte_type*>("<"), 1};
      opts.end_marker =
        irs::bstring{reinterpret_cast<const irs::byte_type*>(">"), 1};
      for (const std::string_view v :
           {std::string_view{"quick brown fox"}, std::string_view{"a"},
            std::string_view{""}}) {
        const auto via_binary =
          FillNgrams<irs::analysis::NGramTokenizerBase::InputType::Binary>(opts,
                                                                           v);
        const auto via_utf8 =
          FillNgrams<irs::analysis::NGramTokenizerBase::InputType::UTF8>(opts,
                                                                         v);
        ASSERT_EQ(via_binary.size(), via_utf8.size());
        for (size_t i = 0; i < via_binary.size(); ++i) {
          ASSERT_EQ(via_binary[i].term, via_utf8[i].term);
          ASSERT_EQ(via_binary[i].pos, via_utf8[i].pos);
          ASSERT_EQ(via_binary[i].start, via_utf8[i].start);
          ASSERT_EQ(via_binary[i].end, via_utf8[i].end);
        }
      }
    }
  }
}

TEST(ngram_token_stream_test, utf8_block_boundary_symbols) {
  std::string data;
  for (size_t i = 0; i < 40; ++i) {
    data += "x\xc2\xa2";
    data += "\xe2\x82\xac";
    data += "\xf0\x9f\x98\x80";
  }
  for (const auto& [mn, mx] :
       std::vector<std::pair<size_t, size_t>>{{1, 2}, {3, 3}}) {
    irs::analysis::NGramTokenizerBase::Options opts;
    opts.min_gram = mn;
    opts.max_gram = mx;
    opts.preserve_original = false;
    const auto pulled =
      PullNgrams<irs::analysis::NGramTokenizerBase::InputType::UTF8>(opts,
                                                                     data);
    const auto filled =
      FillNgrams<irs::analysis::NGramTokenizerBase::InputType::UTF8>(opts,
                                                                     data);
    ASSERT_EQ(pulled.size(), filled.size());
    for (size_t i = 0; i < pulled.size(); ++i) {
      ASSERT_EQ(pulled[i].term, filled[i].term);
      ASSERT_EQ(pulled[i].pos, filled[i].pos);
      ASSERT_EQ(pulled[i].start, filled[i].start);
      ASSERT_EQ(pulled[i].end, filled[i].end);
    }
  }
}

TEST(ngram_token_stream_test, long_grams_out_of_line) {
  irs::analysis::NGramTokenizerBase::Options opts;
  opts.min_gram = 10;
  opts.max_gram = 16;
  opts.preserve_original = true;
  opts.start_marker =
    irs::bstring{reinterpret_cast<const irs::byte_type*>("<<"), 2};
  opts.end_marker =
    irs::bstring{reinterpret_cast<const irs::byte_type*>(">>"), 2};
  std::string data;
  for (size_t i = 0; i < 20; ++i) {
    data += "abcdefghij";
  }
  const auto pulled =
    PullNgrams<irs::analysis::NGramTokenizerBase::InputType::Binary>(opts,
                                                                     data);
  const auto filled =
    FillNgrams<irs::analysis::NGramTokenizerBase::InputType::Binary>(opts,
                                                                     data);
  ASSERT_EQ(pulled.size(), filled.size());
  bool saw_out_of_line = false;
  for (size_t i = 0; i < pulled.size(); ++i) {
    ASSERT_EQ(pulled[i].term, filled[i].term);
    ASSERT_EQ(pulled[i].pos, filled[i].pos);
    ASSERT_EQ(pulled[i].start, filled[i].start);
    ASSERT_EQ(pulled[i].end, filled[i].end);
    saw_out_of_line |= filled[i].term.size() > 12;
  }
  ASSERT_TRUE(saw_out_of_line);
}

TEST(ngram_token_stream_test, column_flush_structure) {
  using Stream = irs::analysis::NGramTokenizer<
    irs::analysis::NGramTokenizerBase::InputType::Binary>;
  irs::analysis::NGramTokenizerBase::Options opts;
  opts.min_gram = 1;
  opts.max_gram = 2;
  opts.preserve_original = false;

  std::vector<std::string> values = {std::string(300, 'a'), "bc",
                                     std::string(1200, 'd'), "e"};
  struct Tok {
    irs::bstring term;
    uint32_t pos;
    uint32_t start;
    uint32_t end;
    bool operator==(const Tok&) const = default;
  };
  std::vector<std::vector<Tok>> expected(values.size());
  for (size_t v = 0; v < values.size(); ++v) {
    for (auto& t :
         FillNgrams<irs::analysis::NGramTokenizerBase::InputType::Binary>(
           opts, values[v])) {
      expected[v].push_back({std::move(t.term), t.pos, t.start, t.end});
    }
  }

  Stream stream{std::move(opts)};
  std::vector<duckdb::string_t> vals;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(i + 1));
  }

  std::vector<std::vector<Tok>> got(values.size());
  size_t flushes = 0;
  const auto drain = [&](irs::TokenBatch& batch,
                         std::span<const irs::DocRun> runs) {
    const bool at_flush = batch.count == irs::TokenBatch::kCapacity;
    if (at_flush) {
      ++flushes;
    }
    ASSERT_FALSE(runs.empty());
    const bool open = runs.back().doc == irs::DocRun::kOpenValue;
    ASSERT_TRUE(!open || at_flush);
    uint32_t tok = 0;
    for (size_t r = 0; r < runs.size(); ++r) {
      const auto& run = runs[r];
      if (run.doc == irs::DocRun::kOpenValue) {
        ASSERT_EQ(runs.size() - 1, r);
        continue;
      }
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        got[run.doc - 1].push_back(
          {irs::bstring{reinterpret_cast<const irs::byte_type*>(t.GetData()),
                        t.GetSize()},
           batch.pos[tok], batch.offs_start[tok], batch.offs_end[tok]});
      }
    }
    ASSERT_EQ(batch.count, tok);
    const auto& last = open ? runs[runs.size() - 2] : runs.back();
    const bool mid_value =
      got[last.doc - 1].size() < expected[last.doc - 1].size();
    ASSERT_EQ(mid_value, open);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, drain};
  stream.Fill(vals, docs, sink.writer, sink.layout);
  sink.writer.Finish();

  size_t total = 0;
  for (const auto& e : expected) {
    total += e.size();
  }
  ASSERT_EQ(total / irs::TokenBatch::kCapacity, flushes);
  ASSERT_EQ(expected.size(), got.size());
  for (size_t v = 0; v < values.size(); ++v) {
    SCOPED_TRACE(testing::Message() << "doc=" << v + 1);
    ASSERT_EQ(expected[v], got[v]);
  }
}
