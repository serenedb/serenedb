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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "tests_shared.hpp"

using namespace irs;

TEST(token_streams_tests, boolean_stream) {
  ASSERT_EQ(1, BooleanTokenizer::value_false().size());
  ASSERT_EQ(0, BooleanTokenizer::value_false().front());
  ASSERT_EQ(1, BooleanTokenizer::value_true().size());
  ASSERT_EQ(irs::byte_type(0xFF),
            irs::byte_type(BooleanTokenizer::value_true().front()));

  ASSERT_NE(BooleanTokenizer::value_false(), BooleanTokenizer::value_true());

  // 'false' stream
  {
    const auto expected =
      irs::ViewCast<irs::byte_type>(BooleanTokenizer::value_false());
    BooleanTokenizer stream;
    stream.reset(false);

    auto* inc = irs::get<IncAttr>(stream);
    ASSERT_FALSE(!inc);
    ASSERT_EQ(1, inc->value);
    auto* value = irs::get<TermAttr>(stream);
    ASSERT_FALSE(!value);
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(expected, value->value);
    ASSERT_FALSE(stream.next());

    /* reset stream */
    stream.reset(false);
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(expected, value->value);
    ASSERT_FALSE(stream.next());
  }

  // 'true' stream
  {
    auto expected =
      irs::ViewCast<irs::byte_type>(BooleanTokenizer::value_true());
    BooleanTokenizer stream;
    stream.reset(true);
    auto* inc = irs::get<IncAttr>(stream);
    ASSERT_FALSE(!inc);
    ASSERT_EQ(1, inc->value);
    auto* value = irs::get<TermAttr>(stream);
    ASSERT_FALSE(!value);
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(expected, value->value);
    ASSERT_FALSE(stream.next());

    /* reset stream */
    stream.reset(true);
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(expected, value->value);
    ASSERT_FALSE(stream.next());
  }
}

TEST(token_streams_tests, null_stream) {
  ASSERT_EQ(0, NullTokenizer::value_null().size());
  ASSERT_NE(nullptr, NullTokenizer::value_null().data());

  const auto expected = bytes_view{};
  NullTokenizer stream;
  auto* inc = irs::get<IncAttr>(stream);
  ASSERT_FALSE(!inc);
  ASSERT_EQ(1, inc->value);
  auto* value = irs::get<TermAttr>(stream);
  ASSERT_FALSE(!value);
  ASSERT_FALSE(stream.next());
  ASSERT_EQ(expected, value->value);
  ASSERT_FALSE(stream.next());

  /* reset stream */
  stream.reset();
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(expected, value->value);
  ASSERT_FALSE(stream.next());
}

TEST(string_token_stream_tests, next_end) {
  const std::string str("QBVnCx4NCizekHA");
  const bytes_view ref =
    bytes_view(reinterpret_cast<const byte_type*>(str.c_str()), str.size());
  StringTokenizer ts;
  ts.reset(str);

  // check attributes
  auto* term = irs::get<TermAttr>(ts);
  ASSERT_FALSE(!term);
  ASSERT_TRUE(IsNull(term->value));
  auto* offs = irs::get<OffsAttr>(ts);
  ASSERT_FALSE(!offs);
  ASSERT_EQ(0, offs->start);
  ASSERT_EQ(0, offs->end);

  ASSERT_TRUE(ts.next());
  ASSERT_EQ(ref, term->value);
  ASSERT_EQ(0, offs->start);
  ASSERT_EQ(ref.size(), offs->end);
  ASSERT_FALSE(ts.next());
  ASSERT_FALSE(ts.next());

  ts.reset(str);
  ASSERT_TRUE(ts.next());
  ASSERT_EQ(ref, term->value);
  ASSERT_EQ(0, offs->start);
  ASSERT_EQ(ref.size(), offs->end);

  ASSERT_FALSE(ts.next());
  ASSERT_FALSE(ts.next());
}

TEST(numeric_token_stream_tests, value) {
  // int
  {
    bstring buf;
    NumericTokenizer ts;
    auto* term = irs::get<TermAttr>(ts);
    ASSERT_FALSE(!term);
    ts.reset(35);

    auto value = NumericTokenizer::value(buf, 35);
    ASSERT_EQ(true, ts.next());
    ASSERT_EQ(term->value, value);  // value same as 1st
    ASSERT_EQ(true, ts.next());
    ASSERT_NE(term->value, value);  // value not same as 2nd
    ASSERT_EQ(true, !ts.next());
  }

  // long
  {
    bstring buf;
    NumericTokenizer ts;
    auto* term = irs::get<TermAttr>(ts);
    ASSERT_FALSE(!term);
    ts.reset(int64_t(75));

    auto value = NumericTokenizer::value(buf, int64_t(75));
    ASSERT_EQ(true, ts.next());
    ASSERT_EQ(term->value, value);  // value same as 1st
    ASSERT_EQ(true, ts.next());
    ASSERT_NE(term->value, value);  // value not same as 2nd
    ASSERT_EQ(true, ts.next());
    ASSERT_NE(term->value, value);  // value not same as 3rd
    ASSERT_EQ(true, ts.next());
    ASSERT_NE(term->value, value);  // value not same as 4th
    ASSERT_EQ(true, !ts.next());
  }

  // float
  {
    bstring buf;
    NumericTokenizer ts;
    auto* term = irs::get<TermAttr>(ts);
    ASSERT_FALSE(!term);
    ts.reset((float_t)35.f);

    auto value = NumericTokenizer::value(buf, (float_t)35.f);
    ASSERT_EQ(true, ts.next());
    ASSERT_EQ(term->value, value);  // value same as 1st
    ASSERT_EQ(true, ts.next());
    ASSERT_NE(term->value, value);  // value not same as 2nd
    ASSERT_EQ(true, !ts.next());
  }

  // double
  {
    bstring buf;
    NumericTokenizer ts;
    auto* term = irs::get<TermAttr>(ts);
    ASSERT_FALSE(!term);
    ts.reset((double_t)35.);

    auto value = NumericTokenizer::value(buf, (double_t)35.);
    ASSERT_EQ(true, ts.next());
    ASSERT_EQ(term->value, value);  // value same as 1st
    ASSERT_EQ(true, ts.next());
    ASSERT_NE(term->value, value);  // value not same as 2nd
    ASSERT_EQ(true, ts.next());
    ASSERT_NE(term->value, value);  // value not same as 3rd
    ASSERT_EQ(true, ts.next());
    ASSERT_NE(term->value, value);  // value not same as 4th
    ASSERT_EQ(true, !ts.next());
  }
}
