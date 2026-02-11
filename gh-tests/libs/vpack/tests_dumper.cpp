////////////////////////////////////////////////////////////////////////////////
/// @brief Library to build up VPack documents.
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
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
/// @author Max Neunhoeffer
/// @author Jan Steemann
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/strings/cord.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>

#include "basics/sink.h"
#include "tests-common.h"

namespace {

static unsigned char gLocalBuffer[4096];

using sdb::basics::CordSink;
using sdb::basics::LenSink;
using sdb::basics::StrSink;

TEST(DumperTest, CreateWithoutOptions) {
  ASSERT_VPACK_EXCEPTION(new Dumper<StrSink>(nullptr),
                         Exception::kInternalError);

  StrSink sink;
  ASSERT_VPACK_EXCEPTION(new Dumper(&sink, nullptr), Exception::kInternalError);

  ASSERT_VPACK_EXCEPTION(new Dumper<StrSink>(nullptr, nullptr),
                         Exception::kInternalError);
}

TEST(DumperTest, InvokeOnSlice) {
  gLocalBuffer[0] = 0x18;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("null"), buffer);
}

TEST(DumperTest, InvokeOnSlicePointer) {
  gLocalBuffer[0] = 0x18;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("null"), buffer);
}

TEST(SinkTest, CharBufferAppenders) {
  StrSink sink;
  auto& buffer = sink.Impl();
  sink.PushChr('1');
  ASSERT_EQ(1UL, buffer.length());
  ASSERT_EQ(0, memcmp("1", buffer.data(), buffer.length()));

  sink.PushStr(std::string("abcdef"));
  ASSERT_EQ(7UL, buffer.length());
  ASSERT_EQ(0, memcmp("1abcdef", buffer.data(), buffer.length()));

  sink.PushStr("foobar");
  ASSERT_EQ(13UL, buffer.length());
  ASSERT_EQ(0, memcmp("1abcdeffoobar", buffer.data(), buffer.length()));

  sink.PushStr("quetzalcoatl");
  ASSERT_EQ(25UL, buffer.length());
  ASSERT_EQ(
    0, memcmp("1abcdeffoobarquetzalcoatl", buffer.data(), buffer.length()));

  sink.PushChr('*');
  ASSERT_EQ(26UL, buffer.length());
  ASSERT_EQ(
    0, memcmp("1abcdeffoobarquetzalcoatl*", buffer.data(), buffer.length()));
}

TEST(SinkTest, StringAppenders) {
  StrSink sink;
  auto& buffer = sink.Impl();
  sink.PushChr('1');
  ASSERT_EQ("1", buffer);

  sink.PushStr(std::string("abcdef"));
  ASSERT_EQ("1abcdef", buffer);

  sink.PushStr("foobar");
  ASSERT_EQ("1abcdeffoobar", buffer);

  sink.PushStr("quetzalcoatl");
  ASSERT_EQ("1abcdeffoobarquetzalcoatl", buffer);

  sink.PushChr('*');
  ASSERT_EQ("1abcdeffoobarquetzalcoatl*", buffer);
}

TEST(SinkTest, OStreamAppenders) {
  StrSink sink;
  auto& result = sink.Impl();
  sink.PushChr('1');
  ASSERT_EQ("1", result);

  sink.PushStr(std::string("abcdef"));
  ASSERT_EQ("1abcdef", result);

  sink.PushStr("foobar");
  ASSERT_EQ("1abcdeffoobar", result);

  sink.PushStr("quetzalcoatl");
  ASSERT_EQ("1abcdeffoobarquetzalcoatl", result);

  sink.PushChr('*');
  ASSERT_EQ("1abcdeffoobarquetzalcoatl*", result);
}

TEST(OutStreamTest, StringifyComplexObject) {
  const std::string value(
    "{\"foo\":\"bar\",\"baz\":[1,2,3,[4]],\"bark\":[{\"troet\\nmann\":1,"
    "\"mötör\":[2,3.4,-42.5,true,false,null,\"some\\nstring\"]}]}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  std::ostringstream result;
  result << s;

  ASSERT_EQ("[Slice object (0x0b), byteSize: 107]", result.str());

  Options dumper_options;
  dumper_options.pretty_print = true;
  std::string pretty_result = s.toJson(&dumper_options);
  ASSERT_EQ(
    std::string(
      "{\n  \"bark\" : [\n    {\n      \"mötör\" : [\n        2,\n        "
      "3.4,\n        -42.5,\n        true,\n        false,\n        null,\n"
      "        \"some\\nstring\"\n      ],\n      \"troet\\nmann\" : 1\n   "
      " "
      "}\n  ],"
      "\n  \"baz\" : [\n    1,\n    2,\n    3,\n    [\n      4\n    ]\n  ],"
      "\n  \"foo\" : \"bar\"\n}"),
    pretty_result);
}

TEST(PrettyDumperTest, SimpleObject) {
  const std::string value("{\"foo\":\"bar\"}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  std::ostringstream result;
  result << s;

  ASSERT_EQ("[Slice object (0x14), byteSize: 11]", result.str());

  Options dumper_options;
  dumper_options.pretty_print = true;
  std::string pretty_result = s.toJson(&dumper_options);
  ASSERT_EQ(std::string("{\n  \"foo\" : \"bar\"\n}"), pretty_result);
}

TEST(PrettyDumperTest, ComplexObject) {
  const std::string value(
    "{\"foo\":\"bar\",\"baz\":[1,2,3,[4]],\"bark\":[{\"troet\\nmann\":1,"
    "\"mötör\":[2,3.4,-42.5,true,false,null,\"some\\nstring\"]}]}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options dumper_options;
  dumper_options.pretty_print = true;
  std::string result = s.toJson(&dumper_options);
  ASSERT_EQ(
    std::string(
      "{\n  \"bark\" : [\n    {\n      \"mötör\" : [\n"
      "        2,\n        3.4,\n        -42.5,\n        true,\n        "
      "false,\n        null,\n        \"some\\nstring\"\n      ],\n      "
      "\"troet\\nmann\" : 1\n    }\n  ],\n  \"baz\" : [\n    1,\n    "
      "2,\n    3,\n    [\n      4\n    ]\n  ],\n  \"foo\" : \"bar\"\n}"),
    result);
}

TEST(PrettyDumperTest, ComplexObjectSingleLine) {
  const std::string value(
    "{\"foo\":\"bar\",\"baz\":[1,2,3,[4]],\"bark\":[{\"troet\\nmann\":1,"
    "\"mötör\":[2,3.4,-42.5,true,false,null,\"some\\nstring\"]}]}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options dumper_options;
  dumper_options.single_line_pretty_print = true;
  std::string result = s.toJson(&dumper_options);
  ASSERT_EQ(
    std::string(
      "{\"bark\": [{\"mötör\": [2, 3.4, -42.5, true, false, "
      "null, \"some\\nstring\"], \"troet\\nmann\": 1}], \"baz\": [1, 2, 3,"
      " [4]], \"foo\": \"bar\"}"),
    result);
}

TEST(StreamDumperTest, SimpleObject) {
  const std::string value("{\"foo\":\"bar\"}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = true;
  StrSink sink;
  auto& result = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(std::string("{\n  \"foo\" : \"bar\"\n}"), result);
}

TEST(StreamDumperTest, UseStringStreamTypedef) {
  const std::string value("{\"foo\":\"bar\"}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = true;
  StrSink sink;
  auto& result = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(std::string("{\n  \"foo\" : \"bar\"\n}"), result);
}

TEST(StreamDumperTest, DumpAttributesInIndexOrder) {
  const std::string value(
    "{\"foo\":\"bar\",\"baz\":[1,2,3,[4]],\"bark\":[{\"troet\\nmann\":1,"
    "\"mötör\":[2,3.4,-42.5,true,false,null,\"some\\nstring\"]}]}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options dumper_options;
  dumper_options.dump_attributes_in_index_order = true;
  dumper_options.pretty_print = false;
  StrSink sink;
  auto& result = sink.Impl();
  Dumper dumper(&sink, &dumper_options);
  dumper.Dump(s);
  ASSERT_EQ(std::string("{\"bark\":[{\"m\xC3\xB6t\xC3\xB6r\":[2,3.4,-42.5,true,"
                        "false,null,\"some\\nstring\"],\"troet\\nmann\":1}],"
                        "\"baz\":[1,2,3,[4]],\"foo\":\"bar\"}"),
            result);
}

TEST(StreamDumperTest, DontDumpAttributesInIndexOrder) {
  const std::string value(
    "{\"foo\":\"bar\",\"baz\":[1,2,3,[4]],\"bark\":[{\"troet\\nmann\":1,"
    "\"mötör\":[2,3.4,-42.5,true,false,null,\"some\\nstring\"]}]}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options dumper_options;
  dumper_options.dump_attributes_in_index_order = false;
  dumper_options.pretty_print = false;
  StrSink sink;
  auto& result = sink.Impl();
  Dumper dumper(&sink, &dumper_options);
  dumper.Dump(s);
  ASSERT_EQ(
    std::string(
      "{\"foo\":\"bar\",\"baz\":[1,2,3,[4]],\"bark\":[{\"troet\\nmann\":1,"
      "\"m\xC3\xB6t\xC3\xB6r\":[2,3.4,-42.5,true,false,null,"
      "\"some\\nstring\"]}]}"),
    result);
}

TEST(StreamDumperTest, ComplexObject) {
  const std::string value(
    "{\"foo\":\"bar\",\"baz\":[1,2,3,[4]],\"bark\":[{\"troet\\nmann\":1,"
    "\"mötör\":[2,3.4,-42.5,true,false,null,\"some\\nstring\"]}]}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options dumper_options;
  dumper_options.pretty_print = true;
  StrSink sink;
  auto& result = sink.Impl();
  Dumper dumper(&sink, &dumper_options);
  dumper.Dump(s);
  ASSERT_EQ(
    std::string(
      "{\n  \"bark\" : [\n    {\n      \"m\xC3\xB6t\xC3\xB6r"
      "\" : [\n        2,\n        3.4,\n        -42.5,\n        true,"
      "\n        false,\n        null,\n        \"some\\nstring\"\n      ],"
      "\n      \"troet\\nmann\" : 1\n    }\n  ],\n  \"baz\" : [\n    1,\n  "
      "  "
      "2,\n    3,\n    [\n      4\n    ]\n  ],\n  \"foo\" : \"bar\"\n}"),
    result);
}

TEST(BufferDumperTest, Null) {
  gLocalBuffer[0] = 0x18;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("null"), std::string(buffer.data(), buffer.size()));
}

TEST(StringDumperTest, Null) {
  gLocalBuffer[0] = 0x18;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("null"), buffer);
}

TEST(StringDumperTest, Numbers) {
  int64_t pp = 2;
  for (int p = 1; p <= 61; p++) {
    int64_t i;

    auto check = [&]() -> void {
      Builder b;
      b.add(i);
      Slice s(b.start());

      StrSink sink;
      auto& buffer = sink.Impl();
      Dumper dumper(&sink);
      dumper.Dump(s);
      ASSERT_EQ(std::to_string(i), buffer);
    };

    i = pp;
    check();
    i = pp + 1;
    check();
    i = pp - 1;
    check();
    i = -pp;
    check();
    i = -pp + 1;
    check();
    i = -pp - 1;
    check();

    pp *= 2;
  }
}

TEST(BufferDumperTest, False) {
  gLocalBuffer[0] = 0x19;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("false"), std::string(buffer.data(), buffer.size()));
}

TEST(StringDumperTest, False) {
  gLocalBuffer[0] = 0x19;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("false"), buffer);
}

TEST(BufferDumperTest, True) {
  gLocalBuffer[0] = 0x1a;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("true"), std::string(buffer.data(), buffer.size()));
}

TEST(StringDumperTest, True) {
  gLocalBuffer[0] = 0x1a;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("true"), buffer);
}

TEST(StringDumperTest, StringSimple) {
  Builder b;
  b.add("foobar");

  Slice slice = b.slice();
  ASSERT_EQ(std::string("\"foobar\""), slice.toJson());
}

TEST(StringDumperTest, StringSpecialChars) {
  Builder b;
  b.add("\"fo\r \n \\to''\\ \\bar\"");

  Slice slice = b.slice();
  ASSERT_EQ(std::string("\"\\\"fo\\r \\n \\\\to''\\\\ \\\\bar\\\"\""),
            slice.toJson());
}

TEST(StringDumperTest, StringControlChars) {
  Builder b;
  b.add(std::string("\x00\x01\x02 baz \x03", 9));

  Slice slice = b.slice();
  ASSERT_EQ(std::string("\"\\u0000\\u0001\\u0002 baz \\u0003\""),
            slice.toJson());
}

TEST(StringDumperTest, SuppressControlChars) {
  Builder b;
  b.add("Before\nAfter\r\t\v\f\x01\x02/\u00B0\uf0f9\u9095\uf0f9\u90b6\b\n\\\"");

  Options options;
  options.escape_control = false;
  ASSERT_EQ(
    std::string(
      "\"Before After      \\/\u00B0\uf0f9\u9095\uf0f9\u90b6  \\\\\\\"\""),
    b.slice().toJson(&options));
}

TEST(StringDumperTest, EscapeControlChars) {
  Builder b;
  b.add(
    "Before\nAfter\r\t\v\f\b\x01\x02/"
    "\u00B0\uf0f9\u9095\uf0f9\u90b6\v\n\\\"");
  Options options;
  options.escape_control = true;
  ASSERT_EQ(std::string("\"Before\\nAfter\\r\\t\\u000B\\f\\b\\u0001\\u0002/"
                        "\u00B0\uf0f9\u9095\uf0f9\u90b6\\u000B\\n\\\\\\\"\""),
            b.slice().toJson(&options));
}

TEST(StringDumperTest, StringUTF8) {
  Builder b;
  b.add("mötör");

  Slice slice = b.slice();
  ASSERT_EQ(std::string("\"mötör\""), slice.toJson());
}

TEST(StringDumperTest, StringUTF8Escaped) {
  Builder b;
  b.add("mötör");

  Options options;
  options.escape_unicode = true;
  ASSERT_EQ(std::string("\"m\\u00F6t\\u00F6r\""), b.slice().toJson(&options));
}

TEST(StringDumperTest, StringTwoByteUTF8) {
  Builder b;
  b.add("\xc2\xa2");

  Slice slice = b.slice();
  ASSERT_EQ(std::string("\"\xc2\xa2\""), slice.toJson());
}

TEST(StringDumperTest, StringTwoByteUTF8Escaped) {
  Builder b;
  b.add("\xc2\xa2");

  Options options;
  options.escape_unicode = true;
  ASSERT_EQ(std::string("\"\\u00A2\""), b.slice().toJson(&options));
}

TEST(StringDumperTest, StringThreeByteUTF8) {
  Builder b;
  b.add("\xe2\x82\xac");

  Slice slice = b.slice();
  ASSERT_EQ(std::string("\"\xe2\x82\xac\""), slice.toJson());
}

TEST(StringDumperTest, StringThreeByteUTF8Escaped) {
  Builder b;
  b.add("\xe2\x82\xac");

  Options options;
  options.escape_unicode = true;
  ASSERT_EQ(std::string("\"\\u20AC\""), b.slice().toJson(&options));
}

TEST(StringDumperTest, StringFourByteUTF8) {
  Builder b;
  b.add("\xf0\xa4\xad\xa2");

  Slice slice = b.slice();
  ASSERT_EQ(std::string("\"\xf0\xa4\xad\xa2\""), slice.toJson());
}

TEST(StringDumperTest, StringFourByteUTF8Escaped) {
  Builder b;
  b.add("\xf0\xa4\xad\xa2");

  Options options;
  options.escape_unicode = true;
  ASSERT_EQ(std::string("\"\\uD852\\uDF62\""), b.slice().toJson(&options));
}

TEST(StringDumperTest, StringMultibytes) {
  std::vector<std::string> expected;
  expected.emplace_back(
    "Lorem ipsum dolor sit amet, te enim mandamus consequat ius, cu eos "
    "timeam bonorum, in nec eruditi tibique. At nec malorum saperet vivendo. "
    "Qui delectus moderatius in. Vivendo expetendis ullamcorper ut mei.");
  expected.emplace_back(
    "Мёнём пауло пытынтёюм ад ыам. Но эрож рыпудяары вим, пожтэа эюрйпйдяч "
    "ентырпрытаряш ад хёз. Мыа дектаж дёжкэрэ котёдиэквюэ ан. Ведят брутэ "
    "мэдиокретатым йн прё");
  expected.emplace_back(
    "Μει ει παρτεμ μολλις δελισατα, σιφιβυς σονσυλατυ ραθιονιβυς συ φις, "
    "φερι μυνερε μεα ετ. Ειρμωδ απεριρι δισενθιετ εα υσυ, κυο θωτα φευγαιθ "
    "δισενθιετ νο");
  expected.emplace_back(
    "供覧必責同界要努新少時購止上際英連動信。同売宗島載団報音改浅研壊趣全。並"
    "嗅整日放横旅関書文転方。天名他賞川日拠隊散境行尚島自模交最葉駒到");
  expected.emplace_back(
    "舞ばい支小ぜ館応ヌエマ得6備ルあ煮社義ゃフおづ報載通ナチセ東帯あスフず案"
    "務革た証急をのだ毎点十はぞじド。1芸キテ成新53験モワサセ断団ニカ働給相づ"
    "らべさ境著ラさ映権護ミオヲ但半モ橋同タ価法ナカネ仙説時オコワ気社オ");
  expected.emplace_back(
    "أي جنوب بداية السبب بلا. تمهيد التكاليف العمليات إذ دول, عن كلّ أراضي "
    "اعتداء, بال الأوروبي الإقتصادية و. دخول تحرّكت بـ حين. أي شاسعة لليابان "
    "استطاعوا مكن. الأخذ الصينية والنرويج هو أخذ.");
  expected.emplace_back(
    "זכר דפים בדפים מה, צילום מדינות היא או, ארץ צרפתית העברית אירועים ב. "
    "שונה קולנוע מתן אם, את אחד הארץ ציור וכמקובל. ויש העיר שימושי מדויקים "
    "בה, היא ויקי ברוכים תאולוגיה או. את זכר קהילה חבריכם ליצירתה, ערכים "
    "התפתחות חפש גם.");

  for (auto& it : expected) {
    Builder b;
    b.add(it);

    std::string dumped = b.slice().toJson();
    ASSERT_EQ(std::string("\"") + it + "\"", dumped);

    Parser parser;
    parser.parse(dumped);

    std::shared_ptr<Builder> builder = parser.steal();
    Slice s(builder->start());
    ASSERT_EQ(it, s.copyString());
  }
}

TEST(StringDumperTest, StringMultibytesEscaped) {
  std::vector<std::pair<std::string, std::string>> expected;
  expected.emplace_back(std::make_pair(
    "Мёнём пауло пытынтёюм ад ыам. Но эрож рыпудяары вим, пожтэа эюрйпйдяч "
    "ентырпрытаряш ад хёз. Мыа дектаж дёжкэрэ котёдиэквюэ ан. Ведят брутэ "
    "мэдиокретатым йн прё",
    "\\u041C\\u0451\\u043D\\u0451\\u043C \\u043F\\u0430\\u0443\\u043B\\u043E "
    "\\u043F\\u044B\\u0442\\u044B\\u043D\\u0442\\u0451\\u044E\\u043C "
    "\\u0430\\u0434 \\u044B\\u0430\\u043C. \\u041D\\u043E "
    "\\u044D\\u0440\\u043E\\u0436 "
    "\\u0440\\u044B\\u043F\\u0443\\u0434\\u044F\\u0430\\u0440\\u044B "
    "\\u0432\\u0438\\u043C, \\u043F\\u043E\\u0436\\u0442\\u044D\\u0430 "
    "\\u044D\\u044E\\u0440\\u0439\\u043F\\u0439\\u0434\\u044F\\u0447 "
    "\\u0435\\u043D\\u0442\\u044B\\u0440\\u043F\\u0440\\u044B\\u0442\\u0430\\"
    "u0440\\u044F\\u0448 \\u0430\\u0434 \\u0445\\u0451\\u0437. "
    "\\u041C\\u044B\\u0430 \\u0434\\u0435\\u043A\\u0442\\u0430\\u0436 "
    "\\u0434\\u0451\\u0436\\u043A\\u044D\\u0440\\u044D "
    "\\u043A\\u043E\\u0442\\u0451\\u0434\\u0438\\u044D\\u043A\\u0432\\u044E\\"
    "u044D \\u0430\\u043D. \\u0412\\u0435\\u0434\\u044F\\u0442 "
    "\\u0431\\u0440\\u0443\\u0442\\u044D "
    "\\u043C\\u044D\\u0434\\u0438\\u043E\\u043A\\u0440\\u0435\\u0442\\u0430\\"
    "u0442\\u044B\\u043C \\u0439\\u043D \\u043F\\u0440\\u0451"));
  expected.emplace_back(std::make_pair(
    "Μει ει παρτεμ μολλις δελισατα, σιφιβυς σονσυλατυ ραθιονιβυς συ φις, "
    "φερι μυνερε μεα ετ. Ειρμωδ απεριρι δισενθιετ εα υσυ, κυο θωτα φευγαιθ "
    "δισενθιετ νο",
    "\\u039C\\u03B5\\u03B9 \\u03B5\\u03B9 "
    "\\u03C0\\u03B1\\u03C1\\u03C4\\u03B5\\u03BC "
    "\\u03BC\\u03BF\\u03BB\\u03BB\\u03B9\\u03C2 "
    "\\u03B4\\u03B5\\u03BB\\u03B9\\u03C3\\u03B1\\u03C4\\u03B1, "
    "\\u03C3\\u03B9\\u03C6\\u03B9\\u03B2\\u03C5\\u03C2 "
    "\\u03C3\\u03BF\\u03BD\\u03C3\\u03C5\\u03BB\\u03B1\\u03C4\\u03C5 "
    "\\u03C1\\u03B1\\u03B8\\u03B9\\u03BF\\u03BD\\u03B9\\u03B2\\u03C5\\u03C2 "
    "\\u03C3\\u03C5 \\u03C6\\u03B9\\u03C2, \\u03C6\\u03B5\\u03C1\\u03B9 "
    "\\u03BC\\u03C5\\u03BD\\u03B5\\u03C1\\u03B5 \\u03BC\\u03B5\\u03B1 "
    "\\u03B5\\u03C4. \\u0395\\u03B9\\u03C1\\u03BC\\u03C9\\u03B4 "
    "\\u03B1\\u03C0\\u03B5\\u03C1\\u03B9\\u03C1\\u03B9 "
    "\\u03B4\\u03B9\\u03C3\\u03B5\\u03BD\\u03B8\\u03B9\\u03B5\\u03C4 "
    "\\u03B5\\u03B1 \\u03C5\\u03C3\\u03C5, \\u03BA\\u03C5\\u03BF "
    "\\u03B8\\u03C9\\u03C4\\u03B1 "
    "\\u03C6\\u03B5\\u03C5\\u03B3\\u03B1\\u03B9\\u03B8 "
    "\\u03B4\\u03B9\\u03C3\\u03B5\\u03BD\\u03B8\\u03B9\\u03B5\\u03C4 "
    "\\u03BD\\u03BF"));
  expected.emplace_back(std::make_pair(
    "供覧必責同界要努新少時購止上際英連動信。同売宗島載団報音改浅研壊趣全。並"
    "嗅整日放横旅関書文転方。天名他賞川日拠隊散境行尚島自模交最葉駒到",
    "\\u4F9B\\u89A7\\u5FC5\\u8CAC\\u540C\\u754C\\u8981\\u52AA\\u65B0\\u5C11\\"
    "u6642\\u8CFC\\u6B62\\u4E0A\\u969B\\u82F1\\u9023\\u52D5\\u4FE1\\u3002\\u5"
    "40C\\u58F2\\u5B97\\u5CF6\\u8F09\\u56E3\\u5831\\u97F3\\u6539\\u6D45\\u781"
    "4\\u58CA\\u8DA3\\u5168\\u3002\\u4E26\\u55C5\\u6574\\u65E5\\u653E\\u6A2A"
    "\\u65C5\\u95A2\\u66F8\\u6587\\u8EE2\\u65B9\\u3002\\u5929\\u540D\\u4ED6\\"
    "u8CDE\\u5DDD\\u65E5\\u62E0\\u968A\\u6563\\u5883\\u884C\\u5C1A\\u5CF6\\u8"
    "1EA\\u6A21\\u4EA4\\u6700\\u8449\\u99D2\\u5230"));
  expected.emplace_back(std::make_pair(
    "舞ばい支小ぜ館応ヌエマ得6備ルあ煮社義ゃフおづ報載通ナチセ東帯あスフず案"
    "務革た証急をのだ毎点十はぞじド。1芸キテ成新53験モワサセ断団ニカ働給相づ"
    "らべさ境著ラさ映権護ミオヲ但半モ橋同タ価法ナカネ仙説時オコワ気社オ",
    "\\u821E\\u3070\\u3044\\u652F\\u5C0F\\u305C\\u9928\\u5FDC\\u30CC\\u30A8\\"
    "u30DE\\u5F976\\u5099\\u30EB\\u3042\\u716E\\u793E\\u7FA9\\u3083\\u30D5\\u"
    "304A\\u3065\\u5831\\u8F09\\u901A\\u30CA\\u30C1\\u30BB\\u6771\\u5E2F\\u30"
    "42\\u30B9\\u30D5\\u305A\\u6848\\u52D9\\u9769\\u305F\\u8A3C\\u6025\\u3092"
    "\\u306E\\u3060\\u6BCE\\u70B9\\u5341\\u306F\\u305E\\u3058\\u30C9\\u30021"
    "\\u82B8\\u30AD\\u30C6\\u6210\\u65B053\\u9A13\\u30E2\\u30EF\\u30B5\\u30BB"
    "\\u65AD\\u56E3\\u30CB\\u30AB\\u50CD\\u7D66\\u76F8\\u3065\\u3089\\u3079\\"
    "u3055\\u5883\\u8457\\u30E9\\u3055\\u6620\\u6A29\\u8B77\\u30DF\\u30AA\\u3"
    "0F2\\u4F46\\u534A\\u30E2\\u6A4B\\u540C\\u30BF\\u4FA1\\u6CD5\\u30CA\\u30A"
    "B\\u30CD\\u4ED9\\u8AAC\\u6642\\u30AA\\u30B3\\u30EF\\u6C17\\u793E\\u30A"
    "A"));
  expected.emplace_back(std::make_pair(
    "أي جنوب بداية السبب بلا. تمهيد التكاليف العمليات إذ دول, عن كلّ أراضي "
    "اعتداء, بال الأوروبي الإقتصادية و. دخول تحرّكت بـ حين. أي شاسعة لليابان "
    "استطاعوا مكن. الأخذ الصينية والنرويج هو أخذ.",
    "\\u0623\\u064A \\u062C\\u0646\\u0648\\u0628 "
    "\\u0628\\u062F\\u0627\\u064A\\u0629 \\u0627\\u0644\\u0633\\u0628\\u0628 "
    "\\u0628\\u0644\\u0627. \\u062A\\u0645\\u0647\\u064A\\u062F "
    "\\u0627\\u0644\\u062A\\u0643\\u0627\\u0644\\u064A\\u0641 "
    "\\u0627\\u0644\\u0639\\u0645\\u0644\\u064A\\u0627\\u062A \\u0625\\u0630 "
    "\\u062F\\u0648\\u0644, \\u0639\\u0646 \\u0643\\u0644\\u0651 "
    "\\u0623\\u0631\\u0627\\u0636\\u064A "
    "\\u0627\\u0639\\u062A\\u062F\\u0627\\u0621, \\u0628\\u0627\\u0644 "
    "\\u0627\\u0644\\u0623\\u0648\\u0631\\u0648\\u0628\\u064A "
    "\\u0627\\u0644\\u0625\\u0642\\u062A\\u0635\\u0627\\u062F\\u064A\\u0629 "
    "\\u0648. \\u062F\\u062E\\u0648\\u0644 "
    "\\u062A\\u062D\\u0631\\u0651\\u0643\\u062A \\u0628\\u0640 "
    "\\u062D\\u064A\\u0646. \\u0623\\u064A "
    "\\u0634\\u0627\\u0633\\u0639\\u0629 "
    "\\u0644\\u0644\\u064A\\u0627\\u0628\\u0627\\u0646 "
    "\\u0627\\u0633\\u062A\\u0637\\u0627\\u0639\\u0648\\u0627 "
    "\\u0645\\u0643\\u0646. \\u0627\\u0644\\u0623\\u062E\\u0630 "
    "\\u0627\\u0644\\u0635\\u064A\\u0646\\u064A\\u0629 "
    "\\u0648\\u0627\\u0644\\u0646\\u0631\\u0648\\u064A\\u062C \\u0647\\u0648 "
    "\\u0623\\u062E\\u0630."));
  expected.emplace_back(std::make_pair(
    "זכר דפים בדפים מה, צילום מדינות היא או, ארץ צרפתית העברית אירועים ב. "
    "שונה קולנוע מתן אם, את אחד הארץ ציור וכמקובל. ויש העיר שימושי מדויקים "
    "בה, היא ויקי ברוכים תאולוגיה או. את זכר קהילה חבריכם ליצירתה, ערכים "
    "התפתחות חפש גם.",
    "\\u05D6\\u05DB\\u05E8 \\u05D3\\u05E4\\u05D9\\u05DD "
    "\\u05D1\\u05D3\\u05E4\\u05D9\\u05DD \\u05DE\\u05D4, "
    "\\u05E6\\u05D9\\u05DC\\u05D5\\u05DD "
    "\\u05DE\\u05D3\\u05D9\\u05E0\\u05D5\\u05EA \\u05D4\\u05D9\\u05D0 "
    "\\u05D0\\u05D5, \\u05D0\\u05E8\\u05E5 "
    "\\u05E6\\u05E8\\u05E4\\u05EA\\u05D9\\u05EA "
    "\\u05D4\\u05E2\\u05D1\\u05E8\\u05D9\\u05EA "
    "\\u05D0\\u05D9\\u05E8\\u05D5\\u05E2\\u05D9\\u05DD \\u05D1. "
    "\\u05E9\\u05D5\\u05E0\\u05D4 \\u05E7\\u05D5\\u05DC\\u05E0\\u05D5\\u05E2 "
    "\\u05DE\\u05EA\\u05DF \\u05D0\\u05DD, \\u05D0\\u05EA "
    "\\u05D0\\u05D7\\u05D3 \\u05D4\\u05D0\\u05E8\\u05E5 "
    "\\u05E6\\u05D9\\u05D5\\u05E8 "
    "\\u05D5\\u05DB\\u05DE\\u05E7\\u05D5\\u05D1\\u05DC. "
    "\\u05D5\\u05D9\\u05E9 \\u05D4\\u05E2\\u05D9\\u05E8 "
    "\\u05E9\\u05D9\\u05DE\\u05D5\\u05E9\\u05D9 "
    "\\u05DE\\u05D3\\u05D5\\u05D9\\u05E7\\u05D9\\u05DD \\u05D1\\u05D4, "
    "\\u05D4\\u05D9\\u05D0 \\u05D5\\u05D9\\u05E7\\u05D9 "
    "\\u05D1\\u05E8\\u05D5\\u05DB\\u05D9\\u05DD "
    "\\u05EA\\u05D0\\u05D5\\u05DC\\u05D5\\u05D2\\u05D9\\u05D4 "
    "\\u05D0\\u05D5. \\u05D0\\u05EA \\u05D6\\u05DB\\u05E8 "
    "\\u05E7\\u05D4\\u05D9\\u05DC\\u05D4 "
    "\\u05D7\\u05D1\\u05E8\\u05D9\\u05DB\\u05DD "
    "\\u05DC\\u05D9\\u05E6\\u05D9\\u05E8\\u05EA\\u05D4, "
    "\\u05E2\\u05E8\\u05DB\\u05D9\\u05DD "
    "\\u05D4\\u05EA\\u05E4\\u05EA\\u05D7\\u05D5\\u05EA \\u05D7\\u05E4\\u05E9 "
    "\\u05D2\\u05DD."));

  for (auto& it : expected) {
    Builder b;
    b.add(it.first);

    Options options;
    options.escape_unicode = true;

    std::string dumped = b.slice().toJson(&options);
    ASSERT_EQ(std::string("\"") + it.second + "\"", dumped);

    Parser parser;
    parser.parse(dumped);

    std::shared_ptr<Builder> builder = parser.steal();
    Slice s(builder->start());
    ASSERT_EQ(it.first, s.copyString());
  }
}

TEST(StringDumperTest, NumberDoubleZero) {
  Builder b;
  b.add(0.0);
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("0"), buffer);
}

TEST(StringDumperTest, NumberDouble1) {
  Builder b;
  b.add(123456.67);
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("123456.67"), buffer);
}

TEST(StringDumperTest, NumberDouble2) {
  Builder b;
  b.add(-123456.67);
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("-123456.67"), buffer);
}

TEST(StringDumperTest, NumberDouble3) {
  Builder b;
  b.add(-0.000442);
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("-0.000442"), buffer);
}

TEST(StringDumperTest, NumberDouble4) {
  Builder b;
  b.add(0.1);
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("0.1"), buffer);
}

TEST(StringDumperTest, NumberDoubleScientific1) {
  Builder b;
  b.add(2.41e-109);
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("2.41e-109"), buffer);
}

TEST(StringDumperTest, NumberDoubleScientific2) {
  Builder b;
  b.add(-3.423e78);
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("-3.423e+78"), buffer);
}

TEST(StringDumperTest, NumberDoubleScientific3) {
  Builder b;
  b.add(3.423e123);
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("3.423e+123"), buffer);
}

TEST(StringDumperTest, NumberDoubleScientific4) {
  Builder b;
  b.add(3.4239493e104);
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("3.4239493e+104"), buffer);
}

TEST(StringDumperTest, NumberInt1) {
  Builder b;
  b.add(static_cast<int64_t>(123456789));
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("123456789"), buffer);
}

TEST(StringDumperTest, NumberInt2) {
  Builder b;
  b.add(static_cast<int64_t>(-123456789));
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("-123456789"), buffer);
}

TEST(StringDumperTest, NumberZero) {
  Builder b;
  b.add(static_cast<int64_t>(0));
  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("0"), buffer);
}

TEST(StringDumperTest, AppendCharTest) {
  const char* p = "this is a simple string";
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.DumpStr(p);

  ASSERT_EQ(std::string("\"this is a simple string\""), buffer);
}

TEST(StringDumperTest, AppendStringTest) {
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.DumpStr("this is a simple string");

  ASSERT_EQ(std::string("\"this is a simple string\""), buffer);
}

TEST(StringDumperTest, AppendCharTestSpecialChars1) {
  Options options;
  options.escape_forward_slashes = true;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.DumpStr(
    std::string("this is a string with special chars / \" \\ ' foo\n\r\t baz"));

  ASSERT_EQ(std::string("\"this is a string with special chars \\/ \\\" \\\\ ' "
                        "foo\\n\\r\\t baz\""),
            buffer);
}

TEST(StringDumperTest, AppendCharTestSpecialChars2) {
  Options options;
  options.escape_forward_slashes = false;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.DumpStr(
    std::string("this is a string with special chars / \" \\ ' foo\n\r\t baz"));

  ASSERT_EQ(std::string("\"this is a string with special chars / \\\" \\\\ ' "
                        "foo\\n\\r\\t baz\""),
            buffer);
}

TEST(StringDumperTest, AppendStringTestSpecialChars1) {
  Options options;
  options.escape_forward_slashes = true;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.DumpStr("this is a string with special chars / \" \\ ' foo\n\r\t baz");

  ASSERT_EQ(std::string("\"this is a string with special chars \\/ \\\" \\\\ ' "
                        "foo\\n\\r\\t baz\""),
            buffer);
}

TEST(StringDumperTest, AppendStringTestSpecialChars2) {
  Options options;
  options.escape_forward_slashes = false;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.DumpStr("this is a string with special chars / \" \\ ' foo\n\r\t baz");

  ASSERT_EQ(std::string("\"this is a string with special chars / \\\" \\\\ ' "
                        "foo\\n\\r\\t baz\""),
            buffer);
}

TEST(StringDumperTest, AppendStringTestTruncatedTwoByteUtf8) {
  StrSink sink;
  Dumper dumper(&sink);
  ASSERT_ANY_THROW(dumper.DumpStr("\xc2"));
}

TEST(StringDumperTest, AppendStringTestTruncatedThreeByteUtf8) {
  StrSink sink;
  Dumper dumper(&sink);
  ASSERT_ANY_THROW(dumper.DumpStr("\xe2\x82"));
}

TEST(StringDumperTest, AppendStringTestTruncatedFourByteUtf8) {
  StrSink sink;
  Dumper dumper(&sink);
  ASSERT_ANY_THROW(dumper.DumpStr("\xf0\xa4\xad"));
}

TEST(StringDumperTest, AppendStringSlice1) {
  Options options;
  options.escape_forward_slashes = true;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);

  const std::string s =
    "this is a string with special chars / \" \\ ' foo\n\r\t baz";
  Builder b;
  b.add(s);
  Slice slice(b.start());
  dumper.Dump(slice);

  ASSERT_EQ(std::string("\"this is a string with special chars \\/ \\\" \\\\ ' "
                        "foo\\n\\r\\t baz\""),
            buffer);
}

TEST(StringDumperTest, AppendStringSlice2) {
  Options options;
  options.escape_forward_slashes = false;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);

  const std::string s =
    "this is a string with special chars / \" \\ ' foo\n\r\t baz";
  Builder b;
  b.add(s);
  Slice slice(b.start());

  dumper.Dump(slice);
  ASSERT_EQ(std::string("\"this is a string with special chars / \\\" \\\\ ' "
                        "foo\\n\\r\\t baz\""),
            buffer);
}

TEST(StringDumperTest, AppendStringSliceRef1) {
  Options options;
  options.escape_forward_slashes = true;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);

  const std::string s =
    "this is a string with special chars / \" \\ ' foo\n\r\t baz";
  Builder b;
  b.add(s);
  Slice slice(b.start());
  dumper.Dump(slice);

  ASSERT_EQ(std::string("\"this is a string with special chars \\/ \\\" \\\\ ' "
                        "foo\\n\\r\\t baz\""),
            buffer);
}

TEST(StringDumperTest, AppendStringSliceRef2) {
  Options options;
  options.escape_forward_slashes = false;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);

  const std::string s =
    "this is a string with special chars / \" \\ ' foo\n\r\t baz";
  Builder b;
  b.add(s);
  Slice slice(b.start());
  dumper.Dump(slice);
  ASSERT_EQ(std::string("\"this is a string with special chars / \\\" \\\\ ' "
                        "foo\\n\\r\\t baz\""),
            buffer);
}

TEST(StringDumperTest, AppendDoubleNan) {
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  sink.PushF64(std::nan("1"));
  ASSERT_EQ("\"NaN\"", buffer);
}

TEST(StringDumperTest, AppendDoubleMinusInf) {
  double v = -3.33e307;
  // go to -inf
  v *= 3.1e90;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  sink.PushF64(v);
  ASSERT_EQ("\"-Infinity\"", buffer);
}

TEST(StringDumperTest, AppendDoublePlusInf) {
  double v = 3.33e307;
  // go to +inf
  v *= v;

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  sink.PushF64(v);
  ASSERT_EQ("\"Infinity\"", buffer);
}

TEST(StringDumperTest, UnsupportedTypeDoubleMinusInf) {
  double v = -3.33e307;
  // go to -inf
  v *= 3.1e90;
  Builder b;
  b.add(v);

  Slice slice = b.slice();

  StrSink sink;
  Dumper dumper(&sink);
  ASSERT_VPACK_EXCEPTION(dumper.Dump(slice), Exception::kNoJsonEquivalent);
}

TEST(StringDumperTest, ConvertTypeDoubleMinusInf) {
  double v = -3.33e307;
  // go to -inf
  v *= 3.1e90;
  Builder b;
  b.add(v);

  Slice slice = b.slice();

  Options options;
  options.unsupported_type_behavior = Options::kNullifyUnsupportedType;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("null"), buffer);
}

TEST(StringDumperTest, UnsupportedTypeDoublePlusInf) {
  double v = 3.33e307;
  // go to +inf
  v *= v;
  Builder b;
  b.add(v);

  Slice slice = b.slice();

  StrSink sink;
  Dumper dumper(&sink);
  ASSERT_VPACK_EXCEPTION(dumper.Dump(slice), Exception::kNoJsonEquivalent);
}

TEST(StringDumperTest, ConvertTypeDoublePlusInf) {
  double v = 3.33e307;
  // go to +inf
  v *= v;
  Builder b;
  b.add(v);

  Slice slice = b.slice();

  Options options;
  options.unsupported_type_behavior = Options::kNullifyUnsupportedType;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("null"), buffer);
}

TEST(StringDumperTest, UnsupportedTypeDoubleNan) {
  double v = std::nan("1");
  ASSERT_TRUE(std::isnan(v));
  Builder b;
  b.add(v);

  Slice slice = b.slice();

  StrSink sink;
  Dumper dumper(&sink);
  ASSERT_VPACK_EXCEPTION(dumper.Dump(slice), Exception::kNoJsonEquivalent);
}

TEST(StringDumperTest, DoubleNanAsString) {
  Options options;
  options.unsupported_doubles_as_string = true;

  double v = std::nan("1");
  ASSERT_TRUE(std::isnan(v));
  Builder b;
  b.add(v);

  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("\"NaN\""), buffer);
}

TEST(StringDumperTest, DoubleInfinityAsString) {
  Options options;
  options.unsupported_doubles_as_string = true;

  double v = INFINITY;
  ASSERT_TRUE(std::isinf(v));
  Builder b;
  b.add(v);

  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("\"Infinity\""), buffer);
}

TEST(StringDumperTest, DoubleMinusInfinityAsString) {
  Options options;
  options.unsupported_doubles_as_string = true;

  double v = -INFINITY;
  ASSERT_TRUE(std::isinf(v));
  Builder b;
  b.add(v);

  Slice slice = b.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("\"-Infinity\""), buffer);
}

TEST(StringDumperTest, ConvertTypeDoubleNan) {
  double v = std::nan("1");
  ASSERT_TRUE(std::isnan(v));
  Builder b;
  b.add(v);

  Slice slice = b.slice();

  Options options;
  options.unsupported_type_behavior = Options::kNullifyUnsupportedType;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("null"), buffer);
}

TEST(StringDumperTest, UnsupportedTypeNone) {
  static const uint8_t kB[] = {0x00};
  Slice slice(&kB[0]);

  ASSERT_VPACK_EXCEPTION(slice.toJson(), Exception::kNoJsonEquivalent);
}

TEST(StringDumperTest, ConvertTypeNone) {
  static const uint8_t kB[] = {0x00};
  Slice slice(&kB[0]);

  Options options;
  options.unsupported_type_behavior = Options::kNullifyUnsupportedType;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("null"), buffer);
}

TEST(StringDumperTest, ConvertUnsupportedTypeNone) {
  static const uint8_t kB[] = {0x00};
  Slice slice(&kB[0]);

  Options options;
  options.unsupported_type_behavior = Options::kConvertUnsupportedType;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  dumper.Dump(slice);
  ASSERT_EQ(std::string("\"(non-representable type none)\""), buffer);
}

TEST(DumperTest, EmptyAttributeName) {
  Builder builder;
  Parser parser(builder);
  parser.parse(R"({"":123,"a":"abc"})");
  Slice slice = builder.slice();

  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink);
  dumper.Dump(slice);
  ASSERT_EQ(std::string(R"({"":123,"a":"abc"})"), buffer);
}

TEST(DumperLengthTest, Null) {
  const std::string value("null");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = false;
  LenSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(strlen("null"), sink.Impl());
}

TEST(DumperLengthTest, True) {
  const std::string value("  true ");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = false;
  LenSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(strlen("true"), sink.Impl());
}

TEST(DumperLengthTest, False) {
  const std::string value("false ");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = false;
  LenSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(strlen("false"), sink.Impl());
}

TEST(DumperLengthTest, String) {
  const std::string value("   \"abcdefgjfjhhgh\"  ");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = false;
  LenSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(strlen("\"abcdefgjfjhhgh\""), sink.Impl());
  ;
}

TEST(DumperLengthTest, EmptyObject) {
  const std::string value("{}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = false;
  LenSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(strlen("{}"), sink.Impl());
}

TEST(DumperLengthTest, SimpleObject) {
  const std::string value("{\"foo\":\"bar\"}");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = false;
  LenSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(strlen("{\"foo\":\"bar\"}"), sink.Impl());
}

TEST(DumperLengthTest, SimpleArray) {
  const std::string value("[1, 2, 3, 4, 5, 6, 7, \"abcdef\"]");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = false;
  LenSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(strlen("[1,2,3,4,5,6,7,\"abcdef\"]"), sink.Impl());
}

TEST(DumperLengthTest, EscapeUnicodeOn) {
  const std::string value("\"mötör\"");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = false;
  options.escape_unicode = true;
  LenSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(strlen("\"m\\uxxxxt\\uxxxxr\""), sink.Impl());
}

TEST(DumperLengthTest, EscapeUnicodeOff) {
  const std::string value("\"mötör\"");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Options options;
  options.pretty_print = false;
  options.escape_unicode = false;
  LenSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  ASSERT_EQ(strlen("\"mötör\""), sink.Impl());
}

TEST(CordDumperTest, StringifyComplexObject) {
  const std::string_view value(
    R"({"foo":"bar","baz":[1,2,3,[4]],"bark":[{"troet\\nmann":1, "mötör":[2,3.4,-42.5,true,false,null,"some\\nstring"]}]})");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  auto s = builder->slice();

  Options options;
  options.pretty_print = true;
  CordSink sink;
  Dumper dumper(&sink, &options);
  dumper.Dump(s);
  sink.Finish();

  std::string dumped;
  absl::CopyCordToString(sink.Impl(), &dumped);

  ASSERT_EQ(s.toJson(&options), dumped);
}

TEST(DumperLargeDoubleTest, TwoToThePowerOf60Double) {
  Options options;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  Builder builder;
  builder.add(ldexp(1.0, 60));
  dumper.Dump(builder.slice());
  ASSERT_EQ("1152921504606846976", buffer);
}

TEST(DumperLargeDoubleTest, TwoToThePowerOf60Plus1Double) {
  Options options;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  Builder builder;
  builder.add(ldexp(1.0, 60) + 1.0);
  dumper.Dump(builder.slice());
  ASSERT_EQ("1152921504606846976", buffer);
}

TEST(DumperLargeDoubleTest, MinusTwoToThePowerOf60Double) {
  Options options;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  Builder builder;
  builder.add(-ldexp(1.0, 60));
  dumper.Dump(builder.slice());
  ASSERT_EQ("-1152921504606846976", buffer);
}

TEST(DumperLargeDoubleTest, MinusTwoToThePowerOf60Plus1Double) {
  Options options;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  Builder builder;
  builder.add(-ldexp(1.0, 60) + 1.0);
  dumper.Dump(builder.slice());
  ASSERT_EQ("-1152921504606846976", buffer);
}

TEST(DumperLargeDoubleTest, TwoToThePowerOf52Double) {
  Options options;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  Builder builder;
  builder.add(ldexp(1.0, 52));
  dumper.Dump(builder.slice());
  ASSERT_EQ("4503599627370496", buffer);
}

TEST(DumperLargeDoubleTest, TwoToThePowerOf53Double) {
  Options options;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  Builder builder;
  builder.add(ldexp(1.0, 53));
  dumper.Dump(builder.slice());
  ASSERT_EQ("9007199254740992", buffer);
}

TEST(DumperLargeDoubleTest, TwoToThePowerOf63Double) {
  Options options;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  Builder builder;
  builder.add(ldexp(1.0, 63));
  dumper.Dump(builder.slice());
  ASSERT_EQ("9223372036854775808", buffer);
}

TEST(DumperLargeDoubleTest, TwoToThePowerOf64Double) {
  Options options;
  StrSink sink;
  auto& buffer = sink.Impl();
  Dumper dumper(&sink, &options);
  Builder builder;
  builder.add(ldexp(1.0, 64));
  dumper.Dump(builder.slice());
  ASSERT_EQ("18446744073709551616", buffer);
}

}  // namespace
