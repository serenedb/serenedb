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

#include <absl/strings/escaping.h>

#include <array>
#include <iostream>
#include <ostream>
#include <string>
#include <string_view>

#include "tests-common.h"

// If changed needs to change same shit in serened
// {"error":true,"errorMessage":"document not found","errorNum":1202}
constexpr std::string_view kNotFoundData =
  "\x14\x36\x85\x65\x72\x72\x6f\x72\x1a\x88\x65\x72\x72\x6f\x72\x4e\x75\x6d"
  "\x21\xb2\x04\x8c\x65\x72\x72\x6f\x72\x4d\x65\x73\x73\x61\x67\x65\x92\x64"
  "\x6f\x63\x75\x6d\x65\x6e\x74\x20\x6e\x6f\x74\x20\x66\x6f\x75\x6e\x64\x03";

TEST(BuilderTest, NotFound) {
  Builder b1;
  b1.openObject(true);
  b1.add("error", true);
  b1.add("errorNum", 1202);
  b1.add("errorMessage", "document not found");
  b1.close();
  auto slice = b1.slice();
  EXPECT_TRUE(slice.isObject());
  EXPECT_TRUE(slice.length() == 3);
  EXPECT_TRUE(slice.get("error").isTrue());
  EXPECT_TRUE(slice.get("errorNum").isNumber<int32_t>());
  EXPECT_TRUE(slice.get("errorNum").getNumber<int32_t>() == 1202);
  EXPECT_TRUE(slice.get("errorMessage").isString());
  EXPECT_TRUE(slice.get("errorMessage").stringView() == "document not found");
  auto actual = std::string_view(slice.startAs<char>(), slice.byteSize());
  EXPECT_EQ(actual, kNotFoundData);
}

TEST(BuilderTest, ConstructWithBufferRef) {
  Builder b1;
  uint32_t u = 1;
  b1.openObject();
  b1.add("test", u);
  b1.close();
  BufferUInt8 buf = *b1.steal();
  Builder b2(buf);
  Slice s(buf.data());

  ASSERT_EQ(ValueType::Object, s.type());
  ASSERT_EQ(s.get("test").getUInt(), u);
  ASSERT_EQ(ValueType::SmallInt, s.get("test").type());
  ASSERT_EQ(u, s.get("test").getUInt());
}

TEST(BuilderTest, AddObjectInArray) {
  Builder b;
  b.openArray();
  b.openObject();
  b.close();
  b.close();
  Slice s(b.slice());
  ASSERT_TRUE(s.isArray());
  ASSERT_EQ(1UL, s.length());
  Slice ss(s.at(0));
  ASSERT_TRUE(ss.isObject());
  ASSERT_EQ(0UL, ss.length());
}

TEST(BuilderTest, AddObjectIteratorEmpty) {
  Builder obj;
  obj.openObject();
  obj.add("1-one", 1);
  obj.add("2-two", 2);
  obj.add("3-three", 3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  ASSERT_TRUE(b.isClosed());
  ASSERT_VPACK_EXCEPTION(
    b.add(ObjectIterator(obj_slice, /*useSequentialIteration*/ false)),
    Exception::kBuilderNeedOpenObject);
  ASSERT_TRUE(b.isClosed());
}

TEST(BuilderTest, AddObjectIteratorKeyAlreadyWritten) {
  Builder obj;
  obj.openObject();
  obj.add("1-one", 1);
  obj.add("2-two", 2);
  obj.add("3-three", 3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  ASSERT_TRUE(b.isClosed());
  b.openObject();
  b.add("foo");
  ASSERT_FALSE(b.isClosed());
  ASSERT_VPACK_EXCEPTION(
    b.add(ObjectIterator(obj_slice, /*useSequentialIteration*/ false)),
    Exception::kBuilderKeyAlreadyWritten);
  ASSERT_FALSE(b.isClosed());
}

TEST(BuilderTest, AddToObjectEmtpyStringView) {
  Builder obj;
  obj.openObject();
  obj.add(std::string_view{}, std::string_view{});
  obj.close();

  ASSERT_EQ(1U, obj.slice().length());
  ObjectIterator it(obj.slice(), /*useSequentialIteration*/ false);
  ASSERT_TRUE(it.valid());
  EXPECT_EQ("", (*it).key.stringView());
  EXPECT_EQ("", (*it).value().stringView());
  EXPECT_TRUE((*it).key.isEmptyString());
  EXPECT_TRUE((*it).value().isEmptyString());
}

TEST(BuilderTest, AddObjectIteratorNonObject) {
  Builder obj;
  obj.openObject();
  obj.add("1-one", 1);
  obj.add("2-two", 2);
  obj.add("3-three", 3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  b.openArray();
  ASSERT_FALSE(b.isClosed());
  ASSERT_VPACK_EXCEPTION(
    b.add(ObjectIterator(obj_slice, /*useSequentialIteration*/ false)),
    Exception::kBuilderNeedOpenObject);
  ASSERT_FALSE(b.isClosed());
}

TEST(BuilderTest, AddObjectIteratorTop) {
  Builder obj;
  obj.openObject();
  obj.add("1-one", 1);
  obj.add("2-two", 2);
  obj.add("3-three", 3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  b.openObject();
  ASSERT_FALSE(b.isClosed());
  b.add(ObjectIterator(obj_slice, /*useSequentialIteration*/ false));
  ASSERT_FALSE(b.isClosed());
  Slice result = b.close().slice();
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ("{\"1-one\":1,\"2-two\":2,\"3-three\":3}", result.toJson());
}

TEST(BuilderTest, AddObjectIteratorReference) {
  Builder obj;
  obj.openObject();
  obj.add("1-one", 1);
  obj.add("2-two", 2);
  obj.add("3-three", 3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  b.openObject();
  ASSERT_FALSE(b.isClosed());
  auto it = ObjectIterator(obj_slice, /*useSequentialIteration*/ false);
  b.add(std::move(it));
  ASSERT_FALSE(b.isClosed());
  Slice result = b.close().slice();
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ("{\"1-one\":1,\"2-two\":2,\"3-three\":3}", result.toJson());
}

TEST(BuilderTest, AddObjectIteratorSub) {
  Builder obj;
  obj.openObject();
  obj.add("1-one", 1);
  obj.add("2-two", 2);
  obj.add("3-three", 3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  b.openObject();
  b.add("1-something", "tennis");
  b.add("2-values");
  b.openObject();
  b.add(ObjectIterator(obj_slice, /*useSequentialIteration*/ false));
  ASSERT_FALSE(b.isClosed());
  b.close();  // close one level
  b.add("3-bark", "qux");
  ASSERT_FALSE(b.isClosed());
  Slice result = b.close().slice();
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ(
    "{\"1-something\":\"tennis\",\"2-values\":{\"1-one\":1,\"2-two\":2,\"3-"
    "three\":3},\"3-bark\":\"qux\"}",
    result.toJson());
}

TEST(BuilderTest, AddArrayIteratorEmpty) {
  Builder obj;
  obj.openArray();
  obj.add(1);
  obj.add(2);
  obj.add(3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  ASSERT_TRUE(b.isClosed());
  ASSERT_VPACK_EXCEPTION(b.add(ArrayIterator(obj_slice)),
                         Exception::kBuilderNeedOpenArray);
  ASSERT_TRUE(b.isClosed());
}

TEST(BuilderTest, AddArrayIteratorNonArray) {
  Builder obj;
  obj.openArray();
  obj.add(1);
  obj.add(2);
  obj.add(3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  b.openObject();
  ASSERT_FALSE(b.isClosed());
  ASSERT_VPACK_EXCEPTION(b.add(ArrayIterator(obj_slice)),
                         Exception::kBuilderNeedOpenArray);
  ASSERT_FALSE(b.isClosed());
}

TEST(BuilderTest, AddArrayIteratorTop) {
  Builder obj;
  obj.openArray();
  obj.add(1);
  obj.add(2);
  obj.add(3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  b.openArray();
  ASSERT_FALSE(b.isClosed());
  b.add(ArrayIterator(obj_slice));
  ASSERT_FALSE(b.isClosed());
  Slice result = b.close().slice();
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ("[1,2,3]", result.toJson());
}

TEST(BuilderTest, AddArrayIteratorReference) {
  Builder obj;
  obj.openArray();
  obj.add(1);
  obj.add(2);
  obj.add(3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  b.openArray();
  ASSERT_FALSE(b.isClosed());
  auto it = ArrayIterator(obj_slice);
  b.add(std::move(it));
  ASSERT_FALSE(b.isClosed());
  Slice result = b.close().slice();
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ("[1,2,3]", result.toJson());
}

TEST(BuilderTest, AddArrayIteratorSub) {
  Builder obj;
  obj.openArray();
  obj.add(1);
  obj.add(2);
  obj.add(3);
  Slice obj_slice = obj.close().slice();

  Builder b;
  b.openArray();
  b.add("tennis");
  b.openArray();
  b.add(ArrayIterator(obj_slice));
  ASSERT_FALSE(b.isClosed());
  b.close();  // close one level
  b.add("qux");
  ASSERT_FALSE(b.isClosed());
  Slice result = b.close().slice();
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ("[\"tennis\",[1,2,3],\"qux\"]", result.toJson());
}

TEST(BuilderTest, CreateWithoutBufferOrOptions) {
  ASSERT_VPACK_EXCEPTION(new Builder(nullptr), Exception::kInternalError);

  std::shared_ptr<BufferUInt8> buffer;
  ASSERT_VPACK_EXCEPTION(new Builder(buffer, nullptr),
                         Exception::kInternalError);

  buffer = std::make_shared<BufferUInt8>();
  ASSERT_VPACK_EXCEPTION(new Builder(buffer, nullptr),
                         Exception::kInternalError);

  Builder b;
  ASSERT_TRUE(b.isEmpty());
  b.add(123);
  ASSERT_FALSE(b.isEmpty());
  Slice s = b.slice();

  ASSERT_VPACK_EXCEPTION(b.clone(s, nullptr), Exception::kInternalError);
}

TEST(BuilderTest, Copy) {
  Builder b;
  ASSERT_TRUE(b.isEmpty());
  b.openArray();
  for (int i = 0; i < 10; i++) {
    b.add("abcdefghijklmnopqrstuvwxyz");
  }
  b.close();
  ASSERT_FALSE(b.isEmpty());

  Builder a(b);
  ASSERT_FALSE(a.isEmpty());
  ASSERT_NE(a.buffer().get(), b.buffer().get());
  ASSERT_TRUE(a.buffer().get() != nullptr);
  ASSERT_TRUE(b.buffer().get() != nullptr);
}

TEST(BuilderTest, CopyAssign) {
  Builder b;
  ASSERT_TRUE(b.isEmpty());

  Builder a;
  ASSERT_TRUE(a.isEmpty());
  a = b;
  ASSERT_TRUE(a.isEmpty());
  ASSERT_TRUE(b.isEmpty());

  ASSERT_NE(a.buffer().get(), b.buffer().get());
  ASSERT_TRUE(a.buffer().get() != nullptr);
  ASSERT_TRUE(b.buffer().get() != nullptr);
}

TEST(BuilderTest, MoveConstructOpenObject) {
  Builder b;
  b.openObject();
  ASSERT_FALSE(b.isClosed());

  Builder a(std::move(b));
  ASSERT_FALSE(a.isClosed());
}

TEST(BuilderTest, MoveConstructOpenArray) {
  Builder b;
  b.openArray();
  ASSERT_FALSE(b.isClosed());

  Builder a(std::move(b));
  ASSERT_FALSE(a.isClosed());
  ASSERT_TRUE(b.isClosed());
}

TEST(BuilderTest, MoveAssignOpenObject) {
  Builder b;
  b.openObject();
  ASSERT_FALSE(b.isClosed());

  Builder a;
  a = std::move(b);
  ASSERT_FALSE(a.isClosed());
  ASSERT_TRUE(b.isClosed());
}

TEST(BuilderTest, MoveAssignOpenArray) {
  Builder b;
  b.openArray();
  ASSERT_FALSE(b.isClosed());

  Builder a;
  a = std::move(b);
  ASSERT_FALSE(a.isClosed());
  ASSERT_TRUE(b.isClosed());
}

TEST(BuilderTest, Move) {
  Builder b;
  ASSERT_TRUE(b.isEmpty());

  auto shptrb = b.buffer();
  Builder a(std::move(b));
  ASSERT_TRUE(a.isEmpty());
  ASSERT_TRUE(b.isEmpty());
  auto shptra = a.buffer();
  ASSERT_EQ(shptrb.get(), shptra.get());
  ASSERT_NE(a.buffer().get(), nullptr);
  ASSERT_EQ(b.buffer().get(), nullptr);
}

TEST(BuilderTest, MoveNonEmpty) {
  Builder b;
  b.add("foobar");
  ASSERT_FALSE(b.isEmpty());

  auto shptrb = b.buffer();
  Builder a(std::move(b));
  ASSERT_FALSE(a.isEmpty());
  ASSERT_TRUE(b.isEmpty());

  auto shptra = a.buffer();
  ASSERT_EQ(shptrb.get(), shptra.get());
  ASSERT_NE(a.buffer().get(), nullptr);
  ASSERT_EQ(b.buffer().get(), nullptr);
}

TEST(BuilderTest, MoveAssign) {
  Builder b;
  ASSERT_TRUE(b.isEmpty());

  auto shptrb = b.buffer();
  Builder a = std::move(b);
  ASSERT_TRUE(a.isEmpty());
  ASSERT_TRUE(b.isEmpty());
  auto shptra = a.buffer();
  ASSERT_EQ(shptrb.get(), shptra.get());
  ASSERT_NE(a.buffer().get(), nullptr);
  ASSERT_EQ(b.buffer().get(), nullptr);
}

TEST(BuilderTest, MoveAssignNonEmpty) {
  Builder b;
  b.add("foobar");
  ASSERT_FALSE(b.isEmpty());

  auto shptrb = b.buffer();
  Builder a = std::move(b);
  ASSERT_FALSE(a.isEmpty());
  ASSERT_TRUE(b.isEmpty());
  auto shptra = a.buffer();
  ASSERT_EQ(shptrb.get(), shptra.get());
  ASSERT_NE(a.buffer().get(), nullptr);
  ASSERT_EQ(b.buffer().get(), nullptr);
}

TEST(BuilderTest, ConstructFromSlice) {
  Builder b1;
  b1.openObject();
  b1.add("foo", "bar");
  b1.add("bar", "baz");
  b1.close();

  Builder b2(b1.slice());
  ASSERT_FALSE(b2.isEmpty());
  ASSERT_TRUE(b2.isClosed());

  ASSERT_TRUE(b2.slice().isObject());
  ASSERT_TRUE(!b2.slice().get("foo").isNone());
  ASSERT_TRUE(!b2.slice().get("bar").isNone());

  b1.clear();
  ASSERT_TRUE(b1.isEmpty());

  ASSERT_FALSE(b2.isEmpty());
  ASSERT_TRUE(b2.slice().isObject());
  ASSERT_TRUE(!b2.slice().get("foo").isNone());
  ASSERT_TRUE(!b2.slice().get("bar").isNone());
}

TEST(BuilderTest, UsingEmptySharedPtr) {
  std::shared_ptr<BufferUInt8> buffer;

  ASSERT_VPACK_EXCEPTION(Builder(buffer), Exception::kInternalError);
}

TEST(BuilderTest, SizeUsingSharedPtr) {
  auto buffer = std::make_shared<BufferUInt8>();
  buffer->append("\x85testi", 6);

  {
    Builder b(buffer);
    ASSERT_FALSE(b.isEmpty());
    ASSERT_TRUE(b.slice().isString());
    ASSERT_EQ("testi", b.slice().copyString());
    ASSERT_EQ(6, b.size());
  }

  buffer->clear();
  {
    Builder b(buffer);
    ASSERT_TRUE(b.isEmpty());
    ASSERT_TRUE(b.slice().isNone());
  }
}

TEST(BuilderTest, SizeUsingBufferReference) {
  BufferUInt8 buffer;
  buffer.append("\x85testi", 6);

  {
    Builder b(buffer);
    ASSERT_FALSE(b.isEmpty());
    ASSERT_TRUE(b.slice().isString());
    ASSERT_EQ("testi", b.slice().copyString());
    ASSERT_EQ(6, b.size());
  }

  buffer.clear();
  {
    Builder b(buffer);
    ASSERT_TRUE(b.isEmpty());
    ASSERT_TRUE(b.slice().isNone());
  }
}

TEST(BuilderTest, UsingExistingBuffer) {
  BufferUInt8 buffer;
  Builder b1(buffer);
  b1.add("the-quick-brown-fox-jumped-over-the-lazy-dog");

  // copy-construct
  Builder b2(b1);
  ASSERT_TRUE(b2.slice().isString());
  ASSERT_EQ("the-quick-brown-fox-jumped-over-the-lazy-dog",
            b2.slice().copyString());

  // copy-assign
  Builder b3;
  b3 = b2;
  ASSERT_TRUE(b3.slice().isString());
  ASSERT_EQ("the-quick-brown-fox-jumped-over-the-lazy-dog",
            b3.slice().copyString());

  // move-construct
  Builder b4(std::move(b3));
  ASSERT_TRUE(b4.slice().isString());
  ASSERT_EQ("the-quick-brown-fox-jumped-over-the-lazy-dog",
            b4.slice().copyString());

  // move-assign
  Builder b5;
  b5 = std::move(b4);
  ASSERT_TRUE(b5.slice().isString());
  ASSERT_EQ("the-quick-brown-fox-jumped-over-the-lazy-dog",
            b5.slice().copyString());

  b5.clear();
  b5.add("the-foxx");
  ASSERT_TRUE(b5.slice().isString());
  ASSERT_EQ("the-foxx", b5.slice().copyString());
}

TEST(BuilderTest, BufferRef) {
  Builder b;
  b.add("the-foxx");

  auto& ref = b.bufferRef();
  ASSERT_TRUE(Slice(ref.data()).isEqualString("the-foxx"));

  auto& ref2 = b.bufferRef();
  ASSERT_TRUE(Slice(ref2.data()).isEqualString("the-foxx"));
}

TEST(BuilderTest, BufferRefAfterStolen) {
  Builder b;
  b.add("the-foxx");

  b.steal();
  ASSERT_VPACK_EXCEPTION(b.bufferRef(), Exception::kInternalError);
}

TEST(BuilderTest, AfterStolen) {
  Builder b1;
  b1.add("the-quick-brown-fox-jumped-over-the-lazy-dog");

  // sets the shared_ptr of the Builder's Buffer to nullptr
  ASSERT_NE(nullptr, b1.steal());

  // copy-construct
  Builder b2(b1);
  ASSERT_EQ(nullptr, b2.steal());

  // copy-assign
  Builder b3;
  b3 = b2;
  ASSERT_EQ(nullptr, b3.steal());

  // move-construct
  Builder b4(std::move(b3));
  ASSERT_EQ(nullptr, b4.steal());

  // move-assign
  Builder b5;
  b5 = std::move(b4);
  ASSERT_EQ(nullptr, b5.steal());
}

TEST(BuilderTest, StealBuffer) {
  Builder b;
  b.openArray();
  for (int i = 0; i < 10; i++) {
    b.add("abcdefghijklmnopqrstuvwxyz");
  }
  b.close();
  ASSERT_FALSE(b.isEmpty());

  auto ptr1 = b.buffer().get();
  std::shared_ptr<BufferUInt8> buf(b.steal());
  ASSERT_TRUE(b.isEmpty());
  auto ptr2 = b.buffer().get();
  auto ptr3 = buf.get();
  ASSERT_EQ(ptr1, ptr3);
  ASSERT_NE(ptr2, ptr1);
}

TEST(BuilderTest, SizeWithOpenObject) {
  Builder b;
  ASSERT_EQ(0UL, b.size());

  b.add(Value(ValueType::Object));
  ASSERT_VPACK_EXCEPTION(b.size(), Exception::kBuilderNotSealed);

  b.close();
  ASSERT_EQ(1UL, b.size());
}

TEST(BuilderTest, BufferSharedPointerNoSharing) {
  Builder b;
  b.add(Value(ValueType::Array));
  // construct a long string that will exceed the Builder's initial buffer
  b.add(
    "skjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjddddddddddddddddddddddddddddddddddd"
    "dddddddjfkdfffffffffffffffffffffffff,"
    "mmmmmmmmmmmmmmmmmmmmmmmmddddddddddddddddddddddddddddddddddddmmmmmmmmmmmm"
    "mmmmmmmmmmmmmmmmdddddddfjf");
  b.close();

  const std::shared_ptr<BufferUInt8>& builder_buffer = b.buffer();

  // only the Builder itself is using the Buffer
  ASSERT_EQ(1, builder_buffer.use_count());
}

TEST(BuilderTest, BufferSharedPointerStealFromParser) {
  Parser parser;
  parser.parse(
    "\"skjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjddddddddddddddddddddddddddddddddd"
    "dddddddddjfkdfffffffffffffffffffffffff,"
    "mmmmmmmmmmmmmmmmmmmmmmmmddddddddddddddddddddddddddddddddddddmmmmmmmmmmmm"
    "mmmmmmmmmmmmmmmmdddddddfjf\"");

  std::shared_ptr<Builder> b = parser.steal();
  // only the Builder itself is using its Buffer
  const std::shared_ptr<BufferUInt8>& builder_buffer = b->buffer();
  ASSERT_EQ(1, builder_buffer.use_count());
}

TEST(BuilderTest, BufferSharedPointerCopy) {
  Builder b;
  b.add(Value(ValueType::Array));
  // construct a long string that will exceed the Builder's initial buffer
  b.add(
    "skjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjddddddddddddddddddddddddddddddddddd"
    "dddddddjfkdfffffffffffffffffffffffff,"
    "mmmmmmmmmmmmmmmmmmmmmmmmddddddddddddddddddddddddddddddddddddmmmmmmmmmmmm"
    "mmmmmmmmmmmmmmmmdddddddfjf");
  b.close();

  const std::shared_ptr<BufferUInt8>& builder_buffer = b.buffer();
  auto ptr = builder_buffer.get();

  // only the Builder itself is using its Buffer
  ASSERT_EQ(1, builder_buffer.use_count());

  std::shared_ptr<BufferUInt8> copy = b.buffer();
  ASSERT_EQ(2, copy.use_count());
  ASSERT_EQ(2, builder_buffer.use_count());

  copy.reset();
  ASSERT_EQ(1, builder_buffer.use_count());
  ASSERT_EQ(ptr, builder_buffer.get());
}

TEST(BuilderTest, BufferSharedPointerStealFromParserExitScope) {
  std::shared_ptr<Builder> b(new Builder());
  std::shared_ptr<BufferUInt8> builder_buffer = b->buffer();
  ASSERT_EQ(2, builder_buffer.use_count());
  auto ptr = builder_buffer.get();

  {
    Parser parser;
    parser.parse(
      "\"skjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjddddddddddddddddddddddddddddddd"
      "dddddddddddjfkdfffffffffffffffffffffffff,"
      "mmmmmmmmmmmmmmmmmmmmmmmmddddddddddddddddddddddddddddddddddddmmmmmmmmmm"
      "mmmmmmmmmmmmmmmmmmdddddddfjf\"");

    ASSERT_EQ(2, builder_buffer.use_count());

    b = parser.steal();
    ASSERT_EQ(1, builder_buffer.use_count());
    const std::shared_ptr<BufferUInt8>& builder_buffer2 = b->buffer();
    ASSERT_NE(ptr, builder_buffer2.get());
    ASSERT_EQ(1, builder_buffer2.use_count());
  }

  ASSERT_EQ(1, builder_buffer.use_count());
  ASSERT_EQ(ptr, builder_buffer.get());
}

TEST(BuilderTest, BufferSharedPointerStealAndReturn) {
  auto func = []() -> std::shared_ptr<Builder> {
    Parser parser;
    parser.parse(
      "\"skjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjddddddddddddddddddddddddddddddd"
      "dddddddddddjfkdfffffffffffffffffffffffff,"
      "mmmmmmmmmmmmmmmmmmmmmmmmddddddddddddddddddddddddddddddddddddmmmmmmmmmm"
      "mmmmmmmmmmmmmmmmmmdddddddfjf\"");

    return parser.steal();
  };

  std::shared_ptr<Builder> b = func();
  EXPECT_EQ(0xff, *(b->buffer()->data()));  // long string...
  EXPECT_EQ(212, b->size());
}

TEST(BuilderTest, BufferSharedPointerStealMultiple) {
  Parser parser;
  parser.parse(
    "\"skjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjddddddddddddddddddddddddddddddddd"
    "dddddddddjfkdfffffffffffffffffffffffff,"
    "mmmmmmmmmmmmmmmmmmmmmmmmddddddddddddddddddddddddddddddddddddmmmmmmmmmmmm"
    "mmmmmmmmmmmmmmmmdddddddfjf\"");

  auto b = parser.steal();
  EXPECT_EQ(0xff, *(b->buffer()->data()));  // long  string...
  EXPECT_EQ(212, b->size());
  EXPECT_EQ(1, b->buffer().use_count());

  // steal again, should work, but Builder should be empty:
  // CHANGED: parser is broken after steal()
  // std::shared_ptr<Builder> b2 = parser.steal();
  // ASSERT_EQ(b2->buffer()->size(), 0UL);
}

TEST(BuilderTest, BufferSharedPointerInject) {
  auto buffer = std::make_shared<BufferUInt8>();
  auto ptr = buffer.get();

  Builder b(buffer);
  const std::shared_ptr<BufferUInt8>& builder_buffer = b.buffer();

  ASSERT_EQ(2, buffer.use_count());
  ASSERT_EQ(2, builder_buffer.use_count());
  ASSERT_EQ(ptr, builder_buffer.get());

  b.add(Value(ValueType::Array));
  // construct a long string that will exceed the Builder's initial buffer
  b.add(
    "skjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjddddddddddddddddddddddddddddddddddd"
    "dddddddjfkdfffffffffffffffffffffffff,"
    "mmmmmmmmmmmmmmmmmmmmmmmmddddddddddddddddddddddddddddddddddddmmmmmmmmmmmm"
    "mmmmmmmmmmmmmmmmdddddddfjf");
  b.close();

  std::shared_ptr<BufferUInt8> copy = b.buffer();
  ASSERT_EQ(3, buffer.use_count());
  ASSERT_EQ(3, copy.use_count());
  ASSERT_EQ(3, builder_buffer.use_count());
  ASSERT_EQ(ptr, copy.get());

  copy.reset();
  ASSERT_EQ(2, buffer.use_count());
  ASSERT_EQ(2, builder_buffer.use_count());

  b.steal();  // steals the buffer, resulting shared_ptr is forgotten
  ASSERT_EQ(1, buffer.use_count());
  ASSERT_EQ(ptr, buffer.get());
}

TEST(BuilderTest, AddNonCompoundTypeAllowUnindexed) {
  Builder b;
  b.add(Value(ValueType::Array, true));
  b.add(Value(ValueType::Object, true));

  ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::None, true)),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::Null, true)),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::Bool, true)),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::Double, true)),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::Int, true)),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::UInt, true)),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::SmallInt, true)),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::String, true)),
                         Exception::kInvalidValueType);
}

TEST(BuilderTest, AddAndOpenArray) {
  Builder b1;
  ASSERT_TRUE(b1.isClosed());
  b1.openArray();
  ASSERT_FALSE(b1.isClosed());
  b1.add("bar");
  b1.close();
  ASSERT_TRUE(b1.isClosed());
  ASSERT_EQ(0x02, b1.slice().head());

  Builder b2;
  ASSERT_TRUE(b2.isClosed());
  b2.openArray();
  ASSERT_FALSE(b2.isClosed());
  b2.add("bar");
  b2.close();
  ASSERT_TRUE(b2.isClosed());
  ASSERT_EQ(0x02, b2.slice().head());
}

TEST(BuilderTest, AddAndOpenObject) {
  Builder b1;
  ASSERT_TRUE(b1.isClosed());
  b1.openObject();
  ASSERT_FALSE(b1.isClosed());
  b1.add("foo", "bar");
  b1.close();
  ASSERT_TRUE(b1.isClosed());
  ASSERT_EQ(0x14, b1.slice().head());
  ASSERT_EQ("{\n  \"foo\" : \"bar\"\n}", b1.toString());
  ASSERT_EQ(1UL, b1.slice().length());

  Builder b2;
  ASSERT_TRUE(b2.isClosed());
  b2.openObject();
  ASSERT_FALSE(b2.isClosed());
  b2.add("foo", "bar");
  b2.close();
  ASSERT_TRUE(b2.isClosed());
  ASSERT_EQ(0x14, b2.slice().head());
  ASSERT_EQ("{\n  \"foo\" : \"bar\"\n}", b2.toString());
  ASSERT_EQ(1UL, b2.slice().length());
}

TEST(BuilderTest, None) {
  Builder b;
  ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::None)),
                         Exception::kBuilderUnexpectedType);
}

TEST(BuilderTest, Null) {
  Builder b;
  b.add(Value(ValueType::Null));
  uint8_t* result = b.start();
  ValueLength len = b.size();

  static const uint8_t kCorrectResult[] = {0x18};

  ASSERT_EQ(sizeof(kCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, kCorrectResult, len));
}

TEST(BuilderTest, False) {
  Builder b;
  b.add(false);
  uint8_t* result = b.start();
  ValueLength len = b.size();

  static const uint8_t kCorrectResult[] = {0x19};

  ASSERT_EQ(sizeof(kCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, kCorrectResult, len));
}

TEST(BuilderTest, True) {
  Builder b;
  b.add(true);
  uint8_t* result = b.start();
  ValueLength len = b.size();

  static const uint8_t kCorrectResult[] = {0x1a};

  ASSERT_EQ(sizeof(kCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, kCorrectResult, len));
}

TEST(BuilderTest, Int64) {
  static int64_t gValue = INT64_MAX;
  Builder b;
  b.add(gValue);
  const uint8_t* result = b.start();
  ValueLength len = b.size();

  const uint8_t correct_result[] = {
    0x27, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7F,
  };

  EXPECT_EQ(sizeof(correct_result), len);
  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(result), len),
            std::string_view(reinterpret_cast<const char*>(correct_result),
                             sizeof(correct_result)));
}

TEST(BuilderTest, UInt64) {
  static uint64_t gValue = 1234;
  Builder b;
  b.add(gValue);
  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[3] = {0x29, 0xd2, 0x04};

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));
}

TEST(BuilderTest, Double) {
  static double gValue = 123.456;
  Builder b;
  b.add(gValue);
  const uint8_t* result = b.start();
  ValueLength len = b.size();

  uint8_t correct_result[9] = {
    0x1f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  };
  ASSERT_EQ(8ULL, sizeof(double));
  DumpDouble(gValue, correct_result + 1);

  EXPECT_EQ(sizeof(correct_result), len);
  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(result), len),
            std::string_view(reinterpret_cast<const char*>(correct_result),
                             sizeof(correct_result)));
}

TEST(BuilderTest, String) {
  Builder b;
  b.add("abcdefghijklmnopqrstuvwxyz");
  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {
    0x9a, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
    0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71,
    0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a,
  };

  EXPECT_EQ(sizeof(gCorrectResult), len);
  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(result), len),
            std::string_view(reinterpret_cast<const char*>(gCorrectResult),
                             sizeof(gCorrectResult)));
}

TEST(BuilderTest, ArrayEmpty) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.close();
  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {0x01};

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));
}

TEST(BuilderTest, ArraySingleEntry) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(uint64_t(1));
  b.close();
  const uint8_t* result = b.start();
  ASSERT_EQ(0x02U, *result);
  ValueLength len = b.size();

  const uint8_t correct_result[] = {0x02, 0x03, 0x36 + 1};

  EXPECT_EQ(sizeof(correct_result), len);
  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(result), len),
            std::string_view(reinterpret_cast<const char*>(correct_result),
                             sizeof(correct_result)));
}

TEST(BuilderTest, ArraySingleEntryLong) {
  const std::string value(
    "ngdddddljjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjsdddffffffffffffmmmmmmmmmmmmmmmsf"
    "dlllllllllllllllllllllllllllllllllllllllllllllllllrjjjjjjsdddddddddddddd"
    "ddddhhhhhhkkkkkkkksssssssssssssssssssssssssssssssssdddddddddddddddddkkkk"
    "kkkkkkkkksddddddddddddssssssssssfvvvvvvvvvvvvvvvvvvvvvvvvvvvfvgfff");
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(value);
  b.close();
  uint8_t* result = b.start();
  ASSERT_EQ(0x03U, *result);
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {
    0x03, 0x2c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x1a, 0x01,
    0x00, 0x00, 0x6e, 0x67, 0x64, 0x64, 0x64, 0x64, 0x64, 0x6c, 0x6a, 0x6a,
    0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a,
    0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a,
    0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x73, 0x64, 0x64, 0x64, 0x66, 0x66, 0x66,
    0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x6d, 0x6d, 0x6d,
    0x6d, 0x6d, 0x6d, 0x6d, 0x6d, 0x6d, 0x6d, 0x6d, 0x6d, 0x6d, 0x6d, 0x6d,
    0x73, 0x66, 0x64, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c,
    0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c,
    0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c,
    0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c, 0x6c,
    0x6c, 0x6c, 0x6c, 0x6c, 0x72, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x6a, 0x73,
    0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
    0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68,
    0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x73, 0x73, 0x73, 0x73,
    0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73,
    0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73,
    0x73, 0x73, 0x73, 0x73, 0x73, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
    0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x6b, 0x6b,
    0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x6b, 0x73,
    0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
    0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x73, 0x66, 0x76,
    0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76,
    0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76, 0x76,
    0x76, 0x76, 0x66, 0x76, 0x67, 0x66, 0x66, 0x66,
  };

  EXPECT_EQ(sizeof(gCorrectResult), len);
  std::string_view actual{reinterpret_cast<const char*>(result), len};
  std::string_view expected{reinterpret_cast<const char*>(gCorrectResult),
                            sizeof(gCorrectResult)};
  EXPECT_EQ(expected, expected);
}

TEST(BuilderTest, ArraySameSizeEntries) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(uint64_t(1));
  b.add(uint64_t(2));
  b.add(uint64_t(3));
  b.close();
  const uint8_t* result = b.start();
  ValueLength len = b.size();

  const uint8_t correct_result[] = {
    0x02, 0x05, 0x36 + 1, 0x36 + 2, 0x36 + 3,
  };

  EXPECT_EQ(sizeof(correct_result), len);
  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(result), len),
            std::string_view(reinterpret_cast<const char*>(correct_result),
                             sizeof(correct_result)));
}

TEST(BuilderTest, ArraySomeValues) {
  double value = 2.3;
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(uint64_t(1200));
  b.add(value);
  b.add("abc");
  b.add(true);
  b.close();

  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {
    0x06, 0x18, 0x04, 0x29, 0xb0, 0x04,                    // uint(1200) = 0x4b0
    0x1f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // double(2.3)
    0x83, 0x61, 0x62, 0x63,                                // "abc"
    0x1a,                                                  // true
    0x03, 0x06, 0x0f, 0x13,
  };
  DumpDouble(value, gCorrectResult + 7);

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));
}

TEST(BuilderTest, ArrayCompact) {
  double value = 2.3;
  Builder b;
  b.add(Value(ValueType::Array, true));
  b.add(uint64_t(1200));
  b.add(value);
  b.add("abc");
  b.add(true);
  b.close();

  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {
    0x13, 0x14, 0x29, 0xb0, 0x04, 0x1f, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // double
    0x83, 0x61, 0x62, 0x63,                    // "abc"
    0x1a,                                      // true
    0x04,
  };
  DumpDouble(value, gCorrectResult + 6);

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));
}

TEST(BuilderTest, ObjectEmpty) {
  Builder b;
  b.add(Value(ValueType::Object));
  b.close();
  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {0x0a};

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));
}

TEST(BuilderTest, ObjectEmptyCompact) {
  Builder b;
  b.add(Value(ValueType::Object, true));
  b.close();
  uint8_t* result = b.start();
  ValueLength len = b.size();

  // should still build the compact variant
  static uint8_t gCorrectResult[] = {0x0a};

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));
}

TEST(BuilderTest, ObjectSorted) {
  double value = 2.3;
  Builder b;
  b.add(Value(ValueType::Object));
  b.add("d", uint64_t(1200));
  b.add("c", value);
  b.add("b", "abc");
  b.add("a", true);
  b.close();

  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {
    0x0b,
    0x20,
    0x04,

    // "d":
    0x81,
    0x64,

    // uint(1200) = 0x4b0
    0x29,
    0xb0,
    0x04,

    // "c":
    0x81,
    0x63,

    // double(2.3)
    0x1f,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,

    // "b":
    0x81,
    0x62,

    // "abc"
    0x83,
    0x61,
    0x62,
    0x63,

    // "a":
    0x81,
    0x61,

    // true
    0x1a,

    0x19,
    0x13,
    0x08,
    0x03,
  };
  DumpDouble(value, gCorrectResult + 11);

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));
}

TEST(BuilderTest, ObjectCompact) {
  double value = 2.3;
  Builder b;
  b.add(Value(ValueType::Object, true));
  b.add("d", uint64_t(1200));
  b.add("c", value);
  b.add("b", "abc");
  b.add("a", true);
  b.close();

  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {
    0x14, 0x1c, 0x81, 0x64, 0x29, 0xb0, 0x04, 0x81, 0x63, 0x1f,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // double
    0x81, 0x62, 0x83, 0x61, 0x62, 0x63, 0x81, 0x61, 0x1a, 0x04};

  DumpDouble(value, gCorrectResult + 10);

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));
}

TEST(BuilderTest, ArrayCompactBytesizeBelowThreshold) {
  Builder b;
  b.add(Value(ValueType::Array, true));
  for (size_t i = 0; i < 124; ++i) {
    b.add(uint64_t(i % 10));
  }
  b.close();

  uint8_t* result = b.start();
  Slice s(result);

  ASSERT_EQ(127UL, s.byteSize());

  ASSERT_EQ(0x13, result[0]);
  ASSERT_EQ(0x7f, result[1]);
  for (size_t i = 0; i < 124; ++i) {
    ASSERT_EQ(0x36 + (i % 10), result[2 + i]);
  }
  ASSERT_EQ(0x7c, result[126]);
}

TEST(BuilderTest, ArrayCompactBytesizeAboveThreshold) {
  Builder b;
  b.add(Value(ValueType::Array, true));
  for (size_t i = 0; i < 125; ++i) {
    b.add(uint64_t(i % 10));
  }
  b.close();

  uint8_t* result = b.start();
  Slice s(result);

  ASSERT_EQ(129UL, s.byteSize());

  ASSERT_EQ(0x13, result[0]);
  ASSERT_EQ(0x81, result[1]);
  ASSERT_EQ(0x01, result[2]);
  for (size_t i = 0; i < 125; ++i) {
    ASSERT_EQ(0x36 + (i % 10), result[3 + i]);
  }
  ASSERT_EQ(0x7d, result[128]);
}

TEST(BuilderTest, ArrayCompactLengthBelowThreshold) {
  Builder b;
  b.add(Value(ValueType::Array, true));
  for (size_t i = 0; i < 127; ++i) {
    b.add("aaa");
  }
  b.close();

  uint8_t* result = b.start();
  Slice s(result);

  ASSERT_EQ(512UL, s.byteSize());

  ASSERT_EQ(0x13, result[0]);
  ASSERT_EQ(0x80, result[1]);
  ASSERT_EQ(0x04, result[2]);
  for (size_t i = 0; i < 127; ++i) {
    ASSERT_EQ(0x83, result[3 + i * 4]);
  }
  ASSERT_EQ(0x7f, result[511]);
}

TEST(BuilderTest, ArrayCompactLengthAboveThreshold) {
  Builder b;
  b.add(Value(ValueType::Array, true));
  for (size_t i = 0; i < 128; ++i) {
    b.add("aaa");
  }
  b.close();

  uint8_t* result = b.start();
  Slice s(result);

  ASSERT_EQ(517UL, s.byteSize());

  ASSERT_EQ(0x13, result[0]);
  ASSERT_EQ(0x85, result[1]);
  ASSERT_EQ(0x04, result[2]);
  for (size_t i = 0; i < 128; ++i) {
    ASSERT_EQ(0x83, result[3 + i * 4]);
  }
  ASSERT_EQ(0x01, result[515]);
  ASSERT_EQ(0x80, result[516]);
}

TEST(BuilderTest, UInt) {
  uint64_t value = 0x12345678abcdef;
  Builder b;
  b.add(value);
  const uint8_t* result = b.start();
  ValueLength len = b.size();

  const uint8_t correct_result[] = {
    0x2e, 0xef, 0xcd, 0xab, 0x78, 0x56, 0x34, 0x12,
  };

  EXPECT_EQ(sizeof(correct_result), len);
  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(result), len),
            std::string_view(reinterpret_cast<const char*>(correct_result),
                             sizeof(correct_result)));
}

TEST(BuilderTest, IntPos) {
  int64_t value = 0x12345678abcdef;
  Builder b;
  b.add(value);
  const uint8_t* result = b.start();
  ValueLength len = b.size();

  const uint8_t correct_result[] = {
    0x26, 0xde, 0x9b, 0x57, 0xf1, 0xac, 0x68, 0x24,
  };

  EXPECT_EQ(sizeof(correct_result), len);
  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(result), len),
            std::string_view(reinterpret_cast<const char*>(correct_result),
                             sizeof(correct_result)));
}

TEST(BuilderTest, IntNeg) {
  int64_t value = -0x12345678abcdef;
  Builder b;
  b.add(value);
  const uint8_t* result = b.start();
  ValueLength len = b.size();

  const uint8_t correct_result[] = {
    0x26, 0xdd, 0x9b, 0x57, 0xf1, 0xac, 0x68, 0x24,
  };

  EXPECT_EQ(sizeof(correct_result), len);
  EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(result), len),
            std::string_view(reinterpret_cast<const char*>(correct_result),
                             sizeof(correct_result)));
}

TEST(BuilderTest, Int1Limits) {
  uint64_t values[] = {
    uint64_t{1} << 0,
    uint64_t{1} << 1,
    uint64_t{1} << 2,
    uint64_t{1} << 3,
    uint64_t{1} << 4,
    uint64_t{1} << 5,
    uint64_t{1} << 6,
    uint64_t{1} << 7,
    uint64_t{1} << 8,
    uint64_t{1} << 9,

    uint64_t{1} << 10,
    uint64_t{1} << 11,
    uint64_t{1} << 12,
    uint64_t{1} << 13,
    uint64_t{1} << 14,
    uint64_t{1} << 15,
    uint64_t{1} << 16,
    uint64_t{1} << 17,
    uint64_t{1} << 18,
    uint64_t{1} << 19,

    uint64_t{1} << 20,
    uint64_t{1} << 21,
    uint64_t{1} << 22,
    uint64_t{1} << 23,
    uint64_t{1} << 24,
    uint64_t{1} << 25,
    uint64_t{1} << 26,
    uint64_t{1} << 27,
    uint64_t{1} << 28,
    uint64_t{1} << 29,

    uint64_t{1} << 30,
    uint64_t{1} << 31,
    uint64_t{1} << 32,
    uint64_t{1} << 33,
    uint64_t{1} << 34,
    uint64_t{1} << 35,
    uint64_t{1} << 36,
    uint64_t{1} << 37,
    uint64_t{1} << 38,
    uint64_t{1} << 39,

    uint64_t{1} << 40,
    uint64_t{1} << 41,
    uint64_t{1} << 42,
    uint64_t{1} << 43,
    uint64_t{1} << 44,
    uint64_t{1} << 45,
    uint64_t{1} << 46,
    uint64_t{1} << 47,
    uint64_t{1} << 48,
    uint64_t{1} << 49,

    uint64_t{1} << 50,
    uint64_t{1} << 51,
    uint64_t{1} << 52,
    uint64_t{1} << 53,
    uint64_t{1} << 54,
    uint64_t{1} << 55,
    uint64_t{1} << 56,
    uint64_t{1} << 57,
    uint64_t{1} << 58,
    uint64_t{1} << 59,

    uint64_t{1} << 60,
    uint64_t{1} << 61,
    uint64_t{1} << 62,
    uint64_t{1} << 63,

    std::numeric_limits<uint64_t>::max() - 1,
  };
  for (size_t i = 0; i < std::size(values); ++i) {
    auto check = [](auto v) {
      Builder b;
      b.add(v);
      uint8_t* result = b.start();
      Slice s(result);
      if constexpr (std::is_signed_v<decltype(v)>) {
        ASSERT_TRUE(s.isInt() || s.isSmallInt()) << v;
        EXPECT_EQ(v, s.getIntUnchecked()) << v;
        EXPECT_EQ(v, s.getInt()) << v;
        if (-6 <= v && v <= 9) {
          EXPECT_TRUE(s.isSmallInt()) << v;
          EXPECT_EQ(s.byteSize(), 1) << v;
          EXPECT_EQ(v, s.getSmallIntUnchecked()) << v;
        } else {
          EXPECT_TRUE(s.isInt()) << v;
        }
      } else {
        ASSERT_TRUE(s.isUInt() || s.isSmallInt()) << v;
        EXPECT_EQ(v, s.getUInt()) << v;
        if (v <= 9) {
          EXPECT_TRUE(s.isSmallInt()) << v;
          EXPECT_EQ(s.byteSize(), 1) << v;
          EXPECT_EQ(v, s.getSmallIntUnchecked()) << v;
        } else {
          EXPECT_TRUE(s.isUInt()) << v;
          EXPECT_EQ(v, s.getUIntUnchecked()) << v;
        }
      }
    };
    const auto v = values[i];
    check(std::bit_cast<int64_t>(v - 1));
    check(std::bit_cast<int64_t>(v));
    check(std::bit_cast<int64_t>(v + 1));
    check(v - 1);
    check(v);
    check(v + 1);
  }
}

TEST(BuilderTest, StringChar) {
  const char* value = "der fuxx ging in den wald und aß pilze";
  Builder b;
  b.add(value);

  Slice slice = Slice(b.start());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(slice.stringView(), value);
  ASSERT_EQ(slice.stringViewUnchecked(), value);
  ASSERT_EQ(slice.copyString(), value);
}

TEST(BuilderTest, StringString) {
  const std::string value("der fuxx ging in den wald und aß pilze");
  Builder b;
  b.add(value);

  Slice slice = Slice(b.start());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(slice.stringView(), value);
  ASSERT_EQ(slice.stringViewUnchecked(), value);
  ASSERT_EQ(slice.copyString(), value);
}

TEST(BuilderTest, StringView) {
  const std::string_view value("der fuxx ging in den wald und aß pilze");
  Builder b;
  b.add(value);

  Slice slice = Slice(b.start());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(slice.stringView(), value);
  ASSERT_EQ(slice.stringViewUnchecked(), value);
  ASSERT_EQ(slice.copyString(), value);
}

class StringFromTwoParts final : public vpack::IStringFromParts {
 public:
  StringFromTwoParts(std::string_view part0, std::string_view part1) noexcept
    : _part0{part0}, _part1{part1} {}

  size_t numberOfParts() const final { return 2; }

  size_t totalLength() const final { return _part0.length() + _part1.length(); }

  std::string_view operator()(size_t index) const final {
    SDB_ASSERT(index < numberOfParts());
    if (index == 0) {
      return _part0;
    }
    return _part1;
  }

 private:
  std::string_view _part0;
  std::string_view _part1;
};

TEST(BuilderTest, String2Parts) {
  const std::string_view value1("der fuxx ging in den wald und aß pilze");
  const std::string_view value2("der hans, der hans, der kanns");
  const auto value = absl::StrCat(value1, value2);
  Builder b;
  b.add(StringFromTwoParts{value1, value2});

  Slice slice = Slice(b.start());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(slice.stringView(), value);
  ASSERT_EQ(slice.stringViewUnchecked(), value);
  ASSERT_EQ(slice.copyString(), value);
}

TEST(BuilderTest, String2PartsFromChars) {
  const char* value1("der fuxx ging in den wald und aß pilze");
  const char* value2("der hans, der hans, der kanns");
  const auto value = absl::StrCat(value1, value2);
  Builder b;
  b.add(StringFromTwoParts{value1, value2});

  Slice slice = Slice(b.start());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(slice.stringView(), value);
  ASSERT_EQ(slice.stringViewUnchecked(), value);
  ASSERT_EQ(slice.copyString(), value);
}

TEST(BuilderTest, String2PartsEmpty) {
  const std::string_view value1;
  const std::string_view value2;
  std::string_view value;
  Builder b;
  b.add(StringFromTwoParts{value1, value2});

  Slice slice = Slice(b.start());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(slice.stringView(), value);
  ASSERT_EQ(slice.stringViewUnchecked(), value);
  ASSERT_EQ(slice.copyString(), value);
}

TEST(BuilderTest, String2PartsLong) {
  const std::string_view value1(
    "der fuxx ging in den wald und aß pilze und findets ziemlich gut. ist "
    "halt ein fuxx, das ist so");
  const std::string_view value2(
    "der hans, der hans, der kanns, und der hund, der steht im wald und "
    "sieht den fuxx und findets auch sehr gut");
  const auto value = absl::StrCat(value1, value2);
  // long string offset
  ASSERT_GT(value.size(), 128);
  Builder b;
  b.add(StringFromTwoParts{value1, value2});

  Slice slice = Slice(b.start());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(slice.stringView(), value);
  ASSERT_EQ(slice.stringViewUnchecked(), value);
  ASSERT_EQ(slice.copyString(), value);
}

TEST(BuilderTest, String2PartsInObject) {
  const std::string_view value1("der fuxx ging in den wald und aß pilze");
  const std::string_view value2("der hans, der hans, der kanns");
  const std::string combined1 = std::string(value1) + std::string(value2);
  const std::string combined2 = std::string(value2) + std::string(value1);
  Builder b;
  b.openObject();
  b.add("foo", StringFromTwoParts(value2, value1));
  b.add("bar", StringFromTwoParts(value1, value2));
  b.close();

  Slice slice = Slice(b.start());
  ASSERT_TRUE(slice.isObject());

  std::string_view c = slice.get("foo").stringView();
  ASSERT_EQ(combined2.size(), c.size());
  ASSERT_EQ(combined2, c);

  c = slice.get("bar").stringView();
  ASSERT_EQ(combined1.size(), c.size());
  ASSERT_EQ(combined1, c);
}

TEST(BuilderTest, ShortStringViaValuePair) {
  const char* p = "the quick brown fox jumped over the lazy dog";

  Builder b;
  b.add(std::string_view{p});
  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {
    0xac, 0x74, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62,
    0x72, 0x6f, 0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d,
    0x70, 0x65, 0x64, 0x20, 0x6f, 0x76, 0x65, 0x72, 0x20, 0x74, 0x68, 0x65,
    0x20, 0x6c, 0x61, 0x7a, 0x79, 0x20, 0x64, 0x6f, 0x67,
  };

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));

  ASSERT_EQ(p, b.slice().copyString());
  ASSERT_EQ(std::string_view(p), b.slice().stringView());
}

TEST(BuilderTest, LongStringViaValuePair) {
  const char* p =
    "the quick brown fox jumped over the lazy dog, and it jumped and jumped "
    "and jumped and went on. But then, the String needed to get even longer "
    "and longer until the test finally worked.";

  Builder b;
  b.add(std::string_view{p});
  uint8_t* result = b.start();
  ValueLength len = b.size();

  static uint8_t gCorrectResult[] = {
    0xff, 0xb7, 0x00, 0x00, 0x00, 0x74, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69,
    0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78,
    0x20, 0x6a, 0x75, 0x6d, 0x70, 0x65, 0x64, 0x20, 0x6f, 0x76, 0x65, 0x72,
    0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79, 0x20, 0x64, 0x6f,
    0x67, 0x2c, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x69, 0x74, 0x20, 0x6a, 0x75,
    0x6d, 0x70, 0x65, 0x64, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x6a, 0x75, 0x6d,
    0x70, 0x65, 0x64, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x6a, 0x75, 0x6d, 0x70,
    0x65, 0x64, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x77, 0x65, 0x6e, 0x74, 0x20,
    0x6f, 0x6e, 0x2e, 0x20, 0x42, 0x75, 0x74, 0x20, 0x74, 0x68, 0x65, 0x6e,
    0x2c, 0x20, 0x74, 0x68, 0x65, 0x20, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67,
    0x20, 0x6e, 0x65, 0x65, 0x64, 0x65, 0x64, 0x20, 0x74, 0x6f, 0x20, 0x67,
    0x65, 0x74, 0x20, 0x65, 0x76, 0x65, 0x6e, 0x20, 0x6c, 0x6f, 0x6e, 0x67,
    0x65, 0x72, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x6c, 0x6f, 0x6e, 0x67, 0x65,
    0x72, 0x20, 0x75, 0x6e, 0x74, 0x69, 0x6c, 0x20, 0x74, 0x68, 0x65, 0x20,
    0x74, 0x65, 0x73, 0x74, 0x20, 0x66, 0x69, 0x6e, 0x61, 0x6c, 0x6c, 0x79,
    0x20, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x64, 0x2e,
  };

  ASSERT_EQ(sizeof(gCorrectResult), len);
  ASSERT_EQ(0, memcmp(result, gCorrectResult, len));

  ASSERT_EQ(p, b.slice().copyString());
  ASSERT_EQ(std::string_view(p), b.slice().stringView());
}

TEST(BuilderTest, AddOnNonArray) {
  Builder b;
  b.add(Value(ValueType::Object));
  ASSERT_VPACK_EXCEPTION(b.add(true), Exception::kBuilderKeyMustBeString);
}

TEST(BuilderTest, AddOnNonObject) {
  Builder b;
  b.add(Value(ValueType::Array));
  ASSERT_VPACK_EXCEPTION(b.add("foo", true), Exception::kBuilderNeedOpenObject);
}

TEST(BuilderTest, StartCalledOnOpenObject) {
  Builder b;
  b.add(Value(ValueType::Object));
  ASSERT_VPACK_EXCEPTION(b.start(), Exception::kBuilderNotSealed);
}

TEST(BuilderTest, StartCalledOnOpenObjectWithSubs) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(Value(ValueType::Array));
  b.add(1);
  b.add(2);
  b.close();
  ASSERT_VPACK_EXCEPTION(b.start(), Exception::kBuilderNotSealed);
}

TEST(BuilderTest, HasKeyNonObject) {
  Builder b;
  b.add(1);
  ASSERT_VPACK_EXCEPTION(b.getKey("foo"), Exception::kBuilderNeedOpenObject);
}

TEST(BuilderTest, HasKeyArray) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(1);
  ASSERT_VPACK_EXCEPTION(b.getKey("foo"), Exception::kBuilderNeedOpenObject);
}

TEST(BuilderTest, HasKeyEmptyObject) {
  Builder b;
  b.add(Value(ValueType::Object));
  ASSERT_FALSE(!b.getKey("foo").isNone());
  ASSERT_FALSE(!b.getKey("bar").isNone());
  ASSERT_FALSE(!b.getKey("baz").isNone());
  ASSERT_FALSE(!b.getKey("quetzalcoatl").isNone());
  b.close();
}

TEST(BuilderTest, HasKeySubObject) {
  Builder b;
  b.add(Value(ValueType::Object));
  b.add("foo", 1);
  b.add("bar", true);
  ASSERT_TRUE(!b.getKey("foo").isNone());
  ASSERT_TRUE(!b.getKey("bar").isNone());
  ASSERT_FALSE(!b.getKey("baz").isNone());

  b.add("bark", Value(ValueType::Object));
  ASSERT_FALSE(!b.getKey("bark").isNone());
  ASSERT_FALSE(!b.getKey("foo").isNone());
  ASSERT_FALSE(!b.getKey("bar").isNone());
  ASSERT_FALSE(!b.getKey("baz").isNone());
  b.close();

  ASSERT_TRUE(!b.getKey("foo").isNone());
  ASSERT_TRUE(!b.getKey("bar").isNone());
  ASSERT_TRUE(!b.getKey("bark").isNone());
  ASSERT_FALSE(!b.getKey("baz").isNone());

  b.add("baz", 42);
  ASSERT_TRUE(!b.getKey("foo").isNone());
  ASSERT_TRUE(!b.getKey("bar").isNone());
  ASSERT_TRUE(!b.getKey("bark").isNone());
  ASSERT_TRUE(!b.getKey("baz").isNone());
  b.close();
}

TEST(BuilderTest, HasKeyCompact) {
  Builder b;
  b.add(Value(ValueType::Object, true));
  b.add("foo", 1);
  b.add("bar", true);
  ASSERT_TRUE(!b.getKey("foo").isNone());
  ASSERT_TRUE(!b.getKey("bar").isNone());
  ASSERT_FALSE(!b.getKey("baz").isNone());

  b.add("bark", Value(ValueType::Object, true));
  ASSERT_FALSE(!b.getKey("bark").isNone());
  ASSERT_FALSE(!b.getKey("foo").isNone());
  ASSERT_FALSE(!b.getKey("bar").isNone());
  ASSERT_FALSE(!b.getKey("baz").isNone());
  b.close();

  ASSERT_TRUE(!b.getKey("foo").isNone());
  ASSERT_TRUE(!b.getKey("bar").isNone());
  ASSERT_TRUE(!b.getKey("bark").isNone());
  ASSERT_FALSE(!b.getKey("baz").isNone());

  b.add("baz", 42);
  ASSERT_TRUE(!b.getKey("foo").isNone());
  ASSERT_TRUE(!b.getKey("bar").isNone());
  ASSERT_TRUE(!b.getKey("bark").isNone());
  ASSERT_TRUE(!b.getKey("baz").isNone());
  b.close();
}

TEST(BuilderTest, GetKeyNonObject) {
  Builder b;
  b.add(1);
  ASSERT_VPACK_EXCEPTION(b.getKey("foo"), Exception::kBuilderNeedOpenObject);
}

TEST(BuilderTest, GetKeyArray) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(1);
  ASSERT_VPACK_EXCEPTION(b.getKey("foo"), Exception::kBuilderNeedOpenObject);
}

TEST(BuilderTest, GetKeyEmptyObject) {
  Builder b;
  b.add(Value(ValueType::Object));
  ASSERT_TRUE(b.getKey("foo").isNone());
  ASSERT_TRUE(b.getKey("bar").isNone());
  ASSERT_TRUE(b.getKey("baz").isNone());
  ASSERT_TRUE(b.getKey("quetzalcoatl").isNone());
  b.close();
}

TEST(BuilderTest, GetKeySubObject) {
  Builder b;
  b.add(Value(ValueType::Object));
  b.add("foo", 1);
  b.add("bar", true);
  ASSERT_EQ(1UL, b.getKey("foo").getUInt());
  ASSERT_TRUE(b.getKey("bar").getBool());
  ASSERT_TRUE(b.getKey("baz").isNone());

  b.add("bark", Value(ValueType::Object));
  ASSERT_TRUE(b.getKey("bark").isNone());
  ASSERT_TRUE(b.getKey("foo").isNone());
  ASSERT_TRUE(b.getKey("bar").isNone());
  ASSERT_TRUE(b.getKey("baz").isNone());
  b.close();

  ASSERT_EQ(1UL, b.getKey("foo").getUInt());
  ASSERT_TRUE(b.getKey("bar").getBool());
  ASSERT_TRUE(b.getKey("baz").isNone());
  ASSERT_TRUE(b.getKey("bark").isObject());

  b.add("baz", 42);
  ASSERT_EQ(1UL, b.getKey("foo").getUInt());
  ASSERT_TRUE(b.getKey("bar").getBool());
  ASSERT_EQ(42UL, b.getKey("baz").getUInt());
  ASSERT_TRUE(b.getKey("bark").isObject());
  b.close();
}

TEST(BuilderTest, GetKeyCompact) {
  Builder b;
  b.add(Value(ValueType::Object, true));
  b.add("foo", 1);
  b.add("bar", true);
  ASSERT_EQ(1UL, b.getKey("foo").getUInt());
  ASSERT_TRUE(b.getKey("bar").getBool());
  ASSERT_TRUE(b.getKey("baz").isNone());

  b.add("bark", Value(ValueType::Object, true));
  ASSERT_FALSE(b.getKey("bark").isObject());
  ASSERT_TRUE(b.getKey("foo").isNone());
  ASSERT_TRUE(b.getKey("bar").isNone());
  ASSERT_TRUE(b.getKey("baz").isNone());
  ASSERT_TRUE(b.getKey("bark").isNone());
  b.close();

  ASSERT_EQ(1UL, b.getKey("foo").getUInt());
  ASSERT_TRUE(b.getKey("bar").getBool());
  ASSERT_TRUE(b.getKey("bark").isObject());
  ASSERT_TRUE(b.getKey("baz").isNone());

  b.add("baz", 42);
  ASSERT_EQ(1UL, b.getKey("foo").getUInt());
  ASSERT_TRUE(b.getKey("bar").getBool());
  ASSERT_TRUE(b.getKey("bark").isObject());
  ASSERT_EQ(42UL, b.getKey("baz").getUInt());
  b.close();
}

TEST(BuilderTest, IsClosedMixed) {
  Builder b;
  ASSERT_TRUE(b.isClosed());
  b.add(Value(ValueType::Null));
  ASSERT_TRUE(b.isClosed());
  b.add(true);
  ASSERT_TRUE(b.isClosed());

  b.add(Value(ValueType::Array));
  ASSERT_FALSE(b.isClosed());

  b.add(true);
  ASSERT_FALSE(b.isClosed());
  b.add(true);
  ASSERT_FALSE(b.isClosed());

  b.close();
  ASSERT_TRUE(b.isClosed());

  b.add(Value(ValueType::Object));
  ASSERT_FALSE(b.isClosed());

  b.add("foo", true);
  ASSERT_FALSE(b.isClosed());

  b.add("bar", true);
  ASSERT_FALSE(b.isClosed());

  b.add("baz", Value(ValueType::Array));
  ASSERT_FALSE(b.isClosed());

  b.close();
  ASSERT_FALSE(b.isClosed());

  b.close();
  ASSERT_TRUE(b.isClosed());
}

TEST(BuilderTest, IsClosedObject) {
  Builder b;
  ASSERT_TRUE(b.isClosed());
  b.add(Value(ValueType::Object));
  ASSERT_FALSE(b.isClosed());

  b.add("foo", true);
  ASSERT_FALSE(b.isClosed());

  b.add("bar", true);
  ASSERT_FALSE(b.isClosed());

  b.add("baz", Value(ValueType::Object));
  ASSERT_FALSE(b.isClosed());

  b.close();
  ASSERT_FALSE(b.isClosed());

  b.close();
  ASSERT_TRUE(b.isClosed());
}

TEST(BuilderTest, CloseClosed) {
  Builder b;
  ASSERT_TRUE(b.isClosed());
  b.add(Value(ValueType::Object));
  ASSERT_FALSE(b.isClosed());
  b.close();

  ASSERT_VPACK_EXCEPTION(b.close(), Exception::kBuilderNeedOpenCompound);
}

TEST(BuilderTest, Clone) {
  Builder b;
  b.add(Value(ValueType::Object));
  b.add("foo", true);
  b.add("bar", false);
  b.add("baz", "foobarbaz");
  b.close();

  Slice s1(b.start());
  Builder clone = b.clone(s1);
  ASSERT_NE(s1.start(), clone.start());

  Slice s2(clone.start());

  ASSERT_TRUE(s1.isObject());
  ASSERT_TRUE(s2.isObject());
  ASSERT_EQ(3UL, s1.length());
  ASSERT_EQ(3UL, s2.length());

  ASSERT_TRUE(!s1.get("foo").isNone());
  ASSERT_TRUE(!s2.get("foo").isNone());
  ASSERT_NE(s1.get("foo").start(), s2.get("foo").start());
  ASSERT_TRUE(!s1.get("bar").isNone());
  ASSERT_TRUE(!s2.get("bar").isNone());
  ASSERT_NE(s1.get("bar").start(), s2.get("bar").start());
  ASSERT_TRUE(!s1.get("baz").isNone());
  ASSERT_TRUE(!s2.get("baz").isNone());
  ASSERT_NE(s1.get("baz").start(), s2.get("baz").start());
}

TEST(BuilderTest, CloneDestroyOriginal) {
  Builder clone;  // empty
  {
    Builder b;
    b.add(Value(ValueType::Object));
    b.add("foo", true);
    b.add("bar", false);
    b.add("baz", "foobarbaz");
    b.close();

    Slice s(b.start());
    clone = b.clone(s);
    ASSERT_NE(b.start(), clone.start());
    // now b goes out of scope. clone should survive!
  }

  Slice s(clone.start());
  ASSERT_TRUE(s.isObject());
  ASSERT_EQ(3UL, s.length());

  ASSERT_TRUE(!s.get("foo").isNone());
  ASSERT_TRUE(s.get("foo").getBool());
  ASSERT_TRUE(!s.get("bar").isNone());
  ASSERT_FALSE(s.get("bar").getBool());
  ASSERT_TRUE(!s.get("baz").isNone());
  ASSERT_EQ("foobarbaz", s.get("baz").copyString());
}

TEST(BuilderTest, ToString) {
  Builder b;
  b.add(Value(ValueType::Object));
  b.add("test1", 123);
  b.add("test2", "foobar");
  b.add("test3", true);
  b.close();

  ASSERT_EQ(
    "{\n  \"test1\" : 123,\n  \"test2\" : \"foobar\",\n  \"test3\" : true\n}",
    b.toString());
}

TEST(BuilderTest, ObjectBuilder) {
  Builder b;
  {
    ASSERT_TRUE(b.isClosed());

    ObjectBuilder ob(&b);
    ASSERT_EQ(&*ob, &b);
    ASSERT_FALSE(b.isClosed());
    ASSERT_FALSE(ob->isClosed());
    ob->add("foo", "aha");
    ob->add("bar", "qux");
    ASSERT_FALSE(ob->isClosed());
    ASSERT_FALSE(b.isClosed());
  }
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ("{\n  \"bar\" : \"qux\",\n  \"foo\" : \"aha\"\n}", b.toString());
}

TEST(BuilderTest, ObjectBuilderNested) {
  Builder b;
  {
    ASSERT_TRUE(b.isClosed());

    ObjectBuilder ob(&b);
    ASSERT_EQ(&*ob, &b);
    ASSERT_FALSE(b.isClosed());
    ASSERT_FALSE(ob->isClosed());
    ob->add("foo", "aha");
    ob->add("bar", "qux");
    {
      ObjectBuilder ob2(&b, "hans");
      ASSERT_EQ(&*ob2, &b);
      ASSERT_FALSE(ob2->isClosed());
      ASSERT_FALSE(ob->isClosed());
      ASSERT_FALSE(b.isClosed());

      ob2->add("bart", "a");
      ob2->add("zoo", "b");
    }
    {
      ObjectBuilder ob2(&b, "foobar");
      ASSERT_EQ(&*ob2, &b);
      ASSERT_FALSE(ob2->isClosed());
      ASSERT_FALSE(ob->isClosed());
      ASSERT_FALSE(b.isClosed());

      ob2->add("bark", 1);
      ob2->add("bonk", 2);
    }

    ASSERT_FALSE(ob->isClosed());
    ASSERT_FALSE(b.isClosed());
  }
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ(
    "{\n  \"bar\" : \"qux\",\n  \"foo\" : \"aha\",\n  \"foobar\" : {\n    "
    "\"bark\" : 1,\n    \"bonk\" : 2\n  },\n  \"hans\" : {\n    \"bart\" : "
    "\"a\",\n    \"zoo\" : \"b\"\n  }\n}",
    b.toString());
}

TEST(BuilderTest, ObjectBuilderNestedArrayInner) {
  Builder b;
  {
    ASSERT_TRUE(b.isClosed());

    ObjectBuilder ob(&b);
    ASSERT_EQ(&*ob, &b);
    ASSERT_FALSE(b.isClosed());
    ASSERT_FALSE(ob->isClosed());
    ob->add("foo", "aha");
    ob->add("bar", "qux");
    {
      ArrayBuilder ab2(&b, "hans");
      ASSERT_EQ(&*ab2, &b);
      ASSERT_FALSE(ab2->isClosed());
      ASSERT_FALSE(ob->isClosed());
      ASSERT_FALSE(b.isClosed());

      ab2->add("a");
      ab2->add("b");
    }
    {
      ArrayBuilder ab2(&b, "foobar");
      ASSERT_EQ(&*ab2, &b);
      ASSERT_FALSE(ab2->isClosed());
      ASSERT_FALSE(ob->isClosed());
      ASSERT_FALSE(b.isClosed());

      ab2->add(1);
      ab2->add(2);
    }

    ASSERT_FALSE(ob->isClosed());
    ASSERT_FALSE(b.isClosed());
  }
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ(
    "{\n  \"bar\" : \"qux\",\n  \"foo\" : \"aha\",\n  \"foobar\" : [\n    "
    "1,\n    2\n  ],\n  \"hans\" : [\n    \"a\",\n    \"b\"\n  ]\n}",
    b.toString());
}

TEST(BuilderTest, ObjectBuilderClosed) {
  Builder b;
  ASSERT_TRUE(b.isClosed());

  // when the object builder goes out of scope, it will close
  // the object. if we manually close it ourselves in addition,
  // this should not fail as we can't allow throwing destructors
  {
    ObjectBuilder ob(&b);
    ASSERT_EQ(&*ob, &b);
    ASSERT_FALSE(b.isClosed());
    ASSERT_FALSE(ob->isClosed());
    ob->add("foo", "aha");
    ob->add("bar", "qux");
    b.close();  // manually close the builder
    ASSERT_TRUE(b.isClosed());
  }
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ("{\n  \"bar\" : \"qux\",\n  \"foo\" : \"aha\"\n}", b.toString());
}

TEST(BuilderTest, ArrayBuilder) {
  Options options;
  Builder b(&options);
  {
    ASSERT_TRUE(b.isClosed());

    ArrayBuilder ob(&b);
    ASSERT_EQ(&*ob, &b);
    ASSERT_FALSE(b.isClosed());
    ASSERT_FALSE(ob->isClosed());
    ob->add("foo");
    ob->add("bar");
    ASSERT_FALSE(ob->isClosed());
    ASSERT_FALSE(b.isClosed());
  }
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ("[\n  \"foo\",\n  \"bar\"\n]", b.toString());
}

TEST(BuilderTest, ArrayBuilderNested) {
  Options options;
  Builder b(&options);
  {
    ASSERT_TRUE(b.isClosed());

    ArrayBuilder ob(&b);
    ASSERT_EQ(&*ob, &b);
    ASSERT_FALSE(b.isClosed());
    ASSERT_FALSE(ob->isClosed());
    ob->add("foo");
    ob->add("bar");
    {
      ArrayBuilder ob2(&b);
      ASSERT_EQ(&*ob2, &b);
      ASSERT_FALSE(ob2->isClosed());
      ASSERT_FALSE(ob->isClosed());
      ASSERT_FALSE(b.isClosed());

      ob2->add("bart");
      ob2->add("qux");
    }
    {
      ArrayBuilder ob2(&b);
      ASSERT_EQ(&*ob2, &b);
      ASSERT_FALSE(ob2->isClosed());
      ASSERT_FALSE(ob->isClosed());
      ASSERT_FALSE(b.isClosed());

      ob2->add(1);
      ob2->add(2);
    }
    ASSERT_FALSE(ob->isClosed());
    ASSERT_FALSE(b.isClosed());
  }
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ(
    "[\n  \"foo\",\n  \"bar\",\n  [\n    \"bart\",\n    \"qux\"\n  ],\n  [\n "
    "   1,\n    2\n  ]\n]",
    b.toString());
}

TEST(BuilderTest, ArrayBuilderClosed) {
  Options options;
  Builder b(&options);
  ASSERT_TRUE(b.isClosed());

  // when the array builder goes out of scope, it will close
  // the object. if we manually close it ourselves in addition,
  // this should not fail as we can't allow throwing destructors
  {
    ArrayBuilder ob(&b);
    ASSERT_EQ(&*ob, &b);
    ASSERT_FALSE(b.isClosed());
    ASSERT_FALSE(ob->isClosed());
    ob->add("foo");
    ob->add("bar");
    b.close();  // manually close the builder
    ASSERT_TRUE(ob->isClosed());
    ASSERT_TRUE(b.isClosed());
  }
  ASSERT_TRUE(b.isClosed());

  ASSERT_EQ("[\n  \"foo\",\n  \"bar\"\n]", b.toString());
}

TEST(BuilderTest, IsOpenObject) {
  Builder b;
  ASSERT_FALSE(b.isOpenObject());
  b.openObject();
  ASSERT_TRUE(b.isOpenObject());
  b.add("baz", "bark");
  ASSERT_TRUE(b.isOpenObject());
  b.add("bar", Value(ValueType::Object));
  ASSERT_TRUE(b.isOpenObject());
  b.close();
  ASSERT_TRUE(b.isOpenObject());
  b.close();
  ASSERT_FALSE(b.isOpenObject());
}

TEST(BuilderTest, IsOpenObjectNoObject) {
  {
    Builder b;
    b.add(Value(ValueType::Null));
    ASSERT_FALSE(b.isOpenObject());
  }

  {
    Builder b;
    b.add(false);
    ASSERT_FALSE(b.isOpenObject());
  }

  {
    Builder b;
    b.add(1234);
    ASSERT_FALSE(b.isOpenObject());
  }

  {
    Builder b;
    b.add("foobar");
    ASSERT_FALSE(b.isOpenObject());
  }

  {
    Builder b;
    b.openArray();
    ASSERT_FALSE(b.isOpenObject());
    b.close();
    ASSERT_FALSE(b.isOpenObject());
  }
}

TEST(BuilderTest, Data) {
  Builder b;
  ASSERT_NE(b.data(), nullptr);
  ASSERT_EQ(b.data(), b.buffer()->data());
}

TEST(BuilderTest, DataWithExternalBuffer) {
  BufferUInt8 buffer;
  Builder b(buffer);
  ASSERT_NE(b.data(), nullptr);
  ASSERT_EQ(b.data(), buffer.data());
}

TEST(BuilderTest, AddUnchecked) {
  Builder b;
  b.openObject();
  b.addUnchecked("baz", std::string_view{"bark"});
  b.addUnchecked("foo", std::string_view{"bar"});
  b.addUnchecked("qux", std::string_view{"b0rk"});
  b.close();
  ASSERT_EQ(R"({"baz":"bark","foo":"bar","qux":"b0rk"})", b.toJson());
}

TEST(BuilderTest, AddKeysSeparately1) {
  Builder b;
  b.openObject();
  b.add("name");
  b.add("Neunhoeffer");
  b.add("firstName");
  b.add("Max");
  b.close();
  ASSERT_EQ(R"({"firstName":"Max","name":"Neunhoeffer"})", b.toJson());
}

TEST(BuilderTest, AddKeysSeparately2) {
  Builder b;

  b.openObject();
  b.add("foo");
  b.openArray();
  b.close();

  b.add("bar");
  b.openObject();
  b.close();

  b.add("baz");
  uint8_t buf[] = {0x36 + 1};
  Slice s(buf);
  b.add(s);

  b.add("bumm");
  Options options;
  options.clear_builder_before_parse = false;
  Parser p(b, &options);
  p.parse("[13]");
  b.close();
  ASSERT_EQ(R"({"bar":{},"baz":1,"bumm":[13],"foo":[]})", b.toJson());
}

TEST(BuilderTest, AddKeysSeparatelyFail) {
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.add(false), Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::Null)),
                           Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::Array)),
                           Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.add(Value(ValueType::Object)),
                           Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.add(1.0), Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.add(1), Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.add(-112), Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.add(113), Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.openObject(), Exception::kBuilderNeedOpenArray);
  }
  {
    Builder b;
    b.openObject();
    ASSERT_VPACK_EXCEPTION(b.openArray(), Exception::kBuilderNeedOpenArray);
  }
  {
    Builder b;
    b.openObject();
    Options opt;
    opt.clear_builder_before_parse = false;
    Parser p(b, &opt);
    ASSERT_VPACK_EXCEPTION(p.parse("[13]"), Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    Options opt;
    opt.clear_builder_before_parse = false;
    Parser p(b, &opt);
    ASSERT_VPACK_EXCEPTION(p.parse("\"max\""),
                           Exception::kBuilderKeyMustBeString);
  }
  {
    Builder b;
    b.openObject();
    b.add("abc");
    ASSERT_VPACK_EXCEPTION(b.add("abc", 1),
                           Exception::kBuilderKeyAlreadyWritten);
  }
}

TEST(BuilderTest, HandInBuffer) {
  BufferUInt8 buf;
  {
    Builder b(buf);
    ASSERT_EQ(&Options::gDefaults, b.options);
    b.openObject();
    b.add("a", 123);
    b.add("b", "abc");
    b.close();
    Slice s(buf.data());
    ASSERT_TRUE(s.isObject());
    ASSERT_EQ(2UL, s.length());
    Slice ss = s.get("a");
    ASSERT_TRUE(ss.isInteger());
    ASSERT_EQ(123L, ss.getInt());
    ss = s.get("b");
    ASSERT_TRUE(ss.isString());
    ASSERT_EQ(std::string("abc"), ss.copyString());
  }
  // check that everthing is still valid
  Slice s(buf.data());
  ASSERT_TRUE(s.isObject());
  ASSERT_EQ(2UL, s.length());
  Slice ss = s.get("a");
  ASSERT_TRUE(ss.isInteger());
  ASSERT_EQ(123L, ss.getInt());
  ss = s.get("b");
  ASSERT_TRUE(ss.isString());
  ASSERT_EQ(std::string("abc"), ss.copyString());
}

TEST(BuilderTest, HandInBufferNoOptions) {
  BufferUInt8 buf;
  ASSERT_VPACK_EXCEPTION(new Builder(buf, nullptr), Exception::kInternalError);
}

TEST(BuilderTest, HandInBufferCustomOptions) {
  BufferUInt8 buf;

  {
    Options options;
    Builder b(buf, &options);
    ASSERT_EQ(&options, b.options);

    b.openObject();
    b.add("a", 123);
    b.add("b", "abc");
    b.close();
  }
  Slice s(buf.data());
  ASSERT_TRUE(s.isObject());
  ASSERT_EQ(2UL, s.length());
  Slice ss = s.get("a");
  ASSERT_TRUE(ss.isInteger());
  ASSERT_EQ(123L, ss.getInt());
  ss = s.get("b");
  ASSERT_TRUE(ss.isString());
  ASSERT_EQ(std::string("abc"), ss.copyString());
}

TEST(BuilderTest, HandInSharedBufferNoOptions) {
  auto buf = std::make_shared<BufferUInt8>();
  ASSERT_VPACK_EXCEPTION(new Builder(buf, nullptr), Exception::kInternalError);
}

TEST(BuilderTest, HandInSharedBufferCustomOptions) {
  auto buf = std::make_shared<BufferUInt8>();

  {
    Options options;
    Builder b(buf, &options);
    ASSERT_EQ(&options, b.options);

    b.openObject();
    b.add("a", 123);
    b.add("b", "abc");
    b.close();
  }
  Slice s(buf->data());
  ASSERT_TRUE(s.isObject());
  ASSERT_EQ(2UL, s.length());
  Slice ss = s.get("a");
  ASSERT_TRUE(ss.isInteger());
  ASSERT_EQ(123L, ss.getInt());
  ss = s.get("b");
  ASSERT_TRUE(ss.isString());
  ASSERT_EQ(std::string("abc"), ss.copyString());
}

TEST(BuilderTest, SliceEmpty) {
  Builder b;
  ASSERT_EQ(ValueType::None, b.slice().type());
  ASSERT_EQ(1ULL, b.slice().byteSize());
}

TEST(BuilderTest, AddKeyToNonObject) {
  Builder b;
  b.openArray();

  ASSERT_VPACK_EXCEPTION(b.add(std::string("bar"), "foobar"),
                         Exception::kBuilderNeedOpenObject);
}

TEST(BuilderTest, KeyWritten) {
  Builder b;
  b.openObject();
  b.add("foo");

  ASSERT_VPACK_EXCEPTION(b.add(std::string("bar"), "foobar"),
                         Exception::kBuilderKeyAlreadyWritten);
}

TEST(BuilderTest, EmptyAttributeNames) {
  Builder b;
  {
    ObjectBuilder guard(&b);
    b.add("", 123);
    b.add("a", "abc");
  }
  Slice s = b.slice();

  ASSERT_EQ(2UL, s.length());

  Slice ss = s.get("");
  ASSERT_FALSE(ss.isNone());
  ASSERT_TRUE(ss.isInteger());
  ASSERT_EQ(123L, ss.getInt());

  ss = s.get("a");
  ASSERT_FALSE(ss.isNone());
  ASSERT_TRUE(ss.isString());
  ASSERT_EQ(std::string("abc"), ss.copyString());

  ss = s.get("b");
  ASSERT_TRUE(ss.isNone());
}

TEST(BuilderTest, EmptyAttributeNamesNotThere) {
  Builder b;
  {
    ObjectBuilder guard(&b);
    b.add("b", 123);
    b.add("a", "abc");
  }
  Slice s = b.slice();

  ASSERT_EQ(2UL, s.length());

  Slice ss = s.get("b");
  ASSERT_FALSE(ss.isNone());
  ASSERT_TRUE(ss.isInteger());
  ASSERT_EQ(123L, ss.getInt());

  ss = s.get("a");
  ASSERT_FALSE(ss.isNone());
  ASSERT_TRUE(ss.isString());
  ASSERT_EQ(std::string("abc"), ss.copyString());

  ss = s.get("");
  ASSERT_TRUE(ss.isNone());
}

TEST(BuilderTest, AddHundredNones) {
  Builder b;
  b.openArray(false);
  for (size_t i = 0; i < 100; ++i) {
    b.add(Slice::noneSlice());
  }
  b.close();

  Slice s = b.slice();

  ASSERT_EQ(100UL, s.length());
  for (size_t i = 0; i < 100; ++i) {
    ASSERT_EQ(ValueType::None, s.at(i).type());
  }
}

TEST(BuilderTest, AddHundredNonesCompact) {
  Builder b;
  b.openArray(true);
  for (size_t i = 0; i < 100; ++i) {
    b.add(Slice::noneSlice());
  }
  b.close();

  Slice s = b.slice();

  ASSERT_EQ(100UL, s.length());
  for (size_t i = 0; i < 100; ++i) {
    ASSERT_EQ(ValueType::None, s.at(i).type());
  }
}

TEST(BuilderTest, AddThousandNones) {
  Builder b;
  b.openArray(false);
  for (size_t i = 0; i < 1000; ++i) {
    b.add(Slice::noneSlice());
  }
  b.close();

  Slice s = b.slice();

  ASSERT_EQ(1000UL, s.length());
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(ValueType::None, s.at(i).type());
  }
}

TEST(BuilderTest, AddThousandNonesCompact) {
  Builder b;
  b.openArray(true);
  for (size_t i = 0; i < 1000; ++i) {
    b.add(Slice::noneSlice());
  }
  b.close();

  Slice s = b.slice();

  ASSERT_EQ(1000UL, s.length());
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(ValueType::None, s.at(i).type());
  }
}

TEST(BuilderTest, UsePaddingForOneByteArray) {
  Options options;
  Builder b(&options);

  auto build = [&b]() {
    b.clear();
    b.openArray();

    for (size_t i = 0; i < 20; ++i) {
      b.add(i);
    }

    b.close();
    return b.slice().start();
  };

  auto test = [&b]() {
    for (uint64_t i = 0; i < 20; ++i) {
      ASSERT_EQ(i, b.slice().at(i).getUInt());
    }
  };

  options.padding_behavior = Options::PaddingBehavior::kNoPadding;
  const uint8_t* data = build();

  EXPECT_EQ(0x06, data[0]);
  EXPECT_EQ(0x35, data[1]);
  EXPECT_EQ(0x14, data[2]);
  EXPECT_EQ(0x36 + 0, data[3]);
  EXPECT_EQ(0x36 + 1, data[4]);
  EXPECT_EQ(0x36 + 2, data[5]);
  EXPECT_EQ(0x36 + 3, data[6]);
  EXPECT_EQ(0x36 + 4, data[7]);
  EXPECT_EQ(0x36 + 5, data[8]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kFlexible;
  data = build();

  EXPECT_EQ(0x06, data[0]);
  EXPECT_EQ(0x35, data[1]);
  EXPECT_EQ(0x14, data[2]);
  EXPECT_EQ(0x36 + 0, data[3]);
  EXPECT_EQ(0x36 + 1, data[4]);
  EXPECT_EQ(0x36 + 2, data[5]);
  EXPECT_EQ(0x36 + 3, data[6]);
  EXPECT_EQ(0x36 + 4, data[7]);
  EXPECT_EQ(0x36 + 5, data[8]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kUsePadding;
  data = build();

  EXPECT_EQ(0x06, data[0]);
  EXPECT_EQ(0x3b, data[1]);
  EXPECT_EQ(0x14, data[2]);
  EXPECT_EQ(0x00, data[3]);
  EXPECT_EQ(0x00, data[4]);
  EXPECT_EQ(0x00, data[5]);
  EXPECT_EQ(0x00, data[6]);
  EXPECT_EQ(0x00, data[7]);
  EXPECT_EQ(0x00, data[8]);
  EXPECT_EQ(0x36 + 0, data[9]);
  EXPECT_EQ(0x36 + 1, data[10]);

  test();
}

TEST(BuilderTest, UsePaddingForTwoByteArray) {
  Options options;
  Builder b(&options);

  auto build = [&b]() {
    b.clear();
    b.openArray();

    for (uint64_t i = 0; i < 260; ++i) {
      b.add(i);
    }

    b.close();
    return b.slice().start();
  };

  auto test = [&b]() {
    for (uint64_t i = 0; i < 260; ++i) {
      ASSERT_EQ(i, b.slice().at(i).getUInt());
    }
  };

  options.padding_behavior = Options::PaddingBehavior::kNoPadding;
  const uint8_t* data = build();

  EXPECT_EQ(0x07, data[0]);
  EXPECT_EQ(0x0f, data[1]);
  EXPECT_EQ(0x04, data[2]);
  EXPECT_EQ(0x04, data[3]);
  EXPECT_EQ(0x01, data[4]);
  EXPECT_EQ(0x36 + 0, data[5]);
  EXPECT_EQ(0x36 + 1, data[6]);
  EXPECT_EQ(0x36 + 2, data[7]);
  EXPECT_EQ(0x36 + 3, data[8]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kFlexible;
  data = build();

  EXPECT_EQ(0x07, data[0]);
  EXPECT_EQ(0x13, data[1]);
  EXPECT_EQ(0x04, data[2]);
  EXPECT_EQ(0x04, data[3]);
  EXPECT_EQ(0x01, data[4]);
  EXPECT_EQ(0x00, data[5]);
  EXPECT_EQ(0x00, data[6]);
  EXPECT_EQ(0x00, data[7]);
  EXPECT_EQ(0x00, data[8]);
  EXPECT_EQ(0x36 + 0, data[9]);
  EXPECT_EQ(0x36 + 1, data[10]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kUsePadding;
  data = build();

  EXPECT_EQ(0x07, data[0]);
  EXPECT_EQ(0x13, data[1]);
  EXPECT_EQ(0x04, data[2]);
  EXPECT_EQ(0x04, data[3]);
  EXPECT_EQ(0x01, data[4]);
  EXPECT_EQ(0x00, data[5]);
  EXPECT_EQ(0x00, data[6]);
  EXPECT_EQ(0x00, data[7]);
  EXPECT_EQ(0x00, data[8]);
  EXPECT_EQ(0x36 + 0, data[9]);
  EXPECT_EQ(0x36 + 1, data[10]);

  test();
}

TEST(BuilderTest, UsePaddingForEquallySizedArray) {
  Options options;
  Builder b(&options);

  auto build = [&b]() {
    b.clear();
    b.openArray();

    for (size_t i = 0; i < 3; ++i) {
      b.add(i);
    }

    b.close();
    return b.slice().start();
  };

  auto test = [&b]() {
    for (uint64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(i, b.slice().at(i).getUInt());
    }
  };

  options.padding_behavior = Options::PaddingBehavior::kNoPadding;
  const uint8_t* data = build();

  EXPECT_EQ(0x02, data[0]);
  EXPECT_EQ(0x05, data[1]);
  EXPECT_EQ(0x36 + 0, data[2]);
  EXPECT_EQ(0x36 + 1, data[3]);
  EXPECT_EQ(0x36 + 2, data[4]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kFlexible;
  data = build();

  EXPECT_EQ(0x02, data[0]);
  EXPECT_EQ(0x05, data[1]);
  EXPECT_EQ(0x36 + 0, data[2]);
  EXPECT_EQ(0x36 + 1, data[3]);
  EXPECT_EQ(0x36 + 2, data[4]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kUsePadding;
  data = build();

  EXPECT_EQ(0x05, data[0]);
  EXPECT_EQ(0x0c, data[1]);
  EXPECT_EQ(0x00, data[2]);
  EXPECT_EQ(0x00, data[3]);
  EXPECT_EQ(0x00, data[4]);
  EXPECT_EQ(0x00, data[5]);
  EXPECT_EQ(0x00, data[6]);
  EXPECT_EQ(0x00, data[7]);
  EXPECT_EQ(0x00, data[8]);
  EXPECT_EQ(0x36 + 0, data[9]);
  EXPECT_EQ(0x36 + 1, data[10]);
  EXPECT_EQ(0x36 + 2, data[11]);

  test();
}

TEST(BuilderTest, UsePaddingForOneByteObject) {
  Options options;
  Builder b(&options);

  auto build = [&b]() {
    b.clear();
    b.openObject();

    for (size_t i = 0; i < 10; ++i) {
      b.add(std::string("test") + std::to_string(i));
      b.add(i);
    }

    b.close();
    return b.slice().start();
  };

  auto test = [&b]() {
    for (uint64_t i = 0; i < 10; ++i) {
      std::string key = std::string("test") + std::to_string(i);
      ASSERT_TRUE(!b.slice().get(key).isNone());
      ASSERT_EQ(i, b.slice().get(key).getUInt());
    }
  };

  options.padding_behavior = Options::PaddingBehavior::kNoPadding;
  const uint8_t* data = build();

  EXPECT_EQ(0x0b, data[0]);
  EXPECT_EQ(0x53, data[1]);
  EXPECT_EQ(0x0a, data[2]);
  EXPECT_EQ(0x85, data[3]);
  EXPECT_EQ(0x74, data[4]);
  EXPECT_EQ(0x65, data[5]);
  EXPECT_EQ(0x73, data[6]);
  EXPECT_EQ(0x74, data[7]);
  EXPECT_EQ(0x30, data[8]);
  EXPECT_EQ(0x36 + 0, data[9]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kFlexible;
  data = build();

  EXPECT_EQ(0x0b, data[0]);
  EXPECT_EQ(0x53, data[1]);
  EXPECT_EQ(0x0a, data[2]);
  EXPECT_EQ(0x85, data[3]);
  EXPECT_EQ(0x74, data[4]);
  EXPECT_EQ(0x65, data[5]);
  EXPECT_EQ(0x73, data[6]);
  EXPECT_EQ(0x74, data[7]);
  EXPECT_EQ(0x30, data[8]);
  EXPECT_EQ(0x36 + 0, data[9]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kUsePadding;
  data = build();

  EXPECT_EQ(0x0b, data[0]);
  EXPECT_EQ(0x59, data[1]);
  EXPECT_EQ(0x0a, data[2]);
  EXPECT_EQ(0x00, data[3]);
  EXPECT_EQ(0x00, data[4]);
  EXPECT_EQ(0x00, data[5]);
  EXPECT_EQ(0x00, data[6]);
  EXPECT_EQ(0x00, data[7]);
  EXPECT_EQ(0x00, data[8]);
  EXPECT_EQ(0x85, data[9]);
  EXPECT_EQ(0x74, data[10]);

  test();
}

TEST(BuilderTest, UsePaddingForTwoByteObject) {
  Options options;
  Builder b(&options);

  auto build = [&b]() {
    b.clear();
    b.openObject();

    for (uint64_t i = 0; i < 260; ++i) {
      b.add(std::string("test") + std::to_string(i));
      b.add(i);
    }

    b.close();
    return b.slice().start();
  };

  auto test = [&b]() {
    for (uint64_t i = 0; i < 260; ++i) {
      std::string key = std::string("test") + std::to_string(i);
      ASSERT_TRUE(!b.slice().get(key).isNone());
      ASSERT_EQ(i, b.slice().get(key).getUInt());
    }
  };

  options.padding_behavior = Options::PaddingBehavior::kNoPadding;
  const uint8_t* data = build();

  ASSERT_EQ(0x0c, data[0]);
  ASSERT_EQ(0xc1, data[1]);
  ASSERT_EQ(0x0b, data[2]);
  ASSERT_EQ(0x04, data[3]);
  ASSERT_EQ(0x01, data[4]);
  ASSERT_EQ(0x85, data[5]);
  ASSERT_EQ(0x74, data[6]);
  ASSERT_EQ(0x65, data[7]);
  ASSERT_EQ(0x73, data[8]);
  ASSERT_EQ(0x74, data[9]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kFlexible;
  data = build();

  ASSERT_EQ(0x0c, data[0]);
  ASSERT_EQ(0xc5, data[1]);
  ASSERT_EQ(0x0b, data[2]);
  ASSERT_EQ(0x04, data[3]);
  ASSERT_EQ(0x01, data[4]);
  ASSERT_EQ(0x00, data[5]);
  ASSERT_EQ(0x00, data[6]);
  ASSERT_EQ(0x00, data[7]);
  ASSERT_EQ(0x00, data[8]);
  ASSERT_EQ(0x85, data[9]);
  ASSERT_EQ(0x74, data[10]);

  test();

  options.padding_behavior = Options::PaddingBehavior::kUsePadding;
  data = build();

  ASSERT_EQ(0x0c, data[0]);
  ASSERT_EQ(0xc5, data[1]);
  ASSERT_EQ(0x0b, data[2]);
  ASSERT_EQ(0x04, data[3]);
  ASSERT_EQ(0x01, data[4]);
  ASSERT_EQ(0x00, data[5]);
  ASSERT_EQ(0x00, data[6]);
  ASSERT_EQ(0x00, data[7]);
  ASSERT_EQ(0x00, data[8]);
  ASSERT_EQ(0x85, data[9]);
  ASSERT_EQ(0x74, data[10]);

  test();
}

TEST(BuilderTest, TestBoundariesWithPaddingButContainingNones) {
  Options options;
  Builder b(&options);

  auto fill = [&b](size_t n) {
    b.clear();
    b.openArray();
    b.add(Slice::noneSlice());
    for (size_t i = 0; i < n; ++i) {
      b.add(1);
    }
    b.add("the-quick-brown-foxx");
    b.close();
  };

  std::array<Options::PaddingBehavior, 3> behaviors = {
    Options::PaddingBehavior::kFlexible, Options::PaddingBehavior::kNoPadding,
    Options::PaddingBehavior::kUsePadding};

  for (const auto& behavior : behaviors) {
    options.padding_behavior = behavior;
    {
      fill(110);
      const uint8_t* data = b.slice().start();
      ASSERT_EQ(6U, data[0]);
      ASSERT_EQ(253U, data[1]);
      ASSERT_EQ(112U, data[2]);
      // padding
      ASSERT_EQ(0U, data[3]);
      ASSERT_EQ(0U, data[4]);
      ASSERT_EQ(0U, data[5]);
      ASSERT_EQ(0U, data[6]);
      ASSERT_EQ(0U, data[7]);
      ASSERT_EQ(0U, data[8]);
    }

    {
      fill(111);
      const uint8_t* data = b.slice().start();
      ASSERT_EQ(6U, data[0]);
      ASSERT_EQ(255U, data[1]);
      ASSERT_EQ(113U, data[2]);
      // padding
      ASSERT_EQ(0U, data[3]);
      ASSERT_EQ(0U, data[4]);
      ASSERT_EQ(0U, data[5]);
      ASSERT_EQ(0U, data[6]);
      ASSERT_EQ(0U, data[7]);
      ASSERT_EQ(0U, data[8]);
    }

    // switch to 2-byte offset sizes
    {
      fill(112);
      const uint8_t* data = b.slice().start();
      ASSERT_EQ(7U, data[0]);
      ASSERT_EQ(115U, data[1]);
      ASSERT_EQ(1U, data[2]);
      ASSERT_EQ(114U, data[3]);
      ASSERT_EQ(0U, data[4]);
      // padding, because of contained None slice
      ASSERT_EQ(0U, data[5]);
      ASSERT_EQ(0U, data[6]);
      ASSERT_EQ(0U, data[7]);
      ASSERT_EQ(0U, data[8]);
    }
  }
}

/*
TEST(BuilderTest, getSharedSliceEmpty) {
  SharedSlice ss;
  Builder b;
  ASSERT_EQ(1, b.buffer().use_count());
  const auto shared_slice = b.sharedSlice();
  ASSERT_EQ(1, b.buffer().use_count());
  ASSERT_EQ(0, shared_slice.buffer().use_count());
}

TEST(BuilderTest, getSharedSlice) {
  const auto check = [](Builder& b, const bool isSmall) {
    const auto slice = b.slice();
    ASSERT_EQ(1, b.buffer().use_count());
    const auto shared_slice = b.sharedSlice();
    ASSERT_EQ(1, b.buffer().use_count());
    ASSERT_EQ(1, shared_slice.buffer().use_count());
    ASSERT_FALSE(haveSameOwnership(b.buffer(), shared_slice.buffer()));
    ASSERT_EQ(slice.byteSize(), shared_slice.byteSize());
    ASSERT_EQ(
      0, memcmp(slice.start(), shared_slice.start().get(), slice.byteSize()));
  };

  auto small_builder = Builder{};
  auto large_builder = Builder{};

  // A buffer can take slices up to 192 bytes without allocating memory.
  // This will fit:
  small_builder.add(0);
  // This will not:
  large_builder.add(std::string(size_t{256}, 'x'));

  check(small_builder, true);
  check(large_builder, false);
}

TEST(BuilderTest, getSharedSliceOpen) {
  SharedSlice ss;
  Builder b;
  b.openObject();
  ASSERT_VPACK_EXCEPTION(ss = b.sharedSlice(), Exception::kBuilderNotSealed);
}

TEST(BuilderTest, stealSharedSlice) {
  const auto check = [](Builder& b, const bool is_small) {
    ASSERT_EQ(is_small, b.buffer()->usesLocalMemory());
    const auto buffer_copy = *b.buffer();
    const auto slice = Slice{buffer_copy.data()};
    ASSERT_EQ(1, b.buffer().use_count());
    const auto shared_slice = std::move(b).sharedSlice();
    ASSERT_EQ(nullptr, b.buffer());
    ASSERT_EQ(0, b.buffer().use_count());
    ASSERT_EQ(1, shared_slice.buffer().use_count());
    ASSERT_EQ(slice.byteSize(), shared_slice.byteSize());
    ASSERT_EQ(
      0, memcmp(slice.start(), shared_slice.start().get(), slice.byteSize()));
  };

  auto small_builder = Builder{};
  auto large_builder = Builder{};

  // A buffer can take slices up to 192 bytes without allocating memory.
  // This will fit:
  small_builder.add(0);
  // This will not:
  large_builder.add(std::string(size_t{256}, 'x'));

  check(small_builder, true);
  check(large_builder, false);
}
*/

TEST(BuilderTest, syntacticSugar) {
  Builder b;

  b.add(Value(ValueType::Object));
  b.add("b", 12);
  b.add("a", true);
  b.add("l", Value(ValueType::Array));
  b.add(1);
  b.add(2);
  b.add(3);
  b.close();
  b.add("name", "Gustav");
  b.add("type", StringFromTwoParts("the", "machine"));
  b.close();

  ASSERT_FALSE(b.isOpenObject());
  ASSERT_TRUE(b.slice().get("b").isInteger());
  ASSERT_EQ(12, b.slice().get("b").getInt());
  ASSERT_TRUE(b.slice().get("a").isBool());
  ASSERT_TRUE(b.slice().get("a").getBool());
  ASSERT_TRUE(b.slice().get("l").isArray());
  ASSERT_EQ(3, b.slice().get("l").length());
  ASSERT_TRUE(b.slice().get("name").isString());
  ASSERT_EQ("Gustav", b.slice().get("name").stringView());
  ASSERT_EQ("themachine", b.slice().get("type").stringView());
}
