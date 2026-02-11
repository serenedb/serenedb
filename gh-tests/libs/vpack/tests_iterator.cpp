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

#include <string>

#include "tests-common.h"

TEST(IteratorTest, IterateNonArray1) {
  const std::string value("null");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ArrayIterator(s), Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateNonArray2) {
  const std::string value("true");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ArrayIterator(s), Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateNonArray3) {
  const std::string value("1");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ArrayIterator(s), Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateNonArray4) {
  const std::string value("\"abc\"");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ArrayIterator(s), Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateNonArray5) {
  const std::string value("{}");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ArrayIterator(s), Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateArrayEmpty) {
  const std::string value("[]");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ArrayIterator it(s);
  ASSERT_EQ(0U, it.size());
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = *it, Exception::kIndexOutOfBounds);

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = (*it), Exception::kIndexOutOfBounds);
}

TEST(IteratorTest, IterateArrayEmptySpecial) {
  ArrayIterator it(ArrayIterator::Empty{});
  ASSERT_EQ(0U, it.size());
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = *it, Exception::kIndexOutOfBounds);

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = (*it), Exception::kIndexOutOfBounds);
}

TEST(IteratorTest, IterateArray) {
  const std::string value("[1,2,3,4,null,true,\"foo\",\"bar\"]");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ArrayIterator it(s);
  ASSERT_EQ(8U, it.size());

  ASSERT_TRUE(it.valid());
  Slice current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(1UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(2UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(3UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(4UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNull());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isBool());
  ASSERT_TRUE(current.getBool());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("foo", current.copyString());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("bar", current.copyString());

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = *it, Exception::kIndexOutOfBounds);
}

TEST(IteratorTest, IterateArrayForward) {
  const std::string value("[1,2,3,4,null,true,\"foo\",\"bar\"]");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ArrayIterator it(s);
  ASSERT_EQ(8U, it.size());

  ASSERT_TRUE(it.valid());
  Slice current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(1UL, current.getUInt());

  it.forward(1);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(2UL, current.getUInt());

  it.forward(1);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(3UL, current.getUInt());

  it.forward(1);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(4UL, current.getUInt());

  it.forward(2);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isBool());
  ASSERT_TRUE(current.getBool());

  it.forward(2);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("bar", current.copyString());

  it.forward(1);

  ASSERT_FALSE(it.valid());
  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = *it, Exception::kIndexOutOfBounds);
}

TEST(IteratorTest, IterateCompactArrayForward) {
  const std::string value("[1,2,3,4,null,true,\"foo\",\"bar\"]");

  Options options;
  options.build_unindexed_arrays = true;

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.start());

  ArrayIterator it(s);
  ASSERT_EQ(8U, it.size());

  ASSERT_TRUE(it.valid());
  Slice current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(1UL, current.getUInt());

  it.forward(1);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(2UL, current.getUInt());

  it.forward(1);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(3UL, current.getUInt());

  it.forward(1);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(4UL, current.getUInt());

  it.forward(2);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isBool());
  ASSERT_TRUE(current.getBool());

  it.forward(2);

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("bar", current.copyString());

  it.forward(1);

  ASSERT_FALSE(it.valid());
  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = *it, Exception::kIndexOutOfBounds);
}

TEST(IteratorTest, IterateSubArray) {
  const std::string value("[[1,2,3],[\"foo\",\"bar\"]]");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ArrayIterator it(s);
  ASSERT_EQ(2U, it.size());

  ASSERT_TRUE(it.valid());
  Slice current = *it;
  ASSERT_TRUE(current.isArray());

  ArrayIterator it2(current);
  ASSERT_EQ(3U, it2.size());
  ASSERT_TRUE(it2.valid());
  Slice sub = it2.value();
  ASSERT_TRUE(sub.isNumber());
  ASSERT_EQ(1UL, sub.getUInt());

  it2.next();

  ASSERT_TRUE(it2.valid());
  sub = it2.value();
  ASSERT_TRUE(sub.isNumber());
  ASSERT_EQ(2UL, sub.getUInt());

  it2.next();

  ASSERT_TRUE(it2.valid());
  sub = it2.value();
  ASSERT_TRUE(sub.isNumber());
  ASSERT_EQ(3UL, sub.getUInt());

  it2.next();
  ASSERT_FALSE(it2.valid());
  ASSERT_VPACK_EXCEPTION(std::ignore = it2.value(),
                         Exception::kIndexOutOfBounds);

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isArray());

  ArrayIterator it3(current);
  ASSERT_EQ(2U, it3.size());

  ASSERT_TRUE(it3.valid());
  sub = it3.value();
  ASSERT_TRUE(sub.isString());
  ASSERT_EQ("foo", sub.copyString());

  it3.next();

  ASSERT_TRUE(it3.valid());
  sub = it3.value();
  ASSERT_TRUE(sub.isString());
  ASSERT_EQ("bar", sub.copyString());

  it3.next();
  ASSERT_FALSE(it3.valid());
  ASSERT_VPACK_EXCEPTION(std::ignore = it3.value(),
                         Exception::kIndexOutOfBounds);

  it.next();
  ASSERT_FALSE(it.valid());
  ASSERT_VPACK_EXCEPTION(std::ignore = *it, Exception::kIndexOutOfBounds);
}

TEST(IteratorTest, IterateArrayUnsorted) {
  const std::string value("[1,2,3,4,null,true,\"foo\",\"bar\"]");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ArrayIterator it(s);
  ASSERT_EQ(8U, it.size());

  ASSERT_TRUE(it.valid());
  Slice current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(1UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(2UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(3UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(4UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isNull());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isBool());
  ASSERT_TRUE(current.getBool());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("foo", current.copyString());

  it.next();

  ASSERT_TRUE(it.valid());
  current = *it;
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("bar", current.copyString());

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = *it, Exception::kIndexOutOfBounds);
}

TEST(IteratorTest, IterateNonObject1) {
  const std::string value("null");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ObjectIterator(s, /*useSequentialIteration*/ true),
                         Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateNonObject2) {
  const std::string value("true");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ObjectIterator(s, /*useSequentialIteration*/ true),
                         Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateNonObject3) {
  const std::string value("1");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ObjectIterator(s, /*useSequentialIteration*/ true),
                         Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateNonObject4) {
  const std::string value("\"abc\"");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ObjectIterator(s, /*useSequentialIteration*/ true),
                         Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateNonObject5) {
  const std::string value("[]");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_VPACK_EXCEPTION(ObjectIterator(s, /*useSequentialIteration*/ true),
                         Exception::kInvalidValueType);
}

TEST(IteratorTest, IterateObjectEmpty) {
  const std::string value("{}");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ObjectIterator it(s, /*useSequentialIteration*/ true);
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = (*it).key, Exception::kIndexOutOfBounds);
  ASSERT_VPACK_EXCEPTION(std::ignore = (*it).value(),
                         Exception::kIndexOutOfBounds);

  it.next();
  ASSERT_FALSE(it.valid());
}

TEST(IteratorTest, IterateObject) {
  const std::string value(
    "{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":null,\"f\":true,\"g\":\"foo\","
    "\"h\":\"bar\"}");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ObjectIterator it(s, /*useSequentialIteration*/ false);

  ASSERT_TRUE(it.valid());
  Slice key = (*it).key;
  Slice current = (*it).value();
  ASSERT_EQ("a", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(1UL, current.getUInt());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("b", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(2UL, current.getUInt());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("c", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(3UL, current.getUInt());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("d", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(4UL, current.getUInt());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("e", key.copyString());
  ASSERT_TRUE(current.isNull());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("f", key.copyString());
  ASSERT_TRUE(current.isBool());
  ASSERT_TRUE(current.getBool());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("g", key.copyString());
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("foo", current.copyString());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("h", key.copyString());
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("bar", current.copyString());

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = (*it).key, Exception::kIndexOutOfBounds);
  ASSERT_VPACK_EXCEPTION(std::ignore = (*it).value(),
                         Exception::kIndexOutOfBounds);
}

TEST(IteratorTest, IterateObjectUnsorted) {
  Options options;

  const std::string value("{\"z\":1,\"y\":2,\"x\":3}");

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.start());

  ObjectIterator it(s, true);

  ASSERT_TRUE(it.valid());
  Slice key = (*it).key;
  Slice current = (*it).value();
  ASSERT_EQ("z", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(1UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("y", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(2UL, current.getUInt());

  it.next();

  ASSERT_TRUE(it.valid());
  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("x", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(3UL, current.getUInt());

  it.next();
  ASSERT_FALSE(it.valid());
}

TEST(IteratorTest, IterateObjectCompact) {
  Options options;
  options.build_unindexed_objects = true;

  const std::string value(
    "{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":null,\"f\":true,\"g\":\"foo\","
    "\"h\":\"bar\"}");

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_EQ(0x14, s.head());

  ObjectIterator it(s, /*useSequentialIteration*/ false);

  ASSERT_TRUE(it.valid());
  Slice key = (*it).key;
  Slice current = (*it).value();
  ASSERT_EQ("a", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(1UL, current.getUInt());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("b", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(2UL, current.getUInt());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("c", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(3UL, current.getUInt());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("d", key.copyString());
  ASSERT_TRUE(current.isNumber());
  ASSERT_EQ(4UL, current.getUInt());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("e", key.copyString());
  ASSERT_TRUE(current.isNull());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("f", key.copyString());
  ASSERT_TRUE(current.isBool());
  ASSERT_TRUE(current.getBool());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("g", key.copyString());
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("foo", current.copyString());

  it.next();
  ASSERT_TRUE(it.valid());

  key = (*it).key;
  current = (*it).value();
  ASSERT_EQ("h", key.copyString());
  ASSERT_TRUE(current.isString());
  ASSERT_EQ("bar", current.copyString());

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_VPACK_EXCEPTION(std::ignore = (*it).key, Exception::kIndexOutOfBounds);
  ASSERT_VPACK_EXCEPTION(std::ignore = (*it).value(),
                         Exception::kIndexOutOfBounds);
}

TEST(IteratorTest, IterateObjectKeys) {
  const std::string value(
    "{\"1foo\":\"bar\",\"2baz\":\"quux\",\"3number\":1,\"4boolean\":true,"
    "\"5empty\":null}");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  size_t state = 0;
  ObjectIterator it(s, /*useSequentialIteration*/ false);

  while (it.valid()) {
    Slice key((*it).key);
    Slice value((*it).value());

    switch (state++) {
      case 0:
        ASSERT_EQ("1foo", key.copyString());
        ASSERT_TRUE(value.isString());
        ASSERT_EQ("bar", value.copyString());
        break;
      case 1:
        ASSERT_EQ("2baz", key.copyString());
        ASSERT_TRUE(value.isString());
        ASSERT_EQ("quux", value.copyString());
        break;
      case 2:
        ASSERT_EQ("3number", key.copyString());
        ASSERT_TRUE(value.isNumber());
        ASSERT_EQ(1ULL, value.getUInt());
        break;
      case 3:
        ASSERT_EQ("4boolean", key.copyString());
        ASSERT_TRUE(value.isBool());
        ASSERT_TRUE(value.getBool());
        break;
      case 4:
        ASSERT_EQ("5empty", key.copyString());
        ASSERT_TRUE(value.isNull());
        break;
    }
    it.next();
  }

  ASSERT_EQ(5U, state);
}

TEST(IteratorTest, IterateObjectKeysCompact) {
  const std::string value(
    "{\"1foo\":\"bar\",\"2baz\":\"quux\",\"3number\":1,\"4boolean\":true,"
    "\"5empty\":null}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_EQ(0x14, s.head());

  size_t state = 0;
  ObjectIterator it(s, /*useSequentialIteration*/ false);

  while (it.valid()) {
    Slice key((*it).key);
    Slice value((*it).value());

    switch (state++) {
      case 0:
        ASSERT_EQ("1foo", key.copyString());
        ASSERT_TRUE(value.isString());
        ASSERT_EQ("bar", value.copyString());
        break;
      case 1:
        ASSERT_EQ("2baz", key.copyString());
        ASSERT_TRUE(value.isString());
        ASSERT_EQ("quux", value.copyString());
        break;
      case 2:
        ASSERT_EQ("3number", key.copyString());
        ASSERT_TRUE(value.isNumber());
        ASSERT_EQ(1ULL, value.getUInt());
        break;
      case 3:
        ASSERT_EQ("4boolean", key.copyString());
        ASSERT_TRUE(value.isBool());
        ASSERT_TRUE(value.getBool());
        break;
      case 4:
        ASSERT_EQ("5empty", key.copyString());
        ASSERT_TRUE(value.isNull());
        break;
    }
    it.next();
  }

  ASSERT_EQ(5U, state);
}

TEST(IteratorTest, IterateObjectValues) {
  const std::string value(
    "{\"1foo\":\"bar\",\"2baz\":\"quux\",\"3number\":1,\"4boolean\":true,"
    "\"5empty\":null}");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  std::vector<std::string> seen_keys;
  ObjectIterator it(s, /*useSequentialIteration*/ false);

  while (it.valid()) {
    seen_keys.emplace_back((*it).key.copyString());
    it.next();
  };

  ASSERT_EQ(5U, seen_keys.size());
  ASSERT_EQ("1foo", seen_keys[0]);
  ASSERT_EQ("2baz", seen_keys[1]);
  ASSERT_EQ("3number", seen_keys[2]);
  ASSERT_EQ("4boolean", seen_keys[3]);
  ASSERT_EQ("5empty", seen_keys[4]);
}

TEST(IteratorTest, IterateObjectValuesCompact) {
  const std::string value(
    "{\"1foo\":\"bar\",\"2baz\":\"quux\",\"3number\":1,\"4boolean\":true,"
    "\"5empty\":null}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_EQ(0x14, s.head());

  std::vector<std::string> seen_keys;
  ObjectIterator it(s, /*useSequentialIteration*/ false);

  while (it.valid()) {
    seen_keys.emplace_back((*it).key.copyString());
    it.next();
  };

  ASSERT_EQ(5U, seen_keys.size());
  ASSERT_EQ("1foo", seen_keys[0]);
  ASSERT_EQ("2baz", seen_keys[1]);
  ASSERT_EQ("3number", seen_keys[2]);
  ASSERT_EQ("4boolean", seen_keys[3]);
  ASSERT_EQ("5empty", seen_keys[4]);
}

TEST(IteratorTest, EmptyArrayIteratorRangeBasedFor) {
  const std::string value("[]");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  size_t seen = 0;
  for (auto it : ArrayIterator(s)) {
    ASSERT_TRUE(false);
    ASSERT_FALSE(it.isNumber());  // only in here to please the compiler
  }
  ASSERT_EQ(0UL, seen);
}

TEST(IteratorTest, ArrayIteratorRangeBasedFor) {
  const std::string value("[1,2,3,4,5]");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  size_t seen = 0;
  for (auto it : ArrayIterator(s)) {
    ASSERT_TRUE(it.isNumber());
    ASSERT_EQ(seen + 1, it.getUInt());
    ++seen;
  }
  ASSERT_EQ(5UL, seen);
}

TEST(IteratorTest, ArrayIteratorRangeBasedForConst) {
  const std::string value("[1,2,3,4,5]");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  size_t seen = 0;
  for (auto it : ArrayIterator(s)) {
    ASSERT_TRUE(it.isNumber());
    ASSERT_EQ(seen + 1, it.getUInt());
    ++seen;
  }
  ASSERT_EQ(5UL, seen);
}

TEST(IteratorTest, ArrayIteratorRangeBasedForConstRef) {
  const std::string value("[1,2,3,4,5]");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  size_t seen = 0;
  for (auto it : ArrayIterator(s)) {
    ASSERT_TRUE(it.isNumber());
    ASSERT_EQ(seen + 1, it.getUInt());
    ++seen;
  }
  ASSERT_EQ(5UL, seen);
}

TEST(IteratorTest, ArrayIteratorRangeBasedForCompact) {
  const std::string value("[1,2,3,4,5]");

  Options options;
  options.build_unindexed_arrays = true;

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_EQ(0x13, s.head());

  size_t seen = 0;
  for (auto it : ArrayIterator(s)) {
    ASSERT_TRUE(it.isNumber());
    ASSERT_EQ(seen + 1, it.getUInt());
    ++seen;
  }
  ASSERT_EQ(5UL, seen);
}

TEST(IteratorTest, ObjectIteratorRangeBasedForEmpty) {
  const std::string value("{}");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  for (auto it : ObjectIterator(s, /*useSequentialIteration*/ true)) {
    ASSERT_TRUE(false);
    ASSERT_FALSE(it.value().isNumber());  // only in here to please the compiler
  }
}

TEST(IteratorTest, ObjectIteratorRangeBasedFor) {
  const std::string value("{\"1foo\":1,\"2bar\":2,\"3qux\":3}");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  size_t seen = 0;
  for (auto it : ObjectIterator(s, /*useSequentialIteration*/ false)) {
    ASSERT_TRUE(it.key.isString());
    if (seen == 0) {
      ASSERT_EQ("1foo", it.key.copyString());
    } else if (seen == 1) {
      ASSERT_EQ("2bar", it.key.copyString());
    } else if (seen == 2) {
      ASSERT_EQ("3qux", it.key.copyString());
    }
    ASSERT_TRUE(it.value().isNumber());
    ASSERT_EQ(seen + 1, it.value().getUInt());
    ++seen;
  }
  ASSERT_EQ(3UL, seen);
}

TEST(IteratorTest, ObjectIteratorRangeBasedForConst) {
  const std::string value("{\"1foo\":1,\"2bar\":2,\"3qux\":3}");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  size_t seen = 0;
  for (auto it : ObjectIterator(s, /*useSequentialIteration*/ false)) {
    ASSERT_TRUE(it.key.isString());
    if (seen == 0) {
      ASSERT_EQ("1foo", it.key.copyString());
    } else if (seen == 1) {
      ASSERT_EQ("2bar", it.key.copyString());
    } else if (seen == 2) {
      ASSERT_EQ("3qux", it.key.copyString());
    }
    ASSERT_TRUE(it.value().isNumber());
    ASSERT_EQ(seen + 1, it.value().getUInt());
    ++seen;
  }
  ASSERT_EQ(3UL, seen);
}

TEST(IteratorTest, ObjectIteratorRangeBasedForConstRef) {
  const std::string value("{\"1foo\":1,\"2bar\":2,\"3qux\":3}");

  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  size_t seen = 0;
  for (auto it : ObjectIterator(s, /*useSequentialIteration*/ false)) {
    ASSERT_TRUE(it.key.isString());
    if (seen == 0) {
      ASSERT_EQ("1foo", it.key.copyString());
    } else if (seen == 1) {
      ASSERT_EQ("2bar", it.key.copyString());
    } else if (seen == 2) {
      ASSERT_EQ("3qux", it.key.copyString());
    }
    ASSERT_TRUE(it.value().isNumber());
    ASSERT_EQ(seen + 1, it.value().getUInt());
    ++seen;
  }
  ASSERT_EQ(3UL, seen);
}

TEST(IteratorTest, ObjectIteratorRangeBasedForCompact) {
  const std::string value("{\"1foo\":1,\"2bar\":2,\"3qux\":3}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.start());

  ASSERT_EQ(0x14, s.head());

  size_t seen = 0;
  for (auto it : ObjectIterator(s, /*useSequentialIteration*/ false)) {
    ASSERT_TRUE(it.key.isString());
    if (seen == 0) {
      ASSERT_EQ("1foo", it.key.copyString());
    } else if (seen == 1) {
      ASSERT_EQ("2bar", it.key.copyString());
    } else if (seen == 2) {
      ASSERT_EQ("3qux", it.key.copyString());
    }
    ASSERT_TRUE(it.value().isNumber());
    ASSERT_EQ(seen + 1, it.value().getUInt());
    ++seen;
  }
  ASSERT_EQ(3UL, seen);
}

TEST(IteratorTest, ArrayIteratorToStream) {
  const std::string value("[1,2,3,4,5]");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ArrayIterator it(s);

  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ArrayIterator 0 / 5]", result.str());
  }

  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ArrayIterator 1 / 5]", result.str());
  }
  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ArrayIterator 2 / 5]", result.str());
  }
  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ArrayIterator 3 / 5]", result.str());
  }
  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ArrayIterator 4 / 5]", result.str());
  }
  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ArrayIterator 5 / 5]", result.str());
  }
  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ArrayIterator 5 / 5]", result.str());
  }
}

TEST(IteratorTest, ObjectIteratorToStream) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3}");
  Parser parser;
  parser.parse(value);
  Slice s(parser.start());

  ObjectIterator it(s, /*useSequentialIteration*/ false);

  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ObjectIterator 0 / 3]", result.str());
  }

  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ObjectIterator 1 / 3]", result.str());
  }
  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ObjectIterator 2 / 3]", result.str());
  }
  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ObjectIterator 3 / 3]", result.str());
  }
  it.next();
  {
    std::ostringstream result;
    result << it;
    ASSERT_EQ("[ObjectIterator 3 / 3]", result.str());
  }
}
