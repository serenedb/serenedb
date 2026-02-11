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

#include <iostream>
#include <ostream>
#include <string>

#include "tests-common.h"

TEST(BufferTest, CreateEmpty) {
  BufferUInt8 buffer;

  ASSERT_EQ(0UL, buffer.size());
  ASSERT_TRUE(buffer.empty());
  ASSERT_NE(nullptr, buffer.data());
}

TEST(BufferTest, CreateEmptyWithSize) {
  BufferUInt8 buffer(10);

  ASSERT_EQ(0UL, buffer.size());
  ASSERT_TRUE(buffer.empty());
  ASSERT_NE(nullptr, buffer.data());
}

TEST(BufferTest, CreateAndAppend) {
  const std::string value("this is a test string");
  BufferUInt8 buffer;

  buffer.append(value.c_str(), value.size());
  ASSERT_EQ(value.size(), buffer.size());
}

TEST(BufferTest, CreateAndAppendLong) {
  const std::string value("this is a test string");
  BufferUInt8 buffer;

  for (size_t i = 0; i < 1000; ++i) {
    buffer.append(value.c_str(), value.size());
  }

  ASSERT_EQ(1000 * value.size(), buffer.size());
}

TEST(BufferTest, CopyConstruct) {
  const std::string value("this is a test string");
  Buffer<char> buffer;
  buffer.append(value.c_str(), value.size());

  Buffer<char> buffer2(buffer);
  ASSERT_EQ(value.size(), buffer2.size());
  ASSERT_EQ(buffer.size(), buffer2.size());
  ASSERT_EQ(value, std::string(buffer2.data(), buffer2.size()));
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, CopyConstructLongValue) {
  const std::string value("this is a test string");

  Buffer<char> buffer;
  for (size_t i = 0; i < 1000; ++i) {
    buffer.append(value.c_str(), value.size());
  }

  Buffer<char> buffer2(buffer);
  ASSERT_EQ(1000 * value.size(), buffer2.size());
  ASSERT_EQ(buffer.size(), buffer2.size());
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, CopyAssign) {
  const std::string value("this is a test string");
  Buffer<char> buffer;
  buffer.append(value.c_str(), value.size());

  Buffer<char> buffer2;
  buffer2 = buffer;
  ASSERT_EQ(value.size(), buffer2.size());
  ASSERT_EQ(buffer.size(), buffer2.size());
  ASSERT_EQ(value, std::string(buffer2.data(), buffer2.size()));
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, CopyAssignLongValue) {
  const std::string value("this is a test string");

  Buffer<char> buffer;
  for (size_t i = 0; i < 1000; ++i) {
    buffer.append(value.c_str(), value.size());
  }

  Buffer<char> buffer2;
  buffer2 = buffer;
  ASSERT_EQ(1000 * value.size(), buffer2.size());
  ASSERT_EQ(buffer.size(), buffer2.size());
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, CopyAssignDiscardOwnValue) {
  const std::string value("this is a test string");

  Buffer<char> buffer;
  for (size_t i = 0; i < 1000; ++i) {
    buffer.append(value.c_str(), value.size());
  }

  Buffer<char> buffer2;
  for (size_t i = 0; i < 100; ++i) {
    buffer2.append(value.c_str(), value.size());
  }

  buffer2 = buffer;
  ASSERT_EQ(1000 * value.size(), buffer2.size());
  ASSERT_EQ(buffer.size(), buffer2.size());
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, MoveConstruct) {
  const std::string value("this is a test string");
  Buffer<char> buffer;
  buffer.append(value.c_str(), value.size());

  Buffer<char> buffer2(std::move(buffer));
  ASSERT_EQ(value.size(), buffer2.size());
  ASSERT_EQ(0UL, buffer.size());  // should be empty
  ASSERT_EQ(value, std::string(buffer2.data(), buffer2.size()));
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, MoveConstructLongValue) {
  const std::string value("this is a test string");

  Buffer<char> buffer;
  for (size_t i = 0; i < 1000; ++i) {
    buffer.append(value.c_str(), value.size());
  }

  Buffer<char> buffer2(std::move(buffer));
  ASSERT_EQ(1000 * value.size(), buffer2.size());
  ASSERT_EQ(0UL, buffer.size());  // should be empty
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, MoveAssign) {
  const std::string value("this is a test string");
  Buffer<char> buffer;
  buffer.append(value.c_str(), value.size());

  Buffer<char> buffer2;
  buffer2 = std::move(buffer);
  ASSERT_EQ(value.size(), buffer2.size());
  ASSERT_EQ(0UL, buffer.size());  // should be empty
  ASSERT_EQ(value, std::string(buffer2.data(), buffer2.size()));
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, MoveAssignLongValue) {
  const std::string value("this is a test string");

  Buffer<char> buffer;
  for (size_t i = 0; i < 1000; ++i) {
    buffer.append(value.c_str(), value.size());
  }

  Buffer<char> buffer2;
  buffer2 = std::move(buffer);
  ASSERT_EQ(1000 * value.size(), buffer2.size());
  ASSERT_EQ(0UL, buffer.size());  // should be empty
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, MoveAssignDiscardOwnValue) {
  const std::string value("this is a test string");

  Buffer<char> buffer;
  for (size_t i = 0; i < 1000; ++i) {
    buffer.append(value.c_str(), value.size());
  }

  Buffer<char> buffer2;
  for (size_t i = 0; i < 100; ++i) {
    buffer2.append(value.c_str(), value.size());
  }

  buffer2 = std::move(buffer);
  ASSERT_EQ(1000 * value.size(), buffer2.size());
  ASSERT_EQ(0UL, buffer.size());  // should be empty
  ASSERT_NE(buffer.data(), buffer2.data());
}

TEST(BufferTest, Clear) {
  BufferUInt8 buffer;

  buffer.clear();
  ASSERT_EQ(0UL, buffer.size());
  ASSERT_TRUE(buffer.empty());

  buffer.append("foobar", 6);

  buffer.clear();
  ASSERT_EQ(0UL, buffer.size());
  ASSERT_TRUE(buffer.empty());

  for (size_t i = 0; i < 256; ++i) {
    buffer.push_back('x');
  }

  ASSERT_EQ(256UL, buffer.size());
  ASSERT_FALSE(buffer.empty());

  buffer.clear();

  ASSERT_EQ(0UL, buffer.size());
  ASSERT_TRUE(buffer.empty());
}

TEST(BufferTest, SizeEmpty) {
  BufferUInt8 buffer;

  ASSERT_EQ(0UL, buffer.size());
  ASSERT_TRUE(buffer.empty());
}

TEST(BufferTest, SizeNonEmpty) {
  BufferUInt8 buffer;
  buffer.append("foobar", 6);

  ASSERT_EQ(6UL, buffer.size());
  ASSERT_TRUE(!buffer.empty());
}

TEST(BufferTest, SizeAfterClear) {
  BufferUInt8 buffer;
  buffer.append("foobar", 6);

  buffer.clear();
  ASSERT_EQ(0UL, buffer.size());
  ASSERT_TRUE(buffer.empty());
}

TEST(BufferTest, SizeAfterReset) {
  BufferUInt8 buffer;
  buffer.append("foobar", 6);

  buffer.reset();
  ASSERT_EQ(0UL, buffer.size());
  ASSERT_TRUE(buffer.empty());
}

TEST(BufferTest, VectorTest) {
  std::vector<BufferUInt8> buffers;

  Builder builder;
  builder.add("der hund, der ist so bunt");

  Slice s = builder.slice();
  ASSERT_TRUE(s.isString());
  BufferUInt8 b;
  b.append(s.start(), s.byteSize());

  buffers.push_back(b);

  BufferUInt8& last = buffers.back();
  Slice copy(last.data());
  ASSERT_TRUE(copy.isString());
  ASSERT_TRUE(copy.binaryEquals(s));
  ASSERT_EQ("der hund, der ist so bunt", copy.copyString());
}

TEST(BufferTest, VectorMoveTest) {
  std::vector<BufferUInt8> buffers;

  Builder builder;
  builder.add("der hund, der ist so bunt");

  Slice s = builder.slice();
  ASSERT_TRUE(s.isString());
  BufferUInt8 b;
  b.append(s.start(), s.byteSize());

  buffers.push_back(std::move(b));

  BufferUInt8& last = buffers.back();
  Slice copy(last.data());
  ASSERT_TRUE(copy.isString());
  ASSERT_TRUE(copy.binaryEquals(s));
  ASSERT_EQ(0UL, b.size());
}

TEST(BufferTest, PushBackTest) {
  BufferUInt8 buffer;

  buffer.push_back('x');
  ASSERT_EQ(1UL, buffer.size());
}

TEST(BufferTest, AppendUInt8Test) {
  BufferUInt8 buffer;

  const uint8_t value[] = "der hund, der ist so bunt";
  const char* p = reinterpret_cast<const char*>(value);
  buffer.append(value, std::strlen(p));
  ASSERT_EQ(std::strlen(p), buffer.size());
}

TEST(BufferTest, AppendCharTest) {
  BufferUInt8 buffer;

  const char* value = "der hund, der ist so bunt";
  buffer.append(value, std::strlen(value));
  ASSERT_EQ(std::strlen(value), buffer.size());
}

TEST(BufferTest, AppendStringTest) {
  BufferUInt8 buffer;

  const std::string value("der hund, der ist so bunt");
  buffer.append(value);
  ASSERT_EQ(value.size(), buffer.size());
}

TEST(BufferTest, AppendBufferTest) {
  BufferUInt8 buffer;

  BufferUInt8 original;
  const std::string value("der hund, der ist so bunt");
  original.append(value);

  buffer.append(original);
  ASSERT_EQ(original.size(), buffer.size());
}

TEST(BufferTest, SubscriptTest) {
  BufferUInt8 buffer;

  const std::string value("der hund, der ist so bunt");
  buffer.append(value);

  ASSERT_EQ('d', buffer[0]);
  ASSERT_EQ('e', buffer[1]);
  ASSERT_EQ('r', buffer[2]);
  ASSERT_EQ(' ', buffer[3]);
  ASSERT_EQ('h', buffer[4]);
  ASSERT_EQ('u', buffer[5]);
  ASSERT_EQ('n', buffer[6]);
  ASSERT_EQ('d', buffer[7]);

  ASSERT_EQ('b', buffer[21]);
  ASSERT_EQ('u', buffer[22]);
  ASSERT_EQ('n', buffer[23]);
  ASSERT_EQ('t', buffer[24]);
}

TEST(BufferTest, SubscriptModifyTest) {
  BufferUInt8 buffer;

  const std::string value("der hans");
  buffer.append(value);

  buffer[0] = 'a';
  buffer[1] = 'b';
  buffer[2] = 'c';
  buffer[3] = 'd';
  // buffer[4] = 'e';
  buffer[5] = 'f';
  buffer[6] = 'g';
  buffer[7] = 'h';

  ASSERT_EQ('a', buffer[0]);
  ASSERT_EQ('b', buffer[1]);
  ASSERT_EQ('c', buffer[2]);
  ASSERT_EQ('d', buffer[3]);
  ASSERT_EQ('h', buffer[4]);
  ASSERT_EQ('f', buffer[5]);
  ASSERT_EQ('g', buffer[6]);
  ASSERT_EQ('h', buffer[7]);
}

TEST(BufferTest, SubscriptAtTest) {
  BufferUInt8 buffer;

  const std::string value("der hund, der ist so bunt");
  buffer.append(value);

  ASSERT_EQ('d', buffer.at(0));
  ASSERT_EQ('e', buffer.at(1));
  ASSERT_EQ('r', buffer.at(2));
  ASSERT_EQ(' ', buffer.at(3));
  ASSERT_EQ('h', buffer.at(4));
  ASSERT_EQ('u', buffer.at(5));
  ASSERT_EQ('n', buffer.at(6));
  ASSERT_EQ('d', buffer.at(7));

  ASSERT_EQ('b', buffer.at(21));
  ASSERT_EQ('u', buffer.at(22));
  ASSERT_EQ('n', buffer.at(23));
  ASSERT_EQ('t', buffer.at(24));

  EXPECT_ANY_THROW(buffer.at(25));
  EXPECT_ANY_THROW(buffer.at(26));
  EXPECT_ANY_THROW(buffer.at(1000));

  buffer.reset();
  EXPECT_ANY_THROW(buffer.at(0));
}

TEST(BufferTest, SubscriptAtModifyTest) {
  BufferUInt8 buffer;

  const std::string value("der hans");
  buffer.append(value);

  buffer.at(0) = 'a';
  buffer.at(1) = 'b';
  buffer.at(2) = 'c';
  buffer.at(3) = 'd';
  // buffer.at(4) = 'e';
  buffer.at(5) = 'f';
  buffer.at(6) = 'g';
  buffer.at(7) = 'h';

  ASSERT_EQ('a', buffer.at(0));
  ASSERT_EQ('b', buffer.at(1));
  ASSERT_EQ('c', buffer.at(2));
  ASSERT_EQ('d', buffer.at(3));
  ASSERT_EQ('h', buffer.at(4));
  ASSERT_EQ('f', buffer.at(5));
  ASSERT_EQ('g', buffer.at(6));
  ASSERT_EQ('h', buffer.at(7));

  EXPECT_ANY_THROW(buffer.at(8));
  EXPECT_ANY_THROW(buffer.at(9));
  EXPECT_ANY_THROW(buffer.at(1000));

  buffer.reset();
  EXPECT_ANY_THROW(buffer.at(0));
}

TEST(BufferTest, ResetToTest) {
  BufferUInt8 buffer;

  ASSERT_ANY_THROW(buffer.resetTo(256));
  buffer.resetTo(0);
  ASSERT_EQ(
    std::string(),
    std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()));

  buffer.push_back('a');
  ASSERT_EQ(
    std::string("a"),
    std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()));

  ASSERT_ANY_THROW(buffer.resetTo(256));

  buffer.resetTo(1);
  ASSERT_EQ(
    std::string("a"),
    std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()));
  buffer.resetTo(0);
  ASSERT_EQ(
    std::string(),
    std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()));

  buffer.append("foobar");
  ASSERT_EQ(
    std::string("foobar"),
    std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()));
  buffer.resetTo(3);

  ASSERT_EQ(
    std::string("foo"),
    std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()));
  buffer.resetTo(6);
  ASSERT_EQ(
    std::string("foobar"),
    std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()));

  buffer.resetTo(1);
  ASSERT_EQ(
    std::string("f"),
    std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()));
}

TEST(BufferTest, BufferNonDeleterTest) {
  BufferUInt8 buffer;
  for (size_t i = 0; i < 256; ++i) {
    buffer.push_back('x');
  }

  {
    std::shared_ptr<BufferUInt8> ptr{std::shared_ptr<BufferUInt8>{}, &buffer};

    buffer.append("test");
    ptr.reset();
  }

  ASSERT_EQ(260, buffer.size());

  // make sure the Buffer is still usable
  for (size_t i = 0; i < 2048; ++i) {
    buffer.push_back('x');
  }

  ASSERT_EQ(2308, buffer.size());
}
