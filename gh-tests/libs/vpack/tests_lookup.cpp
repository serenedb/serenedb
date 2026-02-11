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

#include <fstream>
#include <ostream>
#include <string>

#include "tests-common.h"

TEST(LookupTest, HasKeyShortObject) {
  const std::string value(
    "{\"foo\":null,\"bar\":true,\"baz\":13.53,\"qux\":[1],\"quz\":{}}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_TRUE(s.hasKey("foo"));
  ASSERT_TRUE(s.hasKey("bar"));
  ASSERT_TRUE(s.hasKey("baz"));
  ASSERT_TRUE(s.hasKey("qux"));
  ASSERT_TRUE(s.hasKey("quz"));
  ASSERT_FALSE(s.hasKey("nada"));
  ASSERT_FALSE(s.hasKey("Foo"));
  ASSERT_FALSE(s.hasKey("food"));
  ASSERT_FALSE(s.hasKey("quxx"));
  ASSERT_FALSE(s.hasKey("q"));
  ASSERT_FALSE(s.hasKey(""));
}

TEST(LookupTest, HasKeyShortObjectCompact) {
  const std::string value(
    "{\"foo\":null,\"bar\":true,\"baz\":13.53,\"qux\":[1],\"quz\":{}}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);

  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x14, s.head());

  ASSERT_TRUE(s.hasKey("foo"));
  ASSERT_TRUE(s.hasKey("bar"));
  ASSERT_TRUE(s.hasKey("baz"));
  ASSERT_TRUE(s.hasKey("qux"));
  ASSERT_TRUE(s.hasKey("quz"));
  ASSERT_FALSE(s.hasKey("nada"));
  ASSERT_FALSE(s.hasKey("Foo"));
  ASSERT_FALSE(s.hasKey("food"));
  ASSERT_FALSE(s.hasKey("quxx"));
  ASSERT_FALSE(s.hasKey("q"));
  ASSERT_FALSE(s.hasKey(""));
}

TEST(LookupTest, HasKeyLongObject) {
  std::string value("{");
  for (size_t i = 4; i < 1024; ++i) {
    if (i > 4) {
      value.append(",");
    }
    value.append("\"test");
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_TRUE(s.hasKey("test4"));
  ASSERT_TRUE(s.hasKey("test10"));
  ASSERT_TRUE(s.hasKey("test42"));
  ASSERT_TRUE(s.hasKey("test100"));
  ASSERT_TRUE(s.hasKey("test932"));
  ASSERT_TRUE(s.hasKey("test1000"));
  ASSERT_TRUE(s.hasKey("test1023"));
  ASSERT_FALSE(s.hasKey("test0"));
  ASSERT_FALSE(s.hasKey("test1"));
  ASSERT_FALSE(s.hasKey("test2"));
  ASSERT_FALSE(s.hasKey("test3"));
  ASSERT_FALSE(s.hasKey("test1024"));
}

TEST(LookupTest, HasKeyLongObjectCompact) {
  std::string value("{");
  for (size_t i = 4; i < 1024; ++i) {
    if (i > 4) {
      value.append(",");
    }
    value.append("\"test");
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x14, s.head());

  ASSERT_TRUE(s.hasKey("test4"));
  ASSERT_TRUE(s.hasKey("test10"));
  ASSERT_TRUE(s.hasKey("test42"));
  ASSERT_TRUE(s.hasKey("test100"));
  ASSERT_TRUE(s.hasKey("test932"));
  ASSERT_TRUE(s.hasKey("test1000"));
  ASSERT_TRUE(s.hasKey("test1023"));
  ASSERT_FALSE(s.hasKey("test0"));
  ASSERT_FALSE(s.hasKey("test1"));
  ASSERT_FALSE(s.hasKey("test2"));
  ASSERT_FALSE(s.hasKey("test3"));
  ASSERT_FALSE(s.hasKey("test1024"));
}

TEST(LookupTest, HasKeySubattributes) {
  const std::string value(
    "{\"foo\":{\"bar\":1,\"bark\":[],\"baz\":{\"qux\":{\"qurz\":null}}}}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.get(std::vector<std::string>()),
                         Exception::kInvalidAttributePath);
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo"})).isNone());
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo", "bar"})).isNone());
  ASSERT_FALSE(!s.get(std::vector<std::string>({"boo"})).isNone());
  ASSERT_FALSE(!s.get(std::vector<std::string>({"boo", "far"})).isNone());
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo", "bark"})).isNone());
  ASSERT_FALSE(
    !s.get(std::vector<std::string>({"foo", "bark", "baz"})).isNone());
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo", "baz"})).isNone());
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo", "baz", "qux"})).isNone());
  ASSERT_TRUE(
    !s.get(std::vector<std::string>({"foo", "baz", "qux", "qurz"})).isNone());
  ASSERT_FALSE(
    !s.get(std::vector<std::string>({"foo", "baz", "qux", "qurk"})).isNone());
  ASSERT_FALSE(
    !s.get(std::vector<std::string>({"foo", "baz", "qux", "qurz", "p0rk"}))
       .isNone());
}

TEST(LookupTest, HasKeySubattributesCompact) {
  const std::string value(
    "{\"foo\":{\"bar\":1,\"bark\":[],\"baz\":{\"qux\":{\"qurz\":null}}}}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x14, s.head());

  ASSERT_VPACK_EXCEPTION(s.get(std::vector<std::string>()).isNone(),
                         Exception::kInvalidAttributePath);
  ASSERT_EQ(0x14, s.get(std::vector<std::string>({"foo"})).head());
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo"})).isNone());
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo", "bar"})).isNone());
  ASSERT_FALSE(!s.get(std::vector<std::string>({"boo"})).isNone());
  ASSERT_FALSE(!s.get(std::vector<std::string>({"boo", "far"})).isNone());
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo", "bark"})).isNone());
  ASSERT_FALSE(
    !s.get(std::vector<std::string>({"foo", "bark", "baz"})).isNone());
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo", "baz"})).isNone());
  ASSERT_TRUE(!s.get(std::vector<std::string>({"foo", "baz", "qux"})).isNone());
  ASSERT_TRUE(
    !s.get(std::vector<std::string>({"foo", "baz", "qux", "qurz"})).isNone());
  ASSERT_FALSE(
    !s.get(std::vector<std::string>({"foo", "baz", "qux", "qurk"})).isNone());
  ASSERT_FALSE(
    !s.get(std::vector<std::string>({"foo", "baz", "qux", "qurz", "p0rk"}))
       .isNone());
}

TEST(LookupTest, EmptyObject) {
  const std::string value("{}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Slice v;
  v = s.get("foo");
  ASSERT_TRUE(v.isNone());

  v = s.get("bar");
  ASSERT_TRUE(v.isNone());

  v = s.get("baz");
  ASSERT_TRUE(v.isNone());
}

TEST(LookupTest, AlmostEmptyObject) {
  const std::string value("{\"foo\":1}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Slice v;
  v = s.get("foo");
  ASSERT_TRUE(v.isInteger());
  ASSERT_EQ(1UL, v.getUInt());

  v = s.get("bar");
  ASSERT_TRUE(v.isNone());

  v = s.get("baz");
  ASSERT_TRUE(v.isNone());
}

TEST(LookupTest, LookupShortObject) {
  const std::string value(
    "{\"foo\":null,\"bar\":true,\"baz\":13.53,\"qux\":[1],\"quz\":{}}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Slice v;
  v = s.get("foo");
  ASSERT_TRUE(v.isNull());

  v = s.get("bar");
  ASSERT_TRUE(v.isBool());
  ASSERT_EQ(true, v.getBool());

  v = s.get("baz");
  ASSERT_TRUE(v.isDouble());
  ASSERT_DOUBLE_EQ(13.53, v.getDouble());

  v = s.get("qux");
  ASSERT_TRUE(v.isArray());
  ASSERT_EQ(v.type(), ValueType::Array);
  ASSERT_EQ(1ULL, v.length());

  v = s.get("quz");
  ASSERT_TRUE(v.isObject());
  ASSERT_EQ(v.type(), ValueType::Object);
  ASSERT_EQ(0ULL, v.length());

  // non-present attributes
  v = s.get("nada");
  ASSERT_TRUE(v.isNone());

  v = s.get(std::string("foo\0", 4));
  ASSERT_TRUE(v.isNone());

  v = s.get("Foo");
  ASSERT_TRUE(v.isNone());

  v = s.get("food");
  ASSERT_TRUE(v.isNone());

  v = s.get("");
  ASSERT_TRUE(v.isNone());
}

TEST(LookupTest, LookupShortObjectCompact) {
  const std::string value(
    "{\"foo\":null,\"bar\":true,\"baz\":13.53,\"qux\":[1],\"quz\":{}}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x14, s.head());

  Slice v;
  v = s.get("foo");
  ASSERT_TRUE(v.isNull());

  v = s.get("bar");
  ASSERT_TRUE(v.isBool());
  ASSERT_EQ(true, v.getBool());

  v = s.get("baz");
  ASSERT_TRUE(v.isDouble());
  ASSERT_DOUBLE_EQ(13.53, v.getDouble());

  v = s.get("qux");
  ASSERT_TRUE(v.isArray());
  ASSERT_EQ(v.type(), ValueType::Array);
  ASSERT_EQ(1ULL, v.length());

  v = s.get("quz");
  ASSERT_TRUE(v.isObject());
  ASSERT_EQ(v.type(), ValueType::Object);
  ASSERT_EQ(0ULL, v.length());

  // non-present attributes
  v = s.get("nada");
  ASSERT_TRUE(v.isNone());

  v = s.get(std::string("foo\0", 4));
  ASSERT_TRUE(v.isNone());

  v = s.get("Foo");
  ASSERT_TRUE(v.isNone());

  v = s.get("food");
  ASSERT_TRUE(v.isNone());

  v = s.get("");
  ASSERT_TRUE(v.isNone());
}

TEST(LookupTest, LookupSubattributes) {
  const std::string value(
    "{\"foo\":{\"bar\":1,\"bark\":[],\"baz\":{\"qux\":{\"qurz\":null}}}}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.get(std::vector<std::string>()),
                         Exception::kInvalidAttributePath);

  Slice v;
  v = s.get(std::vector<std::string>({"foo"}));
  ASSERT_TRUE(v.isObject());

  v = s.get(std::vector<std::string>({"foo", "bar"}));
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1ULL, v.getUInt());

  v = s.get(std::vector<std::string>({"boo"}));
  ASSERT_TRUE(v.isNone());

  v = s.get(std::vector<std::string>({"boo", "far"}));
  ASSERT_TRUE(v.isNone());

  v = s.get(std::vector<std::string>({"foo", "bark"}));
  ASSERT_TRUE(v.isArray());

  v = s.get(std::vector<std::string>({"foo", "bark", "baz"}));
  ASSERT_TRUE(v.isNone());

  v = s.get(std::vector<std::string>({"foo", "baz"}));
  ASSERT_TRUE(v.isObject());

  v = s.get(std::vector<std::string>({"foo", "baz", "qux"}));
  ASSERT_TRUE(v.isObject());

  v = s.get(std::vector<std::string>({"foo", "baz", "qux", "qurz"}));
  ASSERT_TRUE(v.isNull());

  v = s.get(std::vector<std::string>({"foo", "baz", "qux", "qurk"}));
  ASSERT_TRUE(v.isNone());

  v = s.get(std::vector<std::string>({"foo", "baz", "qux", "qurz", "p0rk"}));
  ASSERT_TRUE(v.isNone());
}

TEST(LookupTest, LookupSubattributesCompact) {
  const std::string value(
    "{\"foo\":{\"bar\":1,\"bark\":[],\"baz\":{\"qux\":{\"qurz\":null}}}}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x14, s.head());

  ASSERT_VPACK_EXCEPTION(s.get(std::vector<std::string>()),
                         Exception::kInvalidAttributePath);

  Slice v;
  v = s.get(std::vector<std::string>({"foo"}));
  ASSERT_TRUE(v.isObject());
  ASSERT_EQ(0x14, v.head());

  v = s.get(std::vector<std::string>({"foo", "bar"}));
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1ULL, v.getUInt());

  v = s.get(std::vector<std::string>({"boo"}));
  ASSERT_TRUE(v.isNone());

  v = s.get(std::vector<std::string>({"boo", "far"}));
  ASSERT_TRUE(v.isNone());

  v = s.get(std::vector<std::string>({"foo", "bark"}));
  ASSERT_TRUE(v.isArray());

  v = s.get(std::vector<std::string>({"foo", "bark", "baz"}));
  ASSERT_TRUE(v.isNone());

  v = s.get(std::vector<std::string>({"foo", "baz"}));
  ASSERT_TRUE(v.isObject());

  v = s.get(std::vector<std::string>({"foo", "baz", "qux"}));
  ASSERT_TRUE(v.isObject());

  v = s.get(std::vector<std::string>({"foo", "baz", "qux", "qurz"}));
  ASSERT_TRUE(v.isNull());

  v = s.get(std::vector<std::string>({"foo", "baz", "qux", "qurk"}));
  ASSERT_TRUE(v.isNone());

  v = s.get(std::vector<std::string>({"foo", "baz", "qux", "qurz", "p0rk"}));
  ASSERT_TRUE(v.isNone());
}

TEST(LookupTest, LookupLongObject) {
  std::string value("{");
  for (size_t i = 4; i < 1024; ++i) {
    if (i > 4) {
      value.append(",");
    }
    value.append("\"test");
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Slice v;
  v = s.get("test4");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(4ULL, v.getUInt());

  v = s.get("test10");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(10ULL, v.getUInt());

  v = s.get("test42");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(42ULL, v.getUInt());

  v = s.get("test100");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(100ULL, v.getUInt());

  v = s.get("test932");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(932ULL, v.getUInt());

  v = s.get("test1000");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1000ULL, v.getUInt());

  v = s.get("test1023");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1023ULL, v.getUInt());

  // none existing
  v = s.get("test0");
  ASSERT_TRUE(v.isNone());

  v = s.get("test1");
  ASSERT_TRUE(v.isNone());

  v = s.get("test1024");
  ASSERT_TRUE(v.isNone());
}

TEST(LookupTest, LookupLongObjectCompact) {
  std::string value("{");
  for (size_t i = 4; i < 1024; ++i) {
    if (i > 4) {
      value.append(",");
    }
    value.append("\"test");
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x14, s.head());

  Slice v;
  v = s.get("test4");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(4ULL, v.getUInt());

  v = s.get("test10");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(10ULL, v.getUInt());

  v = s.get("test42");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(42ULL, v.getUInt());

  v = s.get("test100");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(100ULL, v.getUInt());

  v = s.get("test932");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(932ULL, v.getUInt());

  v = s.get("test1000");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1000ULL, v.getUInt());

  v = s.get("test1023");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1023ULL, v.getUInt());

  // none existing
  v = s.get("test0");
  ASSERT_TRUE(v.isNone());

  v = s.get("test1");
  ASSERT_TRUE(v.isNone());

  v = s.get("test1024");
  ASSERT_TRUE(v.isNone());
}

TEST(LookupTest, LookupLongObjectUnsorted) {
  std::string value("{");
  for (size_t i = 4; i < 1024; ++i) {
    if (i > 4) {
      value.append(",");
    }
    value.append("\"test");
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Slice v;
  v = s.get("test4");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(4ULL, v.getUInt());

  v = s.get("test10");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(10ULL, v.getUInt());

  v = s.get("test42");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(42ULL, v.getUInt());

  v = s.get("test100");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(100ULL, v.getUInt());

  v = s.get("test932");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(932ULL, v.getUInt());

  v = s.get("test1000");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1000ULL, v.getUInt());

  v = s.get("test1023");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1023ULL, v.getUInt());

  // none existing
  v = s.get("test0");
  ASSERT_TRUE(v.isNone());

  v = s.get("test1");
  ASSERT_TRUE(v.isNone());

  v = s.get("test1024");
  ASSERT_TRUE(v.isNone());
}

TEST(LookupTest, LookupLinear) {
  std::string value("{");
  for (size_t i = 0; i < 4; ++i) {
    if (i > 0) {
      value.append(",");
    }
    value.append("\"test");
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Slice v;
  v = s.get("test0");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(0ULL, v.getUInt());

  v = s.get("test1");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1ULL, v.getUInt());

  v = s.get("test2");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(2ULL, v.getUInt());

  v = s.get("test3");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(3ULL, v.getUInt());
}

TEST(LookupTest, LookupBinary) {
  std::string value("{");
  for (size_t i = 0; i < 128; ++i) {
    if (i > 0) {
      value.append(",");
    }
    value.append("\"test");
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  for (size_t i = 0; i < 128; ++i) {
    std::string key = "test";
    key.append(std::to_string(i));
    Slice v = s.get(key);

    ASSERT_TRUE(v.isNumber());
    ASSERT_EQ(i, v.getUInt());
  }
}

TEST(LookupTest, LookupBinaryCompact) {
  std::string value("{");
  for (size_t i = 0; i < 128; ++i) {
    if (i > 0) {
      value.append(",");
    }
    value.append("\"test");
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x14, s.head());

  for (size_t i = 0; i < 128; ++i) {
    std::string key = "test";
    key.append(std::to_string(i));
    Slice v = s.get(key);

    ASSERT_TRUE(v.isNumber());
    ASSERT_EQ(i, v.getUInt());
  }
}

TEST(LookupTest, LookupBinarySamePrefix) {
  std::string value("{");
  for (size_t i = 0; i < 128; ++i) {
    if (i > 0) {
      value.append(",");
    }
    value.append("\"test");
    for (size_t j = 0; j < i; ++j) {
      value.append("x");
    }
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  for (size_t i = 0; i < 128; ++i) {
    std::string key = "test";
    for (size_t j = 0; j < i; ++j) {
      key.append("x");
    }
    Slice v = s.get(key);

    ASSERT_TRUE(v.isNumber());
    ASSERT_EQ(i, v.getUInt());
  }
}

TEST(LookupTest, LookupBinaryLongObject) {
  std::string value("{");
  for (size_t i = 0; i < 1127; ++i) {
    if (i > 0) {
      value.append(",");
    }
    value.append("\"test");
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.append("}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  for (size_t i = 0; i < 1127; ++i) {
    std::string key = "test";
    key.append(std::to_string(i));
    Slice v = s.get(key);

    ASSERT_TRUE(v.isNumber());
    ASSERT_EQ(i, v.getUInt());
  }
}

TEST(LookupTest, LookupInvalidTypeNull) {
  const std::string value("null");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.get("test"), Exception::kInvalidValueType);
}

TEST(LookupTest, LookupInvalidTypeArray) {
  const std::string value("[]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.get("test"), Exception::kInvalidValueType);
}

TEST(LookupTest, AtNull) {
  const std::string value("null");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.at(0), Exception::kInvalidValueType);
}

TEST(LookupTest, AtObject) {
  const std::string value("{}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.at(0), Exception::kInvalidValueType);
}

TEST(LookupTest, AtArray) {
  const std::string value("[1,2,3,4]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(1, s.at(0).getInt());
  ASSERT_EQ(2, s.at(1).getInt());
  ASSERT_EQ(3, s.at(2).getInt());
  ASSERT_EQ(4, s.at(3).getInt());

  ASSERT_VPACK_EXCEPTION(s.at(4), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, AtArrayCompact) {
  const std::string value("[1,2,3,4]");

  Options options;
  options.build_unindexed_arrays = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x13, s.head());

  ASSERT_EQ(1, s.at(0).getInt());
  ASSERT_EQ(2, s.at(1).getInt());
  ASSERT_EQ(3, s.at(2).getInt());
  ASSERT_EQ(4, s.at(3).getInt());

  ASSERT_VPACK_EXCEPTION(s.at(4), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, AtArrayEmpty) {
  const std::string value("[]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.at(0), Exception::kIndexOutOfBounds);
  ASSERT_VPACK_EXCEPTION(s.at(1), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, KeyAtArray) {
  const std::string value("[]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.keyAt(0), Exception::kInvalidValueType);
}

TEST(LookupTest, KeyAtNull) {
  const std::string value("null");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.keyAt(0), Exception::kInvalidValueType);
}

TEST(LookupTest, KeyAtObject) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3,\"qux\":4}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ("bar", s.keyAt(0).copyString());
  ASSERT_EQ("baz", s.keyAt(1).copyString());
  ASSERT_EQ("foo", s.keyAt(2).copyString());
  ASSERT_EQ("qux", s.keyAt(3).copyString());

  ASSERT_VPACK_EXCEPTION(s.keyAt(4), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, KeyAtObjectSorted) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3,\"qux\":4}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ("bar", s.keyAt(0).copyString());
  ASSERT_EQ("baz", s.keyAt(1).copyString());
  ASSERT_EQ("foo", s.keyAt(2).copyString());
  ASSERT_EQ("qux", s.keyAt(3).copyString());

  ASSERT_VPACK_EXCEPTION(s.keyAt(4), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, KeyAtObjectCompact) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3,\"qux\":4}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x14, s.head());

  ASSERT_EQ("foo", s.keyAt(0).copyString());
  ASSERT_EQ("bar", s.keyAt(1).copyString());
  ASSERT_EQ("baz", s.keyAt(2).copyString());
  ASSERT_EQ("qux", s.keyAt(3).copyString());

  ASSERT_VPACK_EXCEPTION(s.keyAt(4), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, KeyAtObjectEmpty) {
  const std::string value("{}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.keyAt(0), Exception::kIndexOutOfBounds);
  ASSERT_VPACK_EXCEPTION(s.keyAt(1), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, ValueAtArray) {
  const std::string value("[]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.valueAt(0), Exception::kInvalidValueType);
}

TEST(LookupTest, ValueAtNull) {
  const std::string value("null");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.valueAt(0), Exception::kInvalidValueType);
}

TEST(LookupTest, ValueAtObject) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3,\"qux\":4}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(2, s.valueAt(0).getInt());
  ASSERT_EQ(3, s.valueAt(1).getInt());
  ASSERT_EQ(1, s.valueAt(2).getInt());
  ASSERT_EQ(4, s.valueAt(3).getInt());

  ASSERT_VPACK_EXCEPTION(s.valueAt(4), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, ValueAtObjectSorted) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3,\"qux\":4}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(2, s.valueAt(0).getInt());
  ASSERT_EQ(3, s.valueAt(1).getInt());
  ASSERT_EQ(1, s.valueAt(2).getInt());
  ASSERT_EQ(4, s.valueAt(3).getInt());

  ASSERT_VPACK_EXCEPTION(s.valueAt(4), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, ValueAtObjectCompact) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3,\"qux\":4}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_EQ(0x14, s.head());

  ASSERT_EQ(1, s.valueAt(0).getInt());
  ASSERT_EQ(2, s.valueAt(1).getInt());
  ASSERT_EQ(3, s.valueAt(2).getInt());
  ASSERT_EQ(4, s.valueAt(3).getInt());

  ASSERT_VPACK_EXCEPTION(s.valueAt(4), Exception::kIndexOutOfBounds);
}

TEST(LookupTest, ValueAtObjectEmpty) {
  const std::string value("{}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.valueAt(0), Exception::kIndexOutOfBounds);
  ASSERT_VPACK_EXCEPTION(s.valueAt(1), Exception::kIndexOutOfBounds);
}
