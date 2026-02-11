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

#include <ostream>
#include <string>
#include <string_view>

#include "tests-common.h"

using namespace sdb::basics;

TEST(SinkTest, CharBufferSink) {
  StrSink s;
  auto& out = s.Impl();

  ASSERT_TRUE(out.empty());

  s.PushChr('x');
  ASSERT_EQ(1, out.size());

  out.clear();
  s.PushStr(std::string("foobarbaz"));
  ASSERT_EQ(9, out.size());

  out.clear();
  s.PushStr(std::string_view("foobarbaz"));
  ASSERT_EQ(9, out.size());

  out.clear();
  s.PushStr("foobarbaz");
  ASSERT_EQ(9, out.size());

  out.clear();
  s.PushStr("foobarbaz");
  ASSERT_EQ(9, out.size());
}

TEST(SinkTest, StringSink) {
  StrSink s;
  auto& out = s.Impl();

  ASSERT_TRUE(out.empty());

  s.PushChr('x');
  ASSERT_EQ(1, out.size());
  ASSERT_EQ("x", out);

  out.clear();
  s.PushStr(std::string("foobarbaz"));
  ASSERT_EQ(9, out.size());
  ASSERT_EQ("foobarbaz", out);

  out.clear();
  s.PushStr(std::string_view("foobarbaz"));
  ASSERT_EQ(9, out.size());
  ASSERT_EQ("foobarbaz", out);

  out.clear();
  s.PushStr("foobarbaz");
  ASSERT_EQ(9, out.size());
  ASSERT_EQ("foobarbaz", out);

  out.clear();
  s.PushStr("foobarbaz");
  ASSERT_EQ(9, out.size());
  ASSERT_EQ("foobarbaz", out);
}

TEST(SinkTest, SizeConstrainedStringSinkAlwaysEmpty) {
  StrSink s;
  auto& out = s.Impl();

  ASSERT_TRUE(out.empty());

  s.PushChr('x');
  ASSERT_FALSE(out.empty());
  ASSERT_EQ(1, s.Impl().size());

  s.PushStr("foobarbaz");
  ASSERT_FALSE(out.empty());
  ASSERT_EQ(10, s.Impl().size());

  s.PushStr(std::string_view("foobarbaz"));
  ASSERT_FALSE(out.empty());
  ASSERT_EQ(19, s.Impl().size());

  s.PushStr("123");
  ASSERT_FALSE(out.empty());
  ASSERT_EQ(22, s.Impl().size());
}

TEST(SinkTest, SizeConstrainedStringSinkSmall) {
  StrSink s;
  auto& out = s.Impl();

  ASSERT_TRUE(out.empty());

  s.PushChr('x');
  ASSERT_EQ("x", out);
  ASSERT_EQ(1, s.Impl().size());

  s.PushStr("foobarbaz");
  ASSERT_EQ("xfoobarbaz", out);
  ASSERT_EQ(10, s.Impl().size());

  s.PushStr("123");
  ASSERT_EQ("xfoobarbaz123", out);
  ASSERT_EQ(13, s.Impl().size());

  s.PushChr('y');
  ASSERT_EQ("xfoobarbaz123y", out);
  ASSERT_EQ(14, s.Impl().size());

  s.PushStr("123");
  ASSERT_EQ("xfoobarbaz123y123", out);
  ASSERT_EQ(17, s.Impl().size());

  s.PushStr(std::string_view("fuchs"));
  ASSERT_EQ("xfoobarbaz123y123fuchs", out);
  ASSERT_EQ(22, s.Impl().size());
}

TEST(SinkTest, SizeConstrainedStringSinkLarger) {
  StrSink s;
  auto& out = s.Impl();

  ASSERT_TRUE(out.empty());

  for (size_t i = 0; i < 4096; ++i) {
    s.PushChr('x');
    ASSERT_EQ(i + 1, out.size());
  }
}

TEST(SinkTest, SizeConstrainedStringSinkLongStringAppend) {
  StrSink s;
  auto& out = s.Impl();

  ASSERT_TRUE(out.empty());

  s.PushStr("meow");
  ASSERT_EQ(4, out.size());
  ASSERT_EQ(4, s.Impl().size());

  std::string append(16384, 'x');
  s.PushStr(append);
  ASSERT_EQ(16388, out.size());
  ASSERT_EQ(std::string("meow") + append, out);
}

TEST(SinkTest, StringLengthSink) {
  LenSink s;

  ASSERT_EQ(0, s.Impl());

  s.PushChr('x');
  ASSERT_EQ(1, s.Impl());

  s.PushStr(std::string("foobarbaz"));
  ASSERT_EQ(10, s.Impl());

  s.PushStr("foobarbaz");
  ASSERT_EQ(19, s.Impl());

  s.PushStr("foobarbaz");
  ASSERT_EQ(28, s.Impl());

  s.PushStr(std::string_view("foobarbaz"));
  ASSERT_EQ(37, s.Impl());
}

TEST(SinkTest, StringStreamSink) {
  StrSink s;
  auto& out = s.Impl();

  s.PushChr('x');
  ASSERT_EQ("x", out);

  s.PushStr(std::string("foobarbaz"));
  ASSERT_EQ("xfoobarbaz", out);

  s.PushStr("foobarbaz");
  ASSERT_EQ("xfoobarbazfoobarbaz", out);

  s.PushStr("foobarbaz");
  ASSERT_EQ("xfoobarbazfoobarbazfoobarbaz", out);

  s.PushStr(std::string_view("boofar"));
  ASSERT_EQ("xfoobarbazfoobarbazfoobarbazboofar", out);
}
