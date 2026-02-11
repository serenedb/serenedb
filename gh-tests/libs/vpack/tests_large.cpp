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

#include "tests-common.h"

TEST(CordDumperTest, AppendLarge) {
  auto write_values = [](auto& sink) {
    constexpr std::string_view kStr = "quick brown fox jompsfadjfaskdf";
    for (size_t i = 0; i < 150000; ++i) {
      sink.PushChr(static_cast<char>(i));
      sink.PushF64(static_cast<double>(i));
      sink.PushI64(i);
      sink.PushSpaces(i);
      sink.PushU64(i);
      sink.PushStr(kStr);
    }
  };

  auto with_duration = [](std::string_view msg, auto&& func) {
    const auto start = absl::Now();
    auto res = func();
    const auto duration = absl::Now() - start;
    std::cout << msg << " took: " << absl::ToDoubleMilliseconds(duration)
              << " ms\n";
    return res;
  };

  auto str_sink = with_duration("basics::StrSink", [&]() {
    sdb::basics::StrSink str_sink;
    write_values(str_sink);
    return str_sink;
  });

  auto cord_sink = with_duration("vpack::CordSink", [&]() {
    sdb::basics::CordSink cord_sink;
    write_values(cord_sink);
    cord_sink.Finish();
    return cord_sink;
  });

  std::string cord_string;
  absl::CopyCordToString(cord_sink.Impl(), &cord_string);
  ASSERT_TRUE(str_sink.Impl() == cord_string);
}

TEST(BuilderTest, FixedArraysSizes) {
  const ValueLength k_b = 1024;
  const ValueLength gb = 1024 * 1024 * 1024;
  const ValueLength nrs[] = {
    1,                     // bytelen < 256
    2,                     // 256 <= bytelen < 64k
    (64 * k_b) / 127 - 1,  // 256 <= bytelen < 64k
    (64 * k_b) / 127,      // 64k <= bytelen < 4G
    (4 * gb) / 127,        // 64k <= bytelen < 4G
    (4 * gb) / 127 + 1,    // 4G <= bytelen
  };
  const ValueLength byte_sizes[] = {
    1 + 1 + 1 * 127,
    1 + 8 + 2 * 127,
    1 + 8 + ((64 * k_b) / 127 - 1) * 127,
    1 + 8 + ((64 * k_b) / 127) * 127,
    1 + 8 + ((4 * gb) / 127) * 127,
    1 + 8 + ((4 * gb) / 127 + 1) * 127,
  };
  int nr = sizeof(nrs) / sizeof(ValueLength);

  std::string x;
  for (size_t i = 0; i < 128 - 2; i++) {
    x.push_back('x');
  }
  // Now x has length 128-2 and thus will use 127 bytes as an entry in an array

  for (int i = 0; i < nr; i++) {
    Builder b;
    b.reserve(byte_sizes[i]);
    b.add(Value(ValueType::Array));
    for (ValueLength j = 0; j < nrs[i]; j++) {
      b.add(x);
    }
    b.close();
    uint8_t* start = b.start();

    Slice s(start);
    CheckBuild(s, ValueType::Array, byte_sizes[i]);
    ASSERT_TRUE(0x02 <= *start && *start <= 0x05);  // Array without index tab
    ASSERT_TRUE(s.isArray());
    ASSERT_EQ(nrs[i], s.length());
    ASSERT_TRUE(s.at(0).isString());
    auto str = s.at(0).stringView();
    ASSERT_EQ(x.size(), str.size());
    ASSERT_EQ(x, str);
  }
}

TEST(BuilderTest, ArraysSizes) {
  const ValueLength k_b = 1024;
  const ValueLength gb = 1024 * 1024 * 1024;
  const ValueLength nrs[] = {
    1,                     // bytelen < 256
    2,                     // 256 <= bytelen < 64k
    (64 * k_b) / 129 - 1,  // 256 <= bytelen < 64k
    (64 * k_b) / 129,      // 64k <= bytelen < 4G
    (4 * gb) / 131,        // 64k <= bytelen < 4G
    (4 * gb) / 131 + 1,    // 4G <= bytelen
  };
  const ValueLength byte_sizes[] = {
    1 + 1 + 1 + 2 + 1 * 128,
    1 + 8 + 3 + 2 * 129,
    1 + 8 + 3 + ((64 * k_b) / 129 - 1) * 129,
    1 + 8 + 5 + ((64 * k_b) / 129) * 131,
    1 + 8 + 5 + ((4 * gb) / 131) * 131,
    1 + 8 + 9 + ((4 * gb) / 131 + 1) * 135 + 8,
  };
  int nr = sizeof(nrs) / sizeof(ValueLength);

  std::string x;
  for (size_t i = 0; i < 128 - 2; i++) {
    x.push_back('x');
  }
  // Now x has length 128-2 and thus will use 127 bytes as an entry in an array

  for (int i = 0; i < nr; i++) {
    Builder b;
    b.reserve(byte_sizes[i]);
    b.add(Value(ValueType::Array));
    b.add(1);
    for (ValueLength j = 0; j < nrs[i]; j++) {
      b.add(x);
    }
    b.close();
    uint8_t* start = b.start();

    Slice s(start);
    CheckBuild(s, ValueType::Array, byte_sizes[i]);
    ASSERT_TRUE(0x06 <= *start && *start <= 0x09);  // Array without index tab
    ASSERT_TRUE(s.isArray());
    ASSERT_EQ(nrs[i] + 1, s.length());
    ASSERT_TRUE(s.at(0).isSmallInt());
    ASSERT_EQ(1LL, s.at(0).getInt());
    ASSERT_TRUE(s.at(1).isString());
    auto str = s.at(1).stringView();
    ASSERT_EQ(x.size(), str.size());
    ASSERT_EQ(x, str);
  }
}

TEST(BuilderTest, ObjectsSizesSorted) {
  const ValueLength k_b = 1024;
  const ValueLength gb = 1024 * 1024 * 1024;
  const ValueLength nrs[] = {
    1,                     // bytelen < 256
    2,                     // 256 <= bytelen < 64k
    (64 * k_b) / 130,      // 256 <= bytelen < 64k
    (64 * k_b) / 130 + 1,  // 64k <= bytelen < 4G
    (4 * gb) / 132 - 1,    // 64k <= bytelen < 4G
    (4 * gb) / 132,        // 4G <= bytelen
  };
  const ValueLength byte_sizes[] = {
    1 + 1 + 1 + 1 + 1 * 128,
    1 + 8 + 2 * 130,
    1 + 8 + ((64 * k_b) / 130) * 130,
    1 + 8 + ((64 * k_b) / 130 + 1) * 132,
    1 + 8 + ((4 * gb) / 132 - 1) * 132,
    1 + 8 + ((4 * gb) / 132) * 136 + 8,
  };
  int nr = sizeof(nrs) / sizeof(ValueLength);

  std::string x;
  for (size_t i = 0; i < 118 - 1; i++) {
    x.push_back('x');
  }
  // Now x has length 118-1 and thus will use 118 bytes as an entry in an object
  // The attribute name generated below will use another 10.

  for (int i = 0; i < nr; i++) {
    Builder b;
    b.reserve(byte_sizes[i]);
    b.add(Value(ValueType::Object));
    for (ValueLength j = 0; j < nrs[i]; j++) {
      std::string attr_name = "axxxxxxxx";
      ValueLength n = j;
      for (int k = 8; k >= 1; k--) {
        attr_name[k] = (n % 26) + 'A';
        n /= 26;
      }
      b.add(attr_name, x);
    }
    b.close();
    uint8_t* start = b.start();

    Slice s(start);
    CheckBuild(s, ValueType::Object, byte_sizes[i]);
    if (nrs[i] == 1) {
      ASSERT_TRUE(*start = 0x14);
    } else {
      ASSERT_TRUE(0x0b <= *start && *start <= 0x0e);  // Object
    }
    ASSERT_TRUE(s.isObject());
    ASSERT_EQ(nrs[i], s.length());
    ASSERT_TRUE(s.get("aAAAAAAAA").isString());
    auto str = s.get("aAAAAAAAA").stringView();
    ASSERT_EQ(x.size(), str.size());
    ASSERT_EQ(x, str);
  }
}
