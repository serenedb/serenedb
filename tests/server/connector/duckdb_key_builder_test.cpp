////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/algorithm/container.h>

#include <cmath>
#include <cstdint>
#include <duckdb.hpp>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "connector/duckdb_key_builder.hpp"
#include "gtest/gtest.h"

namespace {

using namespace sdb::connector;
using duckdb::Value;

// Concatenates AppendDuckDBValueToKey output for each column value of each
// row, producing one key per row. DuckDBPrimaryKeyBuilder::BuildKeys could be
// used but it adds a constant table_id + column_id prefix and asserts a single
// value per row; the row-discriminating part under test is the concatenated
// suffix, so this helper exercises it directly and supports composite keys.
std::vector<std::string> BuildKeys(
  std::span<const std::vector<Value>> columns) {
  if (columns.empty()) {
    return {};
  }
  const size_t rows = columns.front().size();
  for (const auto& col : columns) {
    EXPECT_EQ(col.size(), rows);
  }
  std::vector<std::string> keys(rows);
  for (size_t r = 0; r < rows; ++r) {
    for (const auto& col : columns) {
      AppendDuckDBValueToKey(keys[r], col[r]);
    }
  }
  return keys;
}

std::vector<std::string> BuildKeys(
  std::initializer_list<std::vector<Value>> columns) {
  return BuildKeys(std::span<const std::vector<Value>>{columns.begin(),
                                                       columns.size()});
}

template<typename T>
Value MakeInt(T v) {
  if constexpr (std::is_same_v<T, int8_t>) {
    return Value::TINYINT(v);
  } else if constexpr (std::is_same_v<T, int16_t>) {
    return Value::SMALLINT(v);
  } else if constexpr (std::is_same_v<T, int32_t>) {
    return Value::INTEGER(v);
  } else if constexpr (std::is_same_v<T, int64_t>) {
    return Value::BIGINT(v);
  } else {
    static_assert(sizeof(T) == 0, "Unsupported integer type");
  }
}

template<typename T>
Value MakeFloat(T v) {
  if constexpr (std::is_same_v<T, float>) {
    return Value::FLOAT(v);
  } else if constexpr (std::is_same_v<T, double>) {
    return Value::DOUBLE(v);
  } else {
    static_assert(sizeof(T) == 0, "Unsupported float type");
  }
}

Value MakeBlob(std::string_view s) {
  return Value::BLOB(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

class DuckDBKeyBuilderTest : public ::testing::Test {
 public:
  template<typename T>
  static std::vector<Value> MakeIntColumn(std::initializer_list<T> values) {
    std::vector<Value> out;
    out.reserve(values.size());
    for (T v : values) {
      out.push_back(MakeInt<T>(v));
    }
    return out;
  }

  template<typename T>
  static std::vector<Value> MakeFloatColumn(std::initializer_list<T> values) {
    std::vector<Value> out;
    out.reserve(values.size());
    for (T v : values) {
      out.push_back(MakeFloat<T>(v));
    }
    return out;
  }

  template<typename T>
  void FloatColumnSortabilityTest() {
    static_assert(std::is_floating_point_v<T>);
    auto col = MakeFloatColumn<T>(
      {-std::numeric_limits<T>::infinity(), std::numeric_limits<T>::lowest(),
       std::numeric_limits<T>::lowest() / T{2}, -std::numeric_limits<T>::min(),
       -std::numeric_limits<T>::denorm_min(), T{0},
       std::numeric_limits<T>::denorm_min(), std::numeric_limits<T>::min(),
       std::numeric_limits<T>::max() / T{2}, std::numeric_limits<T>::max(),
       std::numeric_limits<T>::infinity(),
       std::numeric_limits<T>::quiet_NaN()});
    auto keys = BuildKeys({col});
    ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
  }

  template<typename T>
  void IntegerColumnSortabilityTest() {
    static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>);
    auto col = MakeIntColumn<T>(
      {std::numeric_limits<T>::min(),
       static_cast<T>(std::numeric_limits<T>::min() + 1),
       static_cast<T>(std::numeric_limits<T>::min() / 2), static_cast<T>(-1),
       T{0}, T{1}, static_cast<T>(std::numeric_limits<T>::max() / 2),
       static_cast<T>(std::numeric_limits<T>::max() - 1),
       std::numeric_limits<T>::max()});
    auto keys = BuildKeys({col});
    ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
  }

  // Strict-increase comparator: requires every adjacent pair to be strictly
  // ordered. Our test inputs build distinct full-row keys by construction,
  // so any violation signals a real encoding bug rather than a tie.
  static constexpr auto kKeyCheck = [](const auto& lhs, const auto& rhs) {
    return lhs <= rhs;
  };
};

TEST_F(DuckDBKeyBuilderTest, test_singleFloat) {
  const auto payloaded_nan_123 = std::nanf("123");
  const auto payloaded_nan_456 = std::nanf("456");
  auto keys = BuildKeys({MakeFloatColumn<float>(
    {-std::numeric_limits<float>::quiet_NaN(),
     -std::numeric_limits<float>::signaling_NaN(), -payloaded_nan_123,
     -payloaded_nan_456, std::numeric_limits<float>::quiet_NaN(),
     std::numeric_limits<float>::signaling_NaN(), payloaded_nan_123,
     payloaded_nan_456, -std::numeric_limits<float>::infinity(),
     std::numeric_limits<float>::infinity(), -0.f, +0.f, 1234.5f, -1234.5f})});

  constexpr std::string_view kPositiveNan{"\xFF\xC0\x00\x00", 4};
  ASSERT_EQ(keys[0], kPositiveNan);
  ASSERT_EQ(keys[1], kPositiveNan);
  ASSERT_EQ(keys[2], kPositiveNan);
  ASSERT_EQ(keys[3], kPositiveNan);
  ASSERT_EQ(keys[4], kPositiveNan);
  ASSERT_EQ(keys[5], kPositiveNan);
  ASSERT_EQ(keys[6], kPositiveNan);
  ASSERT_EQ(keys[7], kPositiveNan);

  constexpr std::string_view kNegativeInf{"\x00\x7F\xFF\xFF", 4};
  constexpr std::string_view kPositiveInf{"\xFF\x80\x00\x00", 4};
  ASSERT_EQ(keys[8], kNegativeInf);
  ASSERT_EQ(keys[9], kPositiveInf);

  constexpr std::string_view kZero{"\x80\x00\x00\x00", 4};
  ASSERT_EQ(keys[10], kZero);
  ASSERT_EQ(keys[11], kZero);

  constexpr std::string_view kPositive{"\xC4\x9A\x50\x00", 4};
  constexpr std::string_view kNegative{"\x3B\x65\xAF\xFF", 4};
  ASSERT_EQ(keys[12], kPositive);
  ASSERT_EQ(keys[13], kNegative);

  FloatColumnSortabilityTest<float>();
}

TEST_F(DuckDBKeyBuilderTest, test_singleDouble) {
  const auto payloaded_nan_123 = std::nan("123");
  const auto payloaded_nan_456 = std::nan("456");
  auto keys = BuildKeys({MakeFloatColumn<double>(
    {-std::numeric_limits<double>::quiet_NaN(),
     -std::numeric_limits<double>::signaling_NaN(), -payloaded_nan_123,
     -payloaded_nan_456, std::numeric_limits<double>::quiet_NaN(),
     std::numeric_limits<double>::signaling_NaN(), payloaded_nan_123,
     payloaded_nan_456, -std::numeric_limits<double>::infinity(),
     std::numeric_limits<double>::infinity(), -0., +0., 1234.5, -1234.5})});

  constexpr std::string_view kPositiveNan{"\xFF\xF8\x00\x00\x00\x00\x00\x00",
                                          8};
  ASSERT_EQ(keys[0], kPositiveNan);
  ASSERT_EQ(keys[1], kPositiveNan);
  ASSERT_EQ(keys[2], kPositiveNan);
  ASSERT_EQ(keys[3], kPositiveNan);
  ASSERT_EQ(keys[4], kPositiveNan);
  ASSERT_EQ(keys[5], kPositiveNan);
  ASSERT_EQ(keys[6], kPositiveNan);
  ASSERT_EQ(keys[7], kPositiveNan);

  constexpr std::string_view kNegativeInf{"\x00\x0F\xFF\xFF\xFF\xFF\xFF\xFF",
                                          8};
  constexpr std::string_view kPositiveInf{"\xFF\xF0\x00\x00\x00\x00\x00\x00",
                                          8};
  ASSERT_EQ(keys[8], kNegativeInf);
  ASSERT_EQ(keys[9], kPositiveInf);

  constexpr std::string_view kZero{"\x80\x00\x00\x00\x00\x00\x00\x00", 8};
  ASSERT_EQ(keys[10], kZero);
  ASSERT_EQ(keys[11], kZero);

  constexpr std::string_view kPositive{"\xC0\x93\x4A\x00\x00\x00\x00\x00", 8};
  constexpr std::string_view kNegative{"\x3F\x6C\xB5\xFF\xFF\xFF\xFF\xFF", 8};
  ASSERT_EQ(keys[12], kPositive);
  ASSERT_EQ(keys[13], kNegative);

  FloatColumnSortabilityTest<double>();
}

TEST_F(DuckDBKeyBuilderTest, test_singleTinyInteger) {
  auto keys = BuildKeys({MakeIntColumn<int8_t>(
    {std::numeric_limits<int8_t>::min(), -5, 0, 5,
     std::numeric_limits<int8_t>::max()})});
  ASSERT_EQ(keys[0], std::string_view("\x00", 1));
  ASSERT_EQ(keys[1], std::string_view("\x7B", 1));
  ASSERT_EQ(keys[2], std::string_view("\x80", 1));
  ASSERT_EQ(keys[3], std::string_view("\x85", 1));
  ASSERT_EQ(keys[4], std::string_view("\xFF", 1));
  IntegerColumnSortabilityTest<int8_t>();
}

TEST_F(DuckDBKeyBuilderTest, test_singleSmlInteger) {
  auto keys = BuildKeys({MakeIntColumn<int16_t>(
    {std::numeric_limits<int16_t>::min(), -5, 0, 5,
     std::numeric_limits<int16_t>::max()})});
  ASSERT_EQ(keys[0], std::string_view("\x00\x00", 2));
  ASSERT_EQ(keys[1], std::string_view("\x7F\xFB", 2));
  ASSERT_EQ(keys[2], std::string_view("\x80\x00", 2));
  ASSERT_EQ(keys[3], std::string_view("\x80\x05", 2));
  ASSERT_EQ(keys[4], std::string_view("\xFF\xFF", 2));
  IntegerColumnSortabilityTest<int16_t>();
}

TEST_F(DuckDBKeyBuilderTest, test_singleNrmInteger) {
  auto keys = BuildKeys({MakeIntColumn<int32_t>(
    {std::numeric_limits<int32_t>::min(), -5, 0, 5,
     std::numeric_limits<int32_t>::max()})});
  ASSERT_EQ(keys[0], std::string_view("\x00\x00\x00\x00", 4));
  ASSERT_EQ(keys[1], std::string_view("\x7F\xFF\xFF\xFB", 4));
  ASSERT_EQ(keys[2], std::string_view("\x80\x00\x00\x00", 4));
  ASSERT_EQ(keys[3], std::string_view("\x80\x00\x00\x05", 4));
  ASSERT_EQ(keys[4], std::string_view("\xFF\xFF\xFF\xFF", 4));
  IntegerColumnSortabilityTest<int32_t>();
}

TEST_F(DuckDBKeyBuilderTest, test_singleBigInteger) {
  auto keys = BuildKeys({MakeIntColumn<int64_t>(
    {std::numeric_limits<int64_t>::min(), -5, 0, 5,
     std::numeric_limits<int64_t>::max()})});
  ASSERT_EQ(keys[0], std::string_view("\x00\x00\x00\x00\x00\x00\x00\x00", 8));
  ASSERT_EQ(keys[1], std::string_view("\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFB", 8));
  ASSERT_EQ(keys[2], std::string_view("\x80\x00\x00\x00\x00\x00\x00\x00", 8));
  ASSERT_EQ(keys[3], std::string_view("\x80\x00\x00\x00\x00\x00\x00\x05", 8));
  ASSERT_EQ(keys[4], std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8));
  IntegerColumnSortabilityTest<int64_t>();
}

TEST_F(DuckDBKeyBuilderTest, test_multiSizeIntegerPositive) {
  auto keys = BuildKeys(
    {MakeIntColumn<int8_t>({0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7,
                            std::numeric_limits<int8_t>::max()}),
     MakeIntColumn<int32_t>({0, 0, 0, 1, 0,
                             std::numeric_limits<int32_t>::max(), 0, 0, 1, 2,
                             std::numeric_limits<int32_t>::max(),
                             std::numeric_limits<int32_t>::max()}),
     MakeIntColumn<int64_t>({0, 0, 1, 0, 0,
                             std::numeric_limits<int64_t>::max(), 0, 0, 3, 3,
                             std::numeric_limits<int64_t>::max(),
                             std::numeric_limits<int64_t>::max()}),
     MakeIntColumn<int16_t>({0, 1, 0, 0, 0,
                             std::numeric_limits<int16_t>::max(), 0, 0, 3, 4,
                             std::numeric_limits<int16_t>::max(),
                             std::numeric_limits<int16_t>::max()})});
  ASSERT_FALSE(absl::c_any_of(
    keys, [&](const auto& str) { return str.size() != keys[0].size(); }));
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
}

TEST_F(DuckDBKeyBuilderTest, test_multiSizeInteger) {
  using I8 = int8_t;
  using I16 = int16_t;
  using I32 = int32_t;
  using I64 = int64_t;
  auto keys = BuildKeys(
    {MakeIntColumn<I8>(
       {std::numeric_limits<I8>::min(), std::numeric_limits<I8>::min(),
        std::numeric_limits<I8>::min(), std::numeric_limits<I8>::min(),
        static_cast<I8>(std::numeric_limits<I8>::min() + 1),
        static_cast<I8>(std::numeric_limits<I8>::min() + 2),
        static_cast<I8>(std::numeric_limits<I8>::min() + 3), 0, 1, 2, 4,
        std::numeric_limits<I8>::max()}),
     MakeIntColumn<I32>(
       {std::numeric_limits<I32>::min(), std::numeric_limits<I32>::min(),
        std::numeric_limits<I32>::min(), std::numeric_limits<I32>::min() + 1,
        std::numeric_limits<I32>::min(), std::numeric_limits<I32>::max(),
        std::numeric_limits<I32>::min(), 0, 1, 2,
        std::numeric_limits<I32>::max(), std::numeric_limits<I32>::max()}),
     MakeIntColumn<I64>(
       {std::numeric_limits<I64>::min(), std::numeric_limits<I64>::min(),
        std::numeric_limits<I64>::min() + 1, std::numeric_limits<I64>::min(),
        std::numeric_limits<I64>::min(), std::numeric_limits<I64>::max(),
        std::numeric_limits<I64>::min(), 0, 3, 3,
        std::numeric_limits<I64>::max(), std::numeric_limits<I64>::max()}),
     MakeIntColumn<I16>(
       {std::numeric_limits<I16>::min(),
        static_cast<I16>(std::numeric_limits<I16>::min() + 1),
        std::numeric_limits<I16>::min(), std::numeric_limits<I16>::min(),
        std::numeric_limits<I16>::min(), std::numeric_limits<I16>::max(),
        std::numeric_limits<I16>::min(), 0, 3, 4,
        std::numeric_limits<I16>::max(), std::numeric_limits<I16>::max()})});
  ASSERT_FALSE(absl::c_any_of(
    keys, [&](const auto& str) { return str.size() != keys[0].size(); }));
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
}

TEST_F(DuckDBKeyBuilderTest, test_FloatingNumbers) {
  auto keys = BuildKeys(
    {MakeFloatColumn<float>(
       {std::numeric_limits<float>::lowest(), -1.f, -1.f, 0.f,
        std::numeric_limits<float>::min(), 9.f, 1000.f,
        std::numeric_limits<float>::max(), std::numeric_limits<float>::max(),
        std::numeric_limits<float>::max()}),
     MakeFloatColumn<double>({4., std::numeric_limits<double>::max() * -1.,
                              -1., 0., 6., 5., 4., 0., 1000.,
                              std::numeric_limits<double>::max()})});
  ASSERT_FALSE(absl::c_any_of(
    keys, [&](const auto& str) { return str.size() != keys[0].size(); }));
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
}

TEST_F(DuckDBKeyBuilderTest, test_Bools) {
  auto bools = [](std::initializer_list<bool> vs) {
    std::vector<Value> out;
    out.reserve(vs.size());
    for (bool b : vs) {
      out.push_back(Value::BOOLEAN(b));
    }
    return out;
  };
  auto keys = BuildKeys({bools({false, false, true, true}),
                         bools({false, true, false, true})});
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
  ASSERT_EQ(keys[0], std::string_view("\x00\x00", 2));
  ASSERT_EQ(keys[1], std::string_view("\x00\x01", 2));
  ASSERT_EQ(keys[2], std::string_view("\x01\x00", 2));
  ASSERT_EQ(keys[3], std::string_view("\x01\x01", 2));
}

TEST_F(DuckDBKeyBuilderTest, test_Strings) {
  auto strs = [](std::initializer_list<std::string_view> vs) {
    std::vector<Value> out;
    out.reserve(vs.size());
    for (std::string_view s : vs) {
      out.push_back(Value(std::string(s)));
    }
    return out;
  };
  auto keys = BuildKeys({strs({"foo", "foob", "foobar", "foobar"}),
                         strs({"barfoo", "arfoo", "foo", "goo"})});
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
}

TEST_F(DuckDBKeyBuilderTest, test_BoolsStrings) {
  auto strs = [](std::initializer_list<std::string_view> vs) {
    std::vector<Value> out;
    out.reserve(vs.size());
    for (std::string_view s : vs) {
      out.push_back(Value(std::string(s)));
    }
    return out;
  };
  auto bools = [](std::initializer_list<bool> vs) {
    std::vector<Value> out;
    out.reserve(vs.size());
    for (bool b : vs) {
      out.push_back(Value::BOOLEAN(b));
    }
    return out;
  };
  auto keys = BuildKeys({strs({"foo", "foob", "foob", "foob", "foob"}),
                         bools({true, false, true, true, true}),
                         strs({"a", "a", "a", "b", "bar"})});
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
}

TEST_F(DuckDBKeyBuilderTest, test_StringsNumbers) {
  auto strs = [](std::initializer_list<std::string_view> vs) {
    std::vector<Value> out;
    out.reserve(vs.size());
    for (std::string_view s : vs) {
      out.push_back(Value(std::string(s)));
    }
    return out;
  };
  // -1339019216 would be stored as 0x30303030 ("0000") in big-endian after
  // the sign-bit flip, exercising the delimiter logic for the adjacent
  // string columns.
  auto keys = BuildKeys({strs({"foo", "foo0"}),
                         MakeIntColumn<int32_t>({-1339019216, -1339019216}),
                         strs({"00", "0"})});
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
}

TEST_F(DuckDBKeyBuilderTest, test_StringsWithSeparators) {
  // Raw bytes including embedded NULs and \x01 escape chars. BLOB goes
  // through the same encoding path as VARCHAR but skips UTF-8 validation
  // that duckdb::Value(string) applies to VARCHAR values.
  std::vector<std::string_view> vs{"",
                                   std::string_view{"\0", 1},
                                   std::string_view{"\0\0", 2},
                                   std::string_view{"\0\1", 2},
                                   std::string_view{"\0c", 2},
                                   "c",
                                   std::string_view{"c\0", 2},
                                   std::string_view{"c\1", 2},
                                   "ccc",
                                   std::string_view{"ccc\0", 4},
                                   std::string_view{"ccc\0\0", 5},
                                   std::string_view{"ccc\0\1", 5},
                                   std::string_view{"ccc\0c", 5},
                                   std::string_view{"ccc\1", 4},
                                   "cccc"};
  std::vector<Value> col;
  col.reserve(vs.size());
  for (std::string_view s : vs) {
    col.push_back(MakeBlob(s));
  }
  auto keys = BuildKeys({col});
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
}

TEST_F(DuckDBKeyBuilderTest, test_Timestamp) {
  // Edge and interior timestamp values. duckdb::timestamp_t stores micros
  // since epoch as a signed int64, and AppendDuckDBValueToKey encodes it
  // with a sign-bit flip identical to BIGINT.
  auto ts = [](int64_t micros) {
    return Value::TIMESTAMP(duckdb::timestamp_t{micros});
  };
  auto keys = BuildKeys(
    {{ts(std::numeric_limits<int64_t>::min()), ts(-1'000'000), ts(0),
      ts(1'000'000), ts(std::numeric_limits<int64_t>::max())}});
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
  ASSERT_EQ(keys[0],
            std::string_view("\x00\x00\x00\x00\x00\x00\x00\x00", 8));
  ASSERT_EQ(keys[2],
            std::string_view("\x80\x00\x00\x00\x00\x00\x00\x00", 8));
  ASSERT_EQ(keys[4],
            std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8));
}

TEST_F(DuckDBKeyBuilderTest, test_Date) {
  auto date = [](int32_t days) {
    return Value::DATE(duckdb::date_t{days});
  };
  auto keys = BuildKeys(
    {{date(std::numeric_limits<int32_t>::min()), date(-1), date(0), date(1),
      date(std::numeric_limits<int32_t>::max())}});
  ASSERT_TRUE(absl::c_is_sorted(keys, kKeyCheck));
  ASSERT_EQ(keys[0], std::string_view("\x00\x00\x00\x00", 4));
  ASSERT_EQ(keys[2], std::string_view("\x80\x00\x00\x00", 4));
  ASSERT_EQ(keys[4], std::string_view("\xFF\xFF\xFF\xFF", 4));
}

}  // namespace
