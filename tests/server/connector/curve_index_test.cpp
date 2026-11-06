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

#include <gtest/gtest.h>

#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <limits>
#include <vector>

#include "connector/curve_index.h"

namespace sdb::connector {
namespace {

using duckdb::LogicalType;
using duckdb::Value;

CurveAxis Axis(const LogicalType& type, CurveFamily family) {
  CurveAxis axis;
  axis.type = type;
  axis.family = family;
  return axis;
}

uint64_t KeyOf(const CurveAxis& axis, const Value& bound) {
  const auto key = CurveBoundKey(axis, bound);
  EXPECT_TRUE(key);
  return key ? key->key : 0;
}

bool ExactOf(const CurveAxis& axis, const Value& bound) {
  const auto key = CurveBoundKey(axis, bound);
  EXPECT_TRUE(key);
  return key && key->exact;
}

TEST(CurveSpec, EqualsComparesOptionsAndAxes) {
  CurveSpec base;
  base.options.dimensions = 3;
  base.axes[0].range = CurveKeyRange{.lo = 1, .hi = 2};
  base.axes[1].range = CurveDoubleRange{.lo = 0.5, .hi = 1.5};
  base.axes[2].range = CurveIpv6Range{.lo = 1, .hi = 2};
  EXPECT_TRUE(base.Equals(CurveSpec{base}));

  auto options = base;
  options.options.hilbert = true;
  EXPECT_FALSE(base.Equals(options));

  auto keys = base;
  keys.axes[0].range = CurveKeyRange{.lo = 1, .hi = 3};
  EXPECT_FALSE(base.Equals(keys));

  auto doubles = base;
  doubles.axes[1].range = CurveDoubleRange{.lo = 0.5, .hi = 2.5};
  EXPECT_FALSE(base.Equals(doubles));

  auto ipv6 = base;
  ipv6.axes[2].range = CurveIpv6Range{.lo = 1, .hi = 3};
  EXPECT_FALSE(base.Equals(ipv6));

  auto kind = base;
  kind.axes[0].range = std::monostate{};
  EXPECT_FALSE(base.Equals(kind));
}

TEST(CurveBoundKey, SignedKeysFollowTheValues) {
  const auto axis = Axis(LogicalType::TINYINT, CurveFamily::Signed);
  uint64_t previous = 0;
  for (int32_t value = -128; value <= 127; ++value) {
    const auto key = CurveBoundKey(axis, Value::TINYINT(value));
    ASSERT_TRUE(key);
    EXPECT_TRUE(key->exact);
    EXPECT_EQ(uint64_t{1} << 56, key->step);
    if (value != -128) {
      EXPECT_EQ(previous + key->step, key->key);
    }
    previous = key->key;
  }
}

TEST(CurveBoundKey, UnsignedKeysFollowTheValues) {
  const auto axis = Axis(LogicalType::UTINYINT, CurveFamily::Unsigned);
  for (int32_t value = 0; value <= 255; ++value) {
    const auto key = CurveBoundKey(axis, Value::UTINYINT(value));
    ASSERT_TRUE(key);
    EXPECT_TRUE(key->exact);
    EXPECT_EQ(static_cast<uint64_t>(value) << 56, key->key);
  }
}

TEST(CurveBoundKey, BoundsOutsideASignedTypeClampAndStayInexact) {
  const auto axis = Axis(LogicalType::TINYINT, CurveFamily::Signed);
  const auto low = KeyOf(axis, Value::TINYINT(-128));
  const auto high = KeyOf(axis, Value::TINYINT(127));
  for (const int64_t value :
       {int64_t{128}, int64_t{1000}, std::numeric_limits<int64_t>::max()}) {
    EXPECT_EQ(high, KeyOf(axis, Value::BIGINT(value)));
    EXPECT_FALSE(ExactOf(axis, Value::BIGINT(value)));
  }
  for (const int64_t value :
       {int64_t{-129}, int64_t{-1000}, std::numeric_limits<int64_t>::min()}) {
    EXPECT_EQ(low, KeyOf(axis, Value::BIGINT(value)));
    EXPECT_FALSE(ExactOf(axis, Value::BIGINT(value)));
  }
  const auto huge = Value::UBIGINT(std::numeric_limits<uint64_t>::max());
  EXPECT_EQ(high, KeyOf(axis, huge));
  EXPECT_FALSE(ExactOf(axis, huge));
  EXPECT_TRUE(ExactOf(axis, Value::UBIGINT(127)));
}

TEST(CurveBoundKey, BoundsOutsideAnUnsignedTypeClampAndStayInexact) {
  const auto axis = Axis(LogicalType::UTINYINT, CurveFamily::Unsigned);
  EXPECT_EQ(uint64_t{0}, KeyOf(axis, Value::BIGINT(-1)));
  EXPECT_FALSE(ExactOf(axis, Value::BIGINT(-1)));
  EXPECT_EQ(KeyOf(axis, Value::UTINYINT(255)), KeyOf(axis, Value::BIGINT(256)));
  EXPECT_FALSE(ExactOf(axis, Value::BIGINT(256)));
  EXPECT_FALSE(ExactOf(axis, Value::UBIGINT(256)));
  EXPECT_TRUE(ExactOf(axis, Value::BIGINT(255)));
  EXPECT_TRUE(ExactOf(axis, Value::BIGINT(0)));
}

TEST(CurveBoundKey, WidestAxesKeepEveryValue) {
  const auto bigint = Axis(LogicalType::BIGINT, CurveFamily::Signed);
  const auto min = std::numeric_limits<int64_t>::min();
  const auto max = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(uint64_t{0}, KeyOf(bigint, Value::BIGINT(min)));
  EXPECT_EQ(std::numeric_limits<uint64_t>::max(),
            KeyOf(bigint, Value::BIGINT(max)));
  EXPECT_TRUE(ExactOf(bigint, Value::BIGINT(max)));
  EXPECT_FALSE(ExactOf(bigint, Value::UBIGINT(uint64_t{1} << 63)));

  const auto ubigint = Axis(LogicalType::UBIGINT, CurveFamily::Unsigned);
  EXPECT_EQ(
    std::numeric_limits<uint64_t>::max(),
    KeyOf(ubigint, Value::UBIGINT(std::numeric_limits<uint64_t>::max())));
  EXPECT_EQ(uint64_t{1}, CurveBoundKey(ubigint, Value::UBIGINT(5))->step);
}

TEST(CurveBoundKey, FloatKeysOrderLikeSql) {
  const auto axis = Axis(LogicalType::DOUBLE, CurveFamily::Float);
  const auto infinity = std::numeric_limits<double>::infinity();
  const std::vector<double> ordered{
    -infinity, std::numeric_limits<double>::lowest(),
    -1.5,      -std::numeric_limits<double>::denorm_min(),
    0.0,       std::numeric_limits<double>::denorm_min(),
    1.5,       std::numeric_limits<double>::max(),
    infinity,  std::numeric_limits<double>::quiet_NaN(),
  };
  for (size_t i = 1; i < ordered.size(); ++i) {
    EXPECT_LT(KeyOf(axis, Value::DOUBLE(ordered[i - 1])),
              KeyOf(axis, Value::DOUBLE(ordered[i])));
  }
  EXPECT_EQ(KeyOf(axis, Value::DOUBLE(0.0)), KeyOf(axis, Value::DOUBLE(-0.0)));
  EXPECT_EQ(uint64_t{1}, CurveBoundKey(axis, Value::DOUBLE(1))->step);
}

TEST(CurveBoundKey, IntegerBoundsOfFloatsAreExactOnlyWhenTheyFit) {
  const auto axis = Axis(LogicalType::DOUBLE, CurveFamily::Float);
  EXPECT_TRUE(ExactOf(axis, Value::BIGINT(3)));
  EXPECT_TRUE(ExactOf(axis, Value::BIGINT(int64_t{1} << 53)));
  EXPECT_FALSE(ExactOf(axis, Value::BIGINT((int64_t{1} << 53) + 1)));
  EXPECT_FALSE(
    ExactOf(axis, Value::BIGINT(std::numeric_limits<int64_t>::max())));
  EXPECT_TRUE(ExactOf(axis, Value::UBIGINT(uint64_t{1} << 63)));
  EXPECT_FALSE(
    ExactOf(axis, Value::UBIGINT(std::numeric_limits<uint64_t>::max())));
  EXPECT_FALSE(ExactOf(axis, Value::DECIMAL(int32_t{15}, 4, 1)));
  EXPECT_EQ(KeyOf(axis, Value::DOUBLE(1.5)),
            KeyOf(axis, Value::DECIMAL(int32_t{15}, 4, 1)));
}

TEST(CurveBoundKey, TemporalAxesTakeTheirOwnType) {
  const auto date = Axis(LogicalType::DATE, CurveFamily::Date);
  const auto timestamp = Axis(LogicalType::TIMESTAMP, CurveFamily::Timestamp);
  const auto zoned = Axis(LogicalType::TIMESTAMP_TZ, CurveFamily::TimestampTz);
  const auto day = Value::DATE(duckdb::date_t{19000});
  const auto moment = Value::TIMESTAMP(duckdb::timestamp_t{1000});
  const auto instant = Value::TIMESTAMPTZ(duckdb::timestamp_tz_t{1000});
  EXPECT_TRUE(ExactOf(date, day));
  EXPECT_TRUE(ExactOf(timestamp, moment));
  EXPECT_TRUE(ExactOf(zoned, instant));
  EXPECT_EQ(uint64_t{1} << 32, CurveBoundKey(date, day)->step);
  EXPECT_LT(KeyOf(date, day), KeyOf(date, Value::DATE(duckdb::date_t{19001})));
  EXPECT_FALSE(CurveBoundKey(date, moment));
  EXPECT_FALSE(CurveBoundKey(date, Value::INTEGER(19000)));
  EXPECT_FALSE(CurveBoundKey(timestamp, day));
  EXPECT_FALSE(CurveBoundKey(timestamp, instant));
  EXPECT_FALSE(CurveBoundKey(zoned, moment));
}

TEST(CurveBoundKey, OtherTypesHaveNoKey) {
  const auto integer = Axis(LogicalType::INTEGER, CurveFamily::Signed);
  EXPECT_FALSE(CurveBoundKey(integer, Value{"5"}));
  EXPECT_FALSE(CurveBoundKey(integer, Value::DOUBLE(5)));
  EXPECT_FALSE(CurveBoundKey(integer, Value::HUGEINT(5)));
  const auto unsigned_axis = Axis(LogicalType::UINTEGER, CurveFamily::Unsigned);
  EXPECT_FALSE(CurveBoundKey(unsigned_axis, Value::UHUGEINT(5)));
  EXPECT_FALSE(CurveBoundKey(unsigned_axis, Value::DOUBLE(5)));
  const auto real = Axis(LogicalType::DOUBLE, CurveFamily::Float);
  EXPECT_FALSE(CurveBoundKey(real, Value{"5"}));
  EXPECT_FALSE(CurveBoundKey(real, Value::DATE(duckdb::date_t{1})));
  EXPECT_FALSE(CurveBoundKey(Axis(LogicalType::SQLNULL, CurveFamily::Null),
                             Value::INTEGER(1)));
}

TEST(CurveBoundKey, NormalizedAxesAreNeverExact) {
  auto axis = Axis(LogicalType::BIGINT, CurveFamily::Signed);
  const auto lo = KeyOf(axis, Value::BIGINT(0));
  const auto hi = KeyOf(axis, Value::BIGINT(100));
  axis.range = CurveKeyRange{.lo = lo, .hi = hi};
  EXPECT_FALSE(ExactOf(axis, Value::BIGINT(50)));
  EXPECT_EQ(uint64_t{0}, KeyOf(axis, Value::BIGINT(-50)));
  EXPECT_LT(KeyOf(axis, Value::BIGINT(50)), KeyOf(axis, Value::BIGINT(51)));
  EXPECT_EQ(KeyOf(axis, Value::BIGINT(100)), KeyOf(axis, Value::BIGINT(500)));

  auto real = Axis(LogicalType::DOUBLE, CurveFamily::Float);
  real.range = CurveDoubleRange{.lo = 0, .hi = 10};
  EXPECT_FALSE(ExactOf(real, Value::DOUBLE(5)));
  EXPECT_EQ(uint64_t{0}, KeyOf(real, Value::DOUBLE(-1)));
  EXPECT_EQ(std::numeric_limits<uint64_t>::max(),
            KeyOf(real, Value::DOUBLE(11)));
}

TEST(CurveKeyPadding, CoversTheBitsAxisValuesDoNotUse) {
  EXPECT_EQ((uint64_t{1} << 56) - 1,
            CurveKeyPadding(Axis(LogicalType::TINYINT, CurveFamily::Signed)));
  EXPECT_EQ(
    (uint64_t{1} << 48) - 1,
    CurveKeyPadding(Axis(LogicalType::USMALLINT, CurveFamily::Unsigned)));
  EXPECT_EQ((uint64_t{1} << 32) - 1,
            CurveKeyPadding(Axis(LogicalType::INTEGER, CurveFamily::Signed)));
  EXPECT_EQ((uint64_t{1} << 32) - 1,
            CurveKeyPadding(Axis(LogicalType::DATE, CurveFamily::Date)));
  EXPECT_EQ(uint64_t{0},
            CurveKeyPadding(Axis(LogicalType::BIGINT, CurveFamily::Signed)));
  EXPECT_EQ(uint64_t{0},
            CurveKeyPadding(Axis(LogicalType::DOUBLE, CurveFamily::Float)));
  EXPECT_EQ(uint64_t{0}, CurveKeyPadding(Axis(LogicalType::TIMESTAMP,
                                              CurveFamily::Timestamp)));
  auto normalized = Axis(LogicalType::TINYINT, CurveFamily::Signed);
  normalized.range = CurveKeyRange{.lo = 0, .hi = uint64_t{1} << 60};
  EXPECT_EQ(uint64_t{0}, CurveKeyPadding(normalized));
}

TEST(CurveAxisIsExact, NeedsEveryBitOfTheCoordinate) {
  const auto tiny = Axis(LogicalType::TINYINT, CurveFamily::Signed);
  EXPECT_TRUE(CurveAxisIsExact(tiny, 8));
  EXPECT_TRUE(CurveAxisIsExact(tiny, 64));
  EXPECT_FALSE(CurveAxisIsExact(tiny, 7));
  const auto date = Axis(LogicalType::DATE, CurveFamily::Date);
  EXPECT_TRUE(CurveAxisIsExact(date, 32));
  EXPECT_FALSE(CurveAxisIsExact(date, 31));
  const auto real = Axis(LogicalType::DOUBLE, CurveFamily::Float);
  EXPECT_TRUE(CurveAxisIsExact(real, 64));
  EXPECT_FALSE(CurveAxisIsExact(real, 63));
  const auto single = Axis(LogicalType::FLOAT, CurveFamily::Float);
  EXPECT_FALSE(CurveAxisIsExact(single, 32));
  EXPECT_TRUE(CurveAxisIsExact(single, 64));
}

TEST(CurveAxisIsExact, ApproximateAxesNeverAre) {
  EXPECT_FALSE(
    CurveAxisIsExact(Axis(LogicalType::SQLNULL, CurveFamily::Null), 64));
  EXPECT_FALSE(
    CurveAxisIsExact(Axis(LogicalType::BIGINT, CurveFamily::Inet), 64));
  auto normalized = Axis(LogicalType::BIGINT, CurveFamily::Signed);
  normalized.range = CurveKeyRange{.lo = 0, .hi = 100};
  EXPECT_FALSE(CurveAxisIsExact(normalized, 64));
  auto real = Axis(LogicalType::DOUBLE, CurveFamily::Float);
  real.range = CurveDoubleRange{.lo = 0, .hi = 1};
  EXPECT_FALSE(CurveAxisIsExact(real, 64));
}

}  // namespace
}  // namespace sdb::connector
