////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include <vpack/builder.h>
#include <vpack/literal.h>
#include <vpack/parser.h>

#include "basics/common.h"
#include "gtest/gtest.h"
#include "vpack/vpack_helper.h"

#define VPACK_EXPECT_TRUE(expected, func, lValue, rValue) \
  l = vpack::Parser::fromJson(lValue);                    \
  r = vpack::Parser::fromJson(rValue);                    \
  EXPECT_EQ(expected, func(l->slice(), r->slice()));

struct DoubleValue {
  double d;
  uint8_t sign;
  uint16_t e;
  uint64_t m;

  std::string ToString() {
    return std::to_string(sign) + " " + std::to_string(e) + " " +
           std::to_string(m);
  }
};

static DoubleValue MakeDoubleValue(uint8_t sign, uint16_t e, uint64_t m) {
  EXPECT_LT(sign, 2);
  EXPECT_LT(e, 2048);
  EXPECT_LT(m, uint64_t{1} << 52);
  uint64_t x = (static_cast<uint64_t>(sign) << 63) |
               (static_cast<uint64_t>(e) << 52) | static_cast<uint64_t>(m);
  double y = std::bit_cast<double>(x);
  return DoubleValue{.d = y, .sign = sign, .e = e, .m = m};
}

static constexpr uint64_t kMantmax = (uint64_t(1) << 52) - 1;

namespace test::helper {

using sdb::basics::VPackHelper;

template<typename T>
static vpack::Builder MakeVPack(T x) {
  static_assert(std::is_arithmetic_v<T>);
  vpack::Builder b;
  b.add(x);
  return b;
}

template<typename A, typename B>
static int Comp(A a, B b) {
  auto ba = MakeVPack(a);
  auto bb = MakeVPack(b);
  return VPackHelper::compareNumbers(ba.slice(), bb.slice());
}

template<typename A, typename B>
static int CompGeneric(A a, B b) {
  auto ba = MakeVPack(a);
  auto bb = MakeVPack(b);
  return VPackHelper::compare(ba.slice(), bb.slice());
}

template<typename T>
static void CheckNaN(T t) {
  auto nan = MakeVPack(MakeDoubleValue(0, 2047, 1).d);  // NaN
  auto a = MakeVPack(t);
  EXPECT_EQ(-1, VPackHelper::compareNumbers(a.slice(), nan.slice()));
  EXPECT_EQ(1, VPackHelper::compareNumbers(nan.slice(), a.slice()));
}

////////////////////////////////////////////////////////////////////////////////
/// test compare values with equal values
////////////////////////////////////////////////////////////////////////////////

TEST(VPackHelperTest, tst_compare_values_equal) {
  std::shared_ptr<vpack::Builder> l;
  std::shared_ptr<vpack::Builder> r;

  // With Utf8-mode:
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "null", "null");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "false", "false");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "true", "true");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "0", "0");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "1", "1");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "1.5", "1.5");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "-43.2", "-43.2");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "\"\"", "\"\"");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "\" \"", "\" \"");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "\"the quick brown fox\"",
                    "\"the quick brown fox\"");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "[]", "[]");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "[-1]", "[-1]");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "[0]", "[0]");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "[1]", "[1]");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "[true]", "[true]");
  VPACK_EXPECT_TRUE(0, VPackHelper::compare, "{}", "{}");
}

////////////////////////////////////////////////////////////////////////////////
/// test compare values with unequal values
////////////////////////////////////////////////////////////////////////////////

TEST(VPackHelperTest, tst_compare_values_unequal) {
  std::shared_ptr<vpack::Builder> l;
  std::shared_ptr<vpack::Builder> r;
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "false");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "true");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "-1");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "0");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "1");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "-10");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "\"\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "\"0\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "\" \"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "[]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "[null]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "[false]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "[true]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "[0]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "null", "{}");

  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "true");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "-1");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "0");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "1");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "-10");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "\"\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "\"0\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "\" \"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "[]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "[null]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "[false]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "[true]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "[0]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "false", "{}");

  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "-1");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "0");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "1");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "-10");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "\"\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "\"0\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "\" \"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "[]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "[null]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "[false]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "[true]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "[0]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "{}");

  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "-2", "-1");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "-10", "-9");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "-20", "-5");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "-5", "-2");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "true", "1");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1.5", "1.6");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "10.5", "10.51");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "\"\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "\"0\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "\"-1\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "\"-1\"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "\" \"");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "[]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "[-1]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "[0]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "[1]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "[null]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "[false]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "[true]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "0", "{}");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "[]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "[-1]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "[0]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "[1]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "[null]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "[false]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "[true]");
  VPACK_EXPECT_TRUE(-1, VPackHelper::compare, "1", "{}");
}

TEST(VPackHelperTest, vpack_string_literals) {
  using namespace vpack;
  {
    const auto& s = "4"_vpack;
    ASSERT_EQ(s.getUInt(), 4);
  }

  {
    const auto& array = R"([1,2,3,4])"_vpack;
    ASSERT_EQ(array.slice().at(0).getUInt(), 1);
    ASSERT_EQ(array.slice().at(1).getUInt(), 2);
    ASSERT_EQ(array.slice().at(2).getUInt(), 3);
    ASSERT_EQ(array.slice().at(3).getUInt(), 4);
  }

  {
    const auto& obj = R"({
                 "vertices": [ {"_key" : "A"}, {"_key" : "B"}, {"_key" : "C"} ],
                 "edges": [ {"_from" : "A", "_to" : "B"},
                            {"_from" : "B", "_to" : "C"} ]
    })"_vpack;
    ASSERT_TRUE(obj.slice().get("vertices").isArray());
    ASSERT_TRUE(obj.slice().get("edges").isArray());
  }
}

//////////////////////////////////////////////////////////////////////////////
/// @brief test comparison of numerical values
//////////////////////////////////////////////////////////////////////////////

TEST(VPackHelperTest, test_comparison_numerical_double) {
  for (int n = 0; n < 2; ++n) {
    // Do everything twice, once with +0 and once with -0

    // We create a vector of numerical vpack values which is supposed
    // to be sorted strictly ascending. We also check transitivity by comparing
    // all pairs:
    std::vector<vpack::Builder> v;
    v.push_back(MakeVPack(MakeDoubleValue(1, 2047, 0).d));  // -Inf
    for (uint16_t i = 2046; i >= 1; --i) {
      v.push_back(MakeVPack(MakeDoubleValue(1, i, kMantmax).d));
      v.push_back(MakeVPack(MakeDoubleValue(1, i, 0).d));
    }
    v.push_back(
      MakeVPack(MakeDoubleValue(1, 0, kMantmax).d));     // - denormalized
    v.push_back(MakeVPack(MakeDoubleValue(1, 0, 1).d));  // - denormalized
    if (n == 0) {
      v.push_back(MakeVPack(MakeDoubleValue(0, 0, 0).d));  // + 0
    } else {
      v.push_back(MakeVPack(MakeDoubleValue(1, 0, 0).d));  // - 0
    }
    v.push_back(MakeVPack(MakeDoubleValue(0, 0, 1).d));  // + denormalized
    v.push_back(
      MakeVPack(MakeDoubleValue(0, 0, kMantmax).d));  // + denormalized
    for (uint16_t i = 1; i <= 2046; ++i) {
      v.push_back(MakeVPack(MakeDoubleValue(0, i, 0).d));
      v.push_back(MakeVPack(MakeDoubleValue(0, i, kMantmax).d));
    }
    v.push_back(MakeVPack(MakeDoubleValue(0, 2047, 0).d));  // infinity

    // Now check if our comparator agrees that this is strictly ascending:
    for (size_t i = 0; i < v.size() - 1; ++i) {
      auto c = VPackHelper::compareNumbers(v[i].slice(), v[i + 1].slice());
      EXPECT_EQ(-1, c) << "Not strictly increasing: " << i << " "
                       << v[i].slice().toJson() << " "
                       << v[i + 1].slice().toJson();
      c = VPackHelper::compareNumbers(v[i + 1].slice(), v[i].slice());
      EXPECT_EQ(1, c) << "Not strictly decreasing: " << i << " "
                      << v[i + 1].slice().toJson() << " "
                      << v[i].slice().toJson();
    }
    // Check reflexivity:
    for (size_t i = 0; i < v.size(); ++i) {
      auto c = VPackHelper::compareNumbers(v[i].slice(), v[i].slice());
      EXPECT_EQ(0, c) << "Not reflexive: " << i << " " << v[i].slice().toJson();
    }
    // And check transitivity by comparing all pairs:
    for (size_t i = 0; i < v.size() - 1; ++i) {
      for (size_t j = i + 1; j < v.size(); ++j) {
        auto c = VPackHelper::compareNumbers(v[i].slice(), v[j].slice());
        EXPECT_EQ(-1, c) << "Not transitive: " << i << " "
                         << v[i].slice().toJson() << " " << j << " "
                         << v[j].slice().toJson();
      }
    }
    // And the same the other way round
    for (size_t i = 0; i < v.size() - 1; ++i) {
      for (size_t j = i + 1; j < v.size(); ++j) {
        auto c = VPackHelper::compareNumbers(v[j].slice(), v[i].slice());
        EXPECT_EQ(1, c) << "Not transitive: " << i << " "
                        << v[i].slice().toJson() << " " << j << " "
                        << v[j].slice().toJson();
      }
    }
  }
}

TEST(VPackHelperTest, test_equality_zeros) {
  std::vector<vpack::Builder> v;
  // +0.0:
  v.push_back(MakeVPack(MakeDoubleValue(0, 0, 0).d));
  // -0.0:
  v.push_back(MakeVPack(MakeDoubleValue(1, 0, 0).d));
  // uint64_t{0}:
  v.push_back(MakeVPack(uint64_t{0}));
  // int64_t{0}:
  v.push_back(MakeVPack(int64_t{0}));
  // smallint{0}:
  v.push_back(MakeVPack(0));
  for (size_t i = 0; i < v.size(); ++i) {
    for (size_t j = 0; j < v.size(); ++j) {
      EXPECT_EQ(0, VPackHelper::compareNumbers(v[i].slice(), v[j].slice()));
    }
  }
}

TEST(VPackHelperTest, test_equality_with_integers) {
  std::vector<int64_t> vi;
  std::vector<uint64_t> vu;
  vi.push_back(0);
  vi.push_back(0);
  int64_t x = -1;
  uint64_t y = 1;
  for (int i = 0; i < 62; ++i) {
    vi.push_back(x);
    vu.push_back(y);
    x <<= 1;
    y <<= 1;
  }
  for (int64_t i : vi) {
    vpack::Builder l = MakeVPack(i);
    vpack::Builder r = MakeVPack(static_cast<double>(i));
    EXPECT_EQ(0, VPackHelper::compareNumbers(l.slice(), r.slice()));
    EXPECT_EQ(0, VPackHelper::compareNumbers(r.slice(), l.slice()));
  }
  for (uint64_t u : vu) {
    vpack::Builder l = MakeVPack(u);
    vpack::Builder r = MakeVPack(static_cast<double>(u));
    EXPECT_EQ(0, VPackHelper::compareNumbers(l.slice(), r.slice()));
    EXPECT_EQ(0, VPackHelper::compareNumbers(r.slice(), l.slice()));
  }
}

TEST(VPackHelperTest, test_inequality_with_integers) {
  int64_t x = -2;
  uint64_t y = 2;
  for (int i = 0; i < 61; ++i) {
    vpack::Builder l = MakeVPack(static_cast<double>(x));
    vpack::Builder r = MakeVPack(x - 1);
    EXPECT_EQ(1, VPackHelper::compareNumbers(l.slice(), r.slice()))
      << "Not less: " << i << " " << l.slice().toJson() << " "
      << r.slice().toJson();
    EXPECT_EQ(-1, VPackHelper::compareNumbers(r.slice(), l.slice()))
      << "Not greater: " << i << " " << r.slice().toJson() << " "
      << l.slice().toJson();
    vpack::Builder ll = MakeVPack(y + 1);
    vpack::Builder rr = MakeVPack(static_cast<double>(y));
    EXPECT_EQ(1, VPackHelper::compareNumbers(ll.slice(), rr.slice()))
      << "Not less: " << i << " " << ll.slice().toJson() << " "
      << rr.slice().toJson();
    EXPECT_EQ(-1, VPackHelper::compareNumbers(rr.slice(), ll.slice()))
      << "Not greater: " << i << " " << rr.slice().toJson() << " "
      << ll.slice().toJson();
    x <<= 1;
    y <<= 1;
  }
}

TEST(VPackHelperTest, test_numbers_compare_as_doubles) {
  vpack::Builder a = MakeVPack(std::numeric_limits<int64_t>::max());

  uint64_t v = std::numeric_limits<int64_t>::max();
  vpack::Builder b = MakeVPack(v);

  uint64_t w = v + 1;
  vpack::Builder c = MakeVPack(w);

  EXPECT_EQ(0, VPackHelper::compareNumbers(a.slice(), b.slice()));
  EXPECT_EQ(-1, VPackHelper::compareNumbers(b.slice(), c.slice()));
  EXPECT_EQ(-1, VPackHelper::compareNumbers(a.slice(), c.slice()));
}

TEST(VPackHelperTest, test_nan_greater_than_all) {
  CheckNaN(int64_t{0});
  CheckNaN(uint64_t{0});
  CheckNaN(int64_t{-1});
  CheckNaN(int64_t{1});
  CheckNaN(uint64_t{1});
  CheckNaN(std::numeric_limits<int64_t>::max());
  CheckNaN(std::numeric_limits<int64_t>::min());
  CheckNaN(std::numeric_limits<uint64_t>::max());
  CheckNaN(int64_t{12321222123});
  CheckNaN(int64_t{-12321222123});
  CheckNaN(uint64_t{12321222123});

  CheckNaN(double{0.0});                    // +0
  CheckNaN(MakeDoubleValue(1, 0, 0).d);     // -0
  CheckNaN(MakeDoubleValue(0, 2047, 0).d);  // +infty
  CheckNaN(MakeDoubleValue(1, 2047, 0).d);  // -infty
  CheckNaN(double{1.0});
  CheckNaN(double{-1.0});
  CheckNaN(double{123456.789});
  CheckNaN(double{-123456.789});
  CheckNaN(double{1.23456e89});
  CheckNaN(double{-1.23456e89});
  CheckNaN(double{1.23456e-89});
  CheckNaN(double{-1.23456e-89});
  CheckNaN(MakeDoubleValue(0, 0, 1).d);                        // denormalized
  CheckNaN(MakeDoubleValue(0, 0, 123456789).d);                // denormalized
  CheckNaN(MakeDoubleValue(0, 0, (uint64_t{1} << 52) - 1).d);  // denormalized
  CheckNaN(MakeDoubleValue(1, 0, 1).d);                        // denormalized
  CheckNaN(MakeDoubleValue(1, 0, 123456789).d);                // denormalized
  CheckNaN(MakeDoubleValue(1, 0, (uint64_t{1} << 52) - 1).d);  // denormalized
}

TEST(VPackHelperTest, test_unsigned_double_comparison) {
  // Test a large representable value:
  double d = ldexp(1.0, 52);
  uint64_t u = uint64_t{1} << 52;
  EXPECT_EQ(0, Comp(d, u));
  EXPECT_EQ(0, Comp(u, d));
  EXPECT_EQ(0, Comp(d + 1.0, u + 1));
  EXPECT_EQ(0, Comp(u + 1, d + 1.0));

  // Test a large non-representable value:
  d = ldexp(1.0, 53);
  u = uint64_t{1} << 53;
  EXPECT_EQ(0, Comp(d, u));
  EXPECT_EQ(0, Comp(u, d));
  // d+1.0 is equal to d here due to limited precision!
  EXPECT_EQ(-1, Comp(d + 1.0, u + 1));
  EXPECT_EQ(1, Comp(u + 1, d + 1.0));

  // Test another large non-representable value:
  d = ldexp(1.0, 60);
  u = uint64_t{1} << 60;
  EXPECT_EQ(0, Comp(d, u));
  EXPECT_EQ(0, Comp(u, d));
  // d+1.0 is equal to d here due to limited precision!
  EXPECT_EQ(-1, Comp(d + 1.0, u + 1));
  EXPECT_EQ(1, Comp(u + 1, d + 1.0));

  // Test close to the top:
  d = ldexp(1.0, 63);
  u = uint64_t{1} << 63;
  EXPECT_EQ(0, Comp(d, u));
  EXPECT_EQ(0, Comp(u, d));
  // d+1.0 is equal to d here due to limited precision!
  EXPECT_EQ(-1, Comp(d + 1.0, u + 1));
  EXPECT_EQ(1, Comp(u + 1, d + 1.0));

  // Test rounding down:
  d = ldexp(1.0, 60);
  u = (uint64_t{1} << 61) - 1;
  EXPECT_EQ(-1, Comp(d, u));
  EXPECT_EQ(1, Comp(u, d));
  d = ldexp(1.0, 61);
  EXPECT_EQ(1, Comp(d, u));
  EXPECT_EQ(-1, Comp(u, d));

  // Test doubles between two representable integers:
  d = ldexp(1.0, 51) + 0.5;
  u = uint64_t{1} << 51;
  EXPECT_EQ(1, Comp(d, u));
  EXPECT_EQ(-1, Comp(u, d));
  EXPECT_EQ(-1, Comp(d, u + 1));
  EXPECT_EQ(1, Comp(u + 1, d));

  // Test when no precision is lost by a large margin:
  d = 123456789.0;
  u = 123456789;
  EXPECT_EQ(0, Comp(d, u));
  EXPECT_EQ(0, Comp(u, d));
  EXPECT_EQ(1, Comp(d + 0.5, u));
  EXPECT_EQ(-1, Comp(u, d + 0.5));
  EXPECT_EQ(1, Comp(d + 1.0, u));
  EXPECT_EQ(-1, Comp(u, d + 1.0));
  EXPECT_EQ(1, Comp(d, u - 1));
  EXPECT_EQ(-1, Comp(u - 1, d));
}

TEST(VPackHelperTest, test_signed_double_comparison) {
  // Test a large representable value:
  double d = -ldexp(1.0, 52);
  int64_t i = -(int64_t{1} << 52);
  EXPECT_EQ(0, Comp(d, i));
  EXPECT_EQ(0, Comp(i, d));
  EXPECT_EQ(0, Comp(d + 1.0, i + 1));
  EXPECT_EQ(0, Comp(i + 1, d + 1.0));

  // Test a large non-representable value:
  d = -ldexp(1.0, 53);
  i = -(int64_t{1} << 53);
  EXPECT_EQ(0, Comp(d, i));
  EXPECT_EQ(0, Comp(i, d));
  // d-1.0 is equal to d here due to limited precision!
  EXPECT_EQ(1, Comp(d - 1.0, i - 1));
  EXPECT_EQ(-1, Comp(i - 1, d - 1.0));

  // Test another large non-representable value:
  d = -ldexp(1.0, 60);
  i = -(int64_t{1} << 60);
  EXPECT_EQ(0, Comp(d, i));
  EXPECT_EQ(0, Comp(i, d));
  // d+1.0 is equal to d here due to limited precision!
  EXPECT_EQ(-1, Comp(d + 1.0, i + 1));
  EXPECT_EQ(1, Comp(i + 1, d + 1.0));

  // Test close to the top:
  d = -ldexp(1.0, 62);
  i = -(int64_t{1} << 62);
  EXPECT_EQ(0, Comp(d, i));
  EXPECT_EQ(0, Comp(i, d));
  // d+1.0 is equal to d here due to limited precision!
  EXPECT_EQ(-1, Comp(d + 1.0, i + 1));
  EXPECT_EQ(1, Comp(i + 1, d + 1.0));

  // Test rounding down:
  d = -ldexp(1.0, 60);
  i = -((int64_t{1} << 61) - 1);
  EXPECT_EQ(1, Comp(d, i));
  EXPECT_EQ(-1, Comp(i, d));
  d = -ldexp(1.0, 61);
  EXPECT_EQ(-1, Comp(d, i));
  EXPECT_EQ(1, Comp(i, d));

  // Test doubles between two representable integers:
  d = -ldexp(1.0, 51) + 0.5;
  i = -(int64_t{1} << 51);
  EXPECT_EQ(1, Comp(d, i));
  EXPECT_EQ(-1, Comp(i, d));
  EXPECT_EQ(-1, Comp(d, i + 1));
  EXPECT_EQ(1, Comp(i + 1, d));

  // Test when no precision is lost by a large margin:
  d = -123456789.0;
  i = -123456789;
  EXPECT_EQ(0, Comp(d, i));
  EXPECT_EQ(0, Comp(i, d));
  EXPECT_EQ(1, Comp(d + 0.5, i));
  EXPECT_EQ(-1, Comp(i, d + 0.5));
  EXPECT_EQ(1, Comp(d + 1.0, i));
  EXPECT_EQ(-1, Comp(i, d + 1.0));
  EXPECT_EQ(1, Comp(d, i - 1));
  EXPECT_EQ(-1, Comp(i - 1, d));

  // Test the smallest signed integer:
  i = std::numeric_limits<int64_t>::min();
  d = -ldexp(1.0, 63);
  EXPECT_EQ(0, Comp(d, i));
  EXPECT_EQ(0, Comp(i, d));
  EXPECT_EQ(-1, Comp(d, i + 1));
  EXPECT_EQ(1, Comp(i + 1, d));
}

TEST(VPackHelperTest, test_generic_uses_correct_numerical_comparison) {
  // Test large non-representable value:
  double d = ldexp(1.0, 60);
  uint64_t u = uint64_t{1} << 60;
  EXPECT_EQ(0, CompGeneric(d, u));
  EXPECT_EQ(0, CompGeneric(u, d));
  // d+1.0 is equal to d here due to limited precision!
  EXPECT_EQ(-1, CompGeneric(d + 1.0, u + 1));
  EXPECT_EQ(1, CompGeneric(u + 1, d + 1.0));

  // Test another large non-representable value:
  d = -ldexp(1.0, 60);
  int64_t i = -(int64_t{1} << 60);
  EXPECT_EQ(0, CompGeneric(d, i));
  EXPECT_EQ(0, CompGeneric(i, d));
  // d+1.0 is equal to d here due to limited precision!
  EXPECT_EQ(-1, CompGeneric(d + 1.0, i + 1));
  EXPECT_EQ(1, CompGeneric(i + 1, d + 1.0));

  // Now compare signed and unsigned:
  u = uint64_t{1} << 60;
  i = int64_t{1} << 60;
  EXPECT_EQ(0, CompGeneric(u, i));
  EXPECT_EQ(0, CompGeneric(i, u));
  EXPECT_EQ(0, CompGeneric(u + 1, i + 1));
  EXPECT_EQ(0, CompGeneric(i + 1, u + 1));
  EXPECT_EQ(0, CompGeneric(u - 1, i - 1));
  EXPECT_EQ(0, CompGeneric(i - 1, u - 1));
  EXPECT_EQ(1, CompGeneric(u + 1, i));
  EXPECT_EQ(-1, CompGeneric(i, u + 1));
  EXPECT_EQ(-1, CompGeneric(u - 1, i));
  EXPECT_EQ(1, CompGeneric(i, u - 1));
  EXPECT_EQ(1, CompGeneric(i + 1, u));
  EXPECT_EQ(-1, CompGeneric(u, i + 1));
  EXPECT_EQ(-1, CompGeneric(i - 1, u));
  EXPECT_EQ(1, CompGeneric(u, i - 1));
}

TEST(VPackHelperTest, test_small_int_with_int) {
  int64_t i = int64_t{5};
  EXPECT_EQ(0, CompGeneric(i, i));
  EXPECT_EQ(1, CompGeneric(i + 100, i));
  EXPECT_EQ(-1, CompGeneric(i, i + 100));
}

}  // namespace test::helper
