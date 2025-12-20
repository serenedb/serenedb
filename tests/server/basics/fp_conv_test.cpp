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

#include <basics/sink.h>

#include <cmath>

#include "basics/arithmetic.h"
#include "basics/string_buffer.h"
#include "gtest/gtest.h"

namespace {

using namespace sdb;
using namespace sdb::basics;

////////////////////////////////////////////////////////////////////////////////
/// test nan
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_nan) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = NAN;
  EXPECT_TRUE(std::isnan(value));
  length = dtoa_vpack(value, out) - out;

  auto sv = std::string_view(out, length);
  EXPECT_TRUE(sv == "\"NaN\"") << sv;

  StringBuffer buf;
  buf.PushF64(value);
  sv = std::string_view(buf.data(), buf.size());
  EXPECT_TRUE(sv == "\"NaN\"") << sv;
}

////////////////////////////////////////////////////////////////////////////////
/// test infinity
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_inf) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = INFINITY;
  EXPECT_FALSE(std::isfinite(value));
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("\"Infinity\"", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("\"Infinity\"", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test huge val
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_huge_val) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = HUGE_VAL;
  EXPECT_FALSE(std::isfinite(value));
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("\"Infinity\"", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("\"Infinity\"", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test huge val
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_huge_val_neg) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = -HUGE_VAL;
  EXPECT_FALSE(std::isfinite(value));
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("\"-Infinity\"", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("\"-Infinity\"", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test zero
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_zero) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = 0;
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("0", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("0", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test zero
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_zero_neg) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = -0;
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("0", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("0", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test high
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_value_high) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = 4.32e261;
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("4.32e+261", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("4.32e+261", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test low
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_value_low) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = -4.32e261;
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("-4.32e+261", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("-4.32e+261", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test small
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_value_small) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = 4.32e-261;
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("4.32e-261", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("4.32e-261", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test mchacki's value
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_value_mchacki1) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = 1.374;
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("1.374", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("1.374", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test mchacki's value
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_value_mchacki2) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = 56.94837631946843;
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("56.94837631946843", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("56.94837631946843", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test one third
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_one_third) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = 1.0 / 3.0;
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("0.3333333333333333", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("0.3333333333333333", buf.Impl());
}

////////////////////////////////////////////////////////////////////////////////
/// test 0.4
////////////////////////////////////////////////////////////////////////////////

TEST(CFpconvTest, tst_04) {
  char out[kNumberStrMaxLen];
  double value;
  int length;

  value = 0.1 + 0.3;
  length = dtoa_vpack(value, out) - out;

  EXPECT_EQ("0.4", std::string_view(out, length));

  StringBuffer buf;
  buf.PushF64(value);
  EXPECT_EQ("0.4", buf.Impl());
}

}  // namespace
