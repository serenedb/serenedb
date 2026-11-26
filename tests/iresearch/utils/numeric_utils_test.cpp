////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <absl/algorithm/container.h>
#include <absl/numeric/int128.h>

#include <bit>
#include <cmath>
#include <iresearch/utils/numeric_utils.hpp>
#include <limits>
#include <random>
#include <span>
#include <vector>

#include "tests_shared.hpp"

namespace {

namespace nu = irs::numeric_utils;

duckdb::hugeint_t Wide(__int128 value) {
  return duckdb::hugeint_t::FromInt128(absl::int128{value});
}

duckdb::uhugeint_t Wide(unsigned __int128 value) {
  return duckdb::uhugeint_t::FromUint128(absl::uint128{value});
}

template<typename T>
irs::bstring Term(T value) {
  irs::byte_type buf[nu::kNumericTermMaxSize];
  return irs::bstring{nu::EncodeNumericTerm(buf, value)};
}

template<typename T>
void AssertTermsSortLikeValues(std::span<const T> values) {
  std::vector<irs::bstring> terms;
  for (const auto value : values) {
    auto term = Term(value);
    ASSERT_EQ(sizeof(T), term.size());
    ASSERT_TRUE(value == nu::DecodeNumericTerm<T>(term));
    terms.emplace_back(std::move(term));
  }
  for (size_t i = 1; i < values.size(); ++i) {
    if (values[i - 1] < values[i]) {
      ASSERT_LT(terms[i - 1], terms[i]);
    } else {
      ASSERT_TRUE(values[i - 1] == values[i]);
      ASSERT_EQ(terms[i - 1], terms[i]);
    }
  }
}

template<typename T, typename Bits>
void AssertRandomTermsSortLikeValues(uint64_t seed) {
  std::mt19937_64 gen{seed};
  std::vector<T> values;
  while (values.size() != 4096) {
    Bits bits = static_cast<Bits>(gen());
    if constexpr (sizeof(Bits) > sizeof(uint64_t)) {
      bits = bits << 64 | gen();
    }
    if constexpr (std::is_floating_point_v<T>) {
      const auto value = std::bit_cast<T>(bits);
      if (!std::isnan(value)) {
        values.push_back(value);
      }
    } else if constexpr (sizeof(Bits) > sizeof(uint64_t)) {
      values.push_back(Wide(
        static_cast<std::conditional_t<std::is_same_v<T, duckdb::hugeint_t>,
                                       __int128, unsigned __int128>>(bits)));
    } else {
      values.push_back(static_cast<T>(bits));
    }
  }
  absl::c_sort(values);
  AssertTermsSortLikeValues<T>(values);
}

template<typename T>
void AssertFloatEnds() {
  using Limits = std::numeric_limits<T>;
  const T values[]{
    -Limits::infinity(),  Limits::lowest(), T(-1), -Limits::denorm_min(),
    Limits::denorm_min(), Limits::min(),    T(1),  Limits::max(),
    Limits::infinity()};
  for (const auto value : values) {
    const auto decoded = nu::DecodeNumericTerm<T>(Term(value));
    ASSERT_EQ(value, decoded);
    ASSERT_EQ(std::signbit(value), std::signbit(decoded));
  }
  ASSERT_EQ(T(0), nu::DecodeNumericTerm<T>(Term(-T(0))));
  ASSERT_FALSE(std::signbit(nu::DecodeNumericTerm<T>(Term(-T(0)))));
  ASSERT_LT(Term(-Limits::infinity()), Term(Limits::lowest()));
  ASSERT_LT(Term(Limits::max()), Term(Limits::infinity()));
}

}  // namespace

TEST(numeric_utils_test, terms_sort_like_their_values) {
  constexpr auto kI32Min = std::numeric_limits<int32_t>::min();
  constexpr auto kI32Max = std::numeric_limits<int32_t>::max();
  const int32_t i32[]{
    kI32Min, kI32Min + 1, -(int32_t{1} << 24), -256,        -1,     0, 1,
    255,     256,         int32_t{1} << 24,    kI32Max - 1, kI32Max};
  AssertTermsSortLikeValues<int32_t>(i32);

  constexpr auto kI64Min = std::numeric_limits<int64_t>::min();
  constexpr auto kI64Max = std::numeric_limits<int64_t>::max();
  const int64_t i64[]{kI64Min, kI64Min + 1,      -(int64_t{1} << 32), -1,     0,
                      1,       int64_t{1} << 32, kI64Max - 1,         kI64Max};
  AssertTermsSortLikeValues<int64_t>(i64);

  constexpr uint64_t kU64Max = std::numeric_limits<uint64_t>::max();
  constexpr uint64_t kU64Half = uint64_t{1} << 63;
  const uint64_t u64[]{0, 1, kU64Half - 1, kU64Half, kU64Max - 1, kU64Max};
  AssertTermsSortLikeValues<uint64_t>(u64);

  constexpr auto kI128Min = std::numeric_limits<__int128>::min();
  constexpr auto kI128Max = std::numeric_limits<__int128>::max();
  const duckdb::hugeint_t i128[]{
    Wide(kI128Min),          Wide(kI128Min + 1), Wide(-(__int128{1} << 64)),
    Wide(__int128{-1}),      Wide(__int128{0}),  Wide(__int128{1}),
    Wide(__int128{1} << 64), Wide(kI128Max - 1), Wide(kI128Max)};
  AssertTermsSortLikeValues<duckdb::hugeint_t>(i128);

  using U128 = unsigned __int128;
  constexpr auto kU128Max = std::numeric_limits<U128>::max();
  constexpr U128 kU128Half = U128{1} << 127;
  const duckdb::uhugeint_t u128[]{Wide(U128{0}),       Wide(U128{1}),
                                  Wide(U128{1} << 64), Wide(kU128Half - 1),
                                  Wide(kU128Half),     Wide(kU128Max - 1),
                                  Wide(kU128Max)};
  AssertTermsSortLikeValues<duckdb::uhugeint_t>(u128);

  using F = std::numeric_limits<float>;
  const float f32[]{
    -F::infinity(),  F::lowest(), -1.5f, -F::min(), -F::denorm_min(), 0.f,
    F::denorm_min(), F::min(),    1.5f,  F::max(),  F::infinity()};
  AssertTermsSortLikeValues<float>(f32);

  using D = std::numeric_limits<double>;
  const double f64[]{
    -D::infinity(),  D::lowest(), -1.5, -D::min(), -D::denorm_min(), 0.,
    D::denorm_min(), D::min(),    1.5,  D::max(),  D::infinity()};
  AssertTermsSortLikeValues<double>(f64);
}

TEST(numeric_utils_test, random_terms_sort_like_their_values) {
  AssertRandomTermsSortLikeValues<int32_t, uint32_t>(1);
  AssertRandomTermsSortLikeValues<int64_t, uint64_t>(2);
  AssertRandomTermsSortLikeValues<uint64_t, uint64_t>(3);
  AssertRandomTermsSortLikeValues<duckdb::hugeint_t, unsigned __int128>(4);
  AssertRandomTermsSortLikeValues<duckdb::uhugeint_t, unsigned __int128>(5);
  AssertRandomTermsSortLikeValues<float, uint32_t>(6);
  AssertRandomTermsSortLikeValues<double, uint64_t>(7);
}

TEST(numeric_utils_test, float_terms_are_canonical) {
  EXPECT_EQ(Term(0.f), Term(-0.f));
  EXPECT_EQ(Term(0.), Term(-0.));
  const auto f_nan = std::numeric_limits<float>::quiet_NaN();
  const auto d_nan = std::numeric_limits<double>::quiet_NaN();
  EXPECT_EQ(Term(f_nan), Term(-std::numeric_limits<float>::signaling_NaN()));
  EXPECT_EQ(Term(d_nan), Term(-std::numeric_limits<double>::signaling_NaN()));
  EXPECT_LT(Term(std::numeric_limits<float>::infinity()), Term(f_nan));
  EXPECT_LT(Term(std::numeric_limits<double>::infinity()), Term(d_nan));
  EXPECT_TRUE(std::isnan(nu::DecodeNumericTerm<float>(Term(f_nan))));
  EXPECT_TRUE(std::isnan(nu::DecodeNumericTerm<double>(Term(d_nan))));
}

TEST(numeric_utils_test, float_terms_round_trip_to_the_ends) {
  AssertFloatEnds<float>();
  AssertFloatEnds<double>();
}
