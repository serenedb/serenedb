////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/functions/math_extra.h"

#include <folly/Random.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/type/SimpleFunctionApi.h>

#include <cmath>
#include <numeric>
#include <random>

#include "basics/fwd.h"

namespace sdb::pg::functions {
namespace {

// Logarithm of value in the given base.
template<typename T>
struct PgLogBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double base, double value) {
    result = std::log(value) / std::log(base);
  }
};

// Integer quotient of y/x (truncates towards zero).
template<typename T>
struct PgDiv {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result, int64_t y, int64_t x) {
    VELOX_USER_CHECK(x != 0, "division by zero");
    result = y / x;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, double y, double x) {
    VELOX_USER_CHECK(x != 0.0, "division by zero");
    result = static_cast<int64_t>(std::trunc(y / x));
  }
};

template<typename T>
struct PgGcd {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, int32_t a, int32_t b) {
    result = static_cast<int32_t>(std::gcd(a, b));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, int64_t a, int64_t b) {
    result = std::gcd(a, b);
  }
};

template<typename T>
struct PgLcm {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, int32_t a, int32_t b) {
    result = static_cast<int32_t>(std::lcm(a, b));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, int64_t a, int64_t b) {
    result = std::lcm(a, b);
  }
};

// erf(double) -> double
template<typename T>
struct PgErf {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double x) {
    result = std::erf(x);
  }
};

// erfc(double) -> double
template<typename T>
struct PgErfc {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double x) {
    result = std::erfc(x);
  }
};

// random_normal(mean, stddev) -> double
// Returns a random value from normal distribution.
template<typename T>
struct PgRandomNormal {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void call(double& result, double mean, double stddev) {
    // Box-Muller transform using folly::Random.
    double u1 = folly::Random::randDouble01();
    double u2 = folly::Random::randDouble01();
    double z = std::sqrt(-2.0 * std::log(u1)) * std::cos(2.0 * M_PI * u2);
    result = mean + stddev * z;
  }
};

constexpr double kDegreesToRadians = M_PI / 180.0;
constexpr double kRadiansToDegrees = 180.0 / M_PI;

// Degree-based trigonometric functions.
template<typename T>
struct PgSinD {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double x) {
    result = std::sin(x * kDegreesToRadians);
  }
};

template<typename T>
struct PgCosD {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double x) {
    result = std::cos(x * kDegreesToRadians);
  }
};

template<typename T>
struct PgTanD {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double x) {
    result = std::tan(x * kDegreesToRadians);
  }
};

template<typename T>
struct PgCotD {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double x) {
    result = 1.0 / std::tan(x * kDegreesToRadians);
  }
};

template<typename T>
struct PgASinD {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double x) {
    result = std::asin(x) * kRadiansToDegrees;
  }
};

template<typename T>
struct PgACosD {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double x) {
    result = std::acos(x) * kRadiansToDegrees;
  }
};

template<typename T>
struct PgATanD {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double x) {
    result = std::atan(x) * kRadiansToDegrees;
  }
};

template<typename T>
struct PgATan2D {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double y, double x) {
    result = std::atan2(y, x) * kRadiansToDegrees;
  }
};

// setseed(double) -> void (returns empty text as PG returns void)
template<typename T>
struct PgSetSeed {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result, double seed) {
    std::srand(static_cast<unsigned>(seed * RAND_MAX));
    result.resize(0);
  }
};

}  // namespace

void registerMathExtraFunctions(const std::string& prefix) {
  velox::registerFunction<PgLogBase, double, double, double>({prefix + "log"});

  velox::registerFunction<PgDiv, int64_t, int64_t, int64_t>(
    {prefix + "div"});
  velox::registerFunction<PgDiv, int64_t, double, double>(
    {prefix + "div"});

  velox::registerFunction<PgGcd, int32_t, int32_t, int32_t>({prefix + "gcd"});
  velox::registerFunction<PgGcd, int64_t, int64_t, int64_t>({prefix + "gcd"});
  velox::registerFunction<PgLcm, int32_t, int32_t, int32_t>({prefix + "lcm"});
  velox::registerFunction<PgLcm, int64_t, int64_t, int64_t>({prefix + "lcm"});

  velox::registerFunction<PgSinD, double, double>({prefix + "sind"});
  velox::registerFunction<PgCosD, double, double>({prefix + "cosd"});
  velox::registerFunction<PgTanD, double, double>({prefix + "tand"});
  velox::registerFunction<PgCotD, double, double>({prefix + "cotd"});
  velox::registerFunction<PgASinD, double, double>({prefix + "asind"});
  velox::registerFunction<PgACosD, double, double>({prefix + "acosd"});
  velox::registerFunction<PgATanD, double, double>({prefix + "atand"});
  velox::registerFunction<PgATan2D, double, double, double>(
    {prefix + "atan2d"});

  velox::registerFunction<PgSetSeed, velox::Varchar, double>(
    {prefix + "setseed"});
  velox::registerFunction<PgErf, double, double>({prefix + "erf"});
  velox::registerFunction<PgErfc, double, double>({prefix + "erfc"});
  velox::registerFunction<PgRandomNormal, double, double, double>(
    {prefix + "random_normal"});
}

}  // namespace sdb::pg::functions
