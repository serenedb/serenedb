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

#include "pg/functions/math_extra.h"

#include <folly/Random.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/type/SimpleFunctionApi.h>

#include <cmath>
#include <limits>
#include <numeric>
#include <random>

#include "basics/fwd.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

// Logarithm of value in the given base.
template<typename T>
struct PgLogBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result, double base, double value) {
    if (value == 0.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                      ERR_MSG("cannot take logarithm of zero"));
    }
    if (value < 0.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                      ERR_MSG("cannot take logarithm of a negative number"));
    }
    if (base == 0.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                      ERR_MSG("cannot take logarithm of zero"));
    }
    if (base < 0.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                      ERR_MSG("cannot take logarithm of a negative number"));
    }
    if (base == 1.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                      ERR_MSG("division by zero"));
    }
    result = std::log(value) / std::log(base);
  }
};

// Integer quotient of y/x (truncates towards zero).
template<typename T>
struct PgDiv {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result, int64_t y, int64_t x) {
    if (x == 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                      ERR_MSG("division by zero"));
    }
    if (y == std::numeric_limits<int64_t>::min() && x == -1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("bigint out of range"));
    }
    result = y / x;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, double y, double x) {
    if (x == 0.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                      ERR_MSG("division by zero"));
    }
    double truncated = std::trunc(y / x);
    if (truncated < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
        truncated > static_cast<double>(std::numeric_limits<int64_t>::max())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("bigint out of range"));
    }
    result = static_cast<int64_t>(truncated);
  }
};

template<typename T>
struct PgGcd {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, int32_t a, int32_t b) {
    if (a == std::numeric_limits<int32_t>::min() ||
        b == std::numeric_limits<int32_t>::min()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("integer out of range"));
    }
    result = static_cast<int32_t>(std::gcd(a, b));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, int64_t a, int64_t b) {
    if (a == std::numeric_limits<int64_t>::min() ||
        b == std::numeric_limits<int64_t>::min()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("bigint out of range"));
    }
    result = std::gcd(a, b);
  }
};

template<typename T>
struct PgLcm {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, int32_t a, int32_t b) {
    CallImpl(result, a, b, "integer out of range");
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, int64_t a, int64_t b) {
    CallImpl(result, a, b, "bigint out of range");
  }

 private:
  template<typename S>
  FOLLY_ALWAYS_INLINE static std::make_unsigned_t<S> AbsImpl(S s) {
    if (s >= 0) {
      return s;
    }
    if (s == std::numeric_limits<S>::min()) {
      return -static_cast<std::make_unsigned_t<S>>(s);
    }
    return -s;
  }

  template<typename S>
  FOLLY_ALWAYS_INLINE static void CallImpl(S& sr, S s1, S s2, const char* msg) {
    if (s1 == 0 || s2 == 0) {
      sr = 0;
      return;
    }
    using U = std::make_unsigned_t<S>;
    U u1 = AbsImpl(s1) / std::gcd(AbsImpl(s1), AbsImpl(s2));
    U u2 = AbsImpl(s2);
    U ur;
    if (__builtin_mul_overflow(u1, u2, &ur) ||
        ur > static_cast<U>(std::numeric_limits<S>::max())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG(msg));
    }
    sr = static_cast<S>(ur);
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
    double u1;
    do {
      u1 = folly::Random::randDouble01();
    } while (u1 == 0.0);
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
    double t = std::tan(x * kDegreesToRadians);
    if (t == 0.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                      ERR_MSG("division by zero"));
    }
    result = 1.0 / t;
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

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result, double) {
    result.resize(0);
  }
};

}  // namespace

void registerMathExtraFunctions(const std::string& prefix) {
  velox::registerFunction<PgLogBase, double, double, double>({prefix + "log"});

  velox::registerFunction<PgDiv, int64_t, int64_t, int64_t>({prefix + "div"});
  velox::registerFunction<PgDiv, int64_t, double, double>({prefix + "div"});

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
