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

#include "duckdb/common/types/hugeint.hpp"

#include <benchmark/benchmark.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <random>
#include <type_traits>
#include <vector>

#include "duckdb/common/types/uhugeint.hpp"

namespace {

using duckdb::Hugeint;
using duckdb::hugeint_t;
using duckdb::Uhugeint;
using duckdb::uhugeint_t;
using i128 = __int128;
using u128 = unsigned __int128;

constexpr size_t kCount = 4096;

template<typename T>
constexpr bool kSigned =
  std::is_same_v<T, hugeint_t> || std::is_same_v<T, i128>;

struct Narrow {
  static u128 Draw(std::mt19937_64& rng, bool is_signed) {
    auto value = rng();
    if (is_signed) {
      return static_cast<u128>(static_cast<i128>(static_cast<int64_t>(value)));
    }
    return value;
  }
};

struct Wide {
  static u128 Draw(std::mt19937_64& rng, bool) {
    auto upper = rng();
    return (static_cast<u128>(upper) << 64) | rng();
  }
};

struct Div64 {
  static u128 Draw(std::mt19937_64& rng, bool) { return (rng() >> 1) | 1; }
};

struct Pow2Low {
  static u128 Draw(std::mt19937_64& rng, bool) {
    return static_cast<u128>(1) << (rng() % 64);
  }
};

struct Pow2High {
  static u128 Draw(std::mt19937_64& rng, bool) {
    return static_cast<u128>(1) << (64 + rng() % 63);
  }
};

struct ShiftAmount {
  static u128 Draw(std::mt19937_64& rng, bool) { return rng() % 128; }
};

struct Pow10 {
  static u128 Draw(std::mt19937_64& rng, bool) {
    u128 result = 1;
    for (auto i = rng() % 38 + 1; i > 0; --i) {
      result *= 10;
    }
    return result;
  }
};

template<typename T>
T FromBits(u128 bits) {
  if constexpr (std::is_same_v<T, hugeint_t>) {
    return hugeint_t(static_cast<int64_t>(static_cast<uint64_t>(bits >> 64)),
                     static_cast<uint64_t>(bits));
  } else if constexpr (std::is_same_v<T, uhugeint_t>) {
    return uhugeint_t(static_cast<uint64_t>(bits >> 64),
                      static_cast<uint64_t>(bits));
  } else {
    return static_cast<T>(bits);
  }
}

template<typename T, typename Class, uint64_t kSeed>
const std::vector<T>& Values() {
  static const std::vector<T> kValues = [] {
    std::mt19937_64 rng{kSeed};
    std::vector<T> result;
    result.reserve(kCount);
    for (size_t i = 0; i < kCount; ++i) {
      result.push_back(FromBits<T>(Class::Draw(rng, kSigned<T>)));
    }
    return result;
  }();
  return kValues;
}

template<typename T>
using Stored = std::conditional_t<std::is_same_v<T, bool>, uint8_t, T>;

std::byte* Arena(size_t slot) {
  constexpr size_t kSlotBytes = kCount * 16 + 5 * 1024;
  alignas(4096) static std::byte arena[3 * kSlotBytes];
  return arena + slot * kSlotBytes;
}

template<typename T>
const T* Place(const std::vector<T>& values, size_t slot) {
  auto* data = Arena(slot);
  std::memcpy(data, values.data(), values.size() * sizeof(T));
  return std::launder(reinterpret_cast<const T*>(data));
}

template<typename T>
T* Output(size_t slot) {
  auto* data = Arena(slot);
  std::memset(data, 0, kCount * sizeof(T));
  return std::launder(reinterpret_cast<T*>(data));
}

struct Add {
  template<typename T>
  static T Run(T lhs, T rhs) {
    return lhs + rhs;
  }
};

struct Sub {
  template<typename T>
  static T Run(T lhs, T rhs) {
    return lhs - rhs;
  }
};

struct Mul {
  template<typename T>
  static T Run(T lhs, T rhs) {
    return lhs * rhs;
  }
};

struct Div {
  template<typename T>
  static T Run(T lhs, T rhs) {
    return lhs / rhs;
  }
};

struct Mod {
  template<typename T>
  static T Run(T lhs, T rhs) {
    return lhs % rhs;
  }
};

struct ShiftLeft {
  template<typename T>
  static T Run(T lhs, T rhs) {
    return lhs << rhs;
  }
};

struct ShiftRight {
  template<typename T>
  static T Run(T lhs, T rhs) {
    return lhs >> rhs;
  }
};

struct Xor {
  template<typename T>
  static T Run(T lhs, T rhs) {
    return lhs ^ rhs;
  }
};

struct Less {
  template<typename T>
  static bool Run(T lhs, T rhs) {
    return lhs < rhs;
  }
};

struct Equal {
  template<typename T>
  static bool Run(T lhs, T rhs) {
    return lhs == rhs;
  }
};

struct CheckedAdd {
  static hugeint_t Run(hugeint_t lhs, hugeint_t rhs) {
    return Hugeint::TryAddInPlace(lhs, rhs) ? lhs : hugeint_t(0);
  }
  static uhugeint_t Run(uhugeint_t lhs, uhugeint_t rhs) {
    return Uhugeint::TryAddInPlace(lhs, rhs) ? lhs : uhugeint_t(0);
  }
  template<typename T>
  static T Run(T lhs, T rhs) {
    T result;
    return __builtin_add_overflow(lhs, rhs, &result) ? 0 : result;
  }
};

struct CheckedSub {
  static hugeint_t Run(hugeint_t lhs, hugeint_t rhs) {
    return Hugeint::TrySubtractInPlace(lhs, rhs) ? lhs : hugeint_t(0);
  }
  static uhugeint_t Run(uhugeint_t lhs, uhugeint_t rhs) {
    return Uhugeint::TrySubtractInPlace(lhs, rhs) ? lhs : uhugeint_t(0);
  }
  template<typename T>
  static T Run(T lhs, T rhs) {
    T result;
    return __builtin_sub_overflow(lhs, rhs, &result) ? 0 : result;
  }
};

struct CheckedMul {
  static hugeint_t Run(hugeint_t lhs, hugeint_t rhs) {
    hugeint_t result;
    return Hugeint::TryMultiply(lhs, rhs, result) ? result : hugeint_t(0);
  }
  static uhugeint_t Run(uhugeint_t lhs, uhugeint_t rhs) {
    uhugeint_t result;
    return Uhugeint::TryMultiply(lhs, rhs, result) ? result : uhugeint_t(0);
  }
  template<typename T>
  static T Run(T lhs, T rhs) {
    T result;
    return __builtin_mul_overflow(lhs, rhs, &result) ? 0 : result;
  }
};

struct CheckedDiv {
  static hugeint_t Run(hugeint_t lhs, hugeint_t rhs) {
    hugeint_t result;
    hugeint_t remainder;
    return Hugeint::TryDivMod(lhs, rhs, result, remainder) ? result
                                                           : hugeint_t(0);
  }
  static uhugeint_t Run(uhugeint_t lhs, uhugeint_t rhs) {
    uhugeint_t result;
    uhugeint_t remainder;
    return Uhugeint::TryDivMod(lhs, rhs, result, remainder) ? result
                                                            : uhugeint_t(0);
  }
  template<typename T>
  static T Run(T lhs, T rhs) {
    if (rhs == 0 || (kSigned<T> && rhs == static_cast<T>(-1) &&
                     lhs == std::numeric_limits<T>::min())) {
      return 0;
    }
    return lhs / rhs;
  }
};

struct Negate {
  static hugeint_t Run(hugeint_t value) {
    hugeint_t result;
    return Hugeint::TryNegate(value, result) ? result : hugeint_t(0);
  }
  static uhugeint_t Run(uhugeint_t value) {
    uhugeint_t result;
    return Uhugeint::TryNegate(value, result) ? result : uhugeint_t(0);
  }
  template<typename T>
  static T Run(T value) {
    if constexpr (kSigned<T>) {
      T result;
      return __builtin_sub_overflow(T{0}, value, &result) ? 0 : result;
    } else {
      return -value;
    }
  }
};

template<typename T, typename Op, typename L, typename R>
void BmBinary(benchmark::State& state) {
  const auto* lhs = Place(Values<T, L, 1>(), 0);
  const auto* rhs = Place(Values<T, R, 2>(), 1);
  auto* out = Output<Stored<decltype(Op::Run(lhs[0], rhs[0]))>>(2);
  for (auto _ : state) {
    for (size_t i = 0; i < kCount; ++i) {
      out[i] = Op::Run(lhs[i], rhs[i]);
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * kCount);
}

template<typename T, typename Op, typename L, typename R>
void BmBinaryReused(benchmark::State& state) {
  const auto* lhs = Place(Values<T, L, 1>(), 0);
  const auto rhs = Values<T, R, 2>()[0];
  auto* out = Output<Stored<decltype(Op::Run(lhs[0], rhs))>>(2);
  for (auto _ : state) {
    for (size_t i = 0; i < kCount; ++i) {
      out[i] = Op::Run(lhs[i], rhs);
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * kCount);
}

template<typename T, typename Op, typename C>
void BmUnary(benchmark::State& state) {
  const auto* input = Place(Values<T, C, 3>(), 0);
  auto* out = Output<T>(2);
  for (auto _ : state) {
    for (size_t i = 0; i < kCount; ++i) {
      out[i] = Op::Run(input[i]);
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * kCount);
}

template<typename T, typename Source>
bool ConvertFrom(Source value, T& result) {
  if constexpr (std::is_same_v<T, hugeint_t>) {
    return Hugeint::TryConvert(value, result);
  } else if constexpr (std::is_same_v<T, uhugeint_t>) {
    return Uhugeint::TryConvert(value, result);
  } else if constexpr (std::is_floating_point_v<Source>) {
    if (!(value > static_cast<Source>(std::numeric_limits<T>::min()) - 1 &&
          value < static_cast<Source>(std::numeric_limits<T>::max()))) {
      return false;
    }
    result = static_cast<T>(value);
    return true;
  } else {
    if constexpr (!kSigned<T> && std::is_signed_v<Source>) {
      if (value < 0) {
        return false;
      }
    }
    result = static_cast<T>(value);
    return true;
  }
}

template<typename Target, typename T>
bool ConvertTo(T value, Target& result) {
  if constexpr (std::is_same_v<T, hugeint_t>) {
    return Hugeint::TryCast(value, result);
  } else if constexpr (std::is_same_v<T, uhugeint_t>) {
    return Uhugeint::TryCast(value, result);
  } else if constexpr (std::is_floating_point_v<Target>) {
    result = static_cast<Target>(value);
    return true;
  } else {
    if constexpr (kSigned<T>) {
      if (value < static_cast<T>(std::numeric_limits<Target>::min())) {
        return false;
      }
    }
    if (value > static_cast<T>(std::numeric_limits<Target>::max())) {
      return false;
    }
    result = static_cast<Target>(value);
    return true;
  }
}

template<typename Source>
const std::vector<Source>& Sources() {
  static const std::vector<Source> kSources = [] {
    std::mt19937_64 rng{4};
    std::vector<Source> result;
    result.reserve(kCount);
    for (size_t i = 0; i < kCount; ++i) {
      if constexpr (std::is_floating_point_v<Source>) {
        result.push_back(std::ldexp(static_cast<Source>(rng() >> 11), 40));
      } else {
        result.push_back(static_cast<Source>(rng()));
      }
    }
    return result;
  }();
  return kSources;
}

template<typename T, typename Source>
void BmConvertFrom(benchmark::State& state) {
  const auto* input = Place(Sources<Source>(), 0);
  auto* out = Output<T>(2);
  for (auto _ : state) {
    for (size_t i = 0; i < kCount; ++i) {
      if (!ConvertFrom(input[i], out[i])) {
        out[i] = T{0};
      }
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * kCount);
}

template<typename T, typename Target>
void BmConvertTo(benchmark::State& state) {
  const auto* input = Place(Values<T, Narrow, 5>(), 0);
  auto* out = Output<Target>(2);
  for (auto _ : state) {
    for (size_t i = 0; i < kCount; ++i) {
      if (!ConvertTo(input[i], out[i])) {
        out[i] = 0;
      }
    }
    benchmark::DoNotOptimize(out);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * kCount);
}

#define SDB_HUGEINT_BINARY_WITH(Bm, Op, L, R)  \
  BENCHMARK_TEMPLATE(Bm, i128, Op, L, R);      \
  BENCHMARK_TEMPLATE(Bm, hugeint_t, Op, L, R); \
  BENCHMARK_TEMPLATE(Bm, u128, Op, L, R);      \
  BENCHMARK_TEMPLATE(Bm, uhugeint_t, Op, L, R)

#define SDB_HUGEINT_BINARY(Op, L, R) SDB_HUGEINT_BINARY_WITH(BmBinary, Op, L, R)

SDB_HUGEINT_BINARY(Add, Narrow, Narrow);
SDB_HUGEINT_BINARY(Add, Wide, Wide);
SDB_HUGEINT_BINARY(Sub, Wide, Wide);
SDB_HUGEINT_BINARY(Mul, Narrow, Narrow);
SDB_HUGEINT_BINARY(Mul, Wide, Wide);
SDB_HUGEINT_BINARY(Div, Narrow, Div64);
SDB_HUGEINT_BINARY(Div, Wide, Div64);
SDB_HUGEINT_BINARY(Div, Wide, Pow2Low);
SDB_HUGEINT_BINARY(Div, Wide, Pow2High);
SDB_HUGEINT_BINARY(Div, Wide, Wide);
SDB_HUGEINT_BINARY(Mod, Narrow, Div64);
SDB_HUGEINT_BINARY(Mod, Wide, Div64);
SDB_HUGEINT_BINARY(Mod, Wide, Pow2Low);
SDB_HUGEINT_BINARY(Mod, Wide, Pow2High);
SDB_HUGEINT_BINARY(Mod, Wide, Wide);
SDB_HUGEINT_BINARY(ShiftLeft, Wide, ShiftAmount);
SDB_HUGEINT_BINARY(ShiftRight, Wide, ShiftAmount);
SDB_HUGEINT_BINARY(Xor, Wide, Wide);
SDB_HUGEINT_BINARY(Less, Wide, Wide);
SDB_HUGEINT_BINARY(Equal, Wide, Wide);
SDB_HUGEINT_BINARY(CheckedAdd, Narrow, Narrow);
SDB_HUGEINT_BINARY(CheckedAdd, Wide, Wide);
SDB_HUGEINT_BINARY(CheckedSub, Wide, Wide);
SDB_HUGEINT_BINARY(CheckedMul, Narrow, Narrow);
SDB_HUGEINT_BINARY(CheckedMul, Wide, Narrow);
SDB_HUGEINT_BINARY(CheckedDiv, Wide, Div64);
SDB_HUGEINT_BINARY(CheckedDiv, Wide, Wide);

SDB_HUGEINT_BINARY_WITH(BmBinaryReused, Div, Wide, Div64);
SDB_HUGEINT_BINARY_WITH(BmBinaryReused, Div, Wide, Pow10);
SDB_HUGEINT_BINARY_WITH(BmBinaryReused, Mod, Wide, Div64);
SDB_HUGEINT_BINARY_WITH(BmBinaryReused, Mod, Wide, Pow10);

BENCHMARK_TEMPLATE(BmUnary, i128, Negate, Wide);
BENCHMARK_TEMPLATE(BmUnary, hugeint_t, Negate, Wide);
BENCHMARK_TEMPLATE(BmUnary, u128, Negate, Narrow);
BENCHMARK_TEMPLATE(BmUnary, uhugeint_t, Negate, Narrow);

BENCHMARK_TEMPLATE(BmConvertFrom, i128, int64_t);
BENCHMARK_TEMPLATE(BmConvertFrom, hugeint_t, int64_t);
BENCHMARK_TEMPLATE(BmConvertFrom, u128, uint64_t);
BENCHMARK_TEMPLATE(BmConvertFrom, uhugeint_t, uint64_t);
BENCHMARK_TEMPLATE(BmConvertFrom, i128, double);
BENCHMARK_TEMPLATE(BmConvertFrom, hugeint_t, double);
BENCHMARK_TEMPLATE(BmConvertFrom, u128, double);
BENCHMARK_TEMPLATE(BmConvertFrom, uhugeint_t, double);

BENCHMARK_TEMPLATE(BmConvertTo, i128, int64_t);
BENCHMARK_TEMPLATE(BmConvertTo, hugeint_t, int64_t);
BENCHMARK_TEMPLATE(BmConvertTo, u128, uint64_t);
BENCHMARK_TEMPLATE(BmConvertTo, uhugeint_t, uint64_t);
BENCHMARK_TEMPLATE(BmConvertTo, i128, double);
BENCHMARK_TEMPLATE(BmConvertTo, hugeint_t, double);
BENCHMARK_TEMPLATE(BmConvertTo, u128, double);
BENCHMARK_TEMPLATE(BmConvertTo, uhugeint_t, double);

}  // namespace
