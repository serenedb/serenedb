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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <benchmark/benchmark.h>

#include <bit>
#include <cfloat>
#include <climits>
#include <cstdint>
#include <cstring>
#include <duckdb/common/bswap.hpp>
#include <duckdb/common/operator/comparison_operators.hpp>
#include <duckdb/common/radix.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/value.hpp>
#include <limits>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "iresearch/analysis/numeric_terms.hpp"
#include "iresearch/utils/numeric_utils.hpp"

namespace {

constexpr size_t kValues = 4096;
constexpr size_t kChecked = 1'000'000;

template<typename T>
using Bits =
  std::conditional_t<sizeof(T) == sizeof(uint64_t), uint64_t, uint32_t>;

template<typename U>
constexpr U kSign = U{1} << (sizeof(U) * 8 - 1);

template<typename T>
constexpr Bits<T> kInfinity =
  std::bit_cast<Bits<T>>(std::numeric_limits<T>::infinity());

template<typename U>
U SignMask(U bits) {
  return static_cast<U>(static_cast<std::make_signed_t<U>>(bits) >>
                        (sizeof(U) * 8 - 1)) |
         kSign<U>;
}

struct RawBits {
  template<typename T>
  static Bits<T> One(T x) {
    return std::bit_cast<Bits<T>>(x);
  }
  template<typename T>
  static Bits<T> Block(T x) {
    return One(x);
  }
  template<typename T>
  static T Value(Bits<T> key) {
    return std::bit_cast<T>(key);
  }
};

struct Radix {
  template<typename T>
  static Bits<T> One(T x) {
    if constexpr (sizeof(T) == sizeof(uint32_t)) {
      return duckdb::Radix::EncodeFloat(x);
    } else {
      return duckdb::Radix::EncodeDouble(x);
    }
  }
  template<typename T>
  static Bits<T> Block(T x) {
    return One(x);
  }
  template<typename T>
  static T Value(Bits<T> key) {
    if constexpr (sizeof(T) == sizeof(uint32_t)) {
      return duckdb::Radix::DecodeFloat(key);
    } else {
      return duckdb::Radix::DecodeDouble(key);
    }
  }
};

struct UpstreamRadix {
  static uint32_t Encode(float x) {
    if (x == 0) {
      return kSign<uint32_t>;
    }
    if (duckdb::Value::IsNan(x)) {
      return UINT_MAX;
    }
    if (x > FLT_MAX) {
      return UINT_MAX - 1;
    }
    if (x < -FLT_MAX) {
      return 0;
    }
    const auto bits = std::bit_cast<uint32_t>(x);
    return (bits & kSign<uint32_t>) == 0 ? bits | kSign<uint32_t> : ~bits;
  }
  static uint64_t Encode(double x) {
    if (x == 0) {
      return kSign<uint64_t>;
    }
    if (duckdb::Value::IsNan(x)) {
      return ULLONG_MAX;
    }
    if (x > DBL_MAX) {
      return ULLONG_MAX - 1;
    }
    if (x < -DBL_MAX) {
      return 0;
    }
    const auto bits = std::bit_cast<uint64_t>(x);
    return bits < kSign<uint64_t> ? bits + kSign<uint64_t> : ~bits;
  }
  template<typename T>
  static Bits<T> One(T x) {
    return Encode(x);
  }
  template<typename T>
  static Bits<T> Block(T x) {
    return Encode(x);
  }
  template<typename T>
  static T Value(Bits<T> key) {
    using U = Bits<T>;
    if (key == std::numeric_limits<U>::max()) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    if (key == std::numeric_limits<U>::max() - 1) {
      return std::numeric_limits<T>::infinity();
    }
    if (key == 0) {
      return -std::numeric_limits<T>::infinity();
    }
    return std::bit_cast<T>(static_cast<U>(
      (key & kSign<U>) != 0 ? key ^ kSign<U> : static_cast<U>(~key)));
  }
};

struct MinimalOrder {
  template<typename T>
  static Bits<T> One(T x) {
    using U = Bits<T>;
    const auto bits = std::bit_cast<U>(x);
    const auto magnitude = static_cast<U>(bits & ~kSign<U>);
    if (static_cast<U>(magnitude - 1) >= kInfinity<T>) [[unlikely]] {
      if (magnitude == 0) {
        return kSign<U>;
      }
      return std::numeric_limits<U>::max();
    }
    return static_cast<U>(bits ^ SignMask(bits));
  }
  template<typename T>
  static Bits<T> Block(T x) {
    using U = Bits<T>;
    auto bits = std::bit_cast<U>(x);
    const auto magnitude = static_cast<U>(bits & ~kSign<U>);
    bits = magnitude == 0 ? U{0} : bits;
    const auto key = static_cast<U>(bits ^ SignMask(bits));
    return magnitude > kInfinity<T> ? std::numeric_limits<U>::max() : key;
  }
  template<typename T>
  static T Value(Bits<T> key) {
    using U = Bits<T>;
    return std::bit_cast<T>(
      static_cast<U>(key ^ SignMask(static_cast<U>(~key))));
  }
};

template<typename T, typename Key>
[[gnu::noinline]] void EncodeOne(const T* in, Bits<T>* out, Key key) {
#pragma clang loop vectorize(disable) interleave(disable)
  for (size_t i = 0; i < kValues; ++i) {
    Bits<T> raw;
    std::memcpy(&raw, in + i, sizeof(raw));
    out[i] = duckdb::BSwapIfLE(key(std::bit_cast<T>(raw)));
  }
}

template<typename T, typename Key>
[[gnu::noinline]] void EncodeBlock(const T* in, Bits<T>* out, Key key) {
  for (size_t i = 0; i < kValues; ++i) {
    out[i] = duckdb::BSwapIfLE(key(in[i]));
  }
}

template<typename T, typename Value>
[[gnu::noinline]] void DecodeOne(const Bits<T>* in, T* out, Value value) {
#pragma clang loop vectorize(disable) interleave(disable)
  for (size_t i = 0; i < kValues; ++i) {
    out[i] = value(duckdb::BSwapIfLE(in[i]));
  }
}

template<typename T, typename Value>
[[gnu::noinline]] void DecodeBlock(const Bits<T>* in, T* out, Value value) {
  for (size_t i = 0; i < kValues; ++i) {
    out[i] = value(duckdb::BSwapIfLE(in[i]));
  }
}

template<typename T>
void PerValueTerm(duckdb::string_t& out, T value) {
  out = duckdb::string_t{
    static_cast<uint32_t>(irs::numeric_utils::kNumericTermSize<T>)};
  irs::numeric_utils::EncodeNumericTerm<T>(
    reinterpret_cast<irs::byte_type*>(out.GetDataWriteable()), value);
}

template<typename T>
[[gnu::noinline]] void TermsPerValue(const T* in, duckdb::string_t* out) {
  for (size_t i = 0; i < kValues; ++i) {
    PerValueTerm(out[i], in[i]);
  }
}

template<typename T>
[[gnu::noinline]] void TermsBlock(const T* in, duckdb::string_t* out) {
  irs::AppendNumericTermsBlock<T>(out, std::span<const T>{in, kValues});
}

template<typename T>
bool SqlEqual(T a, T b) {
  return duckdb::Equals::Operation(a, b);
}

template<typename T>
std::vector<T> CheckedValues() {
  using U = Bits<T>;
  std::mt19937_64 rng{42};
  std::vector<T> values;
  values.reserve(kChecked + 16);
  for (size_t i = 0; i < kChecked; ++i) {
    values.push_back(std::bit_cast<T>(static_cast<U>(rng())));
  }
  for (const T special :
       {T{0}, -T{0}, std::numeric_limits<T>::infinity(),
        -std::numeric_limits<T>::infinity(),
        std::numeric_limits<T>::quiet_NaN(),
        -std::numeric_limits<T>::quiet_NaN(),
        std::numeric_limits<T>::signaling_NaN(),
        std::numeric_limits<T>::denorm_min(),
        -std::numeric_limits<T>::denorm_min(), std::numeric_limits<T>::min(),
        std::numeric_limits<T>::max(), std::numeric_limits<T>::lowest(), T{1},
        T{-1}}) {
    values.push_back(special);
  }
  absl::c_sort(values,
               [](T a, T b) { return duckdb::LessThan::Operation(a, b); });
  return values;
}

template<typename Format, typename T>
bool CheckOrder(std::string_view name, std::span<const T> sorted) {
  for (size_t i = 0; i < sorted.size(); ++i) {
    const auto key = Format::One(sorted[i]);
    if (Format::Block(sorted[i]) != key) {
      absl::PrintF("%s: the block key of %g differs\n", name, sorted[i]);
      return false;
    }
    if (!SqlEqual(Format::template Value<T>(key), sorted[i])) {
      absl::PrintF("%s: %g does not decode back\n", name, sorted[i]);
      return false;
    }
    if (i == 0) {
      continue;
    }
    const auto prev = Format::One(sorted[i - 1]);
    if (SqlEqual(sorted[i - 1], sorted[i]) ? prev != key : !(prev < key)) {
      absl::PrintF("%s: the keys of %g and %g are out of order\n", name,
                   sorted[i - 1], sorted[i]);
      return false;
    }
  }
  return true;
}

template<typename T>
bool Check(std::string_view type) {
  const auto sorted = CheckedValues<T>();
  const std::span<const T> values{sorted};
  for (const auto x : values) {
    if (Radix::One(x) != UpstreamRadix::One(x)) {
      absl::PrintF("%s: the key of %g differs from upstream DuckDB's\n", type,
                   x);
      return false;
    }
  }
  if (!CheckOrder<Radix>(absl::StrCat(type, " radix"), values) ||
      !CheckOrder<UpstreamRadix>(absl::StrCat(type, " upstream"), values) ||
      !CheckOrder<MinimalOrder>(absl::StrCat(type, " minimal"), values)) {
    return false;
  }
  std::vector<duckdb::string_t> block(kValues);
  duckdb::string_t expected;
  for (size_t at = 0; at < values.size(); at += kValues) {
    const auto chunk =
      values.subspan(at, std::min(kValues, values.size() - at));
    irs::AppendNumericTermsBlock<T>(block.data(), chunk);
    for (size_t i = 0; i < chunk.size(); ++i) {
      PerValueTerm(expected, chunk[i]);
      if (std::memcmp(&block[i], &expected, sizeof(expected)) != 0) {
        absl::PrintF("%s: the term of %g differs from its single encoding\n",
                     type, chunk[i]);
        return false;
      }
    }
  }
  return true;
}

template<typename T>
void PrintSpecialKeys(std::string_view type) {
  struct Special {
    std::string_view name;
    T value;
  };
  const Special specials[] = {
    {"-inf", -std::numeric_limits<T>::infinity()},
    {"lowest", std::numeric_limits<T>::lowest()},
    {"-1", T{-1}},
    {"-0", -T{0}},
    {"+0", T{0}},
    {"1", T{1}},
    {"max", std::numeric_limits<T>::max()},
    {"+inf", std::numeric_limits<T>::infinity()},
    {"NaN", std::numeric_limits<T>::quiet_NaN()},
    {"-NaN", -std::numeric_limits<T>::quiet_NaN()},
  };
  const int width = static_cast<int>(sizeof(T) * 2);
  absl::PrintF("%s keys: DuckDB's format, the minimal one\n", type);
  for (const auto& [name, value] : specials) {
    const auto radix = Radix::One(value);
    const auto minimal = MinimalOrder::One(value);
    absl::PrintF("  %-6s %0*x %0*x%s\n", name, width, uint64_t{radix}, width,
                 uint64_t{minimal}, radix == minimal ? "" : "  differs");
  }
}

template<typename Fn>
void Measure(benchmark::State& state, Fn fn) {
  for (auto _ : state) {
    fn();
    benchmark::ClobberMemory();
  }
  state.counters["per_value"] =
    benchmark::Counter(static_cast<double>(kValues),
                       benchmark::Counter::kIsIterationInvariantRate |
                         benchmark::Counter::kInvert);
}

template<typename T>
struct Data {
  std::vector<T> values;
  std::vector<Bits<T>> keys;
  std::vector<T> decoded;
  std::vector<duckdb::string_t> terms;
};

template<typename T>
Data<T>& DataOf() {
  static Data<T> data = [] {
    Data<T> result{.keys = std::vector<Bits<T>>(kValues),
                   .decoded = std::vector<T>(kValues),
                   .terms = std::vector<duckdb::string_t>(kValues)};
    std::mt19937_64 rng{7};
    std::uniform_real_distribution<double> real{-1e6, 1e6};
    for (size_t i = 0; i < kValues; ++i) {
      result.values.push_back(static_cast<T>(real(rng)));
    }
    return result;
  }();
  return data;
}

template<typename T, typename Format>
void Register(std::string_view type, std::string_view format) {
  auto& data = DataOf<T>();
  benchmark::RegisterBenchmark(absl::StrCat("encode/", type, "/one/", format),
                               [&data](benchmark::State& state) {
                                 Measure(state, [&] {
                                   EncodeOne(
                                     data.values.data(), data.keys.data(),
                                     [](T x) { return Format::One(x); });
                                 });
                               });
  benchmark::RegisterBenchmark(absl::StrCat("encode/", type, "/block/", format),
                               [&data](benchmark::State& state) {
                                 Measure(state, [&] {
                                   EncodeBlock(
                                     data.values.data(), data.keys.data(),
                                     [](T x) { return Format::Block(x); });
                                 });
                               });
  benchmark::RegisterBenchmark(
    absl::StrCat("decode/", type, "/one/", format),
    [&data](benchmark::State& state) {
      EncodeBlock(data.values.data(), data.keys.data(),
                  [](T x) { return Format::Block(x); });
      Measure(state, [&] {
        DecodeOne<T>(data.keys.data(), data.decoded.data(), [](Bits<T> key) {
          return Format::template Value<T>(key);
        });
      });
    });
  benchmark::RegisterBenchmark(
    absl::StrCat("decode/", type, "/block/", format),
    [&data](benchmark::State& state) {
      EncodeBlock(data.values.data(), data.keys.data(),
                  [](T x) { return Format::Block(x); });
      Measure(state, [&] {
        DecodeBlock<T>(data.keys.data(), data.decoded.data(), [](Bits<T> key) {
          return Format::template Value<T>(key);
        });
      });
    });
}

template<typename T>
void RegisterType(std::string_view type) {
  Register<T, RawBits>(type, "raw_bits");
  Register<T, UpstreamRadix>(type, "upstream_radix");
  Register<T, Radix>(type, "radix");
  Register<T, MinimalOrder>(type, "minimal_order");
  auto& data = DataOf<T>();
  benchmark::RegisterBenchmark(
    absl::StrCat("terms/", type, "/per_value"),
    [&data](benchmark::State& state) {
      Measure(state,
              [&] { TermsPerValue(data.values.data(), data.terms.data()); });
    });
  benchmark::RegisterBenchmark(
    absl::StrCat("terms/", type, "/block"), [&data](benchmark::State& state) {
      Measure(state,
              [&] { TermsBlock(data.values.data(), data.terms.data()); });
    });
}

int Main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  if (!Check<float>("float") || !Check<double>("double")) {
    return 1;
  }
  PrintSpecialKeys<float>("float");
  PrintSpecialKeys<double>("double");
  RegisterType<float>("float");
  RegisterType<double>("double");
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

}  // namespace

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
