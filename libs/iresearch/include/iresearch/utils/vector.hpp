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

#pragma once

#include <bit>
#include <tuple>

#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::vector {

enum class Type : uint8_t {
  Bit = 0,
  Bit0 = 0,
  Bit1,
  Bit2,
  Bit3,
  Bit4,
  Bit5,
  Bit6,
  Bit7,

  I8,
  U8,

  F32,
  F64,

  Invalid = 0xFF,
};

// strict limit is (1 << 16) - 1
inline constexpr size_t kMaxDimBit = 1 << 15;
// strict limit is something like 1 << 16
inline constexpr size_t kMaxDimI8 = 1 << 15;
inline constexpr size_t kMaxDimU8 = 1 << 15;
// there is no strict limit for floating points
inline constexpr size_t kMaxDimF32 = 1 << 15;
inline constexpr size_t kMaxDimF64 = 1 << 15;

inline std::tuple<Type, uint16_t> Info(bytes_view data) {
  if (data.empty()) {
    return {Type::Invalid, 0};
  }
  const auto l = data.size() - 1;
  switch (const auto t = static_cast<Type>(data.front())) {
    case Type::Bit0:
    case Type::Bit1:
    case Type::Bit2:
    case Type::Bit3:
    case Type::Bit4:
    case Type::Bit5:
    case Type::Bit6:
    case Type::Bit7:
      if (l <= kMaxDimBit / 8) {
        return {Type::Bit, 8 * l + std::to_underlying(t)};
      }
      break;
    case Type::I8:
      if (l <= kMaxDimI8) {
        return {t, l};
      }
      break;
    case Type::U8:
      if (l <= kMaxDimU8) {
        return {t, l};
      }
      break;
    case Type::F32:
      if (l <= kMaxDimF32 && l % 4 == 0) {
        return {t, l / 4};
      }
      break;
    case Type::F64:
      if (l <= kMaxDimF64 && l % 8 == 0) {
        return {t, l / 8};
      }
      break;
    default:
      break;
  }
  return {Type::Invalid, 0};
}

// TODO(mbkkt) maybe disable loop unrolling?
// #pragma GCC unroll 1

namespace bit_compute {

enum Enum : uint8_t {
  kLsum = 1U << 0U,
  kConj = 1U << 1U,
  kUneq = 1U << 2U,
  kDisj = 1U << 3U,
  kRsum = 1U << 4U,
};

}  // namespace bit_compute

template<uint8_t E>
struct Bit {
  [[no_unique_address]] utils::Need<(E & bit_compute::kLsum) != 0, uint16_t>
    lsum{};
  [[no_unique_address]] utils::Need<(E & bit_compute::kConj) != 0, uint16_t>
    conj{};
  [[no_unique_address]] utils::Need<(E & bit_compute::kUneq) != 0, uint16_t>
    uneq{};
  [[no_unique_address]] utils::Need<(E & bit_compute::kDisj) != 0, uint16_t>
    disj{};
  [[no_unique_address]] utils::Need<(E & bit_compute::kRsum) != 0, uint16_t>
    rsum{};

  void operator()(const byte_type* l, const byte_type* r, uint16_t d) {
    const uint16_t bytes = (d + 7) / 8;
    for (uint16_t i = 0; i != bytes; ++i) {
      if constexpr (!std::is_empty_v<decltype(lsum)>) {
        lsum += std::popcount(l[i]);
      }
      if constexpr (!std::is_empty_v<decltype(conj)>) {
        conj += std::popcount(static_cast<byte_type>(l[i] & r[i]));
      }
      if constexpr (!std::is_empty_v<decltype(uneq)>) {
        uneq += std::popcount(static_cast<byte_type>(l[i] ^ r[i]));
      }
      if constexpr (!std::is_empty_v<decltype(disj)>) {
        disj += std::popcount(static_cast<byte_type>(l[i] | r[i]));
      }
      if constexpr (!std::is_empty_v<decltype(rsum)>) {
        rsum += std::popcount(r[i]);
      }
    }
  }

  void operator()(bool l, bool r) {
    auto li = static_cast<byte_type>(l);
    auto ri = static_cast<byte_type>(r);
    if constexpr (!std::is_empty_v<decltype(lsum)>) {
      lsum += li;
    }
    if constexpr (!std::is_empty_v<decltype(conj)>) {
      conj += li & ri;  // TODO(mbkkt) maybe &&
    }
    if constexpr (!std::is_empty_v<decltype(uneq)>) {
      uneq += li ^ ri;  // TODO(mbkkt) maybe !=
    }
    if constexpr (!std::is_empty_v<decltype(disj)>) {
      disj += li | ri;  // TODO(mbkkt) maybe ||
    }
    if constexpr (!std::is_empty_v<decltype(rsum)>) {
      rsum += ri;
    }
  }
};

template<typename In, typename Out>
struct DotProductImpl {
  static_assert(std::is_unsigned_v<In> || std::is_signed_v<Out>);

  static Out Compute(const byte_type* l, const byte_type* r, uint16_t d) {
#pragma clang fp reassociate(on) contract(fast)
    Out s{};
    for (uint16_t i = 0; i != d; ++i) {
      Out li = static_cast<Out>(reinterpret_cast<const In*>(l)[i]);
      Out ri = static_cast<Out>(reinterpret_cast<const In*>(r)[i]);
      s += li * ri;
    }
    return s;
  }
};

template<typename In, typename Sum, typename Out>
struct CosineDistanceImpl {
  static_assert(std::is_unsigned_v<In> || std::is_signed_v<Sum>);
  static_assert(std::is_floating_point_v<Out>);

  static auto Compute(const byte_type* l, const byte_type* r, uint16_t d) {
#pragma clang fp reassociate(on) contract(fast)
    Sum ll{};
    Sum lr{};
    Sum rr{};
    for (uint16_t i = 0; i != d; ++i) {
      auto li = static_cast<Sum>(reinterpret_cast<const In*>(l)[i]);
      auto ri = static_cast<Sum>(reinterpret_cast<const In*>(r)[i]);
      ll += li * li;
      lr += li * ri;
      rr += ri * ri;
    }
    return std::tuple{static_cast<Out>(ll), static_cast<Out>(lr),
                      static_cast<Out>(rr)};
  }
};

template<typename In, typename Sum, typename Out>
struct CanberraDistanceImpl {
  static_assert(std::is_signed_v<Sum>);
  static_assert(std::is_floating_point_v<Out>);

  static auto Compute(const byte_type* l, const byte_type* r, uint16_t d) {
    Sum a{};
    Sum b{};
    for (uint16_t i = 0; i != d; ++i) {
      auto li = static_cast<Sum>(reinterpret_cast<const In*>(l)[i]);
      auto ri = static_cast<Sum>(reinterpret_cast<const In*>(r)[i]);
      a += std::abs(li - ri);
      b += std::abs(li) + std::abs(ri);
    }
    return std::tuple{static_cast<Out>(a), static_cast<Out>(b)};
  }
};

template<typename In, typename Sum, typename Out>
struct BrayCurtisDistanceImpl {
  static_assert(std::is_signed_v<Sum>);
  static_assert(std::is_floating_point_v<Out>);

  static auto Compute(const byte_type* l, const byte_type* r, uint16_t d) {
    Sum a{};
    Sum b{};
    for (uint16_t i = 0; i != d; ++i) {
      auto li = static_cast<Sum>(reinterpret_cast<const In*>(l)[i]);
      auto ri = static_cast<Sum>(reinterpret_cast<const In*>(r)[i]);
      a += std::abs(li - ri);
      b += std::abs(li + ri);
    }
    return std::tuple{static_cast<Out>(a), static_cast<Out>(b)};
  }
};

template<typename In, typename Abs, typename Out>
struct L1Space {
  static_assert(std::is_signed_v<Abs>);

  static Out Norm(const byte_type* v, uint16_t d) {
#pragma clang fp reassociate(on) contract(fast)
    Out s{};
    for (uint16_t i = 0; i != d; ++i) {
      auto vi = reinterpret_cast<const In*>(v)[i];
      if constexpr (std::is_signed_v<In>) {
        vi = static_cast<In>(std::abs(vi));
      }
      s += static_cast<Out>(vi);
    }
    return s;
  }

  static Out Dist(const byte_type* l, const byte_type* r, uint16_t d) {
#pragma clang fp reassociate(on) contract(fast)
    Out s{};
    for (uint16_t i = 0; i != d; ++i) {
      auto li = static_cast<Abs>(reinterpret_cast<const In*>(l)[i]);
      auto ri = static_cast<Abs>(reinterpret_cast<const In*>(r)[i]);
      auto lri = li - ri;
      s += static_cast<Out>(std::abs(lri));
    }
    return s;
  }

  static void Normalize(const byte_type* v, uint16_t d, Out* out) {
    Out n = Norm(v, d);
    if (n == Out{}) {
      for (uint16_t i = 0; i != d; ++i) {
        out[i] = Out{};
      }
      return;
    }
    auto inv = Out{1} / static_cast<Out>(n);
    for (uint16_t i = 0; i != d; ++i) {
      auto vi = static_cast<Out>(reinterpret_cast<const In*>(v)[i]);
      out[i] = vi * inv;
    }
  }
};

template<typename In, typename Sqr, typename Out>
struct L2Space {
  static Out Norm(const byte_type* v, uint16_t d) {
#pragma clang fp reassociate(on) contract(fast)
    Out s{};
    for (uint16_t i = 0; i != d; ++i) {
      auto vi = static_cast<Sqr>(reinterpret_cast<const In*>(v)[i]);
      vi *= vi;
      s += static_cast<Out>(vi);
    }
    return s;
  }

  static Out Dist(const byte_type* l, const byte_type* r, uint16_t d) {
#pragma clang fp reassociate(on) contract(fast)
    Out s{};
    for (uint16_t i = 0; i != d; ++i) {
      auto li = static_cast<Sqr>(reinterpret_cast<const In*>(l)[i]);
      auto ri = static_cast<Sqr>(reinterpret_cast<const In*>(r)[i]);
      auto lri = li - ri;
      lri *= lri;
      s += static_cast<Out>(lri);
    }
    return s;
  }

  static void Normalize(const byte_type* v, uint16_t d, Out* out) {
    Out n = Norm(v, d);
    if (n == Out{}) {
      for (uint16_t i = 0; i != d; ++i) {
        out[i] = Out{};
      }
      return;
    }
    auto inv = Out{1} / std::sqrt(static_cast<Out>(n));
    for (uint16_t i = 0; i != d; ++i) {
      auto vi = static_cast<Out>(reinterpret_cast<const In*>(v)[i]);
      out[i] = vi * inv;
    }
  }
};

// TODO(mbkkt) can be faster
template<typename In, typename Abs, typename Out>
struct LPSpace {
  static Out Norm(Out p, const byte_type* v, uint16_t d) {
    Out s{};
    for (uint16_t i = 0; i != d; ++i) {
      auto vi = reinterpret_cast<const In*>(v)[i];
      if constexpr (std::is_signed_v<In>) {
        vi = static_cast<In>(std::abs(vi));
      }
      s += static_cast<Out>(std::pow(vi, p));
    }
    return s;
  }

  static Out Dist(Out p, const byte_type* l, const byte_type* r, uint16_t d) {
    SDB_ASSERT(p >= 1);
    Out s{};
    for (uint16_t i = 0; i != d; ++i) {
      auto li = static_cast<Abs>(reinterpret_cast<const In*>(l)[i]);
      auto ri = static_cast<Abs>(reinterpret_cast<const In*>(r)[i]);
      auto lri = li - ri;
      lri = static_cast<Abs>(std::abs(lri));
      s += static_cast<Out>(std::pow(lri, p));
    }
    return s;
  }
};

template<typename In, typename Out>
struct LInfSpace {
  static_assert(std::is_signed_v<Out>);

  static Out Norm(const byte_type* v, uint16_t d) {
    In m = std::numeric_limits<In>::min();
    for (uint16_t i = 0; i != d; ++i) {
      In vi = reinterpret_cast<const In*>(v)[i];
      if constexpr (std::is_signed_v<In>) {
        vi = static_cast<In>(std::abs(vi));
      }
      m = std::max(m, vi);
    }
    return static_cast<Out>(m);
  }

  static Out Dist(const byte_type* l, const byte_type* r, uint16_t d) {
    Out m = std::numeric_limits<Out>::min();
    for (uint16_t i = 0; i != d; ++i) {
      Out li = static_cast<Out>(reinterpret_cast<const In*>(l)[i]);
      Out ri = static_cast<Out>(reinterpret_cast<const In*>(r)[i]);
      Out lri = li - ri;
      lri = static_cast<Out>(std::abs(lri));
      m = std::max(m, lri);
    }
    return m;
  }
};

}  // namespace irs::vector
