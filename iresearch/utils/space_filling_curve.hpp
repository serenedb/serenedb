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

#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

#if defined(__BMI2__)
#include <immintrin.h>
#endif

namespace irs::curve {

inline constexpr uint32_t kMaxDimensions = 8;
inline constexpr uint32_t kMaxLevel = 64;
inline constexpr uint32_t kMaxCodeBits = 256;
inline constexpr uint32_t kMaxCodeWords = kMaxCodeBits / 64;
using Point = std::array<uint64_t, kMaxDimensions>;
using Code = std::array<uint8_t, kMaxDimensions * sizeof(uint64_t)>;

struct Options {
  uint32_t dimensions = 2;
  uint32_t max_level = kMaxLevel;
  bool hilbert = false;

  bool operator==(const Options&) const = default;
};

inline uint32_t MaxLevel(uint32_t dimensions) noexcept {
  return std::min(kMaxLevel, kMaxCodeBits / dimensions);
}

inline void Validate(const Options& options) {
  if (options.dimensions < 2 || options.dimensions > kMaxDimensions ||
      options.max_level == 0 ||
      options.max_level > MaxLevel(options.dimensions)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("curve: dimensions must be 2..8, max_level 1..64 and at most ",
              kMaxCodeBits, " bits in all"));
  }
}

inline constexpr uint64_t kSignBit = uint64_t{1} << 63;

inline uint64_t EncodeSigned(int64_t value) noexcept {
  return static_cast<uint64_t>(value) ^ kSignBit;
}

inline uint64_t EncodeDouble(double value) noexcept {
  if (std::isnan(value)) {
    return EncodeDouble(std::numeric_limits<double>::infinity()) + 1;
  }
  const auto bits = std::bit_cast<uint64_t>(value == 0 ? 0.0 : value);
  return bits & kSignBit ? ~bits : bits ^ kSignBit;
}

inline uint64_t NormalizeKey(uint64_t key, uint64_t lo, uint64_t hi) noexcept {
  return (std::clamp(key, lo, hi) - lo) << std::countl_zero(hi - lo);
}

inline uint64_t NormalizeDouble(double value, double lo, double hi) noexcept {
  constexpr auto kMax = std::numeric_limits<uint64_t>::max();
  if (std::isnan(value) || value >= hi) {
    return kMax;
  }
  if (!(value > lo)) {
    return 0;
  }
  const auto fraction = (value - lo) / (hi - lo);
  return fraction >= 1 ? kMax : static_cast<uint64_t>(std::ldexp(fraction, 64));
}

struct Box {
  Point min{};
  Point max{};

  bool operator==(const Box&) const = default;
};

enum class Relation : uint8_t {
  Outside,
  Boundary,
  Inside,
};

namespace detail {

#if !defined(__BMI2__)
template<uint32_t Dimensions>
inline constexpr auto kSpreadBytes = [] {
  std::array<uint64_t, 256> table{};
  for (uint32_t value = 0; value < table.size(); ++value) {
    for (uint32_t bit = 0; bit < 8; ++bit) {
      table[value] |= uint64_t{(value >> bit) & 1} << (bit * Dimensions);
    }
  }
  return table;
}();

inline uint64_t SpreadPairs(uint64_t value) noexcept {
  value &= 0xffffffff;
  value = (value | (value << 16)) & 0x0000ffff0000ffff;
  value = (value | (value << 8)) & 0x00ff00ff00ff00ff;
  value = (value | (value << 4)) & 0x0f0f0f0f0f0f0f0f;
  value = (value | (value << 2)) & 0x3333333333333333;
  return (value | (value << 1)) & 0x5555555555555555;
}
#endif

template<uint32_t Dimensions>
inline constexpr auto kMortonMasks = [] {
  std::array<std::array<uint64_t, Dimensions>, Dimensions> result{};
  for (uint32_t word = 0; word < Dimensions; ++word) {
    for (uint32_t bit = 0; bit < 64; ++bit) {
      result[word][(word * 64 + bit) % Dimensions] |= uint64_t{1} << (63 - bit);
    }
  }
  return result;
}();

inline constexpr uint32_t MortonShift(uint32_t dimensions, uint32_t word,
                                      uint32_t axis) noexcept {
  return 64 - (64 * (word + 1) + dimensions - 1 - axis) / dimensions;
}

inline uint64_t ExtractBits(uint64_t value, uint64_t mask) noexcept {
#if defined(__BMI2__)
  return _pext_u64(value, mask);
#else
  uint64_t result = 0;
  for (uint64_t out = 1; mask != 0; out <<= 1) {
    if (value & mask & (uint64_t{0} - mask)) {
      result |= out;
    }
    mask &= mask - 1;
  }
  return result;
#endif
}

inline uint64_t BigEndian(uint64_t value) noexcept {
  if constexpr (std::endian::native == std::endian::little) {
    return std::byteswap(value);
  } else {
    return value;
  }
}

inline void CopyBytes(uint8_t* out, const uint8_t* in, uint32_t size) noexcept {
  if (size >= 16) {
    std::memcpy(out, in, 16);
    std::memcpy(out + size - 16, in + size - 16, 16);
  } else if (size >= 8) {
    std::memcpy(out, in, 8);
    std::memcpy(out + size - 8, in + size - 8, 8);
  } else if (size >= 4) {
    std::memcpy(out, in, 4);
    std::memcpy(out + size - 4, in + size - 4, 4);
  } else {
    for (uint32_t i = 0; i < size; ++i) {
      out[i] = in[i];
    }
  }
}

template<uint32_t Dimensions>
Code Encode(Point& point, bool hilbert) {
  if (hilbert) {
    for (uint64_t q = uint64_t{1} << 63; q > 1; q >>= 1) {
      const auto mask = q - 1;
      auto first = point[0] ^ (mask & (uint64_t{0} - bool(point[0] & q)));
      for (uint32_t axis = 1; axis < Dimensions; ++axis) {
        const auto invert = uint64_t{0} - bool(point[axis] & q);
        const auto exchange = (first ^ point[axis]) & mask & ~invert;
        first ^= exchange ^ (mask & invert);
        point[axis] ^= exchange;
      }
      point[0] = first;
    }
    for (uint32_t axis = 1; axis < Dimensions; ++axis) {
      point[axis] ^= point[axis - 1];
    }
    auto correction = point[Dimensions - 1];
    correction ^= correction >> 1;
    correction ^= correction >> 2;
    correction ^= correction >> 4;
    correction ^= correction >> 8;
    correction ^= correction >> 16;
    correction ^= correction >> 32;
    correction >>= 1;
    for (uint32_t axis = 0; axis < Dimensions; ++axis) {
      point[axis] ^= correction;
    }
  }
  Code code{};
#if defined(__BMI2__)
  for (uint32_t word = 0; word < Dimensions; ++word) {
    uint64_t value = 0;
    for (uint32_t axis = 0; axis < Dimensions; ++axis) {
      value |= _pdep_u64(point[axis] >> MortonShift(Dimensions, word, axis),
                         kMortonMasks<Dimensions>[word][axis]);
    }
    value = BigEndian(value);
    std::memcpy(code.data() + word * 8, &value, sizeof(value));
  }
#else
  if constexpr (Dimensions == 2) {
    for (uint32_t word = 0; word < 2; ++word) {
      const auto shift = (1 - word) * 32;
      const auto value = BigEndian((SpreadPairs(point[0] >> shift) << 1) |
                                   SpreadPairs(point[1] >> shift));
      std::memcpy(code.data() + word * 8, &value, sizeof(value));
    }
  } else {
    for (uint32_t byte = 0; byte < 8; ++byte) {
      uint64_t value = 0;
      for (uint32_t axis = 0; axis < Dimensions; ++axis) {
        value |= kSpreadBytes<Dimensions>[static_cast<uint8_t>(point[axis] >>
                                                               (56 - byte * 8))]
                 << (Dimensions - 1 - axis);
      }
      value = BigEndian(value);
      std::memcpy(code.data() + byte * Dimensions,
                  reinterpret_cast<const uint8_t*>(&value) + 8 - Dimensions,
                  Dimensions);
    }
  }
#endif
  return code;
}

template<uint32_t Dimensions>
Point Decode(const uint8_t* code, uint32_t size, bool hilbert) noexcept {
  Point point{};
  for (uint32_t word = 0; word < Dimensions && word * 8 < size; ++word) {
    uint64_t value = 0;
    std::memcpy(&value, code + word * 8,
                std::min(uint32_t{8}, size - word * 8));
    value = BigEndian(value);
    for (uint32_t axis = 0; axis < Dimensions; ++axis) {
      point[axis] |= ExtractBits(value, kMortonMasks<Dimensions>[word][axis])
                     << MortonShift(Dimensions, word, axis);
    }
  }
  if (hilbert) {
    const auto gray = point[Dimensions - 1] >> 1;
    for (uint32_t axis = Dimensions - 1; axis > 0; --axis) {
      point[axis] ^= point[axis - 1];
    }
    point[0] ^= gray;
    for (uint64_t q = 2; q != 0; q <<= 1) {
      const auto mask = q - 1;
      for (uint32_t axis = Dimensions; axis-- > 0;) {
        if (point[axis] & q) {
          point[0] ^= mask;
        } else {
          const auto exchange = (point[0] ^ point[axis]) & mask;
          point[0] ^= exchange;
          point[axis] ^= exchange;
        }
      }
    }
  }
  return point;
}

}  // namespace detail

inline Point Decode(const uint8_t* code, uint32_t size, uint32_t dimensions,
                    bool hilbert) noexcept {
  switch (dimensions) {
    case 2:
      return detail::Decode<2>(code, size, hilbert);
    case 3:
      return detail::Decode<3>(code, size, hilbert);
    case 4:
      return detail::Decode<4>(code, size, hilbert);
    case 5:
      return detail::Decode<5>(code, size, hilbert);
    case 6:
      return detail::Decode<6>(code, size, hilbert);
    case 7:
      return detail::Decode<7>(code, size, hilbert);
    default:
      return detail::Decode<8>(code, size, hilbert);
  }
}

inline Code Encode(Point point, uint32_t dimensions, bool hilbert) {
  switch (dimensions) {
    case 2:
      return detail::Encode<2>(point, hilbert);
    case 3:
      return detail::Encode<3>(point, hilbert);
    case 4:
      return detail::Encode<4>(point, hilbert);
    case 5:
      return detail::Encode<5>(point, hilbert);
    case 6:
      return detail::Encode<6>(point, hilbert);
    case 7:
      return detail::Encode<7>(point, hilbert);
    case 8:
      return detail::Encode<8>(point, hilbert);
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("curve: dimensions must be 2..8"));
  }
}

inline uint32_t CodeSize(uint32_t dimensions, uint32_t level) noexcept {
  return (dimensions * level + 7) / 8;
}

inline uint32_t LeafCode(const Point& point, const Options& options,
                         uint8_t* out) {
  const auto code = Encode(point, options.dimensions, options.hilbert);
  const auto bits = options.dimensions * options.max_level;
  const auto size = CodeSize(options.dimensions, options.max_level);
  std::memcpy(out, code.data(), size);
  if (bits % 8) {
    out[size - 1] &= static_cast<uint8_t>(0xff << (8 - bits % 8));
  }
  return size;
}

class BoxTest {
 public:
  explicit BoxTest(const Box& box, const Options& options) noexcept
    : _box{box},
      _dimensions{options.dimensions},
      _max_level{options.max_level},
      _hilbert{options.hilbert},
      _size{CodeSize(options.dimensions, options.max_level)},
      _words{(_size + 7) / 8},
      _low{LowBits(options.max_level)} {
    for (uint32_t axis = 0; axis < _dimensions; ++axis) {
      _empty |= box.min[axis] > box.max[axis];
      if (box.min[axis] == 0 &&
          box.max[axis] == std::numeric_limits<uint64_t>::max()) {
        continue;
      }
      auto& bounded = _axes[_bounded++];
      bounded.min = box.min[axis];
      bounded.max = box.max[axis];
      for (uint32_t word = 0; word < _words; ++word) {
        for (uint32_t bit = 0; bit < 64; ++bit) {
          if ((word * 64 + bit) % _dimensions == axis) {
            bounded.masks[word] |= uint64_t{1} << (63 - bit);
          }
        }
        bounded.shifts[word] = detail::MortonShift(_dimensions, word, axis);
      }
    }
  }

  Relation ClassifyRange(const uint8_t* first,
                         const uint8_t* last) const noexcept {
    if (_empty) {
      return Relation::Outside;
    }
    const auto bits = CommonBits(first, last);
    if (_hilbert) {
      return ClassifyCell(first, bits / _dimensions);
    }
    if (bits == _size * 8) {
      return ClassifyPrefix(first, bits);
    }
    const auto left = ClassifyPrefix(first, RunEnd(first, bits + 1, true));
    const auto right = ClassifyPrefix(last, RunEnd(last, bits + 1, false));
    return left == right ? left : Relation::Boundary;
  }

  uint64_t Select(std::span<const uint8_t> prefix, const uint8_t* keys,
                  uint32_t pitch, uint32_t width,
                  uint32_t count) const noexcept {
    if (_empty) {
      return 0;
    }
    Words raw{};
    auto* const bytes = reinterpret_cast<uint8_t*>(raw.data());
    std::memcpy(bytes, prefix.data(), prefix.size());
    uint64_t selected = 0;
    for (uint32_t i = 0; i < count; ++i) {
      detail::CopyBytes(bytes + prefix.size(), keys + uint64_t{i} * pitch,
                        width);
      selected |=
        uint64_t{_hilbert ? ContainsHilbert(bytes) : ContainsMorton(raw)} << i;
    }
    return selected;
  }

 private:
  using Words = std::array<uint64_t, kMaxCodeWords>;

  struct Axis {
    uint64_t min = 0;
    uint64_t max = 0;
    Words masks{};
    std::array<uint32_t, kMaxCodeWords> shifts{};
  };

  static uint64_t LowBits(uint32_t level) noexcept {
    return level >= 64 ? 0 : std::numeric_limits<uint64_t>::max() >> level;
  }

  static uint64_t HighBits(uint32_t bits) noexcept {
    return bits == 0 ? 0 : std::numeric_limits<uint64_t>::max() << (64 - bits);
  }

  uint32_t CommonBits(const uint8_t* lhs, const uint8_t* rhs) const noexcept {
    for (uint32_t byte = 0; byte < _size; ++byte) {
      if (lhs[byte] != rhs[byte]) {
        return byte * 8 +
               std::countl_zero(static_cast<uint8_t>(lhs[byte] ^ rhs[byte]));
      }
    }
    return _size * 8;
  }

  uint32_t RunEnd(const uint8_t* code, uint32_t from,
                  bool value) const noexcept {
    const auto end = _size * 8;
    while (from < end &&
           ((code[from / 8] >> (7 - from % 8)) & 1) == uint32_t{value}) {
      ++from;
    }
    return from;
  }

  bool ContainsMorton(const Words& raw) const noexcept {
    Words words{};
    for (uint32_t word = 0; word < _words; ++word) {
      words[word] = detail::BigEndian(raw[word]);
    }
    for (uint32_t i = 0; i < _bounded; ++i) {
      const auto& axis = _axes[i];
      uint64_t key = 0;
      for (uint32_t word = 0; word < _words; ++word) {
        key |= detail::ExtractBits(words[word], axis.masks[word])
               << axis.shifts[word];
      }
      if ((key | _low) < axis.min || key > axis.max) {
        return false;
      }
    }
    return true;
  }

  bool ContainsHilbert(const uint8_t* code) const noexcept {
    return ClassifyCell(code, _max_level) != Relation::Outside;
  }

  Relation ClassifyPrefix(const uint8_t* code, uint32_t bits) const noexcept {
    Words raw{};
    detail::CopyBytes(reinterpret_cast<uint8_t*>(raw.data()), code, _size);
    Words low{};
    Words high{};
    for (uint32_t word = 0; word < _words; ++word) {
      const auto value = detail::BigEndian(raw[word]);
      const auto used = word * 64;
      const auto mask = HighBits(bits <= used ? 0 : std::min(bits - used, 64U));
      low[word] = value & mask;
      high[word] = value | ~mask;
    }
    bool inside = true;
    for (uint32_t i = 0; i < _bounded; ++i) {
      const auto& axis = _axes[i];
      uint64_t min = 0;
      uint64_t max = _low;
      for (uint32_t word = 0; word < _words; ++word) {
        min |= detail::ExtractBits(low[word], axis.masks[word])
               << axis.shifts[word];
        max |= detail::ExtractBits(high[word], axis.masks[word])
               << axis.shifts[word];
      }
      if (min > axis.max || max < axis.min) {
        return Relation::Outside;
      }
      inside &= min >= axis.min && max <= axis.max;
    }
    return inside ? Relation::Inside : Relation::Boundary;
  }

  Relation ClassifyCell(const uint8_t* code, uint32_t level) const noexcept {
    const auto point = Decode(code, _size, _dimensions, true);
    const auto mask = LowBits(std::min(level, _max_level));
    bool inside = true;
    for (uint32_t axis = 0; axis < _dimensions; ++axis) {
      const auto min = point[axis] & ~mask;
      const auto max = point[axis] | mask;
      if (min > _box.max[axis] || max < _box.min[axis]) {
        return Relation::Outside;
      }
      inside &= min >= _box.min[axis] && max <= _box.max[axis];
    }
    return inside ? Relation::Inside : Relation::Boundary;
  }

  Box _box;
  uint32_t _dimensions;
  uint32_t _max_level;
  bool _hilbert;
  uint32_t _size;
  uint32_t _words;
  uint64_t _low;
  uint32_t _bounded = 0;
  bool _empty = false;
  std::array<Axis, kMaxDimensions> _axes;
};

}  // namespace irs::curve
