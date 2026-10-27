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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <queue>
#include <span>
#include <string>
#include <vector>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

#if defined(__BMI2__)
#include <immintrin.h>
#endif

namespace irs::curve {

inline constexpr uint32_t kMaxDimensions = 8;
inline constexpr uint32_t kMaxLevel = 64;
inline constexpr uint32_t kMaxCells = 4096;
inline constexpr uint32_t kMaxExpansionBits = 12;
inline constexpr uint32_t kTermHeader = 3;
inline constexpr char kAncestorTerm = 'A';
inline constexpr char kLeafTerm = 'L';
using Point = std::array<uint64_t, kMaxDimensions>;
using Code = std::array<uint8_t, kMaxDimensions * sizeof(uint64_t)>;

struct Options {
  uint32_t dimensions = 2;
  uint32_t max_level = kMaxLevel;
  uint32_t max_cells = 64;
  bool hilbert = false;
  uint32_t level_step = 1;

  bool operator==(const Options&) const = default;
};

inline uint32_t DefaultLevelStep(uint32_t dimensions) noexcept {
  return 1 + 4 / dimensions;
}

inline uint32_t DefaultMaxCells(uint32_t dimensions) noexcept {
  return std::min(uint32_t{64}, uint32_t{4} << dimensions);
}

inline uint32_t MaxLevelStep(uint32_t dimensions) noexcept {
  return 1 + kMaxExpansionBits / dimensions;
}

inline void Validate(const Options& options) {
  if (options.dimensions < 2 || options.dimensions > kMaxDimensions ||
      options.max_level > kMaxLevel || options.max_cells == 0 ||
      options.max_cells > kMaxCells || options.level_step == 0 ||
      options.level_step > MaxLevelStep(options.dimensions)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("curve: dimensions must be 2..8, max_level 0..64, max_cells "
              "1..4096, level_step 1..1+12/dimensions"));
  }
}

inline bool IsIndexedLevel(uint32_t level, const Options& options) noexcept {
  return level == options.max_level || level % options.level_step == 0;
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

inline double DecodeDouble(uint64_t value) noexcept {
  return std::bit_cast<double>(value & kSignBit ? value ^ kSignBit : ~value);
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
};

struct Cell {
  Point min{};
  uint32_t level = 0;

  uint64_t Max(uint32_t axis) const noexcept {
    const auto mask = level == 0 ? std::numeric_limits<uint64_t>::max()
                      : level == 64
                        ? 0
                        : std::numeric_limits<uint64_t>::max() >> level;
    return min[axis] | mask;
  }
};

enum class Relation : uint8_t {
  Outside,
  Boundary,
  Inside,
};

inline Relation Classify(const Cell& cell, const Box& box,
                         uint32_t dimensions) noexcept {
  bool inside = true;
  for (uint32_t axis = 0; axis < dimensions; ++axis) {
    if (box.min[axis] > box.max[axis] || cell.min[axis] > box.max[axis] ||
        cell.Max(axis) < box.min[axis]) {
      return Relation::Outside;
    }
    inside &=
      cell.min[axis] >= box.min[axis] && cell.Max(axis) <= box.max[axis];
  }
  return inside ? Relation::Inside : Relation::Boundary;
}

template<typename Classifier, typename Priority>
std::vector<Cell> Cover(const Options& options, Classifier&& classify,
                        Priority&& priority) {
  Validate(options);
  struct Pending {
    Cell cell;
    Relation relation;
    long double priority;
    uint64_t order;
  };
  const auto compare = [](const Pending& a, const Pending& b) {
    return a.priority != b.priority ? a.priority < b.priority
                                    : a.order > b.order;
  };
  std::priority_queue<Pending, std::vector<Pending>, decltype(compare)> pending{
    compare};
  uint64_t order = 0;
  std::vector<Cell> result;
  const Cell root;
  const auto root_relation = classify(root);
  if (root_relation == Relation::Outside) {
    return result;
  }
  pending.emplace(root, root_relation, static_cast<long double>(priority(root)),
                  order++);
  std::vector<Pending> children;
  while (!pending.empty()) {
    const auto top = pending.top();
    pending.pop();
    const auto& cell = top.cell;
    if (cell.level == options.max_level || top.relation == Relation::Inside) {
      result.emplace_back(cell);
      continue;
    }
    children.clear();
    const auto bit = uint64_t{1} << (63 - cell.level);
    for (uint32_t child = 0; child < (uint32_t{1} << options.dimensions);
         ++child) {
      Cell next{cell.min, cell.level + 1};
      for (uint32_t axis = 0; axis < options.dimensions; ++axis) {
        if (child & (uint32_t{1} << axis)) {
          next.min[axis] |= bit;
        }
      }
      const auto next_relation = classify(next);
      if (next_relation != Relation::Outside) {
        children.emplace_back(next, next_relation, 0, 0);
      }
    }
    if (children.size() + pending.size() + result.size() > options.max_cells) {
      result.emplace_back(cell);
      continue;
    }
    for (auto& child : children) {
      child.priority = static_cast<long double>(priority(child.cell));
      child.order = order++;
      pending.emplace(child);
    }
  }
  return result;
}

template<typename Classifier>
std::vector<Cell> Cover(const Options& options, Classifier&& classify) {
  return Cover(options, std::forward<Classifier>(classify),
               [](const Cell& cell) { return 64 - cell.level; });
}

inline std::vector<Cell> CoverBox(const Box& box, const Options& options) {
  return Cover(options, [&](const Cell& cell) {
    return Classify(cell, box, options.dimensions);
  });
}

namespace detail {

template<uint32_t Dimensions>
inline constexpr auto kSpreadBytes = [] {
  std::array<uint64_t, 256> table;
  table.fill(0);
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
  Code code;
  code.fill(0);
#if defined(__BMI2__)
  static constexpr auto masks = [] {
    std::array<std::array<uint64_t, Dimensions>, Dimensions> result;
    for (uint32_t word = 0; word < Dimensions; ++word) {
      result[word].fill(0);
      for (uint32_t bit = 0; bit < 64; ++bit) {
        result[word][(word * 64 + bit) % Dimensions] |= uint64_t{1}
                                                        << (63 - bit);
      }
    }
    return result;
  }();
  for (uint32_t word = 0; word < Dimensions; ++word) {
    uint64_t value = 0;
    for (uint32_t axis = 0; axis < Dimensions; ++axis) {
      const auto shift =
        64 - (64 * (word + 1) + Dimensions - 1 - axis) / Dimensions;
      value |= _pdep_u64(point[axis] >> shift, masks[word][axis]);
    }
    if constexpr (std::endian::native == std::endian::little) {
      value = std::byteswap(value);
    }
    std::memcpy(code.data() + word * 8, &value, sizeof(value));
  }
#else
  if constexpr (Dimensions == 2) {
    for (uint32_t word = 0; word < 2; ++word) {
      const auto shift = (1 - word) * 32;
      auto value =
        (SpreadPairs(point[0] >> shift) << 1) | SpreadPairs(point[1] >> shift);
      if constexpr (std::endian::native == std::endian::little) {
        value = std::byteswap(value);
      }
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
      if constexpr (std::endian::native == std::endian::little) {
        value = std::byteswap(value);
      }
      std::memcpy(code.data() + byte * Dimensions,
                  reinterpret_cast<const uint8_t*>(&value) + 8 - Dimensions,
                  Dimensions);
    }
  }
#endif
  return code;
}

}  // namespace detail

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

inline uint32_t TermSize(uint32_t dimensions, uint32_t level) noexcept {
  return kTermHeader + CodeSize(dimensions, level);
}

inline uint32_t WriteTerm(uint8_t* out, const Code& code, uint32_t dimensions,
                          uint32_t level, char kind) noexcept {
  const auto bits = dimensions * level;
  const auto size = TermSize(dimensions, level);
  out[0] = static_cast<uint8_t>(kind);
  out[1] = static_cast<uint8_t>(dimensions);
  out[2] = static_cast<uint8_t>(level);
  std::memcpy(out + kTermHeader, code.data(), size - kTermHeader);
  if (bits % 8) {
    out[size - 1] &= static_cast<uint8_t>(0xff << (8 - bits % 8));
  }
  return size;
}

inline std::string Term(const Code& code, uint32_t dimensions, uint32_t level,
                        char kind) {
  std::string term(TermSize(dimensions, level), '\0');
  WriteTerm(reinterpret_cast<uint8_t*>(term.data()), code, dimensions, level,
            kind);
  return term;
}

namespace detail {

template<typename Emit>
void EmitTerm(uint8_t* term, uint32_t dimensions, uint32_t level, char kind,
              Emit& emit) {
  const auto bits = dimensions * level;
  const auto size = TermSize(dimensions, level);
  term[0] = static_cast<uint8_t>(kind);
  term[2] = static_cast<uint8_t>(level);
  const auto last = term[size - 1];
  term[size - 1] &= static_cast<uint8_t>(0xff << ((8 - bits % 8) % 8));
  emit(std::span<const uint8_t>{term, size});
  term[size - 1] = last;
}

}  // namespace detail

inline uint32_t LeafCode(const Point& point, const Options& options,
                         uint8_t* out) noexcept {
  const auto code = Encode(point, options.dimensions, options.hilbert);
  const auto bits = options.dimensions * options.max_level;
  const auto size = CodeSize(options.dimensions, options.max_level);
  std::memcpy(out, code.data(), size);
  if (bits % 8) {
    out[size - 1] &= static_cast<uint8_t>(0xff << (8 - bits % 8));
  }
  return size;
}

template<typename Emit>
void CodeTerms(std::span<const uint8_t> code, const Options& options,
               Emit&& emit) {
  std::array<uint8_t, kTermHeader + sizeof(Code)> term;
  term[1] = static_cast<uint8_t>(options.dimensions);
  std::memcpy(term.data() + kTermHeader, code.data(), code.size());
  for (uint32_t level = 0; level < options.max_level;
       level += options.level_step) {
    detail::EmitTerm(term.data(), options.dimensions, level, kAncestorTerm,
                     emit);
  }
  detail::EmitTerm(term.data(), options.dimensions, options.max_level,
                   kLeafTerm, emit);
}

template<typename Emit>
void PointTerms(const Point& point, const Options& options, Emit&& emit) {
  Code code;
  const auto size = LeafCode(point, options, code.data());
  CodeTerms(std::span<const uint8_t>{code.data(), size}, options, emit);
}

inline std::vector<std::string> PointQueryTerms(std::span<const Cell> cells,
                                                const Box& box,
                                                const Options& options) {
  std::vector<std::string> terms;
  terms.reserve(cells.size());
  const auto add = [&](const Cell& cell) {
    const auto code = Encode(cell.min, options.dimensions, options.hilbert);
    terms.emplace_back(
      Term(code, options.dimensions, cell.level,
           cell.level == options.max_level ? kLeafTerm : kAncestorTerm));
  };
  for (const auto& cell : cells) {
    if (IsIndexedLevel(cell.level, options)) {
      add(cell);
      continue;
    }
    const auto level =
      std::min(options.max_level,
               (cell.level / options.level_step + 1) * options.level_step);
    const auto depth = level - cell.level;
    for (uint64_t index = 0;
         index < (uint64_t{1} << (depth * options.dimensions)); ++index) {
      Cell child{cell.min, level};
      for (uint32_t step = 0; step < depth; ++step) {
        const auto bit = uint64_t{1} << (63 - cell.level - step);
        for (uint32_t axis = 0; axis < options.dimensions; ++axis) {
          if ((index >> (step * options.dimensions + axis)) & 1) {
            child.min[axis] |= bit;
          }
        }
      }
      if (Classify(child, box, options.dimensions) != Relation::Outside) {
        add(child);
      }
    }
  }
  absl::c_sort(terms);
  terms.erase(std::ranges::unique(terms).begin(), terms.end());
  return terms;
}

}  // namespace irs::curve
