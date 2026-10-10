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
#include <queue>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

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
  bool cartesian = false;

  bool operator==(const Options&) const = default;
};

inline uint32_t DefaultLevelStep(uint32_t dimensions) noexcept {
  return 1 + 4 / dimensions;
}

inline uint32_t MaxLevelStep(uint32_t dimensions) noexcept {
  return 1 + kMaxExpansionBits / dimensions;
}

inline void Validate(const Options& options) {
  if (options.dimensions < 2 || options.dimensions > kMaxDimensions ||
      options.max_level > kMaxLevel || options.max_cells == 0 ||
      options.max_cells > kMaxCells || options.level_step == 0 ||
      options.level_step > MaxLevelStep(options.dimensions) ||
      (options.cartesian &&
       (options.dimensions != 2 || options.level_step != 1))) {
    throw std::invalid_argument(
      "curve: dimensions must be 2..8 (Cartesian: 2), "
      "max_level 0..64, max_cells 1..4096, level_step 1..1+12/dimensions "
      "(Cartesian: 1)");
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
  pending.push(
    {root, root_relation, static_cast<long double>(priority(root)), order++});
  std::vector<Pending> children;
  while (!pending.empty()) {
    const auto top = pending.top();
    pending.pop();
    const auto& cell = top.cell;
    if (cell.level == options.max_level || top.relation == Relation::Inside) {
      result.push_back(cell);
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
        children.push_back({next, next_relation, 0, 0});
      }
    }
    if (children.size() + pending.size() + result.size() > options.max_cells) {
      result.push_back(cell);
      continue;
    }
    for (auto& child : children) {
      child.priority = static_cast<long double>(priority(child.cell));
      child.order = order++;
      pending.push(child);
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

inline Code Encode(Point point, uint32_t dimensions, bool hilbert) {
  if (dimensions < 2 || dimensions > kMaxDimensions) {
    throw std::invalid_argument("curve dimensions must be 2..8");
  }
  if (hilbert) {
    for (uint64_t q = uint64_t{1} << 63; q > 1; q >>= 1) {
      const auto mask = q - 1;
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        if (point[axis] & q) {
          point[0] ^= mask;
        } else {
          const auto exchange = (point[0] ^ point[axis]) & mask;
          point[0] ^= exchange;
          point[axis] ^= exchange;
        }
      }
    }
    for (uint32_t axis = 1; axis < dimensions; ++axis) {
      point[axis] ^= point[axis - 1];
    }
    uint64_t correction = 0;
    for (uint64_t q = uint64_t{1} << 63; q > 1; q >>= 1) {
      if (point[dimensions - 1] & q) {
        correction ^= q - 1;
      }
    }
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      point[axis] ^= correction;
    }
  }
  Code code;
  code.fill(0);
  for (uint32_t bit = 0; bit < 64; ++bit) {
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      const auto offset = bit * dimensions + axis;
      const auto value = (point[axis] >> (63 - bit)) & 1;
      code[offset / 8] |= static_cast<uint8_t>(value << (7 - offset % 8));
    }
  }
  return code;
}

inline uint32_t TermSize(uint32_t dimensions, uint32_t level) noexcept {
  return kTermHeader + (dimensions * level + 7) / 8;
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

template<typename Emit>
void PointTerms(const Point& point, const Options& options, Emit&& emit) {
  const auto code = Encode(point, options.dimensions, options.hilbert);
  std::array<uint8_t, kTermHeader + sizeof(Code)> term;
  for (uint32_t level = 0; level <= options.max_level; ++level) {
    if (!IsIndexedLevel(level, options)) {
      continue;
    }
    const auto kind = level == options.max_level ? kLeafTerm : kAncestorTerm;
    emit(std::span<const uint8_t>{
      term.data(),
      WriteTerm(term.data(), code, options.dimensions, level, kind)});
  }
}

inline std::vector<std::string> PointQueryTerms(std::span<const Cell> cells,
                                                const Box& box,
                                                const Options& options) {
  std::vector<std::string> terms;
  terms.reserve(cells.size());
  const auto add = [&](const Cell& cell) {
    const auto code = Encode(cell.min, options.dimensions, options.hilbert);
    terms.push_back(
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
  std::ranges::sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
  return terms;
}

inline uint32_t CommonPrefixBits(const Code& a, const Code& b) noexcept {
  for (size_t i = 0; i < a.size(); ++i) {
    if (a[i] != b[i]) {
      return static_cast<uint32_t>(
        i * 8 + std::countl_zero(static_cast<uint8_t>(a[i] ^ b[i])));
    }
  }
  return static_cast<uint32_t>(a.size() * 8);
}

template<typename Emit>
void CellTerms(std::span<const Cell> cells, const Options& options, bool query,
               Emit&& emit) {
  struct Entry {
    Code code;
    uint32_t level;
  };
  std::vector<Entry> entries;
  entries.reserve(cells.size());
  for (const auto& cell : cells) {
    entries.push_back(
      {Encode(cell.min, options.dimensions, options.hilbert), cell.level});
  }
  std::ranges::sort(entries, {}, &Entry::code);
  std::array<uint8_t, kTermHeader + sizeof(Code)> term;
  const auto write = [&](const Entry& entry, uint32_t level, char kind) {
    emit(std::span<const uint8_t>{
      term.data(),
      WriteTerm(term.data(), entry.code, options.dimensions, level, kind)});
  };
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto& entry = entries[i];
    uint32_t first = 0;
    if (i != 0) {
      const auto& prev = entries[i - 1];
      const auto shared =
        CommonPrefixBits(prev.code, entry.code) / options.dimensions;
      first = query ? std::min(prev.level, shared) + 1
                    : std::min(prev.level, shared + 1);
    }
    for (uint32_t level = first; level < entry.level; ++level) {
      write(entry, level, query ? kLeafTerm : kAncestorTerm);
    }
    if (query && first <= entry.level) {
      write(entry, entry.level, kLeafTerm);
    }
    write(entry, entry.level, query ? kAncestorTerm : kLeafTerm);
  }
}

inline std::vector<std::string> Terms(std::span<const Cell> cells,
                                      const Options& options, bool query) {
  std::vector<std::string> terms;
  CellTerms(cells, options, query, [&](std::span<const uint8_t> term) {
    terms.emplace_back(term.begin(), term.end());
  });
  std::ranges::sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
  return terms;
}

}  // namespace irs::curve
