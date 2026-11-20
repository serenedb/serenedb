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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <variant>
#include <vector>

#include "iresearch/formats/index/term_dict_key.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

struct Separators {
  bstring arena;
  std::vector<uint32_t> offsets{0};
};

inline constexpr uint32_t kNoSeparators[]{0};

struct SeparatorsView {
  const byte_type* arena = nullptr;
  std::span<const uint32_t> offsets{kNoSeparators};
};

Separators DecodeSeparators(bytes_view encoded, size_t count);

inline SeparatorsView Slice(const Separators& separators, size_t from,
                            size_t count) noexcept {
  SDB_ASSERT(from + count < separators.offsets.size());
  return {
    .arena = separators.arena.data(),
    .offsets = std::span{separators.offsets}.subspan(from, count + 1),
  };
}

inline SeparatorsView Slice(const Separators& separators) noexcept {
  return Slice(separators, 0, separators.offsets.size() - 1);
}

inline size_t SeparatorCount(SeparatorsView separators) noexcept {
  return separators.offsets.size() - 1;
}

inline bytes_view SeparatorAt(SeparatorsView separators, size_t i) noexcept {
  return {separators.arena + separators.offsets[i],
          separators.offsets[i + 1] - separators.offsets[i]};
}

inline size_t SeparatorBytes(const Separators& separators) noexcept {
  return separators.arena.capacity() +
         separators.offsets.capacity() * sizeof(uint32_t);
}

class SeparatorRoot {
 public:
  size_t Blocks() const noexcept { return SeparatorCount(_separators) + 1; }

 protected:
  SeparatorRoot() = default;
  explicit SeparatorRoot(SeparatorsView separators) noexcept
    : _separators{separators} {}

  SeparatorsView _separators;
};

class EytzingerRoot : public SeparatorRoot {
 public:
  struct alignas(16) Node {
    uint64_t head;
    uint32_t skip;
    uint32_t rank;
  };

  EytzingerRoot() = default;
  explicit EytzingerRoot(SeparatorsView separators);
  EytzingerRoot(EytzingerRoot&&) noexcept = default;
  EytzingerRoot& operator=(EytzingerRoot&&) noexcept = default;

  size_t Find(Probe key) const noexcept;
  size_t MemoryBytes() const noexcept {
    return _storage.capacity() * sizeof(Node);
  }

 private:
  bool NotAfter(const Node& node, Probe key) const noexcept;
  size_t Outside(bytes_view key) const noexcept;
  size_t Resolve(size_t k) const noexcept;

  std::vector<Node> _storage;
  const Node* _nodes = nullptr;
  bytes_view _prefix;
  size_t _count = 0;
};

class TreeRoot : public SeparatorRoot {
 public:
  TreeRoot() = default;
  explicit TreeRoot(SeparatorsView separators);

  static size_t Height(size_t count) { return LevelSizes(count).size(); }

  size_t Find(Probe key) const noexcept;
  size_t MemoryBytes() const noexcept {
    return _nodes.capacity() * sizeof(Node) +
           _levels.capacity() * sizeof(size_t);
  }

 private:
  static constexpr size_t kFanout = 8;

  struct alignas(64) Node {
    uint64_t heads[kFanout];
  };

  struct Located {
    uint64_t head;
    size_t outside;
  };

  static std::vector<size_t> LevelSizes(size_t count);
  Located Locate(bytes_view key) const noexcept;
  size_t Descend(uint64_t head) const noexcept;
  size_t Resolve(size_t upper, uint64_t head, bytes_view key) const noexcept;
  const uint64_t* Heads() const noexcept {
    return reinterpret_cast<const uint64_t*>(_nodes.data() + _leaves);
  }

  bytes_view _prefix;
  std::vector<Node> _nodes;
  std::vector<size_t> _levels;
  size_t _leaves = 0;
  size_t _count = 0;
};

class ArtRoot : public SeparatorRoot {
 public:
  ArtRoot() = default;
  explicit ArtRoot(SeparatorsView separators);

  size_t Find(Probe key) const noexcept;
  size_t MemoryBytes() const noexcept {
    return _words.capacity() * sizeof(uint64_t);
  }
  uint64_t Visits() const noexcept { return _visits; }

 private:
  size_t Leaf(const uint64_t* child, size_t depth,
              bytes_view key) const noexcept;

  std::vector<uint64_t> _words;
  uint64_t _visits = 0;
};

class Root;

struct SeparatorBuffer {
  bstring key;
  const Root* root = nullptr;
  size_t block = 0;
  const byte_type* next = nullptr;
  bool restart = false;
};

class Root {
  IRS_FORCE_INLINE decltype(auto) Visit(auto&& visitor) const noexcept {
    if (const auto* tree = std::get_if<TreeRoot>(&_root)) [[likely]] {
      return visitor(*tree);
    }
    if (const auto* art = std::get_if<ArtRoot>(&_root)) {
      return visitor(*art);
    }
    return visitor(*std::get_if<EytzingerRoot>(&_root));
  }

 public:
  static constexpr size_t kGroup = 16;

  Root() = default;
  Root(SeparatorsView separators, bool fixed);

  size_t Blocks() const noexcept { return _count + 1; }
  size_t Find(Probe key) const noexcept;
  size_t FindFrom(size_t from, Probe key) const noexcept;
  size_t FindFrom(size_t from, Probe key, SeparatorBuffer& buffer) const;
  bytes_view Separator(size_t block, SeparatorBuffer& buffer) const;
  size_t MemoryBytes() const noexcept {
    return Visit([](const auto& root) { return root.MemoryBytes(); }) +
           (_restarts ? SeparatorBytes(*_restarts) + sizeof(Separators) : 0) +
           _packed.capacity() + _groups.capacity() * sizeof(uint32_t);
  }

 private:
  bytes_view Restart(size_t group) const noexcept {
    return SeparatorAt(Slice(*_restarts), group);
  }
  struct Scanned {
    size_t count;
    bool exhausted;
  };

  size_t Groups() const noexcept { return _groups.size() - 1; }
  bool Resumes(const SeparatorBuffer& buffer, size_t block) const noexcept {
    return buffer.root == this && buffer.block != 0 && buffer.block <= block &&
           (buffer.block - 1) / kGroup == (block - 1) / kGroup;
  }
  size_t Scan(size_t group, bytes_view key) const noexcept;
  static Scanned ScanFrom(const byte_type* p, const byte_type* end,
                          size_t count, size_t matched,
                          bytes_view key) noexcept;

  std::unique_ptr<Separators> _restarts;
  bstring _packed;
  std::vector<uint32_t> _groups;
  size_t _count = 0;
  std::variant<TreeRoot, EytzingerRoot, ArtRoot> _root;
};

}  // namespace irs::term_dict
