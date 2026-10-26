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
#include <span>
#include <variant>
#include <vector>

#include "iresearch/utils/string.hpp"
#include "iresearch/utils/string_utils.hpp"

namespace irs::term_dict {

inline constexpr size_t kKeyPad = 2 * sizeof(uint64_t);

struct Probe {
  const byte_type* data;
  size_t size;
};

class PaddedKey {
 public:
  PaddedKey() = default;
  explicit PaddedKey(bytes_view key) { Assign(key); }
  PaddedKey(const PaddedKey&) = delete;
  PaddedKey& operator=(const PaddedKey&) = delete;

  byte_type* Resize(size_t size) {
    if (size + kKeyPad <= sizeof(_inline)) {
      _data = _inline;
    } else {
      if (_heap.size() < size + kKeyPad) {
        utils::StrResizeAmortized(_heap, size + kKeyPad);
      }
      _data = _heap.data();
    }
    std::memset(_data + size, 0, kKeyPad);
    _size = size;
    return _data;
  }

  Probe Assign(bytes_view key) {
    auto* data = Resize(key.size());
    if (!key.empty()) {
      std::memcpy(data, key.data(), key.size());
    }
    return View();
  }

  const byte_type* Data() const noexcept { return _data; }
  Probe View() const noexcept { return {_data, _size}; }
  bytes_view Key() const noexcept { return {_data, _size}; }

 private:
  byte_type _inline[256];
  bstring _heap;
  byte_type* _data = _inline;
  size_t _size = 0;
};

IRS_FORCE_INLINE inline size_t PaddedMismatch(const byte_type* lhs,
                                              const byte_type* rhs,
                                              size_t size) noexcept {
  for (size_t i = 0; i < size; i += sizeof(uint64_t)) {
    const uint64_t diff = absl::little_endian::Load64(lhs + i) ^
                          absl::little_endian::Load64(rhs + i);
    if (diff != 0) {
      return std::min(size, i + std::countr_zero(diff) / 8);
    }
  }
  return size;
}

uint64_t HeadAt(bytes_view key, size_t skip) noexcept;

struct Separators {
  bstring arena;
  std::vector<uint32_t> offsets{0};
};

Separators DecodeSeparators(bytes_view encoded, size_t count);
Separators MakeSeparators(std::span<const bytes_view> separators);

size_t SeparatorCount(const Separators& separators) noexcept;
bytes_view SeparatorAt(const Separators& separators, size_t i) noexcept;
size_t SeparatorBytes(const Separators& separators) noexcept;

class EytzingerRoot {
 public:
  struct alignas(16) Node {
    uint64_t head;
    uint32_t skip;
    uint32_t rank;
  };

  EytzingerRoot() = default;
  explicit EytzingerRoot(Separators separators);
  EytzingerRoot(EytzingerRoot&&) noexcept = default;
  EytzingerRoot& operator=(EytzingerRoot&&) noexcept = default;

  size_t Blocks() const noexcept { return SeparatorCount(_separators) + 1; }
  size_t Find(bytes_view key) const noexcept;
  size_t FindFrom(size_t from, bytes_view key) const noexcept;
  bool NotAfter(size_t block, bytes_view key) const noexcept {
    return Separator(block) <= key;
  }
  bytes_view Separator(size_t block) const noexcept {
    return SeparatorAt(_separators, block - 1);
  }
  size_t MemoryBytes() const noexcept {
    return SeparatorBytes(_separators) + _storage.capacity() * sizeof(Node);
  }

 private:
  bool NotAfter(const Node& node, bytes_view key,
                const byte_type* padded) const noexcept;
  size_t Outside(bytes_view key) const noexcept;
  size_t Resolve(size_t k) const noexcept;

  Separators _separators;
  std::vector<Node> _storage;
  const Node* _nodes = nullptr;
  bstring _prefix;
  size_t _count = 0;
};

class TreeRoot {
 public:
  TreeRoot() = default;
  explicit TreeRoot(Separators separators);

  static size_t Height(size_t count) noexcept {
    size_t nodes = std::max<size_t>(1, (count + kFanout - 1) / kFanout);
    size_t height = 1;
    for (; nodes != 1; ++height) {
      nodes = (nodes + kFanout - 1) / kFanout;
    }
    return height;
  }

  size_t Blocks() const noexcept { return _count + 1; }
  size_t Find(bytes_view key) const noexcept;
  size_t FindFrom(size_t from, bytes_view key) const noexcept;
  bool NotAfter(size_t block, bytes_view key) const noexcept;
  bytes_view Separator(size_t block) const noexcept {
    return SeparatorAt(_separators, block - 1);
  }
  size_t MemoryBytes() const noexcept {
    return SeparatorBytes(_separators) + _nodes.capacity() * sizeof(Node) +
           _levels.capacity() * sizeof(size_t) + _prefix.capacity();
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

  Located Locate(bytes_view key) const noexcept;
  size_t Descend(uint64_t head) const noexcept;
  size_t Resolve(size_t from, size_t upper, uint64_t head,
                 bytes_view key) const noexcept;
  const uint64_t* Heads() const noexcept {
    return reinterpret_cast<const uint64_t*>(_nodes.data() + _leaves);
  }

  Separators _separators;
  bstring _prefix;
  std::vector<Node> _nodes;
  std::vector<size_t> _levels;
  size_t _leaves = 0;
  size_t _count = 0;
};

class ArtRoot {
 public:
  ArtRoot() = default;
  explicit ArtRoot(Separators separators);

  size_t Blocks() const noexcept { return SeparatorCount(_separators) + 1; }
  size_t Find(bytes_view key) const noexcept;
  size_t FindFrom(size_t from, bytes_view key) const noexcept {
    return std::max(from, Find(key));
  }
  bool NotAfter(size_t block, bytes_view key) const noexcept {
    return Separator(block) <= key;
  }
  bytes_view Separator(size_t block) const noexcept {
    return SeparatorAt(_separators, block - 1);
  }
  size_t MemoryBytes() const noexcept {
    return SeparatorBytes(_separators) + _words.capacity() * sizeof(uint64_t);
  }
  uint64_t Visits() const noexcept { return _visits; }

 private:
  size_t Leaf(const uint64_t* child, size_t depth,
              bytes_view key) const noexcept;

  Separators _separators;
  std::vector<uint64_t> _words;
  uint64_t _visits = 0;
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
  Root() = default;
  Root(Separators separators, bool fixed);

  size_t Blocks() const noexcept {
    return Visit([](const auto& root) { return root.Blocks(); });
  }
  size_t Find(bytes_view key) const noexcept {
    return Visit([&](const auto& root) { return root.Find(key); });
  }
  size_t FindFrom(size_t from, bytes_view key) const noexcept {
    return Visit([&](const auto& root) { return root.FindFrom(from, key); });
  }
  bool NotAfter(size_t block, bytes_view key) const noexcept {
    return Visit([&](const auto& root) { return root.NotAfter(block, key); });
  }
  bytes_view Separator(size_t block) const noexcept {
    return Visit([&](const auto& root) { return root.Separator(block); });
  }
  size_t MemoryBytes() const noexcept {
    return Visit([](const auto& root) { return root.MemoryBytes(); });
  }

 private:
  std::variant<TreeRoot, EytzingerRoot, ArtRoot> _root;
};

}  // namespace irs::term_dict
