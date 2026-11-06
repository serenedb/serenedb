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

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr size_t kKeyPad = 2 * sizeof(uint64_t);
inline constexpr size_t kInlineKeyBytes = 256;

struct Probe {
  const byte_type* data;
  size_t size;
};

inline bytes_view KeyOf(Probe probe) noexcept {
  return {probe.data, probe.size};
}

class PaddedKey {
 public:
  PaddedKey() : _bytes(kKeyPad, byte_type{0}) {}
  explicit PaddedKey(bytes_view key) { Assign(key); }
  PaddedKey(const PaddedKey&) = delete;
  PaddedKey& operator=(const PaddedKey&) = delete;

  byte_type* Resize(size_t size) {
    _bytes.resize(size + kKeyPad, boost::container::default_init);
    std::memset(_bytes.data() + size, 0, kKeyPad);
    return _bytes.data();
  }

  Probe Assign(bytes_view key) {
    auto* data = Resize(key.size());
    if (!key.empty()) {
      std::memcpy(data, key.data(), key.size());
    }
    return View();
  }

  Probe Assign(bytes_view prefix, bytes_view key) {
    auto* data = Resize(prefix.size() + key.size());
    std::memcpy(data, prefix.data(), prefix.size());
    if (!key.empty()) {
      std::memcpy(data + prefix.size(), key.data(), key.size());
    }
    return View();
  }

  const byte_type* Data() const noexcept { return _bytes.data(); }
  Probe View() const noexcept { return {_bytes.data(), Size()}; }
  bytes_view Key() const noexcept { return {_bytes.data(), Size()}; }

 private:
  size_t Size() const noexcept { return _bytes.size() - kKeyPad; }

  containers::SmallVector<byte_type, kInlineKeyBytes> _bytes;
};

IRS_FORCE_INLINE inline size_t PaddedMismatch(const byte_type* lhs,
                                              const byte_type* rhs,
                                              size_t size) noexcept {
  for (size_t i = 0; i < size; i += sizeof(uint64_t)) {
    uint64_t diff = absl::little_endian::Load64(lhs + i) ^
                    absl::little_endian::Load64(rhs + i);
    if (const size_t left = size - i; left < sizeof(uint64_t)) {
      diff &= (uint64_t{1} << (8 * left)) - 1;
    }
    if (diff != 0) {
      return i + std::countr_zero(diff) / 8;
    }
  }
  return size;
}

uint64_t HeadAt(bytes_view key, size_t skip) noexcept;

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

class SeparatorRoot {
 public:
  size_t Blocks() const noexcept { return SeparatorCount(_separators) + 1; }
  bytes_view Separator(size_t block) const noexcept {
    return SeparatorAt(_separators, block - 1);
  }
  bool NotAfter(size_t block, Probe key) const noexcept {
    return Separator(block) <= KeyOf(key);
  }

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

  using SeparatorRoot::NotAfter;
  size_t Find(Probe key) const noexcept;
  size_t FindFrom(size_t from, Probe key) const noexcept;
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
  size_t FindFrom(size_t from, Probe key) const noexcept;
  bool NotAfter(size_t block, Probe key) const noexcept;
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
  size_t Resolve(size_t from, size_t upper, uint64_t head,
                 bytes_view key) const noexcept;
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
  size_t FindFrom(size_t from, Probe key) const noexcept {
    return std::max(from, Find(key));
  }
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
  Root(SeparatorsView separators, bool fixed);

  size_t Blocks() const noexcept {
    return Visit([](const auto& root) { return root.Blocks(); });
  }
  size_t Find(Probe key) const noexcept {
    return Visit([&](const auto& root) { return root.Find(key); });
  }
  size_t FindFrom(size_t from, Probe key) const noexcept {
    return Visit([&](const auto& root) { return root.FindFrom(from, key); });
  }
  bool NotAfter(size_t block, Probe key) const noexcept {
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
