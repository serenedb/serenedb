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

#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

uint64_t Head(bytes_view key) noexcept;
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

class SortedRoot {
 public:
  SortedRoot() = default;
  explicit SortedRoot(Separators separators) noexcept
    : _separators{std::move(separators)} {}

  size_t Blocks() const noexcept { return SeparatorCount(_separators) + 1; }
  size_t Find(bytes_view key) const noexcept;
  size_t FindFrom(size_t from, bytes_view key) const noexcept;
  void FindBatch(std::span<const bytes_view> keys,
                 std::span<uint32_t> blocks) const noexcept;
  bytes_view Separator(size_t block) const noexcept {
    return SeparatorAt(_separators, block - 1);
  }
  size_t MemoryBytes() const noexcept { return SeparatorBytes(_separators); }

 private:
  Separators _separators;
};

class SortedHeadsRoot {
 public:
  SortedHeadsRoot() = default;
  explicit SortedHeadsRoot(Separators separators);

  size_t Blocks() const noexcept { return _heads.size() + 1; }
  size_t Find(bytes_view key) const noexcept;
  size_t FindFrom(size_t from, bytes_view key) const noexcept;
  void FindBatch(std::span<const bytes_view> keys,
                 std::span<uint32_t> blocks) const noexcept;
  bytes_view Separator(size_t block) const noexcept {
    return SeparatorAt(_separators, block - 1);
  }
  size_t MemoryBytes() const noexcept {
    return SeparatorBytes(_separators) + _heads.capacity() * sizeof(uint64_t);
  }

 private:
  Separators _separators;
  std::vector<uint64_t> _heads;
};

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
  void FindBatch(std::span<const bytes_view> keys,
                 std::span<uint32_t> blocks) const noexcept;
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
  size_t _height = 0;
};

class BTreeRoot {
 public:
  static constexpr size_t kKeys = 8;

  BTreeRoot() = default;
  explicit BTreeRoot(Separators separators);

  size_t Blocks() const noexcept { return _heads.size() + 1; }
  size_t Find(bytes_view key) const noexcept;
  size_t FindFrom(size_t from, bytes_view key) const noexcept;
  void FindBatch(std::span<const bytes_view> keys,
                 std::span<uint32_t> blocks) const noexcept;
  bytes_view Separator(size_t block) const noexcept {
    return SeparatorAt(_separators, block - 1);
  }
  size_t MemoryBytes() const noexcept {
    return SeparatorBytes(_separators) +
           _heads.capacity() * sizeof(uint64_t) +
           _tree.capacity() * sizeof(uint64_t) +
           _ranks.capacity() * sizeof(uint32_t);
  }

 private:
  Separators _separators;
  std::vector<uint64_t> _heads;
  std::vector<uint64_t> _tree;
  std::vector<uint32_t> _ranks;
  size_t _nodes = 0;
};

class RadixRoot {
 public:
  RadixRoot() = default;
  RadixRoot(Separators separators, uint32_t radix_bytes);

  size_t Blocks() const noexcept { return _heads.size() + 1; }
  size_t Find(bytes_view key) const noexcept;
  size_t FindFrom(size_t from, bytes_view key) const noexcept;
  void FindBatch(std::span<const bytes_view> keys,
                 std::span<uint32_t> blocks) const noexcept;
  bytes_view Separator(size_t block) const noexcept {
    return SeparatorAt(_separators, block - 1);
  }
  size_t MemoryBytes() const noexcept {
    return SeparatorBytes(_separators) +
           _heads.capacity() * sizeof(uint64_t) +
           _table.capacity() * sizeof(uint32_t) + _prefix.capacity();
  }

 private:
  size_t Bucket(bytes_view key) const noexcept;

  Separators _separators;
  std::vector<uint64_t> _heads;
  std::vector<uint32_t> _table;
  bstring _prefix;
  uint32_t _radix_bytes = 1;
};

using Root = SortedHeadsRoot;

}  // namespace irs::term_dict
