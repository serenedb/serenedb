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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <roaring/memory.h>
#include <roaring/roaring.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>

#include "iresearch/store/data_input.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

enum class MaskKind : uint8_t {
  Bitsets,
  Arrays,
  Runs,
  Mixed,
  Bitset,
  Array,
  Run,
};

class DocumentMaskBuilder;

class DocumentMask final {
 public:
  static constexpr uint32_t kChunkShift = 16;
  static constexpr uint64_t kChunkDocs = uint64_t{1} << kChunkShift;

  class Iterator final {
   public:
    Iterator() = default;

    explicit Iterator(const DocumentMask* mask) noexcept : _mask{mask} {}

    bool Empty() const noexcept { return _mask == nullptr || _mask->Empty(); }

    bool Contains(doc_id_t doc) const noexcept {
      return _mask != nullptr && _mask->Contains(doc);
    }

   private:
    const DocumentMask* _mask = nullptr;
  };

  DocumentMask(DocumentMask&& other) noexcept;
  DocumentMask& operator=(DocumentMask&& other) noexcept;
  DocumentMask(const DocumentMask&) = delete;
  DocumentMask& operator=(const DocumentMask&) = delete;

  friend bool operator==(const DocumentMask& lhs, const DocumentMask& rhs);

  static DocumentMask ReadFrozen(IndexInput& in, uint64_t size);

  size_t FrozenSize() const noexcept { return _set.getFrozenSizeInBytes(); }

  void WriteFrozen(char* buf) const noexcept { _set.writeFrozen(buf); }

  bool Contains(doc_id_t doc) const noexcept;

  size_t Count() const noexcept { return _count; }

  bool Empty() const noexcept { return _count == 0; }

  size_t ByteSize() const noexcept;

  size_t ByteCapacity() const noexcept;

  MaskKind Kind() const noexcept { return _kind; }

  uint64_t RunsBound() const noexcept;

  uint32_t ContainerCount() const noexcept {
    return static_cast<uint32_t>(_set.roaring.high_low_container.size);
  }

  const uint16_t* Keys() const noexcept {
    return _set.roaring.high_low_container.keys;
  }

  const uint8_t* Types() const noexcept {
    return _set.roaring.high_low_container.typecodes;
  }

  const void* const* Containers() const noexcept {
    return reinterpret_cast<const void* const*>(
      _set.roaring.high_low_container.containers);
  }

 private:
  friend class DocumentMaskBuilder;

  struct FrozenFree {
    void operator()(byte_type* p) const noexcept { roaring_aligned_free(p); }
  };

  DocumentMask() = default;
  DocumentMask(roaring::api::roaring_bitmap_t& set, size_t count) noexcept;

  std::unique_ptr<byte_type, FrozenFree> _frozen;
  roaring::Roaring _set;
  size_t _count = 0;
  MaskKind _kind = MaskKind::Runs;
};

class DocumentMaskBuilder final {
 public:
  static constexpr uint32_t kCanonical = 4097;
  static constexpr uint32_t kBitsetFrom = 16;

  DocumentMaskBuilder() noexcept;
  explicit DocumentMaskBuilder(const DocumentMask& published);
  ~DocumentMaskBuilder();

  DocumentMaskBuilder(DocumentMaskBuilder&& other) noexcept;
  DocumentMaskBuilder& operator=(DocumentMaskBuilder&& other) noexcept;
  DocumentMaskBuilder(const DocumentMaskBuilder& other);
  DocumentMaskBuilder& operator=(const DocumentMaskBuilder& other);

  bool Contains(doc_id_t doc) const noexcept;

  size_t Count() const noexcept { return _count; }

  bool Empty() const noexcept { return _count == 0; }

  size_t ByteSize() const noexcept;

  size_t ByteCapacity() const noexcept;

  bool Add(doc_id_t doc);
  void AddRange(doc_id_t first, doc_id_t last);
  void Merge(const DocumentMaskBuilder& other);
  void Merge(const DocumentMask& published);
  void Clear() noexcept;

  DocumentMask Finish(uint32_t bitset_from = kBitsetFrom) && noexcept;

 private:
  DocumentMaskBuilder(const roaring::api::roaring_bitmap_t& set, size_t count);

  roaring::api::roaring_bitmap_t _set;
  size_t _count = 0;
};

}  // namespace irs
