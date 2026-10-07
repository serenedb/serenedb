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

#include <roaring/roaring.h>

#include <cstddef>
#include <cstdint>

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace roaring {

class Roaring;
}

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

    explicit Iterator(const DocumentMask* mask,
                      doc_id_t visible_end = doc_limits::eof()) noexcept
      : _mask{mask}, _visible_end{visible_end} {}

    bool Empty() const noexcept {
      return doc_limits::eof(_visible_end) &&
             (_mask == nullptr || _mask->Empty());
    }

    bool Contains(doc_id_t doc) const noexcept {
      return doc >= _visible_end || (_mask != nullptr && _mask->Contains(doc));
    }

   private:
    const DocumentMask* _mask = nullptr;
    doc_id_t _visible_end = doc_limits::eof();
  };

  ~DocumentMask();

  DocumentMask(DocumentMask&& other) noexcept;
  DocumentMask& operator=(DocumentMask&& other) noexcept;
  DocumentMask(const DocumentMask&) = delete;
  DocumentMask& operator=(const DocumentMask&) = delete;

  friend bool operator==(const DocumentMask& lhs, const DocumentMask& rhs);

  roaring::Roaring Compress() const;

  bool Contains(doc_id_t doc) const noexcept;

  size_t Count() const noexcept { return _count; }

  bool Empty() const noexcept { return _count == 0; }

  size_t ByteSize() const noexcept;

  size_t ByteCapacity() const noexcept;

  MaskKind Kind() const noexcept { return _kind; }

  uint64_t RunsBound() const noexcept;

  uint32_t ContainerCount() const noexcept {
    return static_cast<uint32_t>(_set.high_low_container.size);
  }

  const uint16_t* Keys() const noexcept { return _set.high_low_container.keys; }

  const uint8_t* Types() const noexcept {
    return _set.high_low_container.typecodes;
  }

  const void* const* Containers() const noexcept {
    return reinterpret_cast<const void* const*>(
      _set.high_low_container.containers);
  }

 private:
  friend class DocumentMaskBuilder;

  DocumentMask(roaring::api::roaring_bitmap_t& set, size_t count) noexcept;

  roaring::api::roaring_bitmap_t _set;
  size_t _count;
  MaskKind _kind;
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

  static DocumentMaskBuilder Read(const char* buf, size_t size);

  roaring::Roaring Compress() const;

  bool Contains(doc_id_t doc) const noexcept;

  size_t Count() const noexcept { return _count; }

  bool Empty() const noexcept { return _count == 0; }

  size_t ByteSize() const noexcept;

  size_t ByteCapacity() const noexcept;

  bool Add(doc_id_t doc);
  void AddRange(doc_id_t first, doc_id_t last);
  void Truncate(doc_id_t first) noexcept;
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
