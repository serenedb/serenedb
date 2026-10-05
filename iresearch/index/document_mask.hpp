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
};

class DocumentMask final {
 public:
  static constexpr uint32_t kChunkShift = 16;
  static constexpr uint64_t kChunkDocs = uint64_t{1} << kChunkShift;
  static constexpr uint32_t kCanonical = 4097;
  static constexpr uint32_t kBitsetFrom = 16;

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

  DocumentMask() noexcept;
  ~DocumentMask();

  DocumentMask(DocumentMask&& other) noexcept;
  DocumentMask& operator=(DocumentMask&& other) noexcept;
  DocumentMask(const DocumentMask& other);
  DocumentMask& operator=(const DocumentMask& other);

  friend bool operator==(const DocumentMask& lhs, const DocumentMask& rhs);

  static DocumentMask Read(const char* buf, size_t size);

  roaring::Roaring Compress() const;

  bool Contains(doc_id_t doc) const noexcept;

  size_t Count() const noexcept { return _count; }

  bool Empty() const noexcept { return _count == 0; }

  size_t ByteSize() const noexcept;

  size_t ByteCapacity() const noexcept;

  bool Add(doc_id_t doc);
  void AddRange(doc_id_t first, doc_id_t last);
  void Truncate(doc_id_t first) noexcept;
  void Merge(const DocumentMask& other);
  void Clear() noexcept;
  void Trim() noexcept { Trim(kBitsetFrom); }
  void Trim(uint32_t bitset_from) noexcept;

  MaskKind Kind() const noexcept { return _kind; }

  uint64_t SpansAt(uint32_t i) const noexcept;
  uint64_t RunsBound() const noexcept;

  uint32_t ContainerCount() const noexcept {
    return static_cast<uint32_t>(_set.high_low_container.size);
  }

  uint16_t KeyAt(uint32_t i) const noexcept {
    SDB_ASSERT(i < ContainerCount());
    return _set.high_low_container.keys[i];
  }

  const uint16_t* Keys() const noexcept { return _set.high_low_container.keys; }

  const uint8_t* Types() const noexcept {
    return _set.high_low_container.typecodes;
  }

  uint8_t TypeAt(uint32_t i) const noexcept {
    SDB_ASSERT(i < ContainerCount());
    return _set.high_low_container.typecodes[i];
  }

  const void* ContainerAt(uint32_t i) const noexcept {
    SDB_ASSERT(i < ContainerCount());
    return _set.high_low_container.containers[i];
  }

  const void* const* Containers() const noexcept {
    return reinterpret_cast<const void* const*>(
      _set.high_low_container.containers);
  }

  const roaring::api::roaring_bitmap_t& Bitmap() const noexcept { return _set; }

 private:
  void Refresh() noexcept;

  roaring::api::roaring_bitmap_t _set;
  size_t _count = 0;
  MaskKind _kind = MaskKind::Runs;
};

}  // namespace irs
