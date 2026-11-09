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
#include <memory>
#include <roaring/roaring.hh>
#include <vector>

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

  DocumentMask(std::shared_ptr<const byte_type> frozen, uint64_t size);
  DocumentMask(DocumentMask&& other) noexcept;
  DocumentMask& operator=(DocumentMask&& other) noexcept;
  DocumentMask(const DocumentMask&) = delete;
  DocumentMask& operator=(const DocumentMask&) = delete;
  ~DocumentMask();

  friend bool operator==(const DocumentMask& lhs, const DocumentMask& rhs);

  static DocumentMask ReadFrozen(IndexInput& in, uint64_t size);

  static DocumentMask Union(const std::shared_ptr<const DocumentMask>& base,
                            const DocumentMask& added);

  size_t FrozenSize() const noexcept {
    return roaring::api::roaring_bitmap_frozen_size_in_bytes(&_set);
  }

  void WriteFrozen(char* buf) const noexcept {
    roaring::api::roaring_bitmap_frozen_serialize(&_set, buf);
  }

  const roaring::api::roaring_bitmap_t& Bitmap() const noexcept { return _set; }

  bool Contains(doc_id_t doc) const noexcept;

  uint64_t CountInRange(doc_id_t first, doc_id_t last) const noexcept;

  template<typename Visitor>
  void ForEachRun(const DocumentMask* except, const Visitor& visitor) const {
    const auto visit = [&](doc_id_t first, doc_id_t last) {
      return visitor(first, last + 1);
    };
    const auto call = [](uint32_t first, uint32_t last, void* f) -> bool {
      return (*static_cast<const decltype(visit)*>(f))(first, last);
    };
    auto* f = const_cast<void*>(static_cast<const void*>(&visit));
    if (except) {
      roaring::api::roaring_bitmap_andnot_iterate_runs(&_set, &except->_set,
                                                       call, f);
    } else {
      roaring::api::roaring_bitmap_iterate_runs(&_set, call, f);
    }
  }

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

  struct ViewFree {
    void operator()(const roaring::api::roaring_bitmap_t* view) const noexcept {
      roaring::api::roaring_bitmap_free(view);
    }
  };

  DocumentMask() noexcept;
  DocumentMask(roaring::api::roaring_bitmap_t& set, size_t count) noexcept;

  std::shared_ptr<const byte_type> _frozen;
  std::unique_ptr<const roaring::api::roaring_bitmap_t, ViewFree> _view;
  roaring::api::roaring_bitmap_t _set;
  std::vector<std::shared_ptr<const void>> _owners;
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
  DocumentMask Take() && noexcept;

 private:
  DocumentMaskBuilder(const roaring::api::roaring_bitmap_t& set, size_t count);

  roaring::api::roaring_bitmap_t _set;
  size_t _count = 0;
};

}  // namespace irs
