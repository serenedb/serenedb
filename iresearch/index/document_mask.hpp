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
#include <span>
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

using MaskRun = roaring::api::roaring_uint32_range_closed_t;

class DocumentMask final {
 public:
  static constexpr uint32_t kChunkShift = 16;
  static constexpr uint64_t kChunkDocs = uint64_t{1} << kChunkShift;

  class Cursor final {
   public:
    explicit Cursor(const DocumentMask& mask) noexcept {
      roaring::api::roaring_iterator_init(&mask._set, &_it);
    }

    size_t Read(std::span<doc_id_t> docs) noexcept {
      return roaring::api::roaring_uint32_iterator_read(
        &_it, docs.data(), static_cast<uint32_t>(docs.size()));
    }

    size_t ReadRuns(std::span<MaskRun> runs) noexcept {
      return roaring::api::roaring_uint32_iterator_read_ranges(
        &_it, runs.data(), runs.size());
    }

   private:
    roaring::api::roaring_uint32_iterator_t _it;
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

  bool Contains(doc_id_t doc) const noexcept;

  uint64_t CountInRange(doc_id_t first, doc_id_t last) const noexcept;

  bool Intersects(const DocumentMask& other) const noexcept {
    return roaring::api::roaring_bitmap_intersect(&_set, &other._set);
  }

  uint64_t AndCount(const DocumentMask& other) const noexcept {
    return roaring::api::roaring_bitmap_and_cardinality(&_set, &other._set);
  }

  void Ranks(std::span<const doc_id_t> docs,
             std::span<uint64_t> ranks) const noexcept {
    SDB_ASSERT(docs.size() == ranks.size());
    roaring::api::roaring_bitmap_rank_many(
      &_set, docs.data(), docs.data() + docs.size(), ranks.data());
  }

  void Words(uint32_t first_word, std::span<uint64_t> words) const noexcept {
    roaring::api::roaring_bitmap_word_range_to_bitset(
      &_set, first_word, static_cast<uint32_t>(words.size()), words.data());
  }

  size_t Count() const noexcept { return _count; }

  bool Empty() const noexcept { return _count == 0; }

  doc_id_t Max() const noexcept {
    return roaring::api::roaring_bitmap_maximum(&_set);
  }

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

  size_t Count() const noexcept {
    return roaring::api::roaring_bitmap_get_cardinality(&_set);
  }

  bool Empty() const noexcept {
    return roaring::api::roaring_bitmap_is_empty(&_set);
  }

  size_t ByteSize() const noexcept;

  size_t ByteCapacity() const noexcept;

  bool Add(doc_id_t doc);
  void AddRange(doc_id_t first, doc_id_t last);
  void AddSorted(std::span<const doc_id_t> docs);
  void AddShifted(const DocumentMask& mask, doc_id_t offset);
  void AddShifted(const DocumentMaskBuilder& other, doc_id_t offset);
  void Merge(const DocumentMaskBuilder& other);
  void Merge(const DocumentMask& published);
  void Subtract(const DocumentMask& mask);
  void Clear() noexcept;

  DocumentMask Finish(uint32_t bitset_from = kBitsetFrom) && noexcept;
  DocumentMask Take() && noexcept;

 private:
  explicit DocumentMaskBuilder(const roaring::api::roaring_bitmap_t& set);

  void AddShifted(const roaring::api::roaring_bitmap_t& set, doc_id_t offset);

  roaring::api::roaring_bitmap_t _set;
};

}  // namespace irs
