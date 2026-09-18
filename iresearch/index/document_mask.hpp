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

#include <roaring/bitset/bitset.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <roaring/roaring.hh>
#include <span>

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class DocumentMaskBuilder;

class DocumentMask final {
 public:
  class Iterator final {
   public:
    explicit Iterator(const DocumentMask& mask) noexcept : _bits{&mask._bits} {
      _value = Find(0);
    }

    bool Probe(doc_id_t doc) noexcept {
      return roaring::api::bitset_get(_bits, doc - kBase);
    }

    doc_id_t Value() const noexcept { return _value; }

    doc_id_t Next() noexcept {
      if (doc_limits::eof(_value)) {
        return _value;
      }
      return _value = Find(_value - kBase + 1);
    }

    doc_id_t Seek(doc_id_t target) noexcept {
#ifdef SDB_DEV
      SDB_ASSERT(_prev <= target);
      _prev = target;
#endif
      if (target <= _value) {
        return _value;
      }
      return _value = Find(target - kBase);
    }

   private:
    doc_id_t Find(size_t at) const noexcept {
      return roaring::api::bitset_next_set_bit(_bits, &at)
               ? static_cast<doc_id_t>(at + kBase)
               : doc_limits::eof();
    }

    const roaring::api::bitset_t* _bits;
    doc_id_t _value;
#ifdef SDB_DEV
    doc_id_t _prev = doc_limits::invalid();
#endif
  };

  DocumentMask() = default;
  ~DocumentMask();

  DocumentMask(DocumentMask&& other) noexcept;
  DocumentMask& operator=(DocumentMask&& other) noexcept;
  DocumentMask(const DocumentMask& other);
  DocumentMask& operator=(const DocumentMask& other);

  friend bool operator==(const DocumentMask& lhs, const DocumentMask& rhs);

  static DocumentMask Read(const char* buf, size_t size);

  roaring::Roaring Compress() const;

  Iterator Begin() const noexcept { return Iterator{*this}; }

  bool Contains(doc_id_t doc) const noexcept {
    return roaring::api::bitset_get(&_bits, doc - kBase);
  }

  size_t Count() const noexcept { return roaring::api::bitset_count(&_bits); }

  bool Empty() const noexcept { return roaring::api::bitset_empty(&_bits); }

  size_t ByteSize() const noexcept {
    return roaring::api::bitset_size_in_bytes(&_bits);
  }

 private:
  friend class DocumentMaskBuilder;

  static constexpr doc_id_t kBase = doc_limits::min();

  void Assign(const roaring::api::bitset_t& other);

  roaring::api::bitset_t _bits{};
};

class MaskedDocsIterator final {
 public:
  MaskedDocsIterator() = default;

  MaskedDocsIterator(const DocumentMask* mask,
                     doc_id_t uncommitted_begin) noexcept
    : _uncommitted{uncommitted_begin} {
    if (mask != nullptr && !mask->Empty()) {
      _it.emplace(mask->Begin());
    }
  }

  bool Empty() const noexcept { return !_it && doc_limits::eof(_uncommitted); }

  bool Probe(doc_id_t doc) noexcept {
    return doc >= _uncommitted || (_it && _it->Probe(doc));
  }

  doc_id_t Seek(doc_id_t target) noexcept {
    if (target >= _uncommitted) {
      return target;
    }
    if (!_it) {
      return _uncommitted;
    }
    return std::min(_it->Seek(target), _uncommitted);
  }

 private:
  std::optional<DocumentMask::Iterator> _it;
  doc_id_t _uncommitted = doc_limits::eof();
};

class DocumentMaskBuilder final {
 public:
  DocumentMaskBuilder() = default;
  explicit DocumentMaskBuilder(const DocumentMask& mask) : _mask{mask} {}

  const DocumentMask& View() const noexcept { return _mask; }

  bool Contains(doc_id_t doc) const noexcept { return _mask.Contains(doc); }
  size_t Count() const noexcept { return _mask.Count(); }
  bool Empty() const noexcept { return _mask.Empty(); }
  size_t ByteSize() const noexcept { return _mask.ByteSize(); }

  bool Add(doc_id_t doc) {
    SDB_ASSERT(doc_limits::valid(doc));
    SDB_ASSERT(!doc_limits::eof(doc));
    const bool added = !_mask.Contains(doc);
    roaring::api::bitset_set(&_mask._bits, doc - DocumentMask::kBase);
    return added;
  }

  void Add(std::span<const doc_id_t> docs);
  void AddRange(doc_id_t first, doc_id_t last);
  void Truncate(doc_id_t first) noexcept;
  void Merge(const DocumentMask& other);
  void Clear() noexcept;
  DocumentMask Build() &&;

 private:
  DocumentMask _mask;
};

}  // namespace irs
