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
#include <roaring/roaring.hh>
#include <span>

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class DocumentMask final {
 public:
  class Iterator final {
   public:
    Iterator() = default;

    Iterator(const DocumentMask* mask,
             doc_id_t uncommitted = doc_limits::eof()) noexcept
      : _bits{mask != nullptr ? &mask->_bits : nullptr},
        _uncommitted{uncommitted} {}

    bool Empty() const noexcept {
      return doc_limits::eof(_uncommitted) &&
             (_bits == nullptr || roaring::api::bitset_empty(_bits));
    }

    bool Probe(doc_id_t doc) const noexcept {
      return doc >= _uncommitted ||
             (_bits != nullptr && roaring::api::bitset_get(_bits, doc - kBase));
    }

    doc_id_t Value() const noexcept { return _value; }

    doc_id_t Next() noexcept {
      if (_value >= _uncommitted) {
        return _value;
      }
      return _value = std::min(Find(_value - kBase + 1), _uncommitted);
    }

    doc_id_t Seek(doc_id_t target) noexcept {
#ifdef SDB_DEV
      SDB_ASSERT(_prev <= target);
      _prev = target;
#endif
      if (target <= _value) {
        return _value;
      }
      if (target >= _uncommitted) {
        return _value = target;
      }
      return _value = std::min(Find(target - kBase), _uncommitted);
    }

   private:
    doc_id_t Find(size_t at) const noexcept {
      return _bits != nullptr && roaring::api::bitset_next_set_bit(_bits, &at)
               ? static_cast<doc_id_t>(at + kBase)
               : doc_limits::eof();
    }

    const roaring::api::bitset_t* _bits = nullptr;
    doc_id_t _value = doc_limits::invalid();
    doc_id_t _uncommitted = doc_limits::eof();
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

  bool Contains(doc_id_t doc) const noexcept {
    return roaring::api::bitset_get(&_bits, doc - kBase);
  }

  size_t Count() const noexcept { return roaring::api::bitset_count(&_bits); }

  bool Empty() const noexcept { return roaring::api::bitset_empty(&_bits); }

  size_t ByteSize() const noexcept {
    return roaring::api::bitset_size_in_bytes(&_bits);
  }

  bool Add(doc_id_t doc) {
    SDB_ASSERT(doc_limits::valid(doc));
    SDB_ASSERT(!doc_limits::eof(doc));
    const bool added = !Contains(doc);
    roaring::api::bitset_set(&_bits, doc - kBase);
    return added;
  }

  void Add(std::span<const doc_id_t> docs);
  void AddRange(doc_id_t first, doc_id_t last);
  void Truncate(doc_id_t first) noexcept;
  void Merge(const DocumentMask& other) {
    roaring::api::bitset_inplace_union(&_bits, &other._bits);
  }

  void Clear() noexcept { roaring::api::bitset_clear(&_bits); }
  void Trim() noexcept { roaring::api::bitset_trim(&_bits); }

 private:
  static constexpr doc_id_t kBase = doc_limits::min();

  void Assign(const roaring::api::bitset_t& other);

  roaring::api::bitset_t _bits{};
};

}  // namespace irs
