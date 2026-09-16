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

#include <algorithm>
#include <cstddef>
#include <optional>
#include <roaring/roaring.hh>
#include <span>
#include <utility>

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class DocumentMaskBuilder;

class DocumentMask final {
 public:
  class Iterator final {
   public:
    explicit Iterator(const DocumentMask& mask) noexcept {
      roaring::api::roaring_iterator_init(&mask._set.roaring, &_it);
      _value = _it.has_value ? _it.current_value : doc_limits::eof();
    }

    doc_id_t Value() const noexcept { return _value; }

    doc_id_t Next() noexcept {
      return _value = roaring::api::roaring_uint32_iterator_advance(&_it)
                        ? _it.current_value
                        : doc_limits::eof();
    }

    doc_id_t Seek(doc_id_t target) noexcept {
#ifdef SDB_DEV
      SDB_ASSERT(_prev <= target);
      _prev = target;
#endif
      if (target <= _value) {
        return _value;
      }
      return _value = roaring::api::roaring_uint32_iterator_move_equalorlarger(
                        &_it, target)
                        ? _it.current_value
                        : doc_limits::eof();
    }

   private:
    roaring::api::roaring_uint32_iterator_t _it;
    doc_id_t _value;
#ifdef SDB_DEV
    doc_id_t _prev = doc_limits::invalid();
#endif
  };

  Iterator Begin() const noexcept { return Iterator{*this}; }

  static DocumentMask Read(const char* buf, size_t size) {
    DocumentMask mask;
    mask._set = roaring::Roaring::readSafe(buf, size);
    return mask;
  }

  bool operator==(const DocumentMask& other) const noexcept {
    return _set == other._set;
  }

  bool Contains(doc_id_t doc) const noexcept { return _set.contains(doc); }

  size_t Count() const noexcept {
    return static_cast<size_t>(_set.cardinality());
  }

  bool Empty() const noexcept { return _set.isEmpty(); }

  size_t ByteSize() const noexcept {
    return _set.isEmpty() ? 0 : _set.getSizeInBytes();
  }

  size_t Write(char* buf) const noexcept { return _set.write(buf); }

 private:
  friend class DocumentMaskBuilder;

  roaring::Roaring _set;
};

class MaskedDocsIterator final {
 public:
  MaskedDocsIterator() = default;

  MaskedDocsIterator(const DocumentMask* mask,
                     doc_id_t uncommitted_begin) noexcept
    : _uncommitted{uncommitted_begin}, _value{uncommitted_begin} {
    if (mask == nullptr) {
      return;
    }
    _it.emplace(mask->Begin());
    _value = std::min(_it->Value(), _uncommitted);
  }

  doc_id_t UncommittedBegin() const noexcept { return _uncommitted; }

  bool Empty() const noexcept { return doc_limits::eof(_value); }

  doc_id_t Value() const noexcept { return _value; }

  doc_id_t Next() noexcept {
    if (_value >= _uncommitted) {
      return _value;
    }
    return _value = std::min(_it->Next(), _uncommitted);
  }

  doc_id_t Seek(doc_id_t target) noexcept {
    if (target <= _value) {
      return _value;
    }
    if (target >= _uncommitted) {
      return _value = target;
    }
    return _value = std::min(_it->Seek(target), _uncommitted);
  }

 private:
  std::optional<DocumentMask::Iterator> _it;
  doc_id_t _uncommitted = doc_limits::eof();
  doc_id_t _value = doc_limits::eof();
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

  bool Add(doc_id_t doc) noexcept {
    SDB_ASSERT(doc_limits::valid(doc));
    SDB_ASSERT(!doc_limits::eof(doc));
    return _mask._set.addChecked(doc);
  }

  void Add(std::span<const doc_id_t> docs) noexcept {
    SDB_ASSERT(std::ranges::all_of(docs, doc_limits::valid));
    _mask._set.addMany(docs.size(), docs.data());
  }

  void AddRange(doc_id_t first, doc_id_t last) noexcept {
    SDB_ASSERT(doc_limits::valid(first));
    SDB_ASSERT(first <= last);
    _mask._set.addRange(first, last);
  }

  void Truncate(doc_id_t first) noexcept {
    SDB_ASSERT(doc_limits::valid(first));
    _mask._set.removeRange(first, uint64_t{doc_limits::eof()} + 1);
  }

  void Merge(const DocumentMask& other) noexcept { _mask._set |= other._set; }

  void Clear() noexcept { _mask._set = roaring::Roaring{}; }

  DocumentMask Build() && noexcept {
    _mask._set.runOptimize();
    _mask._set.shrinkToFit();
    return std::move(_mask);
  }

 private:
  DocumentMask _mask;
};

}  // namespace irs
