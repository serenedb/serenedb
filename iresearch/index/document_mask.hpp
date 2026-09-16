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
    explicit Iterator(const DocumentMask& mask) noexcept
      : _tail_begin{mask._tail_begin}, _tail_end{mask._tail_end} {
      roaring::api::roaring_iterator_init(&mask._set.roaring, &_it);
      _value = _it.has_value ? _it.current_value : TailOrEof();
    }

    doc_id_t Value() const noexcept { return _value; }

    doc_id_t Next() noexcept {
      if (doc_limits::eof(_value)) {
        return _value;
      }
      if (_value >= _tail_begin) {
        const auto next = _value + 1;
        return _value = next < _tail_end ? next : doc_limits::eof();
      }
      return _value = roaring::api::roaring_uint32_iterator_advance(&_it)
                        ? _it.current_value
                        : TailOrEof();
    }

    doc_id_t Seek(doc_id_t target) noexcept {
      SDB_ASSERT(_prev <= target);
#ifdef SDB_DEV
      _prev = target;
#endif
      if (target <= _value) {
        return _value;
      }
      if (target >= _tail_begin) {
        return _value = target < _tail_end ? target : doc_limits::eof();
      }
      return _value =
               roaring::api::roaring_uint32_iterator_move_equalorlarger(&_it,
                                                                       target)
                 ? _it.current_value
                 : TailOrEof();
    }

   private:
    doc_id_t TailOrEof() const noexcept {
      return _tail_begin < _tail_end ? _tail_begin : doc_limits::eof();
    }

    roaring::api::roaring_uint32_iterator_t _it;
    doc_id_t _value;
    doc_id_t _tail_begin;
    doc_id_t _tail_end;
    doc_id_t _prev = doc_limits::invalid();
  };

  Iterator Begin() const noexcept { return Iterator{*this}; }

  static DocumentMask Read(const char* buf, size_t size, doc_id_t tail_begin,
                           doc_id_t tail_end) {
    SDB_ASSERT(tail_begin <= tail_end);
    DocumentMask mask;
    mask._set = roaring::Roaring::readSafe(buf, size);
    mask._tail_begin = tail_begin;
    mask._tail_end = tail_end;
    return mask;
  }

  bool operator==(const DocumentMask& other) const noexcept {
    return _tail_begin == other._tail_begin && _tail_end == other._tail_end &&
           _set == other._set;
  }

  bool Contains(doc_id_t doc) const noexcept {
    return doc >= _tail_begin ? doc < _tail_end : _set.contains(doc);
  }

  size_t Count() const noexcept {
    return static_cast<size_t>(_set.cardinality()) + (_tail_end - _tail_begin);
  }

  bool Empty() const noexcept {
    return _tail_begin == _tail_end && _set.isEmpty();
  }

  doc_id_t TailBegin() const noexcept { return _tail_begin; }
  doc_id_t TailEnd() const noexcept { return _tail_end; }

  size_t ByteSize() const noexcept {
    return _set.isEmpty() ? 0 : _set.getSizeInBytes();
  }

  size_t Write(char* buf) const noexcept { return _set.write(buf); }

 private:
  friend class DocumentMaskBuilder;

  roaring::Roaring _set;
  doc_id_t _tail_begin = doc_limits::eof();
  doc_id_t _tail_end = doc_limits::eof();
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
    if (doc >= _mask._tail_begin) {
      return false;
    }
    return _mask._set.addChecked(doc);
  }

  void Add(std::span<const doc_id_t> docs) noexcept {
    SDB_ASSERT(std::ranges::all_of(docs, doc_limits::valid));
    SDB_ASSERT(_mask._tail_begin == _mask._tail_end);
    _mask._set.addMany(docs.size(), docs.data());
  }

  void AddRange(doc_id_t first, doc_id_t last) noexcept {
    SDB_ASSERT(doc_limits::valid(first));
    SDB_ASSERT(first <= last);
    SDB_ASSERT(last <= _mask._tail_begin);
    _mask._set.addRange(first, last);
  }

  void MaskTail(doc_id_t first, doc_id_t end) noexcept {
    SDB_ASSERT(doc_limits::valid(first));
    SDB_ASSERT(first <= end);
    if (first == end) {
      return;
    }
    SDB_ASSERT(_mask._tail_begin == _mask._tail_end || _mask._tail_end == end);
    _mask._set.removeRange(first, end);
    _mask._tail_begin = std::min(_mask._tail_begin, first);
    _mask._tail_end = end;
  }

  void Merge(const DocumentMask& other) noexcept {
    _mask._set |= other._set;
    if (other._tail_begin != other._tail_end) {
      SDB_ASSERT(_mask._tail_begin == _mask._tail_end ||
                 _mask._tail_end == other._tail_end);
      _mask._tail_begin = std::min(_mask._tail_begin, other._tail_begin);
      _mask._tail_end = other._tail_end;
    }
    if (_mask._tail_begin != _mask._tail_end) {
      _mask._set.removeRange(_mask._tail_begin, _mask._tail_end);
    }
  }

  void Clear() noexcept {
    _mask._set = roaring::Roaring{};
    _mask._tail_begin = doc_limits::eof();
    _mask._tail_end = doc_limits::eof();
  }

  DocumentMask Build() && noexcept {
    _mask._set.runOptimize();
    _mask._set.shrinkToFit();
    return std::move(_mask);
  }

 private:
  DocumentMask _mask;
};

}  // namespace irs
