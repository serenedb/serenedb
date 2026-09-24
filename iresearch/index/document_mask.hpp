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

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace roaring {

class Roaring;
}

namespace irs {
namespace fill {

class DocsMask;
}
namespace probe {

class DocsMask;
}

class DocumentMask final {
 public:
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

    doc_id_t Seek(doc_id_t target) noexcept {
#ifdef SDB_DEV
      SDB_ASSERT(_prev <= target);
      _prev = target;
#endif
      if (target <= _value) {
        return _value;
      }
      if (target >= _visible_end) {
        return _value = target;
      }
      return _value = std::min(Find(target), _visible_end);
    }

   private:
    doc_id_t Find(size_t at) const noexcept {
      return _mask != nullptr &&
                 roaring::api::bitset_next_set_bit(&_mask->_bits, &at)
               ? static_cast<doc_id_t>(at)
               : doc_limits::eof();
    }

    const DocumentMask* _mask = nullptr;
    doc_id_t _value = doc_limits::invalid();
    doc_id_t _visible_end = doc_limits::eof();
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
    return roaring::api::bitset_get(&_bits, doc);
  }

  size_t Count() const noexcept { return _count; }

  bool Empty() const noexcept { return _count == 0; }

  size_t ByteSize() const noexcept {
    return roaring::api::bitset_size_in_bytes(&_bits);
  }

  size_t ByteCapacity() const noexcept {
    return _bits.capacity * sizeof(uint64_t);
  }

  bool Add(doc_id_t doc) {
    SDB_ASSERT(doc_limits::valid(doc));
    SDB_ASSERT(!doc_limits::eof(doc));
    const bool added = !Contains(doc);
    Grow(WordsFor(doc));
    roaring::api::bitset_set(&_bits, doc);
    _count += added;
    return added;
  }

  void AddRange(doc_id_t first, doc_id_t last);
  void Truncate(doc_id_t first) noexcept;
  void Merge(const DocumentMask& other);

  void Clear() noexcept {
    roaring::api::bitset_clear(&_bits);
    _count = 0;
  }

  void Trim() noexcept;

 private:
  friend class fill::DocsMask;
  friend class probe::DocsMask;

  const uint64_t* Words() const noexcept { return _bits.array; }

  size_t WordCount() const noexcept { return _bits.arraysize; }

  static constexpr size_t WordsFor(doc_id_t doc) noexcept {
    return doc / 64 + 1;
  }

  void Grow(size_t words);

  void Assign(const roaring::api::bitset_t& other);

  roaring::api::bitset_t _bits{};
  size_t _count = 0;
};

}  // namespace irs
