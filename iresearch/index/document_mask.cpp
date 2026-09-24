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

#include "iresearch/index/document_mask.hpp"

#include <absl/strings/str_cat.h>
#include <roaring/bitset_util.h>

#include <algorithm>
#include <cstring>
#include <iterator>
#include <roaring/roaring.hh>
#include <utility>

#include "iresearch/error/error.hpp"

namespace irs {

DocumentMask::~DocumentMask() { roaring_free(_bits.array); }

DocumentMask::DocumentMask(DocumentMask&& other) noexcept
  : _bits{std::exchange(other._bits, {})},
    _count{std::exchange(other._count, 0)} {}

DocumentMask& DocumentMask::operator=(DocumentMask&& other) noexcept {
  if (this != &other) {
    roaring_free(_bits.array);
    _bits = std::exchange(other._bits, {});
    _count = std::exchange(other._count, 0);
  }
  return *this;
}

DocumentMask::DocumentMask(const DocumentMask& other) : _count{other._count} {
  Assign(other._bits);
}

DocumentMask& DocumentMask::operator=(const DocumentMask& other) {
  return *this = DocumentMask{other};
}

void DocumentMask::Assign(const roaring::api::bitset_t& other) {
  if (other.arraysize == 0) {
    return;
  }
  const auto bytes = other.arraysize * sizeof(uint64_t);
  auto* array = static_cast<uint64_t*>(roaring_malloc(bytes));
  if (array == nullptr) [[unlikely]] {
    throw IllegalState{"Failed to allocate a copy of the document mask"};
  }
  std::memcpy(array, other.array, bytes);
  _bits.array = array;
  _bits.arraysize = other.arraysize;
  _bits.capacity = other.arraysize;
}

bool operator==(const DocumentMask& lhs, const DocumentMask& rhs) {
  const auto common = std::min(lhs._bits.arraysize, rhs._bits.arraysize);
  return lhs._count == rhs._count &&
         (common == 0 || std::memcmp(lhs._bits.array, rhs._bits.array,
                                     common * sizeof(uint64_t)) == 0);
}

DocumentMask DocumentMask::Read(const char* buf, size_t size) {
  const auto compressed = [&] {
    try {
      return roaring::Roaring::readSafe(buf, size);
    } catch (const std::exception& e) {
      throw IndexError{absl::StrCat("Corrupted document mask of ", size,
                                    " byte(s): ", e.what())};
    }
  }();

  DocumentMask mask;
  if (!compressed.isEmpty()) {
    const auto max = compressed.maximum();

    if (compressed.minimum() < doc_limits::min() || doc_limits::eof(max))
      [[unlikely]] {
      throw IndexError{absl::StrCat("Invalid document id in a document mask, [",
                                    compressed.minimum(), ", ", max, "]")};
    }

    if (!roaring::api::roaring_bitmap_to_bitset(&compressed.roaring,
                                                &mask._bits)) [[unlikely]] {
      throw IllegalState{"Failed to allocate the document mask"};
    }
    mask._count = compressed.cardinality();
  }
  return mask;
}

roaring::Roaring DocumentMask::Compress() const {
  roaring::Roaring out;
  size_t found[256];
  uint32_t docs[std::size(found)];
  for (size_t at = 0, n = 0; (n = roaring::api::bitset_next_set_bits(
                                &_bits, found, std::size(found), &at)) != 0;
       ++at) {
    std::copy_n(found, n, docs);
    out.addMany(n, docs);
  }
  out.runOptimize();
  out.shrinkToFit();
  return out;
}

void DocumentMask::Merge(const DocumentMask& other) {
  if (!roaring::api::bitset_inplace_union(&_bits, &other._bits)) [[unlikely]] {
    throw IllegalState{"Failed to grow the document mask while merging"};
  }
  _count = roaring::api::bitset_count(&_bits);
}

void DocumentMask::Trim() noexcept {
  if (Empty()) {
    roaring_free(_bits.array);
    _bits = {};
    return;
  }
  roaring::api::bitset_trim(&_bits);
}

void DocumentMask::Grow(size_t words) {
  if (words <= _bits.arraysize) {
    return;
  }
  if (!roaring::api::bitset_grow(&_bits, words)) [[unlikely]] {
    throw IllegalState{"Failed to grow the document mask"};
  }
}

void DocumentMask::AddRange(doc_id_t first, doc_id_t last) {
  SDB_ASSERT(doc_limits::valid(first));
  SDB_ASSERT(first <= last);
  if (first == last) {
    return;
  }
  Grow(WordsFor(last - 1));
  roaring::internal::bitset_set_range(_bits.array, first, last);
  _count = roaring::api::bitset_count(&_bits);
}

void DocumentMask::Truncate(doc_id_t first) noexcept {
  SDB_ASSERT(doc_limits::valid(first));
  const size_t word = first / 64;
  if (word >= _bits.arraysize) {
    return;
  }
  _bits.array[word] &= (uint64_t{1} << (first % 64)) - 1;
  std::fill(_bits.array + word + 1, _bits.array + _bits.arraysize, 0);
  _count = roaring::api::bitset_count(&_bits);
}

}  // namespace irs
