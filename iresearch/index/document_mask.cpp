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

#include <algorithm>
#include <cstring>
#include <utility>

#include "iresearch/error/error.hpp"

namespace irs {

DocumentMask::~DocumentMask() { roaring_free(_bits.array); }

DocumentMask::DocumentMask(DocumentMask&& other) noexcept
  : _bits{std::exchange(other._bits, {})} {}

DocumentMask& DocumentMask::operator=(DocumentMask&& other) noexcept {
  if (this != &other) {
    roaring_free(_bits.array);
    _bits = std::exchange(other._bits, {});
  }
  return *this;
}

DocumentMask::DocumentMask(const DocumentMask& other) { Assign(other._bits); }

DocumentMask& DocumentMask::operator=(const DocumentMask& other) {
  if (this != &other) {
    roaring_free(_bits.array);
    _bits = {};
    Assign(other._bits);
  }
  return *this;
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
  if (common != 0 && std::memcmp(lhs._bits.array, rhs._bits.array,
                                 common * sizeof(uint64_t)) != 0) {
    return false;
  }
  const auto& tail = lhs._bits.arraysize > common ? lhs._bits : rhs._bits;
  return std::all_of(tail.array + common, tail.array + tail.arraysize,
                     [](uint64_t word) { return word == 0; });
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

    if (compressed.minimum() < kBase || doc_limits::eof(max)) [[unlikely]] {
      throw IndexError{absl::StrCat("Invalid document id in a document mask, [",
                                    compressed.minimum(), ", ", max, "]")};
    }

    const size_t words = (max - kBase) / 64 + 1;
    if (!roaring::api::bitset_grow(&mask._bits, words)) [[unlikely]] {
      throw IllegalState{"Failed to grow the document mask"};
    }

    for (const auto doc : compressed) {
      if (doc < kBase || doc > max) [[unlikely]] {
        throw IndexError{
          absl::StrCat("Invalid document id in a document mask: ", doc)};
      }
      roaring::api::bitset_set(&mask._bits, doc - kBase);
    }
  }
  return mask;
}

roaring::Roaring DocumentMask::Compress() const {
  roaring::Roaring out;
  roaring::BulkContext ctx;
  for (size_t at = 0; roaring::api::bitset_next_set_bit(&_bits, &at); ++at) {
    out.addBulk(ctx, static_cast<uint32_t>(at + kBase));
  }
  out.runOptimize();
  out.shrinkToFit();
  return out;
}

void DocumentMask::Merge(const DocumentMask& other) {
  if (!roaring::api::bitset_inplace_union(&_bits, &other._bits)) [[unlikely]] {
    throw IllegalState{"Failed to grow the document mask while merging"};
  }
}

void DocumentMask::Grow(size_t at) {
  const size_t words = at / 64 + 1;
  if (words > _bits.arraysize && !roaring::api::bitset_grow(&_bits, words))
    [[unlikely]] {
    throw IllegalState{"Failed to grow the document mask"};
  }
}

void DocumentMask::AddRange(doc_id_t first, doc_id_t last) {
  SDB_ASSERT(doc_limits::valid(first));
  SDB_ASSERT(first <= last);
  if (first == last) {
    return;
  }
  Grow(last - 1 - kBase);
  for (auto doc = first; doc != last; ++doc) {
    roaring::api::bitset_set(&_bits, doc - kBase);
  }
}

void DocumentMask::Truncate(doc_id_t first) noexcept {
  SDB_ASSERT(doc_limits::valid(first));
  const size_t at = first - kBase;
  const size_t word = at / 64;
  if (word >= _bits.arraysize) {
    return;
  }
  _bits.array[word] &= (uint64_t{1} << (at % 64)) - 1;
  std::fill(_bits.array + word + 1, _bits.array + _bits.arraysize, 0);
}

}  // namespace irs
