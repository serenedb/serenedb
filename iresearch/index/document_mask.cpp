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
#include <roaring/containers/containers.h>

#include <algorithm>
#include <roaring/roaring.hh>
#include <utility>

#include "iresearch/error/error.hpp"

namespace irs {
namespace {

constexpr size_t kSlotBytes =
  sizeof(void*) + sizeof(uint16_t) + sizeof(uint8_t);

size_t ContainerBytes(const roaring::api::roaring_bitmap_t& set) noexcept {
  roaring::api::roaring_statistics_t stats{};
  roaring::api::roaring_bitmap_statistics(&set, &stats);
  return stats.n_bytes_array_containers + stats.n_bytes_run_containers +
         stats.n_bytes_bitset_containers;
}

}  // namespace

DocumentMask::DocumentMask() noexcept {
  roaring::api::roaring_bitmap_init_cleared(&_set);
}

DocumentMask::~DocumentMask() { roaring::api::roaring_bitmap_clear(&_set); }

DocumentMask::DocumentMask(DocumentMask&& other) noexcept
  : _set{other._set},
    _count{std::exchange(other._count, 0)},
    _kind{std::exchange(other._kind, MaskKind::Runs)} {
  roaring::api::roaring_bitmap_init_cleared(&other._set);
}

DocumentMask& DocumentMask::operator=(DocumentMask&& other) noexcept {
  if (this != &other) {
    roaring::api::roaring_bitmap_clear(&_set);
    _set = other._set;
    roaring::api::roaring_bitmap_init_cleared(&other._set);
    _count = std::exchange(other._count, 0);
    _kind = std::exchange(other._kind, MaskKind::Runs);
  }
  return *this;
}

DocumentMask::DocumentMask(const DocumentMask& other) : DocumentMask{} {
  if (other.Empty()) {
    return;
  }
  if (!roaring::api::roaring_bitmap_overwrite(&_set, &other._set))
    [[unlikely]] {
    throw IllegalState{"Failed to allocate a copy of the document mask"};
  }
  _count = other._count;
  _kind = other._kind;
}

DocumentMask& DocumentMask::operator=(const DocumentMask& other) {
  return *this = DocumentMask{other};
}

bool operator==(const DocumentMask& lhs, const DocumentMask& rhs) {
  return lhs._count == rhs._count &&
         roaring::api::roaring_bitmap_equals(&lhs._set, &rhs._set);
}

DocumentMask DocumentMask::Read(const char* buf, size_t size) {
  auto compressed = [&] {
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

    mask._count = compressed.cardinality();
    mask._set = compressed.roaring;
    roaring::api::roaring_bitmap_init_cleared(&compressed.roaring);
  }
  mask.Refresh();
  return mask;
}

roaring::Roaring DocumentMask::Compress() const {
  roaring::Roaring out;
  if (!roaring::api::roaring_bitmap_overwrite(&out.roaring, &_set))
    [[unlikely]] {
    throw IllegalState{"Failed to allocate a copy of the document mask"};
  }
  roaring::api::roaring_bitmap_repair_after_lazy(&out.roaring);
  out.runOptimize();
  out.shrinkToFit();
  return out;
}

bool DocumentMask::Contains(doc_id_t doc) const noexcept {
  return roaring::api::roaring_bitmap_contains(&_set, doc);
}

size_t DocumentMask::ByteSize() const noexcept {
  return ContainerBytes(_set) +
         size_t(_set.high_low_container.size) * kSlotBytes;
}

size_t DocumentMask::ByteCapacity() const noexcept {
  return ContainerBytes(_set) +
         size_t(_set.high_low_container.allocation_size) * kSlotBytes;
}

uint64_t DocumentMask::SpansAt(uint32_t i) const noexcept {
  const auto* container = ContainerAt(i);
  switch (TypeAt(i)) {
    case ARRAY_CONTAINER_TYPE:
      return static_cast<uint64_t>(
        static_cast<const roaring::internal::array_container_t*>(container)
          ->cardinality);
    case RUN_CONTAINER_TYPE:
      return static_cast<uint64_t>(
        static_cast<const roaring::internal::run_container_t*>(container)
          ->n_runs);
    default:
      return static_cast<uint64_t>(
        static_cast<const roaring::internal::bitset_container_t*>(container)
          ->cardinality);
  }
}

uint64_t DocumentMask::RunsBound() const noexcept {
  uint64_t runs = 0;
  for (uint32_t i = 0, count = ContainerCount(); i != count; ++i) {
    runs += SpansAt(i);
  }
  return runs;
}

bool DocumentMask::Add(doc_id_t doc) {
  SDB_ASSERT(doc_limits::valid(doc));
  SDB_ASSERT(!doc_limits::eof(doc));
  const auto& ra = _set.high_low_container;
  const auto size = ra.size;
  const auto key = static_cast<uint16_t>(doc >> kChunkShift);
  const auto at = static_cast<int32_t>(
    std::lower_bound(ra.keys, ra.keys + size, key) - ra.keys);
  const uint8_t type =
    at != size && ra.keys[at] == key ? ra.typecodes[at] : uint8_t{0};
  if (!roaring::api::roaring_bitmap_add_checked(&_set, doc)) {
    return false;
  }
  ++_count;
  if (ra.size != size || ra.typecodes[at] != type) {
    Refresh();
  }
  return true;
}

void DocumentMask::AddRange(doc_id_t first, doc_id_t last) {
  SDB_ASSERT(doc_limits::valid(first));
  SDB_ASSERT(first <= last);
  if (first == last) {
    return;
  }
  roaring::api::roaring_bitmap_add_range(&_set, first, last);
  _count = roaring::api::roaring_bitmap_get_cardinality(&_set);
  Refresh();
}

void DocumentMask::Truncate(doc_id_t first) noexcept {
  SDB_ASSERT(doc_limits::valid(first));
  roaring::api::roaring_bitmap_remove_range(&_set, first, uint64_t{1} << 32);
  _count = roaring::api::roaring_bitmap_get_cardinality(&_set);
  Refresh();
}

void DocumentMask::Merge(const DocumentMask& other) {
  if (other.Empty()) {
    return;
  }
  roaring::api::roaring_bitmap_or_inplace(&_set, &other._set);
  _count = roaring::api::roaring_bitmap_get_cardinality(&_set);
  Refresh();
}

void DocumentMask::Clear() noexcept {
  roaring::api::roaring_bitmap_clear(&_set);
  _count = 0;
  Refresh();
}

void DocumentMask::Trim(uint32_t bitset_from) noexcept {
  roaring::api::roaring_bitmap_run_optimize(&_set);
  roaring::api::roaring_bitmap_shrink_to_fit(&_set);
  auto& ra = _set.high_low_container;
  uint64_t sparse = 0;
  uint64_t spans = 0;
  for (uint32_t i = 0, count = ContainerCount(); i != count; ++i) {
    if (TypeAt(i) != BITSET_CONTAINER_TYPE) {
      ++sparse;
      spans += SpansAt(i);
    }
  }
  if (sparse != 0 && spans >= sparse * bitset_from) {
    for (int32_t i = 0; i != ra.size; ++i) {
      roaring::internal::bitset_container_t* bits = nullptr;
      if (ra.typecodes[i] == ARRAY_CONTAINER_TYPE) {
        auto* array =
          static_cast<roaring::internal::array_container_t*>(ra.containers[i]);
        bits = roaring::internal::bitset_container_from_array(array);
        if (bits != nullptr) [[likely]] {
          roaring::internal::array_container_free(array);
        }
      } else if (ra.typecodes[i] == RUN_CONTAINER_TYPE) {
        auto* runs =
          static_cast<roaring::internal::run_container_t*>(ra.containers[i]);
        bits = roaring::internal::bitset_container_from_run(runs);
        if (bits != nullptr) [[likely]] {
          roaring::internal::run_container_free(runs);
        }
      }
      if (bits != nullptr) {
        ra.containers[i] = bits;
        ra.typecodes[i] = BITSET_CONTAINER_TYPE;
      }
    }
  }
  Refresh();
}

void DocumentMask::Refresh() noexcept {
  const auto& ra = _set.high_low_container;
  const auto count = static_cast<uint32_t>(ra.size);
  if (count == 0) {
    _kind = MaskKind::Runs;
    return;
  }
  const auto type = ra.typecodes[0];
  bool same = true;
  for (uint32_t i = 0; i != count; ++i) {
    SDB_ASSERT(ra.typecodes[i] != SHARED_CONTAINER_TYPE);
    same = same && ra.typecodes[i] == type;
  }
  if (!same) {
    _kind = MaskKind::Mixed;
    return;
  }
  switch (type) {
    case BITSET_CONTAINER_TYPE:
      _kind = uint32_t{ra.keys[count - 1]} - uint32_t{ra.keys[0]} == count - 1
                ? MaskKind::Bitsets
                : MaskKind::Mixed;
      return;
    case ARRAY_CONTAINER_TYPE:
      _kind = MaskKind::Arrays;
      return;
    default:
      SDB_ASSERT(type == RUN_CONTAINER_TYPE);
      _kind = MaskKind::Runs;
      return;
  }
}

}  // namespace irs
