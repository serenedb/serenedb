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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <roaring/bitset_util.h>
#include <roaring/containers/containers.h>
#include <roaring/roaring_array.h>

#include <algorithm>
#include <roaring/roaring.hh>
#include <utility>

#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs {
namespace {

using roaring::api::roaring_bitmap_t;

constexpr size_t kFrozenAlignment = 32;

constexpr size_t kSlotBytes =
  sizeof(void*) + sizeof(uint16_t) + sizeof(uint8_t);

size_t ContainerBytes(const roaring_bitmap_t& set) noexcept {
  roaring::api::roaring_statistics_t stats{};
  roaring::api::roaring_bitmap_statistics(&set, &stats);
  return stats.n_bytes_array_containers + stats.n_bytes_run_containers +
         stats.n_bytes_bitset_containers;
}

size_t ByteSizeOf(const roaring_bitmap_t& set) noexcept {
  return ContainerBytes(set) + size_t(set.high_low_container.size) * kSlotBytes;
}

size_t ByteCapacityOf(const roaring_bitmap_t& set) noexcept {
  return ContainerBytes(set) +
         size_t(set.high_low_container.allocation_size) * kSlotBytes;
}

uint64_t SpansAt(const roaring_bitmap_t& set, int32_t i) noexcept {
  const auto& ra = set.high_low_container;
  const auto* container = ra.containers[i];
  switch (ra.typecodes[i]) {
    case ARRAY_CONTAINER_TYPE:
      return static_cast<uint64_t>(
        static_cast<const roaring::internal::array_container_t*>(container)
          ->cardinality);
    case RUN_CONTAINER_TYPE:
      return static_cast<uint64_t>(
        static_cast<const roaring::internal::run_container_t*>(container)
          ->n_runs);
    case BITSET_CONTAINER_TYPE:
      return static_cast<uint64_t>(
        static_cast<const roaring::internal::bitset_container_t*>(container)
          ->cardinality);
    default:
      SDB_UNREACHABLE();
  }
}

void Densify(roaring_bitmap_t& set, uint32_t bitset_from) noexcept {
  auto& ra = set.high_low_container;
  uint64_t sparse = 0;
  uint64_t spans = 0;
  for (int32_t i = 0; i != ra.size; ++i) {
    if (ra.typecodes[i] != BITSET_CONTAINER_TYPE) {
      ++sparse;
      spans += SpansAt(set, i);
    }
  }
  if (sparse == 0 || spans < sparse * bitset_from) {
    return;
  }
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

uint32_t CountInArray(const roaring::internal::array_container_t& array,
                      uint32_t from, uint32_t to) noexcept {
  const uint16_t* begin = array.array;
  const auto* end = begin + array.cardinality;
  const auto* lower = std::lower_bound(begin, end, from);
  return static_cast<uint32_t>(std::lower_bound(lower, end, to) - lower);
}

uint32_t CountInRuns(const roaring::internal::run_container_t& run,
                     uint32_t from, uint32_t to) noexcept {
  const roaring::internal::rle16_t* begin = run.runs;
  const auto* end = begin + run.n_runs;
  const auto* it = std::lower_bound(
    begin, end, from, [](const roaring::internal::rle16_t& r, uint32_t doc) {
      return uint32_t{r.value} + r.length < doc;
    });
  uint32_t count = 0;
  for (; it != end && it->value < to; ++it) {
    count += std::min(uint32_t{it->value} + it->length + 1, to) -
             std::max(uint32_t{it->value}, from);
  }
  return count;
}

uint32_t CountInContainer(const void* container, uint8_t type, uint32_t from,
                          uint32_t to) noexcept {
  switch (type) {
    case ARRAY_CONTAINER_TYPE:
      return CountInArray(
        *static_cast<const roaring::internal::array_container_t*>(container),
        from, to);
    case RUN_CONTAINER_TYPE:
      return CountInRuns(
        *static_cast<const roaring::internal::run_container_t*>(container),
        from, to);
    default:
      SDB_ASSERT(type == BITSET_CONTAINER_TYPE);
      return static_cast<uint32_t>(
        roaring::internal::bitset_lenrange_cardinality(
          static_cast<const roaring::internal::bitset_container_t*>(container)
            ->words,
          from, to - from - 1));
  }
}

uint64_t CountInRange(const roaring_bitmap_t& set, doc_id_t first,
                      doc_id_t last) noexcept {
  const auto& ra = set.high_low_container;
  const auto high = static_cast<uint16_t>((last - 1) >> 16);
  auto i =
    roaring::internal::ra_get_index(&ra, static_cast<uint16_t>(first >> 16));
  if (i < 0) {
    i = -i - 1;
  }
  uint64_t count = 0;
  for (; i < ra.size && ra.keys[i] <= high; ++i) {
    const auto base = uint32_t{ra.keys[i]} << 16;
    const auto from = std::max(first, base) - base;
    const auto to = static_cast<uint32_t>(
      std::min(uint64_t{last}, base + DocumentMask::kChunkDocs) - base);
    count += CountInContainer(ra.containers[i], ra.typecodes[i], from, to);
  }
  return count;
}

MaskKind KindOf(const roaring_bitmap_t& set) noexcept {
  const auto& ra = set.high_low_container;
  const auto count = static_cast<uint32_t>(ra.size);
  if (count == 0) {
    return MaskKind::Runs;
  }
  const auto type = ra.typecodes[0];
  bool same = true;
  for (uint32_t i = 0; i != count; ++i) {
    SDB_ASSERT(ra.typecodes[i] != SHARED_CONTAINER_TYPE);
    same = same && ra.typecodes[i] == type;
  }
  if (!same) {
    return MaskKind::Mixed;
  }
  const bool single = count == 1;
  switch (type) {
    case BITSET_CONTAINER_TYPE:
      if (single) {
        return MaskKind::Bitset;
      }
      return uint32_t{ra.keys[count - 1]} - uint32_t{ra.keys[0]} == count - 1
               ? MaskKind::Bitsets
               : MaskKind::Mixed;
    case ARRAY_CONTAINER_TYPE:
      return single ? MaskKind::Array : MaskKind::Arrays;
    case RUN_CONTAINER_TYPE:
      return single ? MaskKind::Run : MaskKind::Runs;
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace

DocumentMask::DocumentMask(roaring_bitmap_t& set, size_t count) noexcept
  : _count{count}, _kind{KindOf(set)} {
  _set.roaring = set;
  roaring::api::roaring_bitmap_init_cleared(&set);
}

DocumentMask::DocumentMask(DocumentMask&& other) noexcept
  : _frozen{std::move(other._frozen)},
    _set{std::move(other._set)},
    _count{std::exchange(other._count, 0)},
    _kind{std::exchange(other._kind, MaskKind::Runs)} {}

DocumentMask& DocumentMask::operator=(DocumentMask&& other) noexcept {
  _frozen.swap(other._frozen);
  _set.swap(other._set);
  std::swap(_count, other._count);
  std::swap(_kind, other._kind);
  return *this;
}

bool operator==(const DocumentMask& lhs, const DocumentMask& rhs) {
  return lhs._count == rhs._count && roaring::api::roaring_bitmap_equals(
                                       &lhs._set.roaring, &rhs._set.roaring);
}

DocumentMask DocumentMask::ReadFrozen(IndexInput& in, uint64_t size) {
  DocumentMask mask;
  mask._frozen.reset(
    static_cast<byte_type*>(roaring_aligned_malloc(kFrozenAlignment, size)));
  if (!mask._frozen && size != 0) [[unlikely]] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OUT_OF_MEMORY),
      ERR_MSG("Failed to allocate a document mask of ", size, " byte(s)"));
  }
  auto* buf = mask._frozen.get();
  in.ReadData(0, buf, size);

  const auto* view = roaring::api::roaring_bitmap_frozen_view(
    reinterpret_cast<const char*>(buf), size);
  if (view == nullptr) [[unlikely]] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("Corrupted frozen document mask of ", size, " byte(s)"));
  }
  mask._set.roaring = *view;

  const auto& set = mask._set.roaring;
  mask._count = roaring::api::roaring_bitmap_get_cardinality(&set);
  if (mask._count == 0 ||
      roaring::api::roaring_bitmap_minimum(&set) < doc_limits::min() ||
      doc_limits::eof(roaring::api::roaring_bitmap_maximum(&set)))
    [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("Invalid document id in a frozen document mask "
                            "of ",
                            mask._count, " document(s)"));
  }
  mask._kind = KindOf(set);
  return mask;
}

bool DocumentMask::Contains(doc_id_t doc) const noexcept {
  return roaring::api::roaring_bitmap_contains(&_set.roaring, doc);
}

void DocumentMask::CountUpTo(std::span<const doc_id_t> docs,
                             std::span<uint64_t> counts) const noexcept {
  SDB_ASSERT(docs.size() == counts.size());
  SDB_ASSERT(std::is_sorted(docs.begin(), docs.end()));
  roaring::api::roaring_bitmap_rank_many(
    &_set.roaring, docs.data(), docs.data() + docs.size(), counts.data());
}

size_t DocumentMask::ByteSize() const noexcept {
  return ByteSizeOf(_set.roaring);
}

size_t DocumentMask::ByteCapacity() const noexcept {
  return ByteCapacityOf(_set.roaring);
}

uint64_t DocumentMask::RunsBound() const noexcept {
  uint64_t runs = 0;
  for (int32_t i = 0; i != _set.roaring.high_low_container.size; ++i) {
    runs += SpansAt(_set.roaring, i);
  }
  return runs;
}

DocumentMaskBuilder::DocumentMaskBuilder() noexcept {
  roaring::api::roaring_bitmap_init_cleared(&_set);
}

DocumentMaskBuilder::DocumentMaskBuilder(const roaring_bitmap_t& set,
                                         size_t count)
  : DocumentMaskBuilder{} {
  if (count == 0) {
    return;
  }
  if (!roaring::api::roaring_bitmap_overwrite(&_set, &set)) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("Failed to allocate a copy of the document mask"));
  }
  _count = count;
}

DocumentMaskBuilder::DocumentMaskBuilder(const DocumentMask& published)
  : DocumentMaskBuilder{published._set.roaring, published._count} {}

DocumentMaskBuilder::~DocumentMaskBuilder() {
  roaring::api::roaring_bitmap_clear(&_set);
}

DocumentMaskBuilder::DocumentMaskBuilder(DocumentMaskBuilder&& other) noexcept
  : _set{other._set}, _count{std::exchange(other._count, 0)} {
  roaring::api::roaring_bitmap_init_cleared(&other._set);
}

DocumentMaskBuilder& DocumentMaskBuilder::operator=(
  DocumentMaskBuilder&& other) noexcept {
  if (this != &other) {
    roaring::api::roaring_bitmap_clear(&_set);
    _set = other._set;
    roaring::api::roaring_bitmap_init_cleared(&other._set);
    _count = std::exchange(other._count, 0);
  }
  return *this;
}

DocumentMaskBuilder::DocumentMaskBuilder(const DocumentMaskBuilder& other)
  : DocumentMaskBuilder{other._set, other._count} {}

DocumentMaskBuilder& DocumentMaskBuilder::operator=(
  const DocumentMaskBuilder& other) {
  return *this = DocumentMaskBuilder{other};
}

bool DocumentMaskBuilder::Contains(doc_id_t doc) const noexcept {
  return roaring::api::roaring_bitmap_contains(&_set, doc);
}

size_t DocumentMaskBuilder::ByteSize() const noexcept {
  return ByteSizeOf(_set);
}

size_t DocumentMaskBuilder::ByteCapacity() const noexcept {
  return ByteCapacityOf(_set);
}

bool DocumentMaskBuilder::Add(doc_id_t doc) {
  SDB_ASSERT(doc_limits::valid(doc));
  SDB_ASSERT(!doc_limits::eof(doc));
  if (!roaring::api::roaring_bitmap_add_checked(&_set, doc)) {
    return false;
  }
  ++_count;
  return true;
}

void DocumentMaskBuilder::AddRange(doc_id_t first, doc_id_t last) {
  SDB_ASSERT(doc_limits::valid(first));
  SDB_ASSERT(first <= last);
  if (first == last) {
    return;
  }
  if (last - first == 1) {
    Add(first);
    return;
  }
  const auto present = CountInRange(_set, first, last);
  roaring::api::roaring_bitmap_add_range(&_set, first, last);
  _count += (last - first) - present;
}

void DocumentMaskBuilder::Merge(const DocumentMaskBuilder& other) {
  if (other.Empty()) {
    return;
  }
  roaring::api::roaring_bitmap_or_inplace(&_set, &other._set);
  _count = roaring::api::roaring_bitmap_get_cardinality(&_set);
}

void DocumentMaskBuilder::Merge(const DocumentMask& published) {
  if (published.Empty()) {
    return;
  }
  roaring::api::roaring_bitmap_or_inplace(&_set, &published._set.roaring);
  _count = roaring::api::roaring_bitmap_get_cardinality(&_set);
}

bool DocumentMaskBuilder::MergeLive(const DocumentMask& docs,
                                    const DocumentMask* masked, doc_id_t end) {
  if (masked == nullptr || masked->Empty()) {
    return MergeOwned(roaring::api::roaring_bitmap_copy(&docs._set.roaring),
                      end);
  }
  return MergeOwned(roaring::api::roaring_bitmap_andnot(&docs._set.roaring,
                                                        &masked->_set.roaring),
                    end);
}

bool DocumentMaskBuilder::MergeLiveRange(doc_id_t first, doc_id_t last,
                                         const DocumentMask* masked) {
  SDB_ASSERT(doc_limits::valid(first));
  if (first >= last) {
    return false;
  }
  auto* live = roaring::api::roaring_bitmap_from_range(first, last, 1);
  if (masked != nullptr && !masked->Empty()) {
    roaring::api::roaring_bitmap_andnot_inplace(live, &masked->_set.roaring);
  }
  return MergeOwned(live, last);
}

bool DocumentMaskBuilder::MergeOwned(roaring::api::roaring_bitmap_t* live,
                                     doc_id_t end) {
  if (live == nullptr) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("Failed to allocate the documents to remove"));
  }
  absl::Cleanup free = [live] { roaring::api::roaring_bitmap_free(live); };
  roaring::api::roaring_bitmap_remove_range(live, end,
                                            uint64_t{doc_limits::eof()} + 1);
  const auto before = _count;
  roaring::api::roaring_bitmap_or_inplace(&_set, live);
  _count = roaring::api::roaring_bitmap_get_cardinality(&_set);
  return _count != before;
}

void DocumentMaskBuilder::Clear() noexcept {
  roaring::api::roaring_bitmap_clear(&_set);
  _count = 0;
}

DocumentMask DocumentMaskBuilder::Finish(uint32_t bitset_from) && noexcept {
  roaring::api::roaring_bitmap_run_optimize(&_set);
  roaring::api::roaring_bitmap_shrink_to_fit(&_set);
  Densify(_set, bitset_from);
  return DocumentMask{_set, std::exchange(_count, 0)};
}

}  // namespace irs
