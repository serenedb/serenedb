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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <roaring/containers/containers.h>

#include <algorithm>
#include <new>
#include <roaring/roaring.hh>
#include <utility>

#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs {
namespace {

using roaring::api::roaring_bitmap_t;

constexpr std::align_val_t kFrozenAlignment{32};

struct FrozenFree {
  void operator()(byte_type* p) const noexcept {
    ::operator delete(p, kFrozenAlignment);
  }
};

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

bool Densifies(const roaring_bitmap_t& set, uint32_t bitset_from) noexcept {
  const auto& ra = set.high_low_container;
  uint64_t sparse = 0;
  uint64_t spans = 0;
  for (int32_t i = 0; i != ra.size; ++i) {
    if (ra.typecodes[i] != BITSET_CONTAINER_TYPE) {
      ++sparse;
      spans += SpansAt(set, i);
    }
  }
  return sparse != 0 && spans >= sparse * bitset_from;
}

roaring::internal::bitset_container_t* ToBitset(const roaring_bitmap_t& set,
                                                int32_t i) noexcept {
  const auto& ra = set.high_low_container;
  switch (ra.typecodes[i]) {
    case ARRAY_CONTAINER_TYPE:
      return roaring::internal::bitset_container_from_array(
        static_cast<const roaring::internal::array_container_t*>(
          ra.containers[i]));
    case RUN_CONTAINER_TYPE:
      return roaring::internal::bitset_container_from_run(
        static_cast<const roaring::internal::run_container_t*>(
          ra.containers[i]));
    default:
      return nullptr;
  }
}

void Densify(roaring_bitmap_t& set, uint32_t bitset_from) noexcept {
  if (!Densifies(set, bitset_from)) {
    return;
  }
  auto& ra = set.high_low_container;
  for (int32_t i = 0; i != ra.size; ++i) {
    if (auto* bits = ToBitset(set, i)) {
      roaring::internal::container_free(ra.containers[i], ra.typecodes[i]);
      ra.containers[i] = bits;
      ra.typecodes[i] = BITSET_CONTAINER_TYPE;
    }
  }
}

using roaring::internal::container_t;

struct ContainerFree {
  uint8_t type;

  void operator()(const void* container) const noexcept {
    roaring::internal::container_free(
      static_cast<container_t*>(const_cast<void*>(container)), type);
  }
};

std::shared_ptr<const void> Own(container_t* container, uint8_t type) {
  if (container == nullptr) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("Failed to allocate a document mask container"));
  }
  return {container, ContainerFree{type}};
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

DocumentMask::DocumentMask() noexcept {
  roaring::api::roaring_bitmap_init_cleared(&_set);
}

DocumentMask::DocumentMask(roaring_bitmap_t& set, size_t count) noexcept
  : _set{set}, _count{count}, _kind{KindOf(set)} {
  roaring::api::roaring_bitmap_init_cleared(&set);
}

DocumentMask::DocumentMask(DocumentMask&& other) noexcept
  : _frozen{std::move(other._frozen)},
    _view{std::move(other._view)},
    _set{other._set},
    _owners{std::move(other._owners)},
    _count{std::exchange(other._count, 0)},
    _kind{std::exchange(other._kind, MaskKind::Runs)} {
  roaring::api::roaring_bitmap_init_cleared(&other._set);
}

DocumentMask& DocumentMask::operator=(DocumentMask&& other) noexcept {
  _frozen.swap(other._frozen);
  _view.swap(other._view);
  std::swap(_set, other._set);
  _owners.swap(other._owners);
  std::swap(_count, other._count);
  std::swap(_kind, other._kind);
  return *this;
}

DocumentMask::~DocumentMask() {
  if (_view) {
    return;
  }
  if (_owners.empty()) {
    roaring::internal::ra_clear(&_set.high_low_container);
  } else {
    roaring::internal::ra_clear_without_containers(&_set.high_low_container);
  }
}

bool operator==(const DocumentMask& lhs, const DocumentMask& rhs) {
  return lhs._count == rhs._count &&
         roaring::api::roaring_bitmap_equals(&lhs._set, &rhs._set);
}

DocumentMask DocumentMask::ReadFrozen(IndexInput& in, uint64_t size) {
  std::shared_ptr<byte_type> frozen{
    static_cast<byte_type*>(::operator new(size, kFrozenAlignment)),
    FrozenFree{}};
  in.ReadData(0, frozen.get(), size);
  return DocumentMask{std::move(frozen), size};
}

DocumentMask::DocumentMask(std::shared_ptr<const byte_type> frozen,
                           uint64_t size)
  : _frozen{std::move(frozen)} {
  const auto* view = roaring::api::roaring_bitmap_frozen_view(
    reinterpret_cast<const char*>(_frozen.get()), size);
  if (view == nullptr) [[unlikely]] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INDEX_CORRUPTED),
      ERR_MSG("Corrupted frozen document mask of ", size, " byte(s)"));
  }
  _view.reset(view);
  _set = *view;

  const auto& set = _set;
  _count = roaring::api::roaring_bitmap_get_cardinality(&set);
  if (_count == 0 ||
      roaring::api::roaring_bitmap_minimum(&set) < doc_limits::min() ||
      doc_limits::eof(roaring::api::roaring_bitmap_maximum(&set)))
    [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("Invalid document id in a frozen document mask "
                            "of ",
                            _count, " document(s)"));
  }
  _kind = KindOf(set);
}

DocumentMask DocumentMask::Union(
  const std::shared_ptr<const DocumentMask>& base, const DocumentMask& added) {
  SDB_ASSERT(base);
  const auto& from = base->_set.high_low_container;
  const auto& with = added._set.high_low_container;
  DocumentMask out;
  auto& ra = out._set.high_low_container;
  const auto capacity = static_cast<uint32_t>(from.size + with.size);
  if (!roaring::internal::ra_init_with_capacity(&ra, capacity)) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("Failed to allocate a document mask"));
  }
  out._owners.reserve(capacity);
  const auto append = [&](uint16_t key, container_t* container, uint8_t type,
                          std::shared_ptr<const void> owner) {
    out._owners.push_back(std::move(owner));
    roaring::internal::ra_append(&ra, key, container, type);
  };
  auto count = static_cast<int64_t>(base->_count);
  int32_t i = 0;
  int32_t j = 0;
  while (i != from.size || j != with.size) {
    if (j == with.size || (i != from.size && from.keys[i] < with.keys[j])) {
      auto* shared = from.containers[i];
      append(from.keys[i], shared, from.typecodes[i],
             base->_owners.empty() ? std::shared_ptr<const void>{base, shared}
                                   : base->_owners[i]);
      ++i;
      continue;
    }
    uint8_t type = with.typecodes[j];
    container_t* container = nullptr;
    if (i == from.size || with.keys[j] < from.keys[i]) {
      container = roaring::internal::container_clone(with.containers[j], type);
    } else {
      count -= roaring::internal::container_get_cardinality(from.containers[i],
                                                            from.typecodes[i]);
      container = roaring::internal::container_or(
        from.containers[i], from.typecodes[i], with.containers[j], type, &type);
      ++i;
    }
    if (container == nullptr) [[unlikely]] {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                      ERR_MSG("Failed to allocate a document mask container"));
    }
    container = roaring::internal::convert_run_optimize(container, type, &type);
    roaring::internal::container_shrink_to_fit(container, type);
    count += roaring::internal::container_get_cardinality(container, type);
    append(with.keys[j], container, type, Own(container, type));
    ++j;
  }
  out._count = static_cast<size_t>(count);
  if (Densifies(out._set, DocumentMaskBuilder::kBitsetFrom)) {
    for (int32_t k = 0; k != ra.size; ++k) {
      if (auto* bits = ToBitset(out._set, k)) {
        out._owners[k] = Own(bits, BITSET_CONTAINER_TYPE);
        ra.containers[k] = bits;
        ra.typecodes[k] = BITSET_CONTAINER_TYPE;
      }
    }
  }
  out._kind = KindOf(out._set);
  return out;
}

bool DocumentMask::Contains(doc_id_t doc) const noexcept {
  return roaring::api::roaring_bitmap_contains(&_set, doc);
}

uint64_t DocumentMask::CountInRange(doc_id_t first,
                                    doc_id_t last) const noexcept {
  return roaring::api::roaring_bitmap_range_cardinality(&_set, first, last);
}

size_t DocumentMask::ByteSize() const noexcept { return ByteSizeOf(_set); }

size_t DocumentMask::ByteCapacity() const noexcept {
  return ByteCapacityOf(_set);
}

uint64_t DocumentMask::RunsBound() const noexcept {
  uint64_t runs = 0;
  for (int32_t i = 0; i != _set.high_low_container.size; ++i) {
    runs += SpansAt(_set, i);
  }
  return runs;
}

DocumentMaskBuilder::DocumentMaskBuilder() noexcept {
  roaring::api::roaring_bitmap_init_cleared(&_set);
}

DocumentMaskBuilder::DocumentMaskBuilder(const roaring_bitmap_t& set)
  : DocumentMaskBuilder{} {
  if (roaring::api::roaring_bitmap_is_empty(&set)) {
    return;
  }
  if (!roaring::api::roaring_bitmap_overwrite(&_set, &set)) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("Failed to allocate a copy of the document mask"));
  }
}

DocumentMaskBuilder::DocumentMaskBuilder(const DocumentMask& published)
  : DocumentMaskBuilder{published._set} {}

DocumentMaskBuilder::~DocumentMaskBuilder() {
  roaring::api::roaring_bitmap_clear(&_set);
}

DocumentMaskBuilder::DocumentMaskBuilder(DocumentMaskBuilder&& other) noexcept
  : _set{other._set} {
  roaring::api::roaring_bitmap_init_cleared(&other._set);
}

DocumentMaskBuilder& DocumentMaskBuilder::operator=(
  DocumentMaskBuilder&& other) noexcept {
  if (this != &other) {
    roaring::api::roaring_bitmap_clear(&_set);
    _set = other._set;
    roaring::api::roaring_bitmap_init_cleared(&other._set);
  }
  return *this;
}

DocumentMaskBuilder::DocumentMaskBuilder(const DocumentMaskBuilder& other)
  : DocumentMaskBuilder{other._set} {}

DocumentMaskBuilder& DocumentMaskBuilder::operator=(
  const DocumentMaskBuilder& other) {
  return *this = DocumentMaskBuilder{other};
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
  return roaring::api::roaring_bitmap_add_checked(&_set, doc);
}

void DocumentMaskBuilder::AddRange(doc_id_t first, doc_id_t last) {
  SDB_ASSERT(doc_limits::valid(first));
  SDB_ASSERT(first <= last);
  roaring::api::roaring_bitmap_add_range(&_set, first, last);
}

void DocumentMaskBuilder::AddSorted(std::span<const doc_id_t> docs) {
  SDB_ASSERT(absl::c_is_sorted(docs));
  roaring::api::roaring_bitmap_add_many(&_set, docs.size(), docs.data());
}

void DocumentMaskBuilder::AddShifted(const DocumentMask& mask,
                                     doc_id_t offset) {
  AddShifted(mask._set, offset);
}

void DocumentMaskBuilder::AddShifted(const DocumentMaskBuilder& other,
                                     doc_id_t offset) {
  AddShifted(other._set, offset);
}

void DocumentMaskBuilder::AddShifted(const roaring_bitmap_t& set,
                                     doc_id_t offset) {
  if (roaring::api::roaring_bitmap_is_empty(&set)) {
    return;
  }
  if (offset == 0) {
    roaring::api::roaring_bitmap_or_inplace(&_set, &set);
    return;
  }
  std::unique_ptr<roaring_bitmap_t,
                  decltype(&roaring::api::roaring_bitmap_free)>
    shifted{roaring::api::roaring_bitmap_add_offset(&set, offset),
            &roaring::api::roaring_bitmap_free};
  if (!shifted) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("Failed to allocate a shifted document mask"));
  }
  roaring::api::roaring_bitmap_or_inplace(&_set, shifted.get());
}

void DocumentMaskBuilder::Merge(const DocumentMaskBuilder& other) {
  roaring::api::roaring_bitmap_or_inplace(&_set, &other._set);
}

void DocumentMaskBuilder::Merge(const DocumentMask& published) {
  roaring::api::roaring_bitmap_or_inplace(&_set, &published._set);
}

void DocumentMaskBuilder::Subtract(const DocumentMask& mask) {
  roaring::api::roaring_bitmap_andnot_inplace(&_set, &mask._set);
}

void DocumentMaskBuilder::Clear() noexcept {
  roaring::api::roaring_bitmap_clear(&_set);
}

DocumentMask DocumentMaskBuilder::Finish(uint32_t bitset_from) && noexcept {
  roaring::api::roaring_bitmap_run_optimize(&_set);
  roaring::api::roaring_bitmap_shrink_to_fit(&_set);
  Densify(_set, bitset_from);
  const auto count = roaring::api::roaring_bitmap_get_cardinality(&_set);
  return DocumentMask{_set, count};
}

DocumentMask DocumentMaskBuilder::Take() && noexcept {
  const auto count = roaring::api::roaring_bitmap_get_cardinality(&_set);
  return DocumentMask{_set, count};
}

}  // namespace irs
