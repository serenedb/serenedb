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

uint64_t SpansOf(const void* container, uint8_t type) noexcept {
  switch (type) {
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

uint64_t SpansAt(const roaring_bitmap_t& set, int32_t i) noexcept {
  const auto& ra = set.high_low_container;
  return SpansOf(ra.containers[i], ra.typecodes[i]);
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

using OwnedContainer = std::unique_ptr<container_t, ContainerFree>;

struct UnitedContainer {
  OwnedContainer container;
  uint8_t type = 0;
  int64_t count = 0;
  int64_t replaced = 0;
  uint64_t spans = 0;
  bool sparse = false;
  bool to_run = false;
};

UnitedContainer Unite(const roaring::api::roaring_array_t& from, int32_t i,
                      const roaring::api::roaring_array_t& with, int32_t j) {
  UnitedContainer united;
  uint8_t type = with.typecodes[j];
  container_t* container = nullptr;
  if (i < 0) {
    container = roaring::internal::container_clone(with.containers[j], type);
  } else {
    united.replaced = roaring::internal::container_get_cardinality(
      from.containers[i], from.typecodes[i]);
    container = roaring::internal::container_or(
      from.containers[i], from.typecodes[i], with.containers[j], type, &type);
  }
  if (container == nullptr) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("Failed to allocate a document mask container"));
  }
  if (type == BITSET_CONTAINER_TYPE) {
    united.container = OwnedContainer{container, ContainerFree{type}};
    united.type = type;
    auto* bits = static_cast<roaring::internal::bitset_container_t*>(container);
    united.count = bits->cardinality;
    const auto runs = roaring::internal::bitset_container_number_of_runs(bits);
    if (roaring::internal::run_container_serialized_size_in_bytes(runs) <
        roaring::internal::bitset_container_serialized_size_in_bytes()) {
      united.sparse = true;
      united.spans = static_cast<uint64_t>(runs);
      united.to_run = true;
    }
    return united;
  }
  container = roaring::internal::convert_run_optimize(container, type, &type);
  roaring::internal::container_shrink_to_fit(container, type);
  united.container = OwnedContainer{container, ContainerFree{type}};
  united.type = type;
  united.count = roaring::internal::container_get_cardinality(container, type);
  if (type != BITSET_CONTAINER_TYPE) {
    united.sparse = true;
    united.spans = SpansOf(container, type);
  }
  return united;
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

struct BitmapFree {
  void operator()(roaring_bitmap_t* set) const noexcept {
    roaring::api::roaring_bitmap_free(set);
  }
};

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
  const std::shared_ptr<const DocumentMask>& base, const DocumentMask& added,
  ParallelFor parallel) {
  SDB_ASSERT(base);
  static constexpr size_t kBlock = 16;
  const auto& from = base->_set.high_low_container;
  const auto& with = added._set.high_low_container;
  struct Step {
    uint16_t key = 0;
    int32_t from = -1;
    int32_t with = -1;
  };
  std::vector<Step> steps;
  steps.reserve(static_cast<size_t>(from.size + with.size));
  std::vector<size_t> touched;
  touched.reserve(static_cast<size_t>(with.size));
  for (int32_t i = 0, j = 0; i != from.size || j != with.size;) {
    if (j == with.size || (i != from.size && from.keys[i] < with.keys[j])) {
      steps.push_back({.key = from.keys[i], .from = i++});
      continue;
    }
    touched.push_back(steps.size());
    const bool both = i != from.size && from.keys[i] == with.keys[j];
    steps.push_back({.key = with.keys[j], .from = both ? i++ : -1, .with = j});
    ++j;
  }
  std::vector<UnitedContainer> united(touched.size());
  parallel((touched.size() + kBlock - 1) / kBlock, [&](size_t block) {
    const auto end = std::min(touched.size(), (block + 1) * kBlock);
    for (auto t = block * kBlock; t != end; ++t) {
      const auto& step = steps[touched[t]];
      united[t] = Unite(from, step.from, with, step.with);
    }
  });
  DocumentMask out;
  auto& ra = out._set.high_low_container;
  const auto capacity = static_cast<uint32_t>(steps.size());
  if (!roaring::internal::ra_init_with_capacity(&ra, capacity)) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("Failed to allocate a document mask"));
  }
  out._owners.reserve(capacity);
  auto count = static_cast<int64_t>(base->_count);
  uint64_t sparse = 0;
  uint64_t spans = 0;
  std::vector<std::pair<int32_t, size_t>> to_runs;
  for (size_t t = 0; const auto& step : steps) {
    if (step.with < 0) {
      const auto i = step.from;
      if (from.typecodes[i] != BITSET_CONTAINER_TYPE) {
        ++sparse;
        spans += SpansAt(base->_set, i);
      }
      auto* shared = from.containers[i];
      out._owners.push_back(base->_owners.empty()
                              ? std::shared_ptr<const void>{base, shared}
                              : base->_owners[i]);
      roaring::internal::ra_append(&ra, step.key, shared, from.typecodes[i]);
      continue;
    }
    auto& made = united[t++];
    count += made.count - made.replaced;
    if (made.sparse) {
      ++sparse;
      spans += made.spans;
    }
    auto* container = made.container.get();
    if (made.to_run) {
      to_runs.emplace_back(ra.size, t - 1);
      out._owners.emplace_back();
    } else {
      out._owners.emplace_back(std::move(made.container));
    }
    roaring::internal::ra_append(&ra, step.key, container, made.type);
  }
  out._count = static_cast<size_t>(count);
  if (sparse != 0 && spans >= sparse * DocumentMaskBuilder::kBitsetFrom) {
    for (const auto& [k, t] : to_runs) {
      out._owners[k] =
        std::shared_ptr<const void>{std::move(united[t].container)};
    }
    std::vector<OwnedContainer> dense(static_cast<size_t>(ra.size));
    parallel((dense.size() + kBlock - 1) / kBlock, [&](size_t block) {
      const auto end = std::min(dense.size(), (block + 1) * kBlock);
      for (auto k = block * kBlock; k != end; ++k) {
        dense[k] = OwnedContainer{ToBitset(out._set, static_cast<int32_t>(k)),
                                  ContainerFree{BITSET_CONTAINER_TYPE}};
      }
    });
    for (size_t k = 0; k != dense.size(); ++k) {
      if (auto* bits = dense[k].get()) {
        out._owners[k] = std::shared_ptr<const void>{std::move(dense[k])};
        ra.containers[k] = bits;
        ra.typecodes[k] = BITSET_CONTAINER_TYPE;
      }
    }
  } else {
    for (const auto& [k, t] : to_runs) {
      uint8_t type = BITSET_CONTAINER_TYPE;
      auto* run = roaring::internal::convert_run_optimize(
        united[t].container.release(), type, &type);
      roaring::internal::container_shrink_to_fit(run, type);
      ra.containers[k] = run;
      ra.typecodes[k] = type;
      out._owners[k] = Own(run, type);
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
  auto& ra = _set.high_low_container;
  while (!docs.empty()) {
    const auto key = static_cast<uint16_t>(docs.front() >> 16);
    const auto values = docs.first(static_cast<size_t>(
      absl::c_upper_bound(docs, (uint32_t{key} << 16) | 0xFFFF) -
      docs.begin()));
    docs = docs.subspan(values.size());
    if (ra.size != 0 && ra.keys[ra.size - 1] >= key) {
      roaring::api::roaring_bitmap_add_many(&_set, values.size(),
                                            values.data());
      continue;
    }
    if (values.size() > roaring::internal::DEFAULT_MAX_SIZE) {
      auto bits = NewBitset();
      for (const auto doc : values) {
        bits->words[(doc & 0xFFFF) >> 6] |= uint64_t{1} << (doc & 63);
      }
      AppendBitset(key, std::move(bits));
      continue;
    }
    auto* array = roaring::internal::array_container_create_given_capacity(
      static_cast<int32_t>(values.size()));
    if (!array) [[unlikely]] {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                      ERR_MSG("Failed to allocate a document mask container"));
    }
    int32_t count = 0;
    for (const auto doc : values) {
      const auto low = static_cast<uint16_t>(doc);
      if (count == 0 || array->array[count - 1] != low) {
        array->array[count++] = low;
      }
    }
    array->cardinality = count;
    roaring::internal::ra_append(&ra, key, array, ARRAY_CONTAINER_TYPE);
  }
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
  std::unique_ptr<roaring_bitmap_t, BitmapFree> shifted{
    roaring::api::roaring_bitmap_add_offset(&set, offset)};
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

std::vector<DocumentMaskBuilder> DocumentMaskBuilder::Union(
  std::span<std::vector<DocumentMaskBuilder>> groups, ParallelFor parallel) {
  struct Entry {
    uint16_t key = 0;
    uint8_t type = 0;
    container_t* container = nullptr;
  };
  struct Run {
    size_t group = 0;
    size_t begin = 0;
    size_t end = 0;
  };
  std::vector<DocumentMaskBuilder> out(groups.size());
  std::vector<std::vector<Entry>> entries(groups.size());
  std::vector<Run> runs;
  for (size_t g = 0; g != groups.size(); ++g) {
    auto& parts = groups[g];
    if (parts.size() == 1) {
      out[g] = std::move(parts.front());
      continue;
    }
    auto& list = entries[g];
    size_t total = 0;
    for (const auto& part : parts) {
      total += static_cast<size_t>(part._set.high_low_container.size);
    }
    list.reserve(total);
    for (auto& part : parts) {
      auto& ra = part._set.high_low_container;
      for (int32_t i = 0; i != ra.size; ++i) {
        list.push_back({.key = ra.keys[i],
                        .type = ra.typecodes[i],
                        .container = ra.containers[i]});
      }
      roaring::internal::ra_clear_without_containers(&ra);
    }
    absl::c_sort(list, [](const Entry& lhs, const Entry& rhs) {
      return lhs.key < rhs.key;
    });
    for (size_t i = 0; i != list.size();) {
      auto j = i + 1;
      while (j != list.size() && list[j].key == list[i].key) {
        ++j;
      }
      runs.push_back({.group = g, .begin = i, .end = j});
      i = j;
    }
  }
  constexpr size_t kRunsPerTask = 64;
  parallel((runs.size() + kRunsPerTask - 1) / kRunsPerTask, [&](size_t task) {
    const auto first = task * kRunsPerTask;
    const auto last = std::min(runs.size(), first + kRunsPerTask);
    for (const auto& run : std::span{runs}.subspan(first, last - first)) {
      auto& list = entries[run.group];
      auto& head = list[run.begin];
      for (auto k = run.begin + 1; k != run.end; ++k) {
        uint8_t merged_type = 0;
        auto* merged = roaring::internal::container_ior(
          head.container, head.type, list[k].container, list[k].type,
          &merged_type);
        if (merged != head.container) {
          roaring::internal::container_free(head.container, head.type);
        }
        roaring::internal::container_free(list[k].container, list[k].type);
        head.container = merged;
        head.type = merged_type;
      }
    }
  });
  for (const auto& run : runs) {
    const auto& head = entries[run.group][run.begin];
    roaring::internal::ra_append(&out[run.group]._set.high_low_container,
                                 head.key, head.container, head.type);
  }
  return out;
}

void DocumentMaskBuilder::Subtract(const DocumentMask& mask) {
  roaring::api::roaring_bitmap_andnot_inplace(&_set, &mask._set);
}

void DocumentMaskBuilder::BitsetFree::operator()(Bitset* bits) const noexcept {
  roaring::internal::bitset_container_free(bits);
}

DocumentMaskBuilder::BitsetPtr DocumentMaskBuilder::NewBitset() {
  BitsetPtr bits{roaring::internal::bitset_container_create()};
  if (!bits) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("Failed to allocate a document mask container"));
  }
  return bits;
}

uint64_t* DocumentMaskBuilder::Words(Bitset& bits) noexcept {
  return bits.words;
}

void DocumentMaskBuilder::AppendBitset(uint16_t chunk, BitsetPtr bits) {
  auto& ra = _set.high_low_container;
  SDB_ASSERT(ra.size == 0 || ra.keys[ra.size - 1] < chunk);
  bits->cardinality =
    roaring::internal::bitset_container_compute_cardinality(bits.get());
  if (bits->cardinality != 0) {
    roaring::internal::ra_append(&ra, chunk, bits.release(),
                                 BITSET_CONTAINER_TYPE);
  }
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
