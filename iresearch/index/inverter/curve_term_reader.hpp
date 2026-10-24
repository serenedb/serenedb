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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/index/inverter/columnar_readers.hpp"
#include "iresearch/utils/space_filling_curve.hpp"

namespace irs {

class CurveTermIterator final : public TermOnlyIterator {
 public:
  struct Generated {
    uint32_t term;
    uint32_t size;
    uint32_t docs;
    uint32_t count;
  };

  struct Entry {
    uint32_t rank;
    uint32_t renamed;
  };

  static constexpr uint32_t kOriginal = std::numeric_limits<uint32_t>::max();

  void Reset(const ScatteredField& scattered,
             std::span<const Generated> generated, std::span<const Entry> order,
             const byte_type* bytes, const doc_id_t* docs) noexcept {
    _scattered = &scattered;
    _generated = generated;
    _order = order;
    _bytes = bytes;
    _docs = docs;
    _next = 0;
  }

  bytes_view value() const noexcept final {
    SDB_ASSERT(false, "curve terms are batch-only");
    return {};
  }

  bool next() final {
    SDB_ENSURE(false, "curve terms are batch-only");
    return false;
  }

  TermPostings::ptr postings(IndexFeatures) const final {
    SDB_ENSURE(false, "curve postings are span-only");
    return {};
  }

  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  size_t NextTermsWithPostings(std::span<bytes_view> terms,
                               std::span<PostingRows> postings,
                               IndexFeatures) final;

  bytes_view Term(const Generated& term) const noexcept {
    return {_bytes + term.term, term.size};
  }

  bytes_view Term(const Entry& entry) const noexcept;

  size_t Size() const noexcept { return _generated.size() + _order.size(); }

  bytes_view TermAt(size_t i) const noexcept {
    return i < _generated.size() ? Term(_generated[i])
                                 : Term(_order[i - _generated.size()]);
  }

  const FieldMeta& Meta() const noexcept { return _scattered->Field().Meta(); }

 private:
  PostingRows Postings(uint32_t rank) const noexcept;

  const ScatteredField* _scattered = nullptr;
  std::span<const Generated> _generated;
  std::span<const Entry> _order;
  const byte_type* _bytes = nullptr;
  const doc_id_t* _docs = nullptr;
  size_t _next = 0;
};

class CurveTermReader final : public BasicTermReader,
                              private util::Noncopyable {
 public:
  explicit CurveTermReader(IResourceManager& rm)
    : _shared{ManagedTypedAllocator<uint16_t>{rm}},
      _before{ManagedTypedAllocator<uint32_t>{rm}},
      _rank_of{ManagedTypedAllocator<uint32_t>{rm}},
      _groups{ManagedTypedAllocator<Group>{rm}},
      _next_groups{ManagedTypedAllocator<Group>{rm}},
      _fill{ManagedTypedAllocator<uint32_t>{rm}},
      _generated{ManagedTypedAllocator<CurveTermIterator::Generated>{rm}},
      _order{ManagedTypedAllocator<CurveTermIterator::Entry>{rm}},
      _moved{ManagedTypedAllocator<CurveTermIterator::Entry>{rm}},
      _bytes{ManagedTypedAllocator<byte_type>{rm}},
      _docs{ManagedTypedAllocator<doc_id_t>{rm}} {}

  void Reset(const ScatteredField& scattered, const curve::Options& options);

  bytes_view(min)() const noexcept final { return _min; }
  bytes_view(max)() const noexcept final { return _max; }
  const FieldMeta& Meta() const noexcept { return _it.Meta(); }
  field_id id() const noexcept final { return Meta().id; }
  FieldProperties properties() const noexcept final { return Meta(); }

  TermOnlyIterator::ptr iterator() const noexcept final {
    return memory::to_managed<TermOnlyIterator>(_it);
  }

 private:
  struct Group {
    uint32_t begin;
    uint32_t end;
    uint32_t docs;
    uint32_t count;
  };

  void Split(const Group& group, uint32_t bits);

  ManagedVector<uint16_t> _shared;
  ManagedVector<uint32_t> _before;
  ManagedVector<uint32_t> _rank_of;
  ManagedVector<Group> _groups;
  ManagedVector<Group> _next_groups;
  ManagedVector<uint32_t> _fill;
  ManagedVector<CurveTermIterator::Generated> _generated;
  ManagedVector<CurveTermIterator::Entry> _order;
  ManagedVector<CurveTermIterator::Entry> _moved;
  ManagedVector<byte_type> _bytes;
  ManagedVector<doc_id_t> _docs;
  mutable CurveTermIterator _it;
  bytes_view _min;
  bytes_view _max;
};

}  // namespace irs
