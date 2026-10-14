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

namespace irs {

class CurveTermIterator final : public TermOnlyIterator {
 public:
  struct Entry {
    uint32_t rank;
    uint32_t renamed;
  };

  static constexpr uint32_t kOriginal = std::numeric_limits<uint32_t>::max();

  void Reset(const ScatteredField& scattered, std::span<const Entry> order,
             const byte_type* renamed) noexcept {
    _scattered = &scattered;
    _order = order;
    _renamed = renamed;
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

  bytes_view Term(const Entry& entry) const noexcept;

  const FieldMeta& Meta() const noexcept { return _scattered->Field().Meta(); }

 private:
  PostingRows Postings(uint32_t rank) const noexcept;

  const ScatteredField* _scattered = nullptr;
  std::span<const Entry> _order;
  const byte_type* _renamed = nullptr;
  size_t _next = 0;
};

class CurveTermReader final : public BasicTermReader,
                              private util::Noncopyable {
 public:
  explicit CurveTermReader(IResourceManager& rm)
    : _lone{ManagedTypedAllocator<uint8_t>{rm}},
      _kept{ManagedTypedAllocator<CurveTermIterator::Entry>{rm}},
      _moved{ManagedTypedAllocator<CurveTermIterator::Entry>{rm}},
      _order{ManagedTypedAllocator<CurveTermIterator::Entry>{rm}},
      _renamed{ManagedTypedAllocator<byte_type>{rm}} {}

  void Reset(const ScatteredField& scattered);

  bytes_view(min)() const noexcept final { return _min; }
  bytes_view(max)() const noexcept final { return _max; }
  const FieldMeta& Meta() const noexcept { return _it.Meta(); }
  field_id id() const noexcept final { return Meta().id; }
  FieldProperties properties() const noexcept final { return Meta(); }

  TermOnlyIterator::ptr iterator() const noexcept final {
    return memory::to_managed<TermOnlyIterator>(_it);
  }

 private:
  ManagedVector<uint8_t> _lone;
  ManagedVector<CurveTermIterator::Entry> _kept;
  ManagedVector<CurveTermIterator::Entry> _moved;
  ManagedVector<CurveTermIterator::Entry> _order;
  ManagedVector<byte_type> _renamed;
  mutable CurveTermIterator _it;
  bytes_view _min{};
  bytes_view _max{};
};

}  // namespace irs
