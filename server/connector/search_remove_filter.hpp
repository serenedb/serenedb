////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/functional/any_invocable.h>

#include <iresearch/search/filter.hpp>
#include <limits>
#include <optional>

#include "basics/assert.h"

namespace sdb::connector {

class SearchRemoveFilter final : public irs::Filter, public irs::DocIterator {
 public:
  SearchRemoveFilter(size_t batch_size, irs::field_id pk_field_id) noexcept
    : _pk_field_id{pk_field_id} {
    _pks.reserve(batch_size);
  }

  bool Empty() const noexcept { return _pks.empty(); }

  void Add(std::string_view pk) {
    _pks.emplace_back(reinterpret_cast<const irs::byte_type*>(pk.data()),
                      pk.size());
  }

  irs::DocIterator::ptr MakeIterator(const irs::SubReader& segment,
                                     const irs::ExecutionContext& ctx) const;

  irs::doc_id_t advance() final;

  IRS_DOC_ITERATOR_DEFAULTS

 private:
  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<SearchRemoveFilter>::id();
  }

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final;

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

  irs::doc_id_t seek(irs::doc_id_t) noexcept final {
    SDB_ASSERT(false);
    return _doc = irs::doc_limits::eof();
  }

  const irs::field_id _pk_field_id;
  mutable const irs::DocumentMask* _pending_mask{};
  mutable const irs::DocumentMask* _segment_mask{};
  mutable const irs::TermReader* _pk_field{};
  mutable size_t _pos{0};
  // TODO(Dronplane) use persistent duckdb memory pool for proper memory
  // accounting currently available query duckdb memory pool is discarded after
  // query execution but this allocations must survive until IndexWriter Commit.
  // See Issue cluster #37
  mutable std::vector<irs::bstring> _pks;
};

class SearchRemovePrefixFilter final : public irs::Filter,
                                       public irs::DocIterator {
 public:
  SearchRemovePrefixFilter(size_t batch_size,
                           irs::field_id pk_field_id) noexcept
    : _pk_field_id{pk_field_id} {
    _prefixes.reserve(batch_size);
  }

  // Prefixes must arrive in ascending byte order: advance() walks the sorted
  // term dictionary once, front to back.
  void Add(std::string_view prefix) {
    const irs::bytes_view bytes{
      reinterpret_cast<const irs::byte_type*>(prefix.data()), prefix.size()};
    SDB_ASSERT(_prefixes.empty() || _prefixes.back() < bytes);
    _prefixes.emplace_back(bytes);
  }

  irs::DocIterator::ptr MakeIterator(const irs::SubReader& segment,
                                     const irs::ExecutionContext& ctx) const;

  irs::doc_id_t advance() final;

  IRS_DOC_ITERATOR_DEFAULTS

 private:
  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<SearchRemovePrefixFilter>::id();
  }

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final;

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

  irs::doc_id_t seek(irs::doc_id_t) noexcept final {
    SDB_ASSERT(false);
    return _doc = irs::doc_limits::eof();
  }

  const irs::field_id _pk_field_id;
  mutable const irs::DocumentMask* _pending_mask{};
  mutable const irs::DocumentMask* _segment_mask{};
  mutable irs::SeekTermIterator::ptr _terms;
  mutable irs::DocIterator::ptr _docs;
  mutable size_t _pos{0};
  mutable bool _seek_pending{true};
  mutable std::vector<irs::bstring> _prefixes;
};

// The accumulated-mask road: for a file whose dead set is LARGE (an
// accumulating deletion vector, or a first refresh over a bulk delete),
// per-key point lookups cost O(dead) root-to-leaf dictionary walks. This
// filter instead LEAPFROGS the two sorted sequences -- the dead rows and
// the file's pk-term range (the (file, row) encoding is order-preserving)
// -- on ONE forward term iterator: seek_ge to the next dead row's exact
// term, and let a miss fast-forward the dead cursor to the landed term's
// row. O(min(dead, file terms)) monotone seeks, never a full range scan;
// already-masked docs are skipped, so re-masking an accumulated set only
// emits the new deltas. Entries must arrive in ascending prefix order.
class SearchRemoveRangeMaskFilter final : public irs::Filter,
                                          public irs::DocIterator {
 public:
  // "Smallest dead row >= min_row", nullopt once exhausted; called with
  // non-decreasing arguments.
  using DeadRowCursor = absl::AnyInvocable<std::optional<int64_t>(int64_t)>;

  explicit SearchRemoveRangeMaskFilter(irs::field_id pk_field_id) noexcept
    : _pk_field_id{pk_field_id} {}

  void Add(std::string_view prefix, DeadRowCursor dead) {
    const irs::bytes_view bytes{
      reinterpret_cast<const irs::byte_type*>(prefix.data()), prefix.size()};
    SDB_ASSERT(_ranges.empty() || _ranges.back().prefix < bytes);
    _ranges.push_back({irs::bstring{bytes}, std::move(dead)});
  }

  irs::DocIterator::ptr MakeIterator(const irs::SubReader& segment,
                                     const irs::ExecutionContext& ctx) const;

  irs::doc_id_t advance() final;

  IRS_DOC_ITERATOR_DEFAULTS

 private:
  struct Range {
    irs::bstring prefix;
    DeadRowCursor dead;
  };

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<SearchRemoveRangeMaskFilter>::id();
  }

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final;

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

  irs::doc_id_t seek(irs::doc_id_t) noexcept final {
    SDB_ASSERT(false);
    return _doc = irs::doc_limits::eof();
  }

  const irs::field_id _pk_field_id;
  mutable const irs::DocumentMask* _pending_mask{};
  mutable const irs::DocumentMask* _segment_mask{};
  mutable irs::SeekTermIterator::ptr _terms;
  mutable irs::DocIterator::ptr _docs;
  mutable size_t _pos{0};
  mutable int64_t _lower{std::numeric_limits<int64_t>::min()};
  mutable std::string _target;
  mutable std::vector<Range> _ranges;
};

}  // namespace sdb::connector
