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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/filter.hpp>
#include <memory>
#include <optional>
#include <vector>

#include "basics/assert.h"
#include "basics/memory.hpp"

namespace sdb::connector {

// something that never match user created fields id.
constexpr inline std::string_view kPkFieldName{"\x00", 1};

class SearchRemoveFilter : public irs::Filter, public irs::DocIterator {
 public:
  SearchRemoveFilter(size_t batch_size, irs::field_id pk_field_id)
    : _pk_field_id{pk_field_id} {
    _pks.reserve(batch_size);
  }

  void reset() {
    _pos = 0;
    _pks.clear();
  }

  bool Empty() const noexcept { return _pks.empty(); }

  void Add(std::string_view pk) {
    _pks.emplace_back(reinterpret_cast<const irs::byte_type*>(pk.data()),
                      pk.size());
  }

  irs::DocIterator::ptr MakeIterator(const irs::SubReader& segment,
                                     const irs::ExecutionContext& ctx) const;

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<SearchRemoveFilter>::id();
  }

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final;

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return nullptr;
  }

  irs::doc_id_t advance() final;

  irs::doc_id_t seek(irs::doc_id_t) noexcept final {
    SDB_ASSERT(false);
    return _doc = irs::doc_limits::eof();
  }

  IRS_DOC_ITERATOR_DEFAULTS

 private:
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

// Removals for pk-TERM view indexes: the kPKFieldId dictionary holds one
// (file, row) term per row (two sortable signed halves), so the 8-byte
// file half alone is a whole-file prefix AND terms under it ascend by row.
// Entries ascend by prefix; each gets its own dictionary iterator:
// a whole-file entry seeks the prefix once and walks, masking every
// posting; a cursor entry LEAPFROGS -- the cursor names the next dead row,
// seek_ge jumps to its term, and a landed alive term gallops the cursor
// forward, so neither the row set nor the dictionary is enumerated.
// Re-evaluated per segment at apply time like every remove filter, so
// merges/compaction cannot invalidate it.
class SearchRemovePrefixFilter final : public irs::Filter,
                                       public irs::DocIterator {
 public:
  // "Smallest dead row >= min_row", nullopt once exhausted; called with
  // non-decreasing arguments.
  using DeadRowCursor = absl::AnyInvocable<std::optional<int64_t>(int64_t)>;

  explicit SearchRemovePrefixFilter(irs::field_id pk_field_id);
  ~SearchRemovePrefixFilter() final;

  // Every row under `prefix` dies.
  void AddFile(std::string_view prefix) { PushEntry(prefix); }

  // The cursor's rows under `prefix` die. Whatever the cursor reads must
  // outlive the filter (the observe owns it until the remove commits).
  void AddFileRows(std::string_view prefix, DeadRowCursor dead) {
    PushEntry(prefix).dead = std::move(dead);
  }

  irs::DocIterator::ptr MakeIterator(const irs::SubReader& segment,
                                     const irs::ExecutionContext& ctx) const;

  irs::doc_id_t advance() final;

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

  IRS_DOC_ITERATOR_DEFAULTS

 private:
  struct Entry {
    irs::bstring prefix;
    // nullopt = whole file.
    std::optional<DeadRowCursor> dead;
  };

  Entry& PushEntry(std::string_view prefix);

  void NextEntry() const noexcept;

  const irs::field_id _pk_field_id;
  mutable const irs::DocumentMask* _pending_mask{};
  mutable const irs::DocumentMask* _segment_mask{};
  mutable const irs::TermReader* _pk_field{};
  // Per-ENTRY dictionary iterator: the whole-file arm seeks once then
  // walks, the cursor arm issues seeks only -- one instance never mixes
  // the two patterns.
  mutable irs::SeekTermIterator::ptr _terms;
  mutable irs::DocIterator::ptr _postings;
  mutable size_t _pos{0};
  mutable int64_t _resume_row{0};
  mutable std::string _key_scratch;
  mutable std::vector<Entry> _entries;
};

}  // namespace sdb::connector
