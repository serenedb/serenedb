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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/memory.hpp>
#include <memory>
#include <optional>
#include <roaring/roaring64map.hh>
#include <vector>

namespace sdb::connector {

class SearchRemoveFilter : public irs::Filter, public irs::lead::Node {
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

  irs::lead::Node::ptr MakeLead(const irs::SubReader& segment,
                                const irs::DocumentMask* pending) const;

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<SearchRemoveFilter>::id();
  }

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final;

  irs::doc_id_t Next() final;

  // The removal walk reads a segment front to back, so nothing seeks it.
  irs::doc_id_t Seek(irs::doc_id_t) noexcept final {
    SDB_ASSERT(false);
    return _doc = irs::doc_limits::eof();
  }

 private:
  irs::doc_id_t _doc = irs::doc_limits::invalid();
  const irs::field_id _pk_field_id;
  mutable irs::DocumentMask::Iterator _segment_mask;
  mutable irs::DocumentMask::Iterator _pending_mask;
  mutable const irs::TermReader* _pk_field{};
  mutable size_t _pos{0};
  // TODO(Dronplane) use persistent duckdb memory pool for proper memory
  // accounting currently available query duckdb memory pool is discarded after
  // query execution but this allocations must survive until IndexWriter Commit.
  // See Issue cluster #37
  mutable std::vector<irs::bstring> _pks;
};

class SearchRemovePrefixFilter final : public irs::Filter,
                                       public irs::lead::Node {
 public:
  explicit SearchRemovePrefixFilter(irs::field_id field_id);
  ~SearchRemovePrefixFilter() final;

  // Every row under `prefix` dies.
  void AddFile(std::string_view prefix) { PushEntry(prefix); }

  void AddFileRows(std::string_view prefix, roaring::Roaring64Map rows) {
    PushEntry(prefix).dead = std::move(rows);
  }

  irs::lead::Node::ptr MakeLead(const irs::SubReader& segment,
                                const irs::DocumentMask* pending) const;

  irs::doc_id_t Next() final;

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<SearchRemovePrefixFilter>::id();
  }

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final;

  irs::doc_id_t Seek(irs::doc_id_t) noexcept final {
    SDB_ASSERT(false);
    return _doc = irs::doc_limits::eof();
  }

 private:
  irs::doc_id_t _doc = irs::doc_limits::invalid();

  struct Entry {
    irs::bstring prefix;
    // nullopt = whole file.
    std::optional<roaring::Roaring64Map> dead;
  };

  Entry& PushEntry(std::string_view prefix);

  void NextEntry() const noexcept;

  const irs::field_id _field_id;
  mutable irs::DocumentMask::Iterator _segment_mask;
  mutable irs::DocumentMask::Iterator _pending_mask;
  mutable const irs::TermReader* _field{};
  mutable irs::SeekTermIterator::ptr _terms;
  mutable irs::TermPostings::ptr _postings;
  mutable size_t _pos{0};
  mutable size_t _end{0};
  mutable uint64_t _next_row{0};
  mutable std::string _key_scratch;
  mutable std::vector<Entry> _entries;
};

}  // namespace sdb::connector
