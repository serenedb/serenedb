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
#include <iresearch/search/filter.hpp>

#include "basics/memory.hpp"

namespace sdb::connector {

// something that never match user created fields id.
constexpr inline std::string_view kPkFieldName{"\x00", 1};

class SearchRemoveFilterBase : public irs::Filter, public irs::DocIterator {
 public:
  explicit SearchRemoveFilterBase(irs::field_id pk_field_id) noexcept
    : _pk_field_id{pk_field_id} {}

  bool Empty() const noexcept { return _pks.empty(); }

  void Add(std::string_view pk) {
    _pks.emplace_back(reinterpret_cast<const irs::byte_type*>(pk.data()),
                      pk.size());
  }

  irs::DocIterator::ptr MakeIterator(const irs::SubReader& segment,
                                     const irs::ExecutionContext& ctx) const;

 protected:
  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<SearchRemoveFilterBase>::id();
  }

  irs::QueryBuilder::ptr PrepareSegment(
    const irs::SubReader& segment, const irs::PrepareContext& ctx) const final;

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return nullptr;
  }

  irs::doc_id_t seek(irs::doc_id_t) noexcept final {
    SDB_ASSERT(false);
    return _doc = irs::doc_limits::eof();
  }

  const irs::field_id _pk_field_id;
  mutable const irs::SubReader* _segment{};
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

class SearchRemoveFilter final : public SearchRemoveFilterBase {
 public:
  SearchRemoveFilter(size_t batch_size, irs::field_id pk_field_id)
    : SearchRemoveFilterBase{pk_field_id} {
    _pks.reserve(batch_size);
  }

  void reset() {
    _pos = 0;
    _pks.clear();
  }

  irs::doc_id_t advance() final;

  IRS_DOC_ITERATOR_DEFAULTS
};

}  // namespace sdb::connector
