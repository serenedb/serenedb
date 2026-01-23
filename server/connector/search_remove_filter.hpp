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

#include <basics/memory.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/filter.hpp>

#include "common.h"

namespace sdb::connector::search {

// something that never match user created fields id.
constexpr inline std::string_view kPkFieldName{"\x00", 1};

class SearchRemoveFilterBase : public irs::Filter,
                               public irs::Filter::Query,
                               public irs::DocIterator {
 public:
  bool Empty() const noexcept { return _pks.empty(); }

  void Add(std::string_view pk) {
    _pks.emplace_back(reinterpret_cast<const irs::byte_type*>(pk.data()),
                      pk.size());
  }

 protected:
  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<SearchRemoveFilterBase>::id();
  }

  Filter::Query::ptr prepare(const irs::PrepareContext& ctx) const final {
    if (_pks.empty()) {
      return irs::Filter::Query::empty();
    }
    return irs::memory::to_managed<const irs::Filter::Query>(*this);
  }

  irs::DocIterator::ptr execute(const irs::ExecutionContext& ctx) const final;

  void visit(const irs::SubReader&, irs::PreparedStateVisitor&,
             irs::score_t) const final {}

  irs::score_t Boost() const noexcept final { return irs::kNoBoost; }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return irs::Type<irs::DocAttr>::id() == id ? &_doc : nullptr;
  }

  irs::doc_id_t value() const noexcept final { return _doc.value; }

  irs::doc_id_t seek(irs::doc_id_t) noexcept final {
    SDB_ASSERT(false);
    return _doc.value = irs::doc_limits::eof();
  }
  mutable const irs::SubReader* _segment{};
  mutable const irs::DocumentMask* _pending_mask{};
  mutable const irs::DocumentMask* _segment_mask{};
  mutable const irs::TermReader* _pk_field{};
  mutable size_t _pos{0};
  mutable irs::DocAttr _doc;
  // TODO(Dronplane) use persistent velox memory pool for proper memory
  // accounting currently available query velox memory pool is discarded after
  // query execution but this allocations must survive until IndexWriter Commit.
  // See Issue cluster #37
  mutable std::vector<irs::bstring> _pks;
};

class SearchRemoveFilter final : public SearchRemoveFilterBase {
 public:
  explicit SearchRemoveFilter(size_t batch_size) { _pks.reserve(batch_size); }

  void reset() {
    _pos = 0;
    _pks.clear();
  }

  irs::doc_id_t advance() final;
};

}  // namespace sdb::connector::search
