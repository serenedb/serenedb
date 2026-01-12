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

#include <velox/common/memory/MemoryPool.h>

#include <basics/memory.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/filter.hpp>

#include "common.h"

namespace sdb::connector::search {

// this matches generated PK column id but it is not stored so should be fine.
constexpr inline std::string_view kPkFieldName{
  "\xff\xff\xff\xff\xff\xff\xff\xff", 8};

class SearchRemoveFilterBase : public irs::Filter,
                               public irs::Filter::Query,
                               public irs::DocIterator {
 public:
  SearchRemoveFilterBase(velox::memory::MemoryPool& memory_pool)
    : _memory_pool{memory_pool}, _pks{{memory_pool}} {}

  bool Empty() const noexcept { return _pks.empty(); }

  void Add(std::string_view pk) {
    _pks.emplace_back(ManagedString{pk, {_memory_pool}});
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
  velox::memory::MemoryPool& _memory_pool;
  mutable const irs::SubReader* _segment{};
  mutable const irs::DocumentMask* _pending_doc_mask{};
  mutable const irs::TermReader* _pk_field{};
  mutable size_t _pos{0};
  mutable irs::DocAttr _doc;
  // We need to store Pk copies as they would be applied on writer commit and
  // should stay alive until that.
  mutable ManagedVector<ManagedString> _pks;
};

class SearchRemoveFilter : public SearchRemoveFilterBase {
 public:
  explicit SearchRemoveFilter(velox::memory::MemoryPool& memory_pool,
                              size_t batch_size)
    : SearchRemoveFilterBase(memory_pool) {
    _pks.reserve(batch_size);
  }

  void reset() {
    _pos = 0;
    _pks.clear();
  }

  irs::doc_id_t advance() override;
};

}  // namespace sdb::connector::search
