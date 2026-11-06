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

#include <span>
#include <utility>
#include <variant>

#include "iresearch/search/detail/bitset_storage.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {

class DocSetQuery : public QueryBuilderImpl<DocSetQuery> {
 public:
  explicit DocSetQuery(const SubReader& segment, detail::BitsetStorage&& set,
                       uint32_t docs, score_t boost) noexcept
    : QueryBuilderImpl{segment, docs, QueryKind::Other},
      _docs{std::move(set)},
      _boost{boost} {
    _estimate_min = docs;
  }

  explicit DocSetQuery(const SubReader& segment, ManagedVector<doc_id_t>&& docs,
                       score_t boost) noexcept
    : QueryBuilderImpl{segment, static_cast<uint32_t>(docs.size()),
                       QueryKind::Other},
      _docs{std::move(docs)},
      _boost{boost} {
    _estimate_min = _estimate_max;
  }

  bool Sparse() const noexcept {
    return std::holds_alternative<ManagedVector<doc_id_t>>(_docs);
  }

  std::span<const doc_id_t> Docs() const noexcept {
    return *std::get_if<ManagedVector<doc_id_t>>(&_docs);
  }

  detail::BitsetStorage Share() const noexcept {
    return std::get_if<detail::BitsetStorage>(&_docs)->Share();
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  std::variant<detail::BitsetStorage, ManagedVector<doc_id_t>> _docs;
  score_t _boost;
};

extern template class QueryBuilderImpl<DocSetQuery>;

}  // namespace irs
