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

#include <cstdint>
#include <span>
#include <utility>

#include "iresearch/search/count/root.hpp"
#include "iresearch/search/detail/fold_reach.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/queries/term_state.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

bool Disjoint(std::span<const detail::PostingClause> clauses);

uint64_t SumOf(std::span<const detail::PostingClause> clauses) noexcept;

bool ShouldSums(const BooleanQuery& query, const Context& ctx);

template<typename Make>
class WholeCount : public Root {
 public:
  WholeCount(uint64_t count, doc_id_t reach, Make make)
    : _count{count}, _reach{reach}, _make{std::move(make)} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    if (!_rest && min == doc_limits::min() && doc_limits::eof(max)) {
      return _count;
    }
    if (!_rest) {
      const detail::FoldReachScope reach{_reach};
      _rest = _make();
      SDB_ASSERT(_rest);
    }
    return _rest->Run(min, max);
  }

  uint64_t Finish() final { return _rest ? _rest->Finish() : 0; }

 private:
  uint64_t _count;
  doc_id_t _reach;
  Make _make;
  Root::ptr _rest;
};

template<typename Make>
Root::ptr MakeWholeCount(uint64_t count, const Context& ctx, Make make) {
  return memory::make_managed<WholeCount<Make>>(count, ctx.span,
                                                std::move(make));
}

}  // namespace irs::count
