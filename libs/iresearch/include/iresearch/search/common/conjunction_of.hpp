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

#include <limits>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/conjunction_bitset.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/sparse_conjunction_docs.hpp"

namespace irs::search {

template<typename Result, typename Term>
Result BuildConjunctionOf(std::span<const Term> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const TermReader* field, const SubReader& segment,
                          uint64_t interrogations) {
  constexpr bool kFilled = std::is_same_v<Result, FillNode::ptr>;
  if (auto folded = MakeConjunctionBitset<Result>(terms, filters, field,
                                                  segment, nullptr)) {
    return folded;
  }
  return BuildConjunction<Result, Term>(
    terms, filters, field, segment, interrogations,
    []<typename Lead, typename Others>(auto&& lead, auto&& others) -> Result {
      using Node = lead::SparseConjunctionDocs<Lead, Others>;
      if constexpr (kFilled) {
        return memory::make_managed<fill::ByWalkDocs<Node>>(
          std::piecewise_construct, std::forward<decltype(lead)>(lead),
          std::forward<decltype(others)>(others));
      } else {
        return memory::make_managed<lead::Impl<Node>>(
          std::piecewise_construct, std::forward<decltype(lead)>(lead),
          std::forward<decltype(others)>(others));
      }
    });
}

}  // namespace irs::search
