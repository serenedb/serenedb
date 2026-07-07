////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Yuriy Popov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/shared.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct SubReader;
struct TermReader;

template<typename Terms, typename Visitor>
void VisitTerms(Terms& terms, Visitor& visitor) {
  do {
    if constexpr (requires(Visitor& v, Terms& t) { v.SetIndex(t.Index()); }) {
      visitor.SetIndex(terms.Index());
    }
    auto boost = kNoBoost;
    if constexpr (requires(Terms& t) { t.Boost(); }) {
      boost = terms.Boost();
    }
    if (!visitor.Visit(boost)) {
      return;
    }
  } while (terms.next());
}

//////////////////////////////////////////////////////////////////////////////
/// @class filter_visitor
/// @brief base filter visitor interface
//////////////////////////////////////////////////////////////////////////////
struct FilterVisitor {
  virtual ~FilterVisitor() = default;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief makes preparations for a visitor
  //////////////////////////////////////////////////////////////////////////////
  virtual void Prepare(const SubReader& segment, const TermReader& field,
                       SeekTermIterator& terms) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief applies actions to a current term iterator
  /// @returns true to continue visiting, false to stop visiting
  //////////////////////////////////////////////////////////////////////////////
  virtual bool Visit(score_t boost) = 0;
};

}  // namespace irs
