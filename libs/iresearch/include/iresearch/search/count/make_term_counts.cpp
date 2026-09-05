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

#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/lazy_bitset.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/count/term_counts.hpp"
#include "iresearch/search/count/term_counts_of.hpp"
#include "iresearch/search/filter.hpp"

namespace irs::count {

TermCounts::ptr MakeTermCounts(search::LazyBitset& set, const TermReader& field,
                               size_t terms) {
  SDB_ASSERT(search::DocOf(field) != nullptr);
  if (terms < 2) {
    return {};
  }
  return search::ResolveInput(
    *search::DocOf(field), [&]<typename Input> -> TermCounts::ptr {
      using Shape = TermCountsOf<Input>;
      return memory::make_managed<TermCounts, Shape>(set, *search::DocOf(field),
                                                     search::LayoutOf(field),
                                                     search::BoundsOf(field));
    });
}

}  // namespace irs::count
